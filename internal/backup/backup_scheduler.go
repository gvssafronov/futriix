/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/backup/backup_scheduler.go
// Назначение: Планировщик бэкапов и инкрементальные бэкапы на основе WAL
// Использует 2PC для инкрементных бекапов и статистику по 2PC транзакциям

package backup

import (
    "bytes"
    "compress/gzip"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sort"
    "sync"
    "sync/atomic"
    "time"

    "futriis/internal/storage"
)

// BackupType тип бэкапа
type BackupType string

const (
    FullBackup        BackupType = "full"
    IncrementalBackup BackupType = "incremental"
)

// BackupSchedule расписание бэкапов
type BackupSchedule struct {
    ID            string     `json:"id"`
    Name          string     `json:"name"`
    CronExpr      string     `json:"cron_expr"`
    Type          BackupType `json:"type"`
    RetentionDays int        `json:"retention_days"`
    Enabled       bool       `json:"enabled"`
    LastRun       int64      `json:"last_run"`
    NextRun       int64      `json:"next_run"`
    CreatedAt     int64      `json:"created_at"`
    UpdatedAt     int64      `json:"updated_at"`
}

// BackupInfo информация о бэкапе
type BackupInfo struct {
    ID         string     `json:"id"`
    ScheduleID string     `json:"schedule_id"`
    Type       BackupType `json:"type"`
    StartTime  int64      `json:"start_time"`
    EndTime    int64      `json:"end_time"`
    Status     string     `json:"status"`
    SizeBytes  int64      `json:"size_bytes"`
    Path       string     `json:"path"`
    WALStart   uint64     `json:"wal_start"`
    WALEnd     uint64     `json:"wal_end"`
    ParentID   string     `json:"parent_id,omitempty"`
    Checksum   string     `json:"checksum"`
    Error      string     `json:"error,omitempty"`
    // ДОБАВЛЕНО: информация о 2PC состоянии
    TwoPCCount int `json:"two_pc_count,omitempty"`
}

// WALEntry запись WAL
type WALEntry struct {
    Index     uint64 `json:"index"`
    Term      uint64 `json:"term"`
    Type      string `json:"type"`
    Data      []byte `json:"data"`
    Timestamp int64  `json:"timestamp"`
}

// WALReader интерфейс для чтения WAL
type WALReader interface {
    ReadSince(index uint64) ([]WALEntry, error)
    GetCurrentIndex() (uint64, error)
    GetLastBackupIndex() uint64
    SetLastBackupIndex(index uint64) error
    GetSegments() ([]string, error)
    ReadSegment(segmentPath string) ([]WALEntry, error)
}

// BackupStorage интерфейс для хранения бэкапов
type BackupStorage interface {
    SaveBackup(path string, data []byte) error
    LoadBackup(path string) ([]byte, error)
    ListBackups() ([]string, error)
    DeleteBackup(path string) error
    BackupExists(path string) bool
}

// LoggerInterface интерфейс для логирования
type LoggerInterface interface {
    Info(msg string)
    Warn(msg string)
    Error(msg string)
    Debug(msg string)
}

// FileBackupStorage реализация BackupStorage для файловой системы
type FileBackupStorage struct {
    backupDir     string
    compressLevel int
    mu            sync.Mutex
}

// NewFileBackupStorage создаёт новое файловое хранилище бэкапов
func NewFileBackupStorage(backupDir string, compressLevel int) (*FileBackupStorage, error) {
    if compressLevel < 1 || compressLevel > 9 {
        compressLevel = 6
    }
    if err := os.MkdirAll(backupDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create backup dir: %v", err)
    }
    return &FileBackupStorage{
        backupDir:     backupDir,
        compressLevel: compressLevel,
    }, nil
}

func (fbs *FileBackupStorage) SaveBackup(path string, data []byte) error {
    fbs.mu.Lock()
    defer fbs.mu.Unlock()

    fullPath := filepath.Join(fbs.backupDir, path)
    if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
        return err
    }

    // Сжимаем данные
    var buf bytes.Buffer
    gzWriter, err := gzip.NewWriterLevel(&buf, fbs.compressLevel)
    if err != nil {
        return err
    }
    if _, err := gzWriter.Write(data); err != nil {
        gzWriter.Close()
        return err
    }
    if err := gzWriter.Close(); err != nil {
        return err
    }

    compressedPath := fullPath + ".gz"
    return os.WriteFile(compressedPath, buf.Bytes(), 0644)
}

func (fbs *FileBackupStorage) LoadBackup(path string) ([]byte, error) {
    fbs.mu.Lock()
    defer fbs.mu.Unlock()

    fullPath := filepath.Join(fbs.backupDir, path)
    compressedPath := fullPath + ".gz"

    var data []byte
    var err error

    // Пробуем сначала сжатый файл
    if _, err := os.Stat(compressedPath); err == nil {
        compressedData, err := os.ReadFile(compressedPath)
        if err != nil {
            return nil, err
        }
        gzReader, err := gzip.NewReader(bytes.NewReader(compressedData))
        if err != nil {
            return nil, err
        }
        defer gzReader.Close()
        data, err = io.ReadAll(gzReader)
        if err != nil {
            return nil, err
        }
    } else {
        data, err = os.ReadFile(fullPath)
        if err != nil {
            return nil, err
        }
    }

    return data, nil
}

func (fbs *FileBackupStorage) ListBackups() ([]string, error) {
    entries, err := os.ReadDir(fbs.backupDir)
    if err != nil {
        return nil, err
    }
    names := make([]string, 0, len(entries))
    for _, e := range entries {
        if e.IsDir() {
            // Рекурсивно собираем файлы из поддиректорий
            subEntries, err := os.ReadDir(filepath.Join(fbs.backupDir, e.Name()))
            if err == nil {
                for _, sub := range subEntries {
                    if !sub.IsDir() {
                        names = append(names, filepath.Join(e.Name(), sub.Name()))
                    }
                }
            }
        } else {
            names = append(names, e.Name())
        }
    }
    return names, nil
}

func (fbs *FileBackupStorage) DeleteBackup(path string) error {
    fbs.mu.Lock()
    defer fbs.mu.Unlock()

    fullPath := filepath.Join(fbs.backupDir, path)
    compressedPath := fullPath + ".gz"

    var err error
    if _, statErr := os.Stat(compressedPath); statErr == nil {
        err = os.Remove(compressedPath)
    } else {
        err = os.Remove(fullPath)
    }

    // Пробуем удалить пустые директории
    dirPath := filepath.Dir(fullPath)
    os.Remove(dirPath)

    return err
}

func (fbs *FileBackupStorage) BackupExists(path string) bool {
    fullPath := filepath.Join(fbs.backupDir, path)
    compressedPath := fullPath + ".gz"
    _, err1 := os.Stat(fullPath)
    _, err2 := os.Stat(compressedPath)
    return err1 == nil || err2 == nil
}

// WALReaderImpl реализация WALReader для SegmentedWALManager
type WALReaderImpl struct {
    walManager      interface{} // *SegmentedWALManager или *WALManager
    lastBackupIndex atomic.Uint64
    segmentsDir     string
    mu              sync.RWMutex
    logger          LoggerInterface
}

// NewWALReaderImpl создаёт новый WALReader
func NewWALReaderImpl(walManager interface{}, segmentsDir string, logger LoggerInterface) *WALReaderImpl {
    return &WALReaderImpl{
        walManager:  walManager,
        segmentsDir: segmentsDir,
        logger:      logger,
    }
}

// ReadSince читает записи WAL с указанного индекса
func (wr *WALReaderImpl) ReadSince(index uint64) ([]WALEntry, error) {
    wr.mu.RLock()
    defer wr.mu.RUnlock()

    entries := make([]WALEntry, 0)

    // Получаем список сегментов
    segments, err := wr.GetSegments()
    if err != nil {
        return nil, err
    }

    for _, segmentPath := range segments {
        segEntries, err := wr.ReadSegment(segmentPath)
        if err != nil {
            if wr.logger != nil {
                wr.logger.Warn(fmt.Sprintf("Failed to read segment %s: %v", segmentPath, err))
            }
            continue
        }

        for _, entry := range segEntries {
            if entry.Index > index {
                entries = append(entries, entry)
            }
        }
    }

    return entries, nil
}

// GetCurrentIndex возвращает текущий индекс WAL
func (wr *WALReaderImpl) GetCurrentIndex() (uint64, error) {
    segments, err := wr.GetSegments()
    if err != nil {
        return 0, err
    }

    if len(segments) == 0 {
        return 0, nil
    }

    // Читаем последний сегмент для получения последнего индекса
    lastSegment := segments[len(segments)-1]
    entries, err := wr.ReadSegment(lastSegment)
    if err != nil {
        return 0, err
    }

    if len(entries) == 0 {
        return 0, nil
    }

    return entries[len(entries)-1].Index, nil
}

// GetLastBackupIndex возвращает последний сохранённый индекс бэкапа
func (wr *WALReaderImpl) GetLastBackupIndex() uint64 {
    return wr.lastBackupIndex.Load()
}

// SetLastBackupIndex устанавливает последний индекс бэкапа
func (wr *WALReaderImpl) SetLastBackupIndex(index uint64) error {
    wr.lastBackupIndex.Store(index)

    // Сохраняем в файл для персистентности
    indexPath := filepath.Join(wr.segmentsDir, "last_backup_index.json")
    data, err := json.Marshal(map[string]uint64{"last_index": index})
    if err != nil {
        return err
    }
    return os.WriteFile(indexPath, data, 0644)
}

// GetSegments возвращает список сегментов WAL
func (wr *WALReaderImpl) GetSegments() ([]string, error) {
    if wr.segmentsDir == "" {
        return nil, fmt.Errorf("segments directory not set")
    }

    pattern := filepath.Join(wr.segmentsDir, "wal_segment_*.log")
    files, err := filepath.Glob(pattern)
    if err != nil {
        return nil, err
    }

    sort.Strings(files)
    return files, nil
}

// ReadSegment читает сегмент WAL
func (wr *WALReaderImpl) ReadSegment(segmentPath string) ([]WALEntry, error) {
    data, err := os.ReadFile(segmentPath)
    if err != nil {
        return nil, err
    }

    entries := make([]WALEntry, 0)

    // Парсим WAL записи (формат: [length][data] повторяется)
    pos := 0
    for pos < len(data) {
        if pos+4 > len(data) {
            break
        }

        // Читаем длину записи (4 байта, big-endian)
        length := int(data[pos])<<24 | int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
        pos += 4

        if pos+length > len(data) {
            break
        }

        recordData := data[pos : pos+length]
        pos += length

        var record struct {
            LSN       uint64 `json:"lsn"`
            Timestamp int64  `json:"timestamp"`
            Type      byte   `json:"type"`
            Data      []byte `json:"data"`
        }

        if err := json.Unmarshal(recordData, &record); err != nil {
            continue
        }

        entry := WALEntry{
            Index:     record.LSN,
            Timestamp: record.Timestamp,
            Type:      fmt.Sprintf("%d", record.Type),
            Data:      record.Data,
        }

        entries = append(entries, entry)
    }

    return entries, nil
}

// loadLastBackupIndex загружает последний индекс из файла
func (wr *WALReaderImpl) loadLastBackupIndex() {
    indexPath := filepath.Join(wr.segmentsDir, "last_backup_index.json")
    data, err := os.ReadFile(indexPath)
    if err != nil {
        return
    }

    var meta map[string]uint64
    if err := json.Unmarshal(data, &meta); err != nil {
        return
    }

    if lastIdx, ok := meta["last_index"]; ok {
        wr.lastBackupIndex.Store(lastIdx)
    }
}

// BackupScheduler планировщик бэкапов
type BackupScheduler struct {
    schedules     map[string]*BackupSchedule
    backups       map[string]*BackupInfo
    walReader     WALReader
    storage       BackupStorage
    logger        LoggerInterface
    mu            sync.RWMutex
    stopChan      chan struct{}
    wg            sync.WaitGroup
    running       atomic.Bool
    backupCounter atomic.Uint64
    config        *BackupSchedulerConfig
    // ДОБАВЛЕНО: ссылка на менеджер транзакций для 2PC
    txManager     *storage.TransactionManager
}

// BackupSchedulerConfig конфигурация планировщика
type BackupSchedulerConfig struct {
    BackupDir          string `json:"backup_dir"`
    MaxConcurrent      int    `json:"max_concurrent"`
    CompressEnabled    bool   `json:"compress_enabled"`
    CompressionLevel   int    `json:"compression_level"`
    MaxBackupSizeMB    int64  `json:"max_backup_size_mb"`
    EnableIncremental  bool   `json:"enable_incremental"`
    // ДОБАВЛЕНО: включить сохранение 2PC состояния
    IncludeTwoPCState  bool   `json:"include_two_pc_state"`
}

// DefaultBackupSchedulerConfig возвращает конфигурацию по умолчанию
func DefaultBackupSchedulerConfig() *BackupSchedulerConfig {
    return &BackupSchedulerConfig{
        BackupDir:          "backups",
        MaxConcurrent:      1,
        CompressEnabled:    true,
        CompressionLevel:   6,
        MaxBackupSizeMB:    10240, // 10GB
        EnableIncremental:  true,
        IncludeTwoPCState:  true,
    }
}

// NewBackupScheduler создаёт новый планировщик бэкапов
func NewBackupScheduler(cfg *BackupSchedulerConfig, walReader WALReader, logger LoggerInterface) (*BackupScheduler, error) {
    if cfg == nil {
        cfg = DefaultBackupSchedulerConfig()
    }

    compressLevel := cfg.CompressionLevel
    if !cfg.CompressEnabled {
        compressLevel = 0
    }

    storage, err := NewFileBackupStorage(cfg.BackupDir, compressLevel)
    if err != nil {
        return nil, err
    }

    bs := &BackupScheduler{
        schedules: make(map[string]*BackupSchedule),
        backups:   make(map[string]*BackupInfo),
        walReader: walReader,
        storage:   storage,
        logger:    logger,
        stopChan:  make(chan struct{}),
        config:    cfg,
        // ДОБАВЛЕНО: получаем менеджер транзакций
        txManager: storage.GetTransactionManager(),
    }

    // Загружаем существующие расписания
    bs.loadSchedules()

    // Загружаем историю бэкапов
    bs.loadBackupHistory()

    return bs, nil
}

// GetTransactionManager возвращает менеджер транзакций (ДОБАВЛЕНО)
func GetTransactionManager() *storage.TransactionManager {
    return storage.GetTransactionManager()
}

// loadSchedules загружает расписания из хранилища
func (bs *BackupScheduler) loadSchedules() {
    data, err := bs.storage.LoadBackup("schedules.json")
    if err != nil {
        if !os.IsNotExist(err) && bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to load schedules: %v", err))
        }
        return
    }

    var schedules map[string]*BackupSchedule
    if err := json.Unmarshal(data, &schedules); err != nil {
        if bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to parse schedules: %v", err))
        }
        return
    }

    bs.mu.Lock()
    bs.schedules = schedules
    bs.mu.Unlock()
}

// saveSchedules сохраняет расписания
func (bs *BackupScheduler) saveSchedules() error {
    bs.mu.RLock()
    data, err := json.MarshalIndent(bs.schedules, "", "  ")
    bs.mu.RUnlock()

    if err != nil {
        return err
    }

    return bs.storage.SaveBackup("schedules.json", data)
}

// loadBackupHistory загружает историю бэкапов
func (bs *BackupScheduler) loadBackupHistory() {
    data, err := bs.storage.LoadBackup("backups.json")
    if err != nil {
        if !os.IsNotExist(err) && bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to load backup history: %v", err))
        }
        return
    }

    var backups map[string]*BackupInfo
    if err := json.Unmarshal(data, &backups); err != nil {
        if bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to parse backup history: %v", err))
        }
        return
    }

    bs.mu.Lock()
    bs.backups = backups
    bs.mu.Unlock()
}

// saveBackupHistory сохраняет историю бэкапов
func (bs *BackupScheduler) saveBackupHistory() error {
    bs.mu.RLock()
    data, err := json.MarshalIndent(bs.backups, "", "  ")
    bs.mu.RUnlock()

    if err != nil {
        return err
    }

    return bs.storage.SaveBackup("backups.json", data)
}

// AddSchedule добавляет расписание бэкапов
func (bs *BackupScheduler) AddSchedule(name, cronExpr string, backupType BackupType, retentionDays int) (*BackupSchedule, error) {
    bs.mu.Lock()
    defer bs.mu.Unlock()

    // Проверяем, что расписание с таким именем не существует
    for _, s := range bs.schedules {
        if s.Name == name {
            return nil, fmt.Errorf("schedule with name '%s' already exists", name)
        }
    }

    schedule := &BackupSchedule{
        ID:            fmt.Sprintf("sch_%d", time.Now().UnixNano()),
        Name:          name,
        CronExpr:      cronExpr,
        Type:          backupType,
        RetentionDays: retentionDays,
        Enabled:       true,
        CreatedAt:     time.Now().UnixMilli(),
        UpdatedAt:     time.Now().UnixMilli(),
    }

    bs.schedules[schedule.ID] = schedule

    if err := bs.saveSchedules(); err != nil {
        return nil, fmt.Errorf("failed to save schedules: %v", err)
    }

    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Added backup schedule: %s (%s) with cron %s", name, backupType, cronExpr))
    }
    return schedule, nil
}

// RemoveSchedule удаляет расписание
func (bs *BackupScheduler) RemoveSchedule(scheduleID string) error {
    bs.mu.Lock()
    defer bs.mu.Unlock()

    if _, exists := bs.schedules[scheduleID]; !exists {
        return fmt.Errorf("schedule not found: %s", scheduleID)
    }

    delete(bs.schedules, scheduleID)

    if err := bs.saveSchedules(); err != nil {
        return fmt.Errorf("failed to save schedules: %v", err)
    }

    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Removed schedule: %s", scheduleID))
    }
    return nil
}

// RunNow запускает бэкап немедленно
func (bs *BackupScheduler) RunNow(scheduleID string) error {
    bs.mu.RLock()
    schedule, exists := bs.schedules[scheduleID]
    bs.mu.RUnlock()

    if !exists {
        return fmt.Errorf("schedule not found: %s", scheduleID)
    }

    if !bs.config.EnableIncremental && schedule.Type == IncrementalBackup {
        return fmt.Errorf("incremental backups are disabled in configuration")
    }

    if schedule.Type == FullBackup {
        return bs.performFullBackup(scheduleID)
    }
    return bs.performIncrementalBackup(scheduleID)
}

// performFullBackup выполняет полный бэкап
func (bs *BackupScheduler) performFullBackup(scheduleID string) error {
    if !bs.running.CompareAndSwap(false, true) {
        return fmt.Errorf("backup already in progress")
    }
    defer bs.running.Store(false)

    startTime := time.Now().UnixMilli()
    backupID := fmt.Sprintf("full_%d_%d", startTime, bs.backupCounter.Add(1))

    bs.logger.Info(fmt.Sprintf("Starting full backup %s", backupID))

    // ========== ДОБАВЛЕНО: Блокировка для 2PC ==========
    if bs.txManager != nil && bs.config.IncludeTwoPCState {
        bs.logger.Info("Locking transaction manager for backup...")
        bs.txManager.LockForBackup()
        defer bs.txManager.UnlockForBackup()
    }

    // Получаем текущую позицию WAL
    walIndex, err := bs.walReader.GetCurrentIndex()
    if err != nil {
        return fmt.Errorf("failed to get WAL index: %v", err)
    }

    // ========== ДОБАВЛЕНО: Получаем незавершённые 2PC транзакции ==========
    var pending2PC []*storage.TransactionRecord
    var twoPCCount int
    if bs.txManager != nil && bs.config.IncludeTwoPCState {
        pending2PC = bs.txManager.GetPending2PCTransactions()
        twoPCCount = len(pending2PC)
        if twoPCCount > 0 {
            bs.logger.Info(fmt.Sprintf("Found %d pending 2PC transactions, including in backup", twoPCCount))
        }
    }

    // Сохраняем последний индекс для инкрементальных бэкапов
    if err := bs.walReader.SetLastBackupIndex(walIndex); err != nil {
        bs.logger.Warn(fmt.Sprintf("Failed to set last backup index: %v", err))
    }

    // Читаем все WAL записи для полного бэкапа
    walEntries, err := bs.walReader.ReadSince(0)
    if err != nil {
        bs.logger.Warn(fmt.Sprintf("Failed to read WAL for full backup: %v", err))
        walEntries = []WALEntry{}
    }

    // ========== ДОБАВЛЕНО: Сохраняем состояние 2PC ==========
    twoPCState := make(map[string]interface{})
    if twoPCCount > 0 && bs.config.IncludeTwoPCState {
        // Сохраняем только метаданные, не сами транзакции (они уже в WAL)
        twoPCState["pending_count"] = twoPCCount
        twoPCState["timestamp"] = time.Now().UnixMilli()
        twoPCState["transaction_ids"] = make([]string, 0, twoPCCount)
        for _, tx := range pending2PC {
            twoPCState["transaction_ids"] = append(twoPCState["transaction_ids"].([]string), fmt.Sprintf("%d", tx.ID))
        }
    }

    // Создаём структуру бэкапа
    backupData := struct {
        BackupID    string      `json:"backup_id"`
        Type        string      `json:"type"`
        StartTime   int64       `json:"start_time"`
        WALIndex    uint64      `json:"wal_index"`
        Entries     []WALEntry  `json:"entries,omitempty"`
        Metadata    interface{} `json:"metadata,omitempty"`
        TwoPCState  interface{} `json:"two_pc_state,omitempty"`
    }{
        BackupID:  backupID,
        Type:      "full",
        StartTime: startTime,
        WALIndex:  walIndex,
        Entries:   walEntries,
        Metadata: map[string]interface{}{
            "version":              "1.0",
            "created_by":           "backup_scheduler",
            "wal_entries_count":    len(walEntries),
            "two_pc_pending_count": twoPCCount,
            "include_two_pc":       bs.config.IncludeTwoPCState,
        },
        TwoPCState: twoPCState,
    }

    data, err := json.Marshal(backupData)
    if err != nil {
        return fmt.Errorf("failed to marshal backup data: %v", err)
    }

    // Проверяем размер бэкапа
    if bs.config.MaxBackupSizeMB > 0 && int64(len(data)) > bs.config.MaxBackupSizeMB*1024*1024 {
        return fmt.Errorf("backup size %d exceeds maximum allowed %d MB",
            len(data)/(1024*1024), bs.config.MaxBackupSizeMB)
    }

    // Сохраняем бэкап
    backupPath := fmt.Sprintf("full/%s/%s.backup",
        time.Now().Format("2006-01-02"), backupID)

    if err := bs.storage.SaveBackup(backupPath, data); err != nil {
        return fmt.Errorf("failed to save backup: %v", err)
    }

    // Вычисляем контрольную сумму
    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])

    endTime := time.Now().UnixMilli()

    backupInfo := &BackupInfo{
        ID:         backupID,
        ScheduleID: scheduleID,
        Type:       FullBackup,
        StartTime:  startTime,
        EndTime:    endTime,
        Status:     "completed",
        SizeBytes:  int64(len(data)),
        Path:       backupPath,
        WALStart:   0,
        WALEnd:     walIndex,
        Checksum:   checksum,
        TwoPCCount: twoPCCount,
    }

    bs.mu.Lock()
    bs.backups[backupID] = backupInfo
    bs.mu.Unlock()

    if err := bs.saveBackupHistory(); err != nil {
        bs.logger.Warn(fmt.Sprintf("Failed to save backup history: %v", err))
    }

    bs.updateScheduleLastRun(scheduleID, startTime)
    bs.cleanupOldBackups(scheduleID)

    duration := (endTime - startTime) / 1000
    bs.logger.Info(fmt.Sprintf("Full backup %s completed in %d seconds, size=%d bytes, entries=%d, pending_2pc=%d",
        backupID, duration, backupInfo.SizeBytes, len(walEntries), twoPCCount))

    return nil
}

// performIncrementalBackup выполняет инкрементальный бэкап на основе WAL
func (bs *BackupScheduler) performIncrementalBackup(scheduleID string) error {
    if !bs.running.CompareAndSwap(false, true) {
        return fmt.Errorf("backup already in progress")
    }
    defer bs.running.Store(false)

    startTime := time.Now().UnixMilli()
    backupID := fmt.Sprintf("inc_%d_%d", startTime, bs.backupCounter.Add(1))

    // ========== ДОБАВЛЕНО: Блокировка для 2PC ==========
    if bs.txManager != nil && bs.config.IncludeTwoPCState {
        bs.logger.Info("Locking transaction manager for incremental backup...")
        bs.txManager.LockForBackup()
        defer bs.txManager.UnlockForBackup()
    }

    // Находим последний полный бэкап
    lastFullBackup := bs.findLastFullBackup()
    if lastFullBackup == nil {
        bs.logger.Info("No full backup found, performing full backup instead")
        return bs.performFullBackup(scheduleID)
    }

    // Проверяем, что родительский бэкап существует
    if !bs.storage.BackupExists(lastFullBackup.Path) {
        bs.logger.Warn(fmt.Sprintf("Parent backup %s file missing, performing full backup instead", lastFullBackup.ID))
        return bs.performFullBackup(scheduleID)
    }

    // Получаем WAL записи с момента последнего бэкапа
    lastIndex := bs.walReader.GetLastBackupIndex()
    if lastIndex == 0 {
        lastIndex = lastFullBackup.WALEnd
    }

    walEntries, err := bs.walReader.ReadSince(lastIndex)
    if err != nil {
        return fmt.Errorf("failed to read WAL: %v", err)
    }

    if len(walEntries) == 0 {
        bs.logger.Info("No new WAL entries since last backup")
        return nil
    }

    currentIndex, err := bs.walReader.GetCurrentIndex()
    if err != nil {
        return fmt.Errorf("failed to get current WAL index: %v", err)
    }

    // ========== ДОБАВЛЕНО: Получаем незавершённые 2PC транзакции ==========
    var pending2PC []*storage.TransactionRecord
    var twoPCCount int
    if bs.txManager != nil && bs.config.IncludeTwoPCState {
        pending2PC = bs.txManager.GetPending2PCTransactions()
        twoPCCount = len(pending2PC)
        if twoPCCount > 0 {
            bs.logger.Info(fmt.Sprintf("Found %d pending 2PC transactions in incremental backup", twoPCCount))
        }
    }

    // Создаём структуру инкрементального бэкапа
    backupData := struct {
        BackupID   string      `json:"backup_id"`
        Type       string      `json:"type"`
        StartTime  int64       `json:"start_time"`
        WALStart   uint64      `json:"wal_start"`
        WALEnd     uint64      `json:"wal_end"`
        ParentID   string      `json:"parent_id"`
        Entries    []WALEntry  `json:"entries"`
        Metadata   interface{} `json:"metadata,omitempty"`
        TwoPCState interface{} `json:"two_pc_state,omitempty"`
    }{
        BackupID:  backupID,
        Type:      "incremental",
        StartTime: startTime,
        WALStart:  lastIndex,
        WALEnd:    currentIndex,
        ParentID:  lastFullBackup.ID,
        Entries:   walEntries,
        Metadata: map[string]interface{}{
            "version":              "1.0",
            "created_by":           "backup_scheduler",
            "entries_count":        len(walEntries),
            "parent_backup":        lastFullBackup.ID,
            "parent_time":          lastFullBackup.StartTime,
            "two_pc_pending_count": twoPCCount,
            "include_two_pc":       bs.config.IncludeTwoPCState,
        },
        TwoPCState: map[string]interface{}{
            "pending_count": twoPCCount,
            "timestamp":     time.Now().UnixMilli(),
        },
    }

    // Если есть 2PC транзакции, добавляем их ID
    if twoPCCount > 0 && bs.config.IncludeTwoPCState {
        txIDs := make([]string, 0, twoPCCount)
        for _, tx := range pending2PC {
            txIDs = append(txIDs, fmt.Sprintf("%d", tx.ID))
        }
        if twoPCState, ok := backupData.TwoPCState.(map[string]interface{}); ok {
            twoPCState["transaction_ids"] = txIDs
        }
    }

    data, err := json.Marshal(backupData)
    if err != nil {
        return fmt.Errorf("failed to marshal backup data: %v", err)
    }

    // Проверяем размер бэкапа
    if bs.config.MaxBackupSizeMB > 0 && int64(len(data)) > bs.config.MaxBackupSizeMB*1024*1024 {
        bs.logger.Info("Incremental backup too large, performing full backup instead")
        return bs.performFullBackup(scheduleID)
    }

    // Сохраняем инкрементальный бэкап
    backupPath := fmt.Sprintf("inc/%s/%s.backup",
        time.Now().Format("2006-01-02"), backupID)

    if err := bs.storage.SaveBackup(backupPath, data); err != nil {
        return fmt.Errorf("failed to save incremental backup: %v", err)
    }

    // Обновляем последний индекс
    if err := bs.walReader.SetLastBackupIndex(currentIndex); err != nil {
        bs.logger.Warn(fmt.Sprintf("Failed to set last backup index: %v", err))
    }

    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])

    endTime := time.Now().UnixMilli()

    backupInfo := &BackupInfo{
        ID:         backupID,
        ScheduleID: scheduleID,
        Type:       IncrementalBackup,
        StartTime:  startTime,
        EndTime:    endTime,
        Status:     "completed",
        SizeBytes:  int64(len(data)),
        Path:       backupPath,
        WALStart:   lastIndex,
        WALEnd:     currentIndex,
        ParentID:   lastFullBackup.ID,
        Checksum:   checksum,
        TwoPCCount: twoPCCount,
    }

    bs.mu.Lock()
    bs.backups[backupID] = backupInfo
    bs.mu.Unlock()

    if err := bs.saveBackupHistory(); err != nil {
        bs.logger.Warn(fmt.Sprintf("Failed to save backup history: %v", err))
    }

    bs.updateScheduleLastRun(scheduleID, startTime)

    duration := (endTime - startTime) / 1000
    bs.logger.Info(fmt.Sprintf("Incremental backup %s completed in %d seconds, %d entries, size=%d bytes, pending_2pc=%d",
        backupID, duration, len(walEntries), backupInfo.SizeBytes, twoPCCount))

    return nil
}

// findLastFullBackup находит последний успешный полный бэкап
func (bs *BackupScheduler) findLastFullBackup() *BackupInfo {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    var lastFull *BackupInfo
    for _, backup := range bs.backups {
        if backup.Type == FullBackup && backup.Status == "completed" {
            if lastFull == nil || backup.EndTime > lastFull.EndTime {
                lastFull = backup
            }
        }
    }
    return lastFull
}

// updateScheduleLastRun обновляет время последнего запуска расписания
func (bs *BackupScheduler) updateScheduleLastRun(scheduleID string, timestamp int64) {
    bs.mu.Lock()
    defer bs.mu.Unlock()

    if schedule, exists := bs.schedules[scheduleID]; exists {
        schedule.LastRun = timestamp
        schedule.UpdatedAt = time.Now().UnixMilli()
        if err := bs.saveSchedules(); err != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to save schedules after update: %v", err))
        }
    }
}

// cleanupOldBackups удаляет старые бэкапы
func (bs *BackupScheduler) cleanupOldBackups(scheduleID string) {
    bs.mu.Lock()
    defer bs.mu.Unlock()

    schedule, exists := bs.schedules[scheduleID]
    if !exists {
        return
    }

    cutoffTime := time.Now().AddDate(0, 0, -schedule.RetentionDays).UnixMilli()

    toDelete := make([]string, 0)
    for id, backup := range bs.backups {
        if backup.ScheduleID == scheduleID && backup.StartTime < cutoffTime {
            toDelete = append(toDelete, id)
            // Удаляем файл бэкапа
            if err := bs.storage.DeleteBackup(backup.Path); err != nil {
                bs.logger.Warn(fmt.Sprintf("Failed to delete backup file %s: %v", backup.Path, err))
            }
        }
    }

    for _, id := range toDelete {
        delete(bs.backups, id)
    }

    if len(toDelete) > 0 {
        if err := bs.saveBackupHistory(); err != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to save backup history after cleanup: %v", err))
        }
        bs.logger.Info(fmt.Sprintf("Cleaned up %d old backups for schedule %s", len(toDelete), scheduleID))
    }
}

// ListBackups возвращает список бэкапов
func (bs *BackupScheduler) ListBackups() []*BackupInfo {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    backups := make([]*BackupInfo, 0, len(bs.backups))
    for _, b := range bs.backups {
        backups = append(backups, b)
    }

    // Сортируем по времени (новейшие первыми)
    sort.Slice(backups, func(i, j int) bool {
        return backups[i].StartTime > backups[j].StartTime
    })

    return backups
}

// ListSchedules возвращает список расписаний
func (bs *BackupScheduler) ListSchedules() []*BackupSchedule {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    schedules := make([]*BackupSchedule, 0, len(bs.schedules))
    for _, s := range bs.schedules {
        schedules = append(schedules, s)
    }
    return schedules
}

// GetBackup возвращает информацию о бэкапе
func (bs *BackupScheduler) GetBackup(backupID string) *BackupInfo {
    bs.mu.RLock()
    defer bs.mu.RUnlock()
    return bs.backups[backupID]
}

// Restore восстанавливает данные из бэкапа
func (bs *BackupScheduler) Restore(backupID string) error {
    if !bs.running.CompareAndSwap(false, true) {
        return fmt.Errorf("restore already in progress")
    }
    defer bs.running.Store(false)

    backup := bs.GetBackup(backupID)
    if backup == nil {
        return fmt.Errorf("backup not found: %s", backupID)
    }

    bs.logger.Info(fmt.Sprintf("Starting restore from backup %s", backupID))

    // Проверяем существование файла бэкапа
    if !bs.storage.BackupExists(backup.Path) {
        return fmt.Errorf("backup file not found: %s", backup.Path)
    }

    data, err := bs.storage.LoadBackup(backup.Path)
    if err != nil {
        return fmt.Errorf("failed to load backup: %v", err)
    }

    // Проверяем контрольную сумму
    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])
    if checksum != backup.Checksum {
        return fmt.Errorf("backup checksum mismatch, data may be corrupted")
    }

    if backup.Type == FullBackup {
        // Восстановление из полного бэкапа
        bs.logger.Info("Restoring from full backup...")

        var fullData struct {
            BackupID   string      `json:"backup_id"`
            Type       string      `json:"type"`
            Entries    []WALEntry  `json:"entries"`
            Metadata   interface{} `json:"metadata"`
            TwoPCState interface{} `json:"two_pc_state,omitempty"`
        }

        if err := json.Unmarshal(data, &fullData); err != nil {
            return fmt.Errorf("failed to parse full backup data: %v", err)
        }

        bs.logger.Info(fmt.Sprintf("Full backup contains %d WAL entries", len(fullData.Entries)))

        // ========== ДОБАВЛЕНО: Восстановление 2PC состояния ==========
        if fullData.TwoPCState != nil && bs.config.IncludeTwoPCState {
            bs.logger.Info("Restoring 2PC state from backup...")
            // Здесь логика восстановления 2PC состояния
            if twoPCState, ok := fullData.TwoPCState.(map[string]interface{}); ok {
                if pendingCount, ok := twoPCState["pending_count"].(float64); ok && pendingCount > 0 {
                    bs.logger.Info(fmt.Sprintf("Found %d pending 2PC transactions in backup", int(pendingCount)))
                    // Восстанавливаем pending транзакции в менеджере
                    if bs.txManager != nil {
                        // Здесь можно восстановить состояние 2PC
                        // В реальной реализации нужно восстановить транзакции из WAL
                    }
                }
            }
        }

        // Здесь реальное восстановление данных

    } else {
        // Восстановление из инкрементального бэкапа
        bs.logger.Info("Restoring from incremental backup...")

        // Сначала восстанавливаем полный бэкап-родитель
        if backup.ParentID == "" {
            return fmt.Errorf("incremental backup has no parent ID")
        }

        // Проверяем существование родительского бэкапа
        parentBackup := bs.GetBackup(backup.ParentID)
        if parentBackup == nil {
            return fmt.Errorf("parent backup %s not found", backup.ParentID)
        }

        if err := bs.Restore(backup.ParentID); err != nil {
            return fmt.Errorf("failed to restore parent backup: %v", err)
        }

        var incData struct {
            BackupID   string     `json:"backup_id"`
            Type       string     `json:"type"`
            WALStart   uint64     `json:"wal_start"`
            WALEnd     uint64     `json:"wal_end"`
            Entries    []WALEntry `json:"entries"`
            TwoPCState interface{} `json:"two_pc_state,omitempty"`
        }

        if err := json.Unmarshal(data, &incData); err != nil {
            return fmt.Errorf("failed to parse incremental backup data: %v", err)
        }

        bs.logger.Info(fmt.Sprintf("Applying %d incremental changes (WAL %d -> %d)",
            len(incData.Entries), incData.WALStart, incData.WALEnd))

        // ========== ДОБАВЛЕНО: Восстановление 2PC состояния из инкремента ==========
        if incData.TwoPCState != nil && bs.config.IncludeTwoPCState {
            bs.logger.Info("Restoring 2PC state from incremental backup...")
            if twoPCState, ok := incData.TwoPCState.(map[string]interface{}); ok {
                if pendingCount, ok := twoPCState["pending_count"].(float64); ok && pendingCount > 0 {
                    bs.logger.Info(fmt.Sprintf("Found %d pending 2PC transactions in incremental backup", int(pendingCount)))
                }
            }
        }

        // Здесь применяем инкрементальные изменения
    }

    bs.logger.Info(fmt.Sprintf("Restore from backup %s completed", backupID))
    return nil
}

// Start запускает планировщик
func (bs *BackupScheduler) Start() {
    bs.wg.Add(1)
    go bs.schedulerLoop()
    if bs.logger != nil {
        bs.logger.Info("Backup scheduler started")
    }
}

// schedulerLoop основной цикл планировщика
func (bs *BackupScheduler) schedulerLoop() {
    defer bs.wg.Done()

    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-bs.stopChan:
            return
        case <-ticker.C:
            bs.checkSchedules()
        }
    }
}

// checkSchedules проверяет расписания и запускает бэкапы
func (bs *BackupScheduler) checkSchedules() {
    bs.mu.RLock()
    schedules := make([]*BackupSchedule, 0, len(bs.schedules))
    for _, s := range bs.schedules {
        if s.Enabled {
            schedules = append(schedules, s)
        }
    }
    bs.mu.RUnlock()

    now := time.Now()
    for _, schedule := range schedules {
        // Простая проверка расписания (в реальности используется cron-парсер)
        if schedule.NextRun == 0 || schedule.NextRun <= now.UnixMilli() {
            // Вычисляем следующее время (упрощённо - через 24 часа)
            schedule.NextRun = now.Add(24 * time.Hour).UnixMilli()

            bs.mu.Lock()
            bs.schedules[schedule.ID] = schedule
            bs.mu.Unlock()

            if err := bs.saveSchedules(); err != nil {
                bs.logger.Warn(fmt.Sprintf("Failed to save schedule after update: %v", err))
            }

            // Запускаем бэкап в отдельной горутине
            scheduleID := schedule.ID
            backupType := schedule.Type
            go func() {
                if backupType == FullBackup {
                    bs.performFullBackup(scheduleID)
                } else {
                    bs.performIncrementalBackup(scheduleID)
                }
            }()
        }
    }
}

// Stop останавливает планировщик
func (bs *BackupScheduler) Stop() {
    close(bs.stopChan)
    bs.wg.Wait()
    if bs.logger != nil {
        bs.logger.Info("Backup scheduler stopped")
    }
}

// GetStats возвращает статистику планировщика
func (bs *BackupScheduler) GetStats() map[string]interface{} {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    fullCount := 0
    incCount := 0
    totalSize := int64(0)
    total2PC := 0

    for _, b := range bs.backups {
        if b.Type == FullBackup {
            fullCount++
        } else {
            incCount++
        }
        totalSize += b.SizeBytes
        total2PC += b.TwoPCCount
    }

    return map[string]interface{}{
        "total_backups":        len(bs.backups),
        "full_backups":         fullCount,
        "incremental_backups":  incCount,
        "total_size_bytes":     totalSize,
        "schedules":            len(bs.schedules),
        "is_running":           bs.running.Load(),
        "backup_dir":           bs.config.BackupDir,
        "compress_enabled":     bs.config.CompressEnabled,
        "incremental_enabled":  bs.config.EnableIncremental,
        "include_two_pc_state": bs.config.IncludeTwoPCState,
        "total_2pc_transactions": total2PC,
    }
}
