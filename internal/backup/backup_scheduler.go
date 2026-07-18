/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

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

    "futriis/internal/config"
    "futriis/internal/storage"
)

type BackupType string

const (
    FullBackup        BackupType = "full"
    IncrementalBackup BackupType = "incremental"
)

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
    SagaCount  int        `json:"saga_count,omitempty"`
    SagaID     string     `json:"saga_id,omitempty"`
}

type WALEntry struct {
    Index     uint64 `json:"index"`
    Term      uint64 `json:"term"`
    Type      string `json:"type"`
    Data      []byte `json:"data"`
    Timestamp int64  `json:"timestamp"`
}

type WALReader interface {
    ReadSince(index uint64) ([]WALEntry, error)
    GetCurrentIndex() (uint64, error)
    GetLastBackupIndex() uint64
    SetLastBackupIndex(index uint64) error
    GetSegments() ([]string, error)
    ReadSegment(segmentPath string) ([]WALEntry, error)
}

type BackupStorage interface {
    SaveBackup(path string, data []byte) error
    LoadBackup(path string) ([]byte, error)
    ListBackups() ([]string, error)
    DeleteBackup(path string) error
    BackupExists(path string) bool
}

type LoggerInterface interface {
    Info(msg string)
    Warn(msg string)
    Error(msg string)
    Debug(msg string)
}

type FileBackupStorage struct {
    backupDir     string
    compressLevel int
    mu            sync.Mutex
}

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

func (fbs *FileBackupStorage) GetTransactionManager() *storage.TransactionManager {
    return storage.GetTransactionManager()
}

type WALReaderImpl struct {
    walManager      interface{}
    lastBackupIndex atomic.Uint64
    segmentsDir     string
    mu              sync.RWMutex
    logger          LoggerInterface
}

func NewWALReaderImpl(walManager interface{}, segmentsDir string, logger LoggerInterface) *WALReaderImpl {
    wr := &WALReaderImpl{
        walManager:  walManager,
        segmentsDir: segmentsDir,
        logger:      logger,
    }
    wr.loadLastBackupIndex()
    return wr
}

func (wr *WALReaderImpl) ReadSince(index uint64) ([]WALEntry, error) {
    wr.mu.RLock()
    defer wr.mu.RUnlock()

    entries := make([]WALEntry, 0)

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

func (wr *WALReaderImpl) GetCurrentIndex() (uint64, error) {
    segments, err := wr.GetSegments()
    if err != nil {
        return 0, err
    }

    if len(segments) == 0 {
        return 0, nil
    }

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

func (wr *WALReaderImpl) GetLastBackupIndex() uint64 {
    return wr.lastBackupIndex.Load()
}

func (wr *WALReaderImpl) SetLastBackupIndex(index uint64) error {
    wr.lastBackupIndex.Store(index)

    indexPath := filepath.Join(wr.segmentsDir, "last_backup_index.json")
    data, err := json.Marshal(map[string]uint64{"last_index": index})
    if err != nil {
        return err
    }
    return os.WriteFile(indexPath, data, 0644)
}

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

func (wr *WALReaderImpl) ReadSegment(segmentPath string) ([]WALEntry, error) {
    data, err := os.ReadFile(segmentPath)
    if err != nil {
        return nil, err
    }

    entries := make([]WALEntry, 0)

    pos := 0
    for pos < len(data) {
        if pos+4 > len(data) {
            break
        }

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
    config        *config.BackupConfig
    sagaManager   *storage.SagaManager
}

func NewBackupScheduler(cfg *config.BackupConfig, walReader WALReader, logger LoggerInterface) (*BackupScheduler, error) {
    if cfg == nil {
        return nil, fmt.Errorf("backup config is required")
    }

    compressLevel := 6
    if cfg.CompressEnabled {
        compressLevel = 6
    } else {
        compressLevel = 0
    }

    storage, err := NewFileBackupStorage(cfg.BackupDir, compressLevel)
    if err != nil {
        return nil, err
    }

    bs := &BackupScheduler{
        schedules:   make(map[string]*BackupSchedule),
        backups:     make(map[string]*BackupInfo),
        walReader:   walReader,
        storage:     storage,
        logger:      logger,
        stopChan:    make(chan struct{}),
        config:      cfg,
        sagaManager: storage.NewSagaManager(logger),
    }

    bs.loadSchedules()
    bs.loadBackupHistory()

    return bs, nil
}

func (bs *BackupScheduler) GetSagaManager() *storage.SagaManager {
    return bs.sagaManager
}

func (bs *BackupScheduler) ReloadConfig(cfg *config.BackupConfig) {
    bs.mu.Lock()
    defer bs.mu.Unlock()

    oldBackupDir := bs.config.BackupDir
    oldCompressEnabled := bs.config.CompressEnabled
    oldIncludeSagaState := bs.config.IncludeSagaState
    oldEnableIncremental := bs.config.EnableIncremental

    bs.config = cfg

    // Если изменилась директория или сжатие - пересоздаём хранилище
    if oldBackupDir != cfg.BackupDir || oldCompressEnabled != cfg.CompressEnabled {
        compressLevel := 6
        if cfg.CompressEnabled {
            compressLevel = 6
        } else {
            compressLevel = 0
        }
        newStorage, err := NewFileBackupStorage(cfg.BackupDir, compressLevel)
        if err != nil {
            if bs.logger != nil {
                bs.logger.Error(fmt.Sprintf("Failed to reload backup storage: %v", err))
            }
            return
        }
        bs.storage = newStorage
        // Перезагружаем данные из нового хранилища
        bs.loadSchedules()
        bs.loadBackupHistory()
    }

    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Backup scheduler configuration reloaded: enabled=%v, backup_dir=%s, compress=%v, incremental=%v, saga_state=%v",
            cfg.Enabled, cfg.BackupDir, cfg.CompressEnabled, cfg.EnableIncremental, cfg.IncludeSagaState))
    }
}

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

func (bs *BackupScheduler) saveSchedules() error {
    bs.mu.RLock()
    data, err := json.MarshalIndent(bs.schedules, "", "  ")
    bs.mu.RUnlock()

    if err != nil {
        return err
    }

    return bs.storage.SaveBackup("schedules.json", data)
}

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

func (bs *BackupScheduler) saveBackupHistory() error {
    bs.mu.RLock()
    data, err := json.MarshalIndent(bs.backups, "", "  ")
    bs.mu.RUnlock()

    if err != nil {
        return err
    }

    return bs.storage.SaveBackup("backups.json", data)
}

func (bs *BackupScheduler) AddSchedule(name, cronExpr string, backupType BackupType, retentionDays int) (*BackupSchedule, error) {
    bs.mu.Lock()
    defer bs.mu.Unlock()

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

func (bs *BackupScheduler) performFullBackup(scheduleID string) error {
    if !bs.running.CompareAndSwap(false, true) {
        return fmt.Errorf("backup already in progress")
    }
    defer bs.running.Store(false)

    startTime := time.Now().UnixMilli()
    backupID := fmt.Sprintf("full_%d_%d", startTime, bs.backupCounter.Add(1))

    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Starting full backup %s", backupID))
    }

    walIndex, err := bs.walReader.GetCurrentIndex()
    if err != nil {
        return fmt.Errorf("failed to get WAL index: %v", err)
    }

    var activeSagas []*storage.SagaTransaction
    var sagaCount int
    if bs.sagaManager != nil && bs.config.IncludeSagaState {
        activeSagas = bs.sagaManager.GetActiveSagas()
        sagaCount = len(activeSagas)
        if sagaCount > 0 && bs.logger != nil {
            bs.logger.Info(fmt.Sprintf("Found %d active SAGA transactions, including in backup", sagaCount))
        }
    }

    if err := bs.walReader.SetLastBackupIndex(walIndex); err != nil {
        if bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to set last backup index: %v", err))
        }
    }

    walEntries, err := bs.walReader.ReadSince(0)
    if err != nil {
        if bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to read WAL for full backup: %v", err))
        }
        walEntries = []WALEntry{}
    }

    sagaState := make(map[string]interface{})
    if sagaCount > 0 && bs.config.IncludeSagaState {
        sagaState["active_count"] = sagaCount
        sagaState["timestamp"] = time.Now().UnixMilli()
        sagaIDs := make([]string, 0, sagaCount)
        sagaStatuses := make([]string, 0, sagaCount)
        for _, saga := range activeSagas {
            sagaIDs = append(sagaIDs, saga.ID)
            sagaStatuses = append(sagaStatuses, saga.Status)
        }
        sagaState["saga_ids"] = sagaIDs
        sagaState["saga_statuses"] = sagaStatuses
    }

    backupData := struct {
        BackupID   string      `json:"backup_id"`
        Type       string      `json:"type"`
        StartTime  int64       `json:"start_time"`
        WALIndex   uint64      `json:"wal_index"`
        Entries    []WALEntry  `json:"entries,omitempty"`
        Metadata   interface{} `json:"metadata,omitempty"`
        SagaState  interface{} `json:"saga_state,omitempty"`
    }{
        BackupID:  backupID,
        Type:      "full",
        StartTime: startTime,
        WALIndex:  walIndex,
        Entries:   walEntries,
        Metadata: map[string]interface{}{
            "version":            "1.0",
            "created_by":         "backup_scheduler",
            "wal_entries_count":  len(walEntries),
            "saga_active_count":  sagaCount,
            "include_saga_state": bs.config.IncludeSagaState,
            "backup_config": map[string]interface{}{
                "backup_dir":         bs.config.BackupDir,
                "compress_enabled":   bs.config.CompressEnabled,
                "enable_incremental": bs.config.EnableIncremental,
                "retention_days":     bs.config.RetentionDays,
            },
        },
        SagaState: sagaState,
    }

    data, err := json.Marshal(backupData)
    if err != nil {
        return fmt.Errorf("failed to marshal backup data: %v", err)
    }

    backupPath := fmt.Sprintf("full/%s/%s.backup",
        time.Now().Format("2006-01-02"), backupID)

    if err := bs.storage.SaveBackup(backupPath, data); err != nil {
        return fmt.Errorf("failed to save backup: %v", err)
    }

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
        SagaCount:  sagaCount,
    }

    bs.mu.Lock()
    bs.backups[backupID] = backupInfo
    bs.mu.Unlock()

    if err := bs.saveBackupHistory(); err != nil {
        if bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Failed to save backup history: %v", err))
        }
    }

    bs.updateScheduleLastRun(scheduleID, startTime)
    bs.cleanupOldBackups(scheduleID)

    duration := (endTime - startTime) / 1000
    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Full backup %s completed in %d seconds, size=%d bytes, entries=%d, active_sagas=%d",
            backupID, duration, backupInfo.SizeBytes, len(walEntries), sagaCount))
    }

    return nil
}

func (bs *BackupScheduler) performIncrementalBackup(scheduleID string) error {
    if !bs.running.CompareAndSwap(false, true) {
        return fmt.Errorf("backup already in progress")
    }
    defer bs.running.Store(false)

    startTime := time.Now().UnixMilli()
    backupID := fmt.Sprintf("inc_%d_%d", startTime, bs.backupCounter.Add(1))

    sagaID := fmt.Sprintf("backup_saga_%s", backupID)
    saga := bs.sagaManager.BeginSaga(sagaID)

    saga.AddStep("validate_parent", func() error {
        lastFull := bs.findLastFullBackup()
        if lastFull == nil {
            return fmt.Errorf("no full backup found")
        }
        if !bs.storage.BackupExists(lastFull.Path) {
            return fmt.Errorf("parent backup file missing: %s", lastFull.ID)
        }
        return nil
    }, func() error {
        if bs.logger != nil {
            bs.logger.Warn("Compensating: parent backup validation failed")
        }
        return nil
    }, map[string]interface{}{
        "step": "validate_parent",
    })

    saga.AddStep("read_wal", func() error {
        lastIndex := bs.walReader.GetLastBackupIndex()
        if lastIndex == 0 {
            lastFull := bs.findLastFullBackup()
            if lastFull != nil {
                lastIndex = lastFull.WALEnd
            }
        }

        entries, err := bs.walReader.ReadSince(lastIndex)
        if err != nil {
            return fmt.Errorf("failed to read WAL: %v", err)
        }

        if len(entries) == 0 {
            if bs.logger != nil {
                bs.logger.Info("No new WAL entries since last backup")
            }
            return nil
        }

        saga.SetData("wal_entries", entries)
        saga.SetData("last_index", lastIndex)
        return nil
    }, func() error {
        if bs.logger != nil {
            bs.logger.Warn("Compensating: WAL read failed")
        }
        return nil
    }, map[string]interface{}{
        "step": "read_wal",
    })

    saga.AddStep("get_saga_state", func() error {
        if bs.sagaManager != nil && bs.config.IncludeSagaState {
            activeSagas := bs.sagaManager.GetActiveSagas()
            saga.SetData("active_sagas", activeSagas)
            saga.SetData("saga_count", len(activeSagas))
            if len(activeSagas) > 0 && bs.logger != nil {
                bs.logger.Info(fmt.Sprintf("Found %d active SAGA transactions in incremental backup", len(activeSagas)))
            }
        }
        return nil
    }, func() error {
        if bs.logger != nil {
            bs.logger.Warn("Compensating: SAGA state retrieval failed")
        }
        return nil
    }, map[string]interface{}{
        "step": "get_saga_state",
    })

    saga.AddStep("create_backup_file", func() error {
        entriesVal, _ := saga.GetData("wal_entries")
        entries, _ := entriesVal.([]WALEntry)
        lastIndexVal, _ := saga.GetData("last_index")
        lastIndex, _ := lastIndexVal.(uint64)
        sagaCountVal, _ := saga.GetData("saga_count")
        sagaCount, _ := sagaCountVal.(int)
        activeSagasVal, _ := saga.GetData("active_sagas")
        activeSagas, _ := activeSagasVal.([]*storage.SagaTransaction)

        currentIndex, err := bs.walReader.GetCurrentIndex()
        if err != nil {
            return fmt.Errorf("failed to get current WAL index: %v", err)
        }

        lastFull := bs.findLastFullBackup()
        if lastFull == nil {
            return fmt.Errorf("no full backup found")
        }

        sagaState := make(map[string]interface{})
        if sagaCount > 0 && bs.config.IncludeSagaState {
            sagaState["active_count"] = sagaCount
            sagaState["timestamp"] = time.Now().UnixMilli()
            if activeSagas != nil {
                sagaIDs := make([]string, 0, len(activeSagas))
                sagaStatuses := make([]string, 0, len(activeSagas))
                for _, s := range activeSagas {
                    sagaIDs = append(sagaIDs, s.ID)
                    sagaStatuses = append(sagaStatuses, s.Status)
                }
                sagaState["saga_ids"] = sagaIDs
                sagaState["saga_statuses"] = sagaStatuses
            }
        }

        backupData := struct {
            BackupID   string      `json:"backup_id"`
            Type       string      `json:"type"`
            StartTime  int64       `json:"start_time"`
            WALStart   uint64      `json:"wal_start"`
            WALEnd     uint64      `json:"wal_end"`
            ParentID   string      `json:"parent_id"`
            Entries    []WALEntry  `json:"entries"`
            Metadata   interface{} `json:"metadata,omitempty"`
            SagaState  interface{} `json:"saga_state,omitempty"`
            SagaID     string      `json:"saga_id"`
        }{
            BackupID:  backupID,
            Type:      "incremental",
            StartTime: startTime,
            WALStart:  lastIndex,
            WALEnd:    currentIndex,
            ParentID:  lastFull.ID,
            Entries:   entries,
            Metadata: map[string]interface{}{
                "version":            "1.0",
                "created_by":         "backup_scheduler",
                "entries_count":      len(entries),
                "parent_backup":      lastFull.ID,
                "parent_time":        lastFull.StartTime,
                "saga_active_count":  sagaCount,
                "include_saga_state": bs.config.IncludeSagaState,
                "saga_id":            sagaID,
                "backup_config": map[string]interface{}{
                    "backup_dir":         bs.config.BackupDir,
                    "compress_enabled":   bs.config.CompressEnabled,
                    "enable_incremental": bs.config.EnableIncremental,
                    "retention_days":     bs.config.RetentionDays,
                },
            },
            SagaState: sagaState,
            SagaID:    sagaID,
        }

        data, err := json.Marshal(backupData)
        if err != nil {
            return fmt.Errorf("failed to marshal backup data: %v", err)
        }

        backupPath := fmt.Sprintf("inc/%s/%s.backup",
            time.Now().Format("2006-01-02"), backupID)

        if err := bs.storage.SaveBackup(backupPath, data); err != nil {
            return fmt.Errorf("failed to save incremental backup: %v", err)
        }

        saga.SetData("backup_path", backupPath)
        saga.SetData("backup_data", data)
        saga.SetData("backup_size", int64(len(data)))
        return nil
    }, func() error {
        backupPathVal, _ := saga.GetData("backup_path")
        if backupPath, ok := backupPathVal.(string); ok && backupPath != "" {
            bs.storage.DeleteBackup(backupPath)
            if bs.logger != nil {
                bs.logger.Warn(fmt.Sprintf("Compensating: deleted backup file %s", backupPath))
            }
        }
        return nil
    }, map[string]interface{}{
        "step": "create_backup_file",
    })

    saga.AddStep("update_metadata", func() error {
        backupPathVal, _ := saga.GetData("backup_path")
        backupPath, _ := backupPathVal.(string)
        backupDataVal, _ := saga.GetData("backup_data")
        backupDataBytes, _ := backupDataVal.([]byte)
        backupSizeVal, _ := saga.GetData("backup_size")
        backupSize, _ := backupSizeVal.(int64)
        sagaCountVal, _ := saga.GetData("saga_count")
        sagaCount, _ := sagaCountVal.(int)

        if backupPath == "" {
            return fmt.Errorf("backup path not found")
        }

        currentIndex, err := bs.walReader.GetCurrentIndex()
        if err != nil {
            if bs.logger != nil {
                bs.logger.Warn(fmt.Sprintf("Failed to get current WAL index: %v", err))
            }
        } else {
            if err := bs.walReader.SetLastBackupIndex(currentIndex); err != nil {
                if bs.logger != nil {
                    bs.logger.Warn(fmt.Sprintf("Failed to set last backup index: %v", err))
                }
            }
        }

        hash := sha256.Sum256(backupDataBytes)
        checksum := hex.EncodeToString(hash[:])

        endTime := time.Now().UnixMilli()
        lastFull := bs.findLastFullBackup()
        lastIndex := bs.walReader.GetLastBackupIndex()

        backupInfo := &BackupInfo{
            ID:         backupID,
            ScheduleID: scheduleID,
            Type:       IncrementalBackup,
            StartTime:  startTime,
            EndTime:    endTime,
            Status:     "completed",
            SizeBytes:  backupSize,
            Path:       backupPath,
            WALStart:   lastIndex,
            WALEnd:     currentIndex,
            ParentID:   lastFull.ID,
            Checksum:   checksum,
            SagaCount:  sagaCount,
            SagaID:     sagaID,
        }

        bs.mu.Lock()
        bs.backups[backupID] = backupInfo
        bs.mu.Unlock()

        if err := bs.saveBackupHistory(); err != nil {
            if bs.logger != nil {
                bs.logger.Warn(fmt.Sprintf("Failed to save backup history: %v", err))
            }
        }

        bs.updateScheduleLastRun(scheduleID, startTime)

        duration := (endTime - startTime) / 1000
        if bs.logger != nil {
            bs.logger.Info(fmt.Sprintf("Incremental backup %s completed in %d seconds, entries=%d, size=%d bytes, active_sagas=%d",
                backupID, duration, len(entries), backupSize, sagaCount))
        }

        return nil
    }, func() error {
        if bs.logger != nil {
            bs.logger.Warn(fmt.Sprintf("Compensating: rolling back metadata for backup %s", backupID))
        }
        bs.mu.Lock()
        delete(bs.backups, backupID)
        bs.mu.Unlock()
        bs.saveBackupHistory()
        return nil
    }, map[string]interface{}{
        "step": "update_metadata",
    })

    if err := bs.sagaManager.Execute(saga); err != nil {
        if bs.logger != nil {
            bs.logger.Error(fmt.Sprintf("SAGA backup transaction failed: %v", err))
        }
        return err
    }

    return nil
}

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

func (bs *BackupScheduler) updateScheduleLastRun(scheduleID string, timestamp int64) {
    bs.mu.Lock()
    defer bs.mu.Unlock()

    if schedule, exists := bs.schedules[scheduleID]; exists {
        schedule.LastRun = timestamp
        schedule.UpdatedAt = time.Now().UnixMilli()
        if err := bs.saveSchedules(); err != nil {
            if bs.logger != nil {
                bs.logger.Warn(fmt.Sprintf("Failed to save schedules after update: %v", err))
            }
        }
    }
}

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
            if err := bs.storage.DeleteBackup(backup.Path); err != nil {
                if bs.logger != nil {
                    bs.logger.Warn(fmt.Sprintf("Failed to delete backup file %s: %v", backup.Path, err))
                }
            }
        }
    }

    for _, id := range toDelete {
        delete(bs.backups, id)
    }

    if len(toDelete) > 0 {
        if err := bs.saveBackupHistory(); err != nil {
            if bs.logger != nil {
                bs.logger.Warn(fmt.Sprintf("Failed to save backup history after cleanup: %v", err))
            }
        }
        if bs.logger != nil {
            bs.logger.Info(fmt.Sprintf("Cleaned up %d old backups for schedule %s", len(toDelete), scheduleID))
        }
    }
}

func (bs *BackupScheduler) ListBackups() []*BackupInfo {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    backups := make([]*BackupInfo, 0, len(bs.backups))
    for _, b := range bs.backups {
        backups = append(backups, b)
    }

    sort.Slice(backups, func(i, j int) bool {
        return backups[i].StartTime > backups[j].StartTime
    })

    return backups
}

func (bs *BackupScheduler) ListSchedules() []*BackupSchedule {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    schedules := make([]*BackupSchedule, 0, len(bs.schedules))
    for _, s := range bs.schedules {
        schedules = append(schedules, s)
    }
    return schedules
}

func (bs *BackupScheduler) GetBackup(backupID string) *BackupInfo {
    bs.mu.RLock()
    defer bs.mu.RUnlock()
    return bs.backups[backupID]
}

func (bs *BackupScheduler) Restore(backupID string) error {
    if !bs.running.CompareAndSwap(false, true) {
        return fmt.Errorf("restore already in progress")
    }
    defer bs.running.Store(false)

    backup := bs.GetBackup(backupID)
    if backup == nil {
        return fmt.Errorf("backup not found: %s", backupID)
    }

    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Starting restore from backup %s", backupID))
    }

    if !bs.storage.BackupExists(backup.Path) {
        return fmt.Errorf("backup file not found: %s", backup.Path)
    }

    data, err := bs.storage.LoadBackup(backup.Path)
    if err != nil {
        return fmt.Errorf("failed to load backup: %v", err)
    }

    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])
    if checksum != backup.Checksum {
        return fmt.Errorf("backup checksum mismatch, data may be corrupted")
    }

    if backup.Type == FullBackup {
        if bs.logger != nil {
            bs.logger.Info("Restoring from full backup...")
        }

        var fullData struct {
            BackupID   string      `json:"backup_id"`
            Type       string      `json:"type"`
            Entries    []WALEntry  `json:"entries"`
            Metadata   interface{} `json:"metadata"`
            SagaState  interface{} `json:"saga_state,omitempty"`
        }

        if err := json.Unmarshal(data, &fullData); err != nil {
            return fmt.Errorf("failed to parse full backup data: %v", err)
        }

        if bs.logger != nil {
            bs.logger.Info(fmt.Sprintf("Full backup contains %d WAL entries", len(fullData.Entries)))
        }

        if fullData.SagaState != nil && bs.config.IncludeSagaState {
            if bs.logger != nil {
                bs.logger.Info("Restoring SAGA state from backup...")
            }
            if sagaState, ok := fullData.SagaState.(map[string]interface{}); ok {
                if activeCount, ok := sagaState["active_count"].(float64); ok && activeCount > 0 {
                    if bs.logger != nil {
                        bs.logger.Info(fmt.Sprintf("Found %d active SAGA transactions in backup", int(activeCount)))
                    }
                }
            }
        }

    } else {
        if bs.logger != nil {
            bs.logger.Info("Restoring from incremental backup...")
        }

        if backup.ParentID == "" {
            return fmt.Errorf("incremental backup has no parent ID")
        }

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
            SagaState  interface{} `json:"saga_state,omitempty"`
            SagaID     string     `json:"saga_id,omitempty"`
        }

        if err := json.Unmarshal(data, &incData); err != nil {
            return fmt.Errorf("failed to parse incremental backup data: %v", err)
        }

        if bs.logger != nil {
            bs.logger.Info(fmt.Sprintf("Applying %d incremental changes (WAL %d -> %d)",
                len(incData.Entries), incData.WALStart, incData.WALEnd))
        }

        if incData.SagaState != nil && bs.config.IncludeSagaState {
            if bs.logger != nil {
                bs.logger.Info("Restoring SAGA state from incremental backup...")
            }
            if sagaState, ok := incData.SagaState.(map[string]interface{}); ok {
                if activeCount, ok := sagaState["active_count"].(float64); ok && activeCount > 0 {
                    if bs.logger != nil {
                        bs.logger.Info(fmt.Sprintf("Found %d active SAGA transactions in incremental backup", int(activeCount)))
                    }
                    if incData.SagaID != "" && bs.sagaManager != nil {
                        if bs.logger != nil {
                            bs.logger.Info(fmt.Sprintf("Restoring SAGA transaction %s", incData.SagaID))
                        }
                    }
                }
            }
        }
    }

    if bs.logger != nil {
        bs.logger.Info(fmt.Sprintf("Restore from backup %s completed", backupID))
    }
    return nil
}

func (bs *BackupScheduler) Start() {
    bs.wg.Add(1)
    go bs.schedulerLoop()
    if bs.logger != nil {
        bs.logger.Info("Backup scheduler started")
    }
}

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
        if schedule.NextRun == 0 || schedule.NextRun <= now.UnixMilli() {
            schedule.NextRun = now.Add(24 * time.Hour).UnixMilli()

            bs.mu.Lock()
            bs.schedules[schedule.ID] = schedule
            bs.mu.Unlock()

            if err := bs.saveSchedules(); err != nil {
                if bs.logger != nil {
                    bs.logger.Warn(fmt.Sprintf("Failed to save schedule after update: %v", err))
                }
            }

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

func (bs *BackupScheduler) Stop() {
    close(bs.stopChan)
    bs.wg.Wait()
    if bs.logger != nil {
        bs.logger.Info("Backup scheduler stopped")
    }
}

func (bs *BackupScheduler) GetStats() map[string]interface{} {
    bs.mu.RLock()
    defer bs.mu.RUnlock()

    fullCount := 0
    incCount := 0
    totalSize := int64(0)
    totalSaga := 0

    for _, b := range bs.backups {
        if b.Type == FullBackup {
            fullCount++
        } else {
            incCount++
        }
        totalSize += b.SizeBytes
        totalSaga += b.SagaCount
    }

    return map[string]interface{}{
        "total_backups":          len(bs.backups),
        "full_backups":           fullCount,
        "incremental_backups":    incCount,
        "total_size_bytes":       totalSize,
        "schedules":              len(bs.schedules),
        "is_running":             bs.running.Load(),
        "backup_dir":             bs.config.BackupDir,
        "compress_enabled":       bs.config.CompressEnabled,
        "incremental_enabled":    bs.config.EnableIncremental,
        "include_saga_state":     bs.config.IncludeSagaState,
        "total_saga_transactions": totalSaga,
        "retention_days":         bs.config.RetentionDays,
        "max_concurrent":         bs.config.MaxConcurrent,
    }
}
