/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/persistence.go
// Назначение: Персистентное хранение данных на диске с поддержкой checkpoint и recovery

package storage

import (
    "bytes"
    "compress/gzip"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

// ========== WALReader интерфейс и реализация ==========

// WALEntry представляет запись WAL
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

// walReaderImpl реализация WALReader для работы с файлами WAL
type walReaderImpl struct {
    segmentsDir     string
    lastBackupIndex atomic.Uint64
    mu              sync.RWMutex
    logger          LoggerInterface
}

// NewWALReaderImpl создаёт новый WALReader
func NewWALReaderImpl(segmentsDir string, logger LoggerInterface) WALReader {
    wr := &walReaderImpl{
        segmentsDir: segmentsDir,
        logger:      logger,
    }
    wr.loadLastBackupIndex()
    return wr
}

// loadLastBackupIndex загружает последний индекс из файла
func (wr *walReaderImpl) loadLastBackupIndex() {
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

// ReadSince читает записи WAL с указанного индекса
func (wr *walReaderImpl) ReadSince(index uint64) ([]WALEntry, error) {
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

// GetCurrentIndex возвращает текущий индекс WAL
func (wr *walReaderImpl) GetCurrentIndex() (uint64, error) {
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

// GetLastBackupIndex возвращает последний сохранённый индекс бэкапа
func (wr *walReaderImpl) GetLastBackupIndex() uint64 {
    return wr.lastBackupIndex.Load()
}

// SetLastBackupIndex устанавливает последний индекс бэкапа
func (wr *walReaderImpl) SetLastBackupIndex(index uint64) error {
    wr.lastBackupIndex.Store(index)

    indexPath := filepath.Join(wr.segmentsDir, "last_backup_index.json")
    data, err := json.Marshal(map[string]uint64{"last_index": index})
    if err != nil {
        return err
    }
    return os.WriteFile(indexPath, data, 0644)
}

// GetSegments возвращает список сегментов WAL
func (wr *walReaderImpl) GetSegments() ([]string, error) {
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
func (wr *walReaderImpl) ReadSegment(segmentPath string) ([]WALEntry, error) {
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

// ========== PersistenceConfig ==========

// PersistenceConfig конфигурация персистентного хранения
type PersistenceConfig struct {
    DataDir             string        `json:"data_dir"`
    CheckpointInterval  time.Duration `json:"checkpoint_interval"`
    MaxCheckpoints      int           `json:"max_checkpoints"`
    CompressEnabled     bool          `json:"compress_enabled"`
    SyncWrites          bool          `json:"sync_writes"`
    WalPath             string        `json:"wal_path"`
    UseWalForCheckpoint bool          `json:"use_wal_for_checkpoint"`
    AtomicWrites        bool          `json:"atomic_writes"`
}

// DefaultPersistenceConfig возвращает конфигурацию по умолчанию
func DefaultPersistenceConfig() *PersistenceConfig {
    return &PersistenceConfig{
        DataDir:             "futriis_data",
        CheckpointInterval:  5 * time.Minute,
        MaxCheckpoints:      10,
        CompressEnabled:     true,
        SyncWrites:          true,
        WalPath:             "futriis.wal",
        UseWalForCheckpoint: true,
        AtomicWrites:        true,
    }
}

// DatabaseSnapshot представляет снапшот базы данных
type DatabaseSnapshot struct {
    Name        string                 `json:"name"`
    Collections map[string]interface{} `json:"collections"`
    CreatedAt   int64                  `json:"created_at"`
    Version     uint64                 `json:"version"`
    Checksum    string                 `json:"checksum"`
    WalLSN      uint64                 `json:"wal_lsn,omitempty"`
    WalSegment  string                 `json:"wal_segment,omitempty"`
}

// CheckpointRecoveryInfo содержит информацию о восстановлении из чекпоинта
type CheckpointRecoveryInfo struct {
    CheckpointTime   int64   `json:"checkpoint_time"`
    CheckpointVersion uint64  `json:"checkpoint_version"`
    WalLSN           uint64  `json:"wal_lsn"`
    AppliedWalCount  int     `json:"applied_wal_count"`
    RestoredDocs     int64   `json:"restored_docs"`
    RestoredColls    int     `json:"restored_collections"`
    DurationMs       int64   `json:"duration_ms"`
    Success          bool    `json:"success"`
    Error            string  `json:"error,omitempty"`
}

// PersistenceManager управляет персистентным хранением
type PersistenceManager struct {
    config          *PersistenceConfig
    storage         *Storage
    logger          LoggerInterface
    mu              sync.RWMutex
    stopChan        chan struct{}
    wg              sync.WaitGroup
    lastCheckpoint  int64
    checkpointID    atomic.Uint64
    walManager      interface{}
    walReader       WALReader
    recoveryInfo    *CheckpointRecoveryInfo
    isRestoring     atomic.Bool
}

// NewPersistenceManager создаёт новый менеджер персистентности
func NewPersistenceManager(config *PersistenceConfig, storage *Storage, logger LoggerInterface) *PersistenceManager {
    if config == nil {
        config = DefaultPersistenceConfig()
    }

    pm := &PersistenceManager{
        config:         config,
        storage:        storage,
        logger:         logger,
        stopChan:       make(chan struct{}),
        lastCheckpoint: time.Now().UnixMilli(),
    }

    if err := os.MkdirAll(config.DataDir, 0755); err != nil {
        if logger != nil {
            logger.Error(fmt.Sprintf("Failed to create data directory: %v", err))
        }
    }

    if config.UseWalForCheckpoint && config.WalPath != "" {
        pm.walReader = NewWALReaderImpl(filepath.Dir(config.WalPath), logger)
    }

    return pm
}

// SetWALManager устанавливает WAL менеджер для синхронизации чекпоинтов
func (pm *PersistenceManager) SetWALManager(walManager interface{}) {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    pm.walManager = walManager

    if walManager != nil && pm.config.UseWalForCheckpoint {
        pm.walReader = NewWALReaderImpl(filepath.Dir(pm.config.WalPath), pm.logger)
    }
}

// Start запускает фоновое сохранение чекпоинтов
func (pm *PersistenceManager) Start() {
    pm.wg.Add(1)
    go pm.checkpointLoop()

    if pm.logger != nil {
        pm.logger.Info(fmt.Sprintf("Persistence manager started, data dir: %s", pm.config.DataDir))
    }
}

// Stop останавливает менеджер
func (pm *PersistenceManager) Stop() {
    close(pm.stopChan)
    pm.wg.Wait()

    pm.SaveAll()

    if pm.logger != nil {
        pm.logger.Info("Persistence manager stopped")
    }
}

// checkpointLoop периодически создаёт чекпоинты
func (pm *PersistenceManager) checkpointLoop() {
    defer pm.wg.Done()

    ticker := time.NewTicker(pm.config.CheckpointInterval)
    defer ticker.Stop()

    for {
        select {
        case <-pm.stopChan:
            return
        case <-ticker.C:
            if pm.isRestoring.Load() {
                if pm.logger != nil {
                    pm.logger.Info("Skipping checkpoint during restore")
                }
                continue
            }
            if err := pm.SaveAll(); err != nil {
                if pm.logger != nil {
                    pm.logger.Error(fmt.Sprintf("Failed to save checkpoint: %v", err))
                }
            } else {
                pm.cleanupOldCheckpoints()
            }
        }
    }
}

// SaveDatabase сохраняет базу данных на диск с атомарной записью
func (pm *PersistenceManager) SaveDatabase(dbName string) error {
    if pm.isRestoring.Load() {
        return fmt.Errorf("cannot save checkpoint during restore")
    }

    pm.mu.Lock()
    defer pm.mu.Unlock()

    db, err := pm.storage.GetDatabase(dbName)
    if err != nil {
        return fmt.Errorf("database not found: %s", dbName)
    }

    var walLSN uint64
    var walSegment string

    if pm.config.UseWalForCheckpoint && pm.walManager != nil {
        if segWal, ok := pm.walManager.(*SegmentedWALManager); ok {
            if segWal.currentSegment != nil {
                segWal.currentSegment.Writer.Flush()
                RealFsync(segWal.currentSegment.File)
                walLSN = segWal.currentSegment.EndLSN
                walSegment = fmt.Sprintf("%d", segWal.currentSegment.ID)
            }
        }
    }

    snapshot := &DatabaseSnapshot{
        Name:        dbName,
        Collections: make(map[string]interface{}),
        CreatedAt:   time.Now().UnixMilli(),
        Version:     pm.checkpointID.Add(1),
        WalLSN:      walLSN,
        WalSegment:  walSegment,
    }

    for _, collName := range db.ListCollections() {
        coll, err := db.GetCollection(collName)
        if err != nil {
            continue
        }

        docs := coll.GetAllDocuments()
        collData := make([]map[string]interface{}, 0, len(docs))
        for _, doc := range docs {
            collData = append(collData, map[string]interface{}{
                "_id":        doc.ID,
                "fields":     doc.GetFields(),
                "created_at": doc.CreatedAt,
                "updated_at": doc.UpdatedAt,
                "deleted_at": doc.DeletedAt,
                "version":    doc.Version,
            })
        }
        snapshot.Collections[collName] = collData
    }

    data, err := json.Marshal(snapshot)
    if err != nil {
        return err
    }

    // Вычисляем контрольную сумму
    hash := sha256.Sum256(data)
    snapshot.Checksum = hex.EncodeToString(hash[:])

    // Пере-сериализуем с контрольной суммой
    data, err = json.Marshal(snapshot)
    if err != nil {
        return err
    }

    if pm.config.CompressEnabled {
        data, err = pm.compress(data)
        if err != nil {
            return err
        }
    }

    // Атомарная запись с использованием временного файла
    filename := pm.getSnapshotFilename(dbName, snapshot.Version)
    if err := pm.atomicWriteFile(filename, data); err != nil {
        return err
    }

    pm.lastCheckpoint = snapshot.CreatedAt

    if pm.logger != nil {
        pm.logger.Info(fmt.Sprintf("Saved database %s checkpoint %d (%d bytes, checksum: %s, WAL LSN: %d)",
            dbName, snapshot.Version, len(data), snapshot.Checksum[:8], walLSN))
    }

    return nil
}

// atomicWriteFile выполняет атомарную запись файла
func (pm *PersistenceManager) atomicWriteFile(filename string, data []byte) error {
    if pm.config.AtomicWrites {
        // Создаём временный файл в той же директории
        tempFile := filename + ".tmp"
        if err := os.WriteFile(tempFile, data, 0644); err != nil {
            return fmt.Errorf("failed to write temp file: %v", err)
        }

        // Синхронизируем временный файл
        if pm.config.SyncWrites {
            if f, err := os.OpenFile(tempFile, os.O_RDWR, 0644); err == nil {
                RealFsync(f)
                f.Close()
            }
        }

        // Атомарное переименование
        if err := os.Rename(tempFile, filename); err != nil {
            os.Remove(tempFile)
            return fmt.Errorf("failed to rename temp file: %v", err)
        }

        // Синхронизируем директорию
        FsyncDir(pm.config.DataDir)

        return nil
    }

    // Обычная запись (не атомарная)
    return os.WriteFile(filename, data, 0644)
}

// SaveAll сохраняет все базы данных
func (pm *PersistenceManager) SaveAll() error {
    if pm.isRestoring.Load() {
        return fmt.Errorf("cannot save checkpoints during restore")
    }

    databases := pm.storage.ListDatabases()

    var lastErr error
    for _, dbName := range databases {
        if err := pm.SaveDatabase(dbName); err != nil {
            lastErr = err
            if pm.logger != nil {
                pm.logger.Error(fmt.Sprintf("Failed to save database %s: %v", dbName, err))
            }
        }
    }

    return lastErr
}

// LoadDatabase загружает базу данных с диска с применением WAL
func (pm *PersistenceManager) LoadDatabase(dbName string) error {
    pm.isRestoring.Store(true)
    defer pm.isRestoring.Store(false)

    pm.mu.Lock()
    defer pm.mu.Unlock()

    startTime := time.Now()

    recoveryInfo := &CheckpointRecoveryInfo{
        CheckpointTime: time.Now().UnixMilli(),
        Success:        false,
    }

    snapshot, err := pm.findLatestSnapshot(dbName)
    if err != nil {
        recoveryInfo.Error = err.Error()
        pm.recoveryInfo = recoveryInfo
        return err
    }

    if snapshot == nil {
        recoveryInfo.Error = "no snapshot found"
        pm.recoveryInfo = recoveryInfo
        return fmt.Errorf("no snapshot found for database: %s", dbName)
    }

    recoveryInfo.CheckpointVersion = snapshot.Version
    recoveryInfo.WalLSN = snapshot.WalLSN

    // Проверяем целостность чекпоинта
    if err := pm.verifySnapshot(snapshot); err != nil {
        recoveryInfo.Error = err.Error()
        pm.recoveryInfo = recoveryInfo
        return fmt.Errorf("snapshot integrity check failed: %v", err)
    }

    // Создаём базу данных если не существует
    if !pm.storage.ExistsDatabase(dbName) {
        if err := pm.storage.CreateDatabase(dbName); err != nil {
            recoveryInfo.Error = err.Error()
            pm.recoveryInfo = recoveryInfo
            return err
        }
    }

    db, err := pm.storage.GetDatabase(dbName)
    if err != nil {
        recoveryInfo.Error = err.Error()
        pm.recoveryInfo = recoveryInfo
        return err
    }

    // Восстанавливаем коллекции
    restoredColls := 0
    restoredDocs := int64(0)

    for collName, collDataRaw := range snapshot.Collections {
        collData, ok := collDataRaw.([]interface{})
        if !ok {
            continue
        }

        // Удаляем существующую коллекцию если есть
        if _, err := db.GetCollection(collName); err == nil {
            db.DropCollection(collName)
        }

        if err := db.CreateCollection(collName); err != nil {
            if pm.logger != nil {
                pm.logger.Error(fmt.Sprintf("Failed to create collection %s: %v", collName, err))
            }
            continue
        }

        coll, err := db.GetCollection(collName)
        if err != nil {
            continue
        }

        restoredColls++

        for _, docRaw := range collData {
            docMap, ok := docRaw.(map[string]interface{})
            if !ok {
                continue
            }

            docID, ok := docMap["_id"].(string)
            if !ok {
                continue
            }

            doc := NewDocumentWithID(docID)

            if fields, ok := docMap["fields"].(map[string]interface{}); ok {
                for k, v := range fields {
                    doc.SetField(k, v)
                }
            }

            if createdAt, ok := docMap["created_at"].(float64); ok {
                doc.CreatedAt = int64(createdAt)
            } else if createdAt, ok := docMap["created_at"].(int64); ok {
                doc.CreatedAt = createdAt
            }

            if updatedAt, ok := docMap["updated_at"].(float64); ok {
                doc.UpdatedAt = int64(updatedAt)
            } else if updatedAt, ok := docMap["updated_at"].(int64); ok {
                doc.UpdatedAt = updatedAt
            }

            if deletedAt, ok := docMap["deleted_at"].(float64); ok {
                doc.DeletedAt = int64(deletedAt)
            } else if deletedAt, ok := docMap["deleted_at"].(int64); ok {
                doc.DeletedAt = deletedAt
            }

            if version, ok := docMap["version"].(float64); ok {
                doc.Version = uint64(version)
            } else if version, ok := docMap["version"].(uint64); ok {
                doc.Version = version
            }

            if err := coll.Insert(doc); err != nil {
                if pm.logger != nil {
                    pm.logger.Warn(fmt.Sprintf("Failed to restore document %s: %v", docID, err))
                }
                continue
            }
            restoredDocs++
        }
    }

    recoveryInfo.RestoredDocs = restoredDocs
    recoveryInfo.RestoredColls = restoredColls

    // Применяем WAL записи после чекпоинта
    appliedWalCount := 0
    if pm.config.UseWalForCheckpoint && snapshot.WalLSN > 0 && pm.walReader != nil {
        currentLSN, err := pm.walReader.GetCurrentIndex()
        if err == nil && currentLSN > snapshot.WalLSN {
            if pm.logger != nil {
                pm.logger.Info(fmt.Sprintf("Database %s: Applying %d WAL entries after checkpoint (LSN %d -> %d)",
                    dbName, currentLSN-snapshot.WalLSN, snapshot.WalLSN, currentLSN))
            }

            entries, err := pm.walReader.ReadSince(snapshot.WalLSN)
            if err != nil {
                if pm.logger != nil {
                    pm.logger.Error(fmt.Sprintf("Failed to read WAL entries: %v", err))
                }
            } else {
                for _, entry := range entries {
                    if err := pm.applyWALEntry(db, entry); err != nil {
                        if pm.logger != nil {
                            pm.logger.Error(fmt.Sprintf("Failed to apply WAL entry %d: %v", entry.Index, err))
                        }
                        continue
                    }
                    appliedWalCount++
                }
            }
        }
    }

    recoveryInfo.AppliedWalCount = appliedWalCount
    recoveryInfo.DurationMs = time.Since(startTime).Milliseconds()
    recoveryInfo.Success = true

    pm.recoveryInfo = recoveryInfo

    if pm.logger != nil {
        pm.logger.Info(fmt.Sprintf("Loaded database %s from snapshot (version %d, WAL LSN: %d, applied %d WAL entries, %d docs, %d colls, duration: %dms)",
            dbName, snapshot.Version, snapshot.WalLSN, appliedWalCount, restoredDocs, restoredColls, recoveryInfo.DurationMs))
    }

    return nil
}

// applyWALEntry применяет одну запись WAL к базе данных
func (pm *PersistenceManager) applyWALEntry(db *Database, entry WALEntry) error {
    // Проверяем тип записи (1 = Transaction) - ИСПРАВЛЕНО
    if entry.Type != "1" {
        return nil
    }
    
    var txRecord struct {
        ID         uint64 `json:"id"`
        State      int32  `json:"state"`
        Timestamp  int64  `json:"timestamp"`
        Operations []struct {
            Type       string                 `json:"type"`
            Database   string                 `json:"database"`
            Collection string                 `json:"collection"`
            DocumentID string                 `json:"document_id"`
            Data       map[string]interface{} `json:"data"`
            Version    uint64                 `json:"version"`
        } `json:"operations"`
    }

    if err := json.Unmarshal(entry.Data, &txRecord); err != nil {
        return err
    }

    // Применяем только закоммиченные транзакции (state = 1)
    if txRecord.State != 1 { // TransactionCommitted
        return nil
    }

    for _, op := range txRecord.Operations {
        if op.Database != db.name {
            continue
        }

        coll, err := db.GetCollection(op.Collection)
        if err != nil {
            continue
        }

        switch op.Type {
        case "insert":
            doc := NewDocumentWithID(op.DocumentID)
            for k, v := range op.Data {
                doc.SetField(k, v)
            }
            doc.Version = op.Version
            if err := coll.Insert(doc); err != nil {
                return err
            }

        case "update":
            if err := coll.Update(op.DocumentID, op.Data); err != nil {
                return err
            }

        case "delete":
            if err := coll.Delete(op.DocumentID); err != nil {
                return err
            }
        }
    }

    return nil
}

// verifySnapshot проверяет целостность снапшота
func (pm *PersistenceManager) verifySnapshot(snapshot *DatabaseSnapshot) error {
    if snapshot.Checksum == "" {
        return fmt.Errorf("snapshot has no checksum")
    }

    // Пересоздаём данные без контрольной суммы для проверки
    tempSnapshot := &DatabaseSnapshot{
        Name:        snapshot.Name,
        Collections: snapshot.Collections,
        CreatedAt:   snapshot.CreatedAt,
        Version:     snapshot.Version,
        WalLSN:      snapshot.WalLSN,
        WalSegment:  snapshot.WalSegment,
    }

    data, err := json.Marshal(tempSnapshot)
    if err != nil {
        return fmt.Errorf("failed to marshal for checksum: %v", err)
    }

    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])

    if checksum != snapshot.Checksum {
        return fmt.Errorf("checksum mismatch: expected %s, got %s", snapshot.Checksum[:8], checksum[:8])
    }

    return nil
}

// LoadAll загружает все базы данных с диска
func (pm *PersistenceManager) LoadAll() error {
    files, err := filepath.Glob(filepath.Join(pm.config.DataDir, "snapshot_*.json*"))
    if err != nil {
        return err
    }

    databases := make(map[string]bool)
    for _, file := range files {
        base := filepath.Base(file)
        parts := strings.Split(base, "_")
        if len(parts) >= 2 {
            dbName := parts[1]
            databases[dbName] = true
        }
    }

    for dbName := range databases {
        if err := pm.LoadDatabase(dbName); err != nil {
            if pm.logger != nil {
                pm.logger.Error(fmt.Sprintf("Failed to load database %s: %v", dbName, err))
            }
        }
    }

    return nil
}

// getSnapshotFilename возвращает имя файла для снапшота
func (pm *PersistenceManager) getSnapshotFilename(dbName string, version uint64) string {
    filename := fmt.Sprintf("snapshot_%s_%d.json", dbName, version)
    if pm.config.CompressEnabled {
        filename += ".gz"
    }
    return filepath.Join(pm.config.DataDir, filename)
}

// findLatestSnapshot находит последний снапшот базы данных
func (pm *PersistenceManager) findLatestSnapshot(dbName string) (*DatabaseSnapshot, error) {
    pattern := filepath.Join(pm.config.DataDir, fmt.Sprintf("snapshot_%s_*.json*", dbName))
    files, err := filepath.Glob(pattern)
    if err != nil {
        return nil, err
    }

    if len(files) == 0 {
        return nil, nil
    }

    sort.Slice(files, func(i, j int) bool {
        infoI, _ := os.Stat(files[i])
        infoJ, _ := os.Stat(files[j])
        if infoI == nil || infoJ == nil {
            return false
        }
        return infoI.ModTime().After(infoJ.ModTime())
    })

    data, err := os.ReadFile(files[0])
    if err != nil {
        return nil, err
    }

    if pm.config.CompressEnabled && strings.HasSuffix(files[0], ".gz") {
        data, err = pm.decompress(data)
        if err != nil {
            return nil, err
        }
    }

    var snapshot DatabaseSnapshot
    if err := json.Unmarshal(data, &snapshot); err != nil {
        return nil, err
    }

    return &snapshot, nil
}

// compress сжимает данные с помощью gzip
func (pm *PersistenceManager) compress(data []byte) ([]byte, error) {
    var buf bytes.Buffer
    w := gzip.NewWriter(&buf)

    if _, err := w.Write(data); err != nil {
        return nil, fmt.Errorf("failed to write compressed data: %v", err)
    }

    if err := w.Close(); err != nil {
        return nil, fmt.Errorf("failed to close gzip writer: %v", err)
    }

    return buf.Bytes(), nil
}

// decompress разжимает данные с помощью gzip
func (pm *PersistenceManager) decompress(data []byte) ([]byte, error) {
    reader, err := gzip.NewReader(bytes.NewReader(data))
    if err != nil {
        return nil, fmt.Errorf("failed to create gzip reader: %v", err)
    }
    defer reader.Close()

    var buf bytes.Buffer
    if _, err := buf.ReadFrom(reader); err != nil {
        return nil, fmt.Errorf("failed to decompress data: %v", err)
    }

    return buf.Bytes(), nil
}

// cleanupOldCheckpoints удаляет старые чекпоинты
func (pm *PersistenceManager) cleanupOldCheckpoints() {
    pattern := filepath.Join(pm.config.DataDir, "snapshot_*.json*")
    files, err := filepath.Glob(pattern)
    if err != nil {
        return
    }

    if len(files) <= pm.config.MaxCheckpoints {
        return
    }

    sort.Slice(files, func(i, j int) bool {
        infoI, _ := os.Stat(files[i])
        infoJ, _ := os.Stat(files[j])
        if infoI == nil || infoJ == nil {
            return false
        }
        return infoI.ModTime().Before(infoJ.ModTime())
    })

    toDelete := files[:len(files)-pm.config.MaxCheckpoints]
    for _, f := range toDelete {
        os.Remove(f)
        if pm.logger != nil {
            pm.logger.Debug(fmt.Sprintf("Removed old checkpoint: %s", f))
        }
    }
}

// GetLastCheckpointInfo возвращает информацию о последнем чекпоинте
func (pm *PersistenceManager) GetLastCheckpointInfo() map[string]interface{} {
    pm.mu.RLock()
    defer pm.mu.RUnlock()

    info := map[string]interface{}{
        "last_checkpoint_time":      pm.lastCheckpoint,
        "last_checkpoint_time_str":  time.UnixMilli(pm.lastCheckpoint).Format("2006-01-02 15:04:05.000"),
        "checkpoint_id":             pm.checkpointID.Load(),
        "wal_path":                  pm.config.WalPath,
        "use_wal":                   pm.config.UseWalForCheckpoint,
        "atomic_writes":             pm.config.AtomicWrites,
        "is_restoring":              pm.isRestoring.Load(),
    }

    if pm.recoveryInfo != nil {
        info["last_recovery"] = pm.recoveryInfo
    }

    return info
}

// GetRecoveryInfo возвращает информацию о последнем восстановлении
func (pm *PersistenceManager) GetRecoveryInfo() *CheckpointRecoveryInfo {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    return pm.recoveryInfo
}

// IsRestoring возвращает статус восстановления
func (pm *PersistenceManager) IsRestoring() bool {
    return pm.isRestoring.Load()
}
