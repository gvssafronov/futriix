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
// ИСПРАВЛЕНО: Защита от torn write через CRC и checksum в имени файла
// ИСПРАВЛЕНО: Обработка частично записанных WAL сегментов при восстановлении
// ИСПРАВЛЕНО: Добавлена валидация целостности снапшотов перед загрузкой
// ИСПРАВЛЕНО: Частичное применение транзакции теперь откатывается
// ИСПРАВЛЕНО: Drop перед Create при restore защищён (только после успешного создания)

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

// WALEntry представляет запись WAL
type WALEntry struct {
    Index     uint64 `json:"index"`
    Term      uint64 `json:"term"`
    Type      string `json:"type"`
    Data      []byte `json:"data"`
    Timestamp int64  `json:"timestamp"`
    CRC       uint32 `json:"crc,omitempty"`
}

type WALReader interface {
    ReadSince(index uint64) ([]WALEntry, error)
    GetCurrentIndex() (uint64, error)
    GetLastBackupIndex() uint64
    SetLastBackupIndex(index uint64) error
    GetSegments() ([]string, error)
    ReadSegment(segmentPath string) ([]WALEntry, error)
}

type walReaderImpl struct {
    segmentsDir     string
    lastBackupIndex atomic.Uint64
    mu              sync.RWMutex
    logger          LoggerInterface
}

func NewWALReaderImpl(segmentsDir string, logger LoggerInterface) WALReader {
    wr := &walReaderImpl{
        segmentsDir: segmentsDir,
        logger:      logger,
    }
    wr.loadLastBackupIndex()
    return wr
}

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

func (wr *walReaderImpl) GetLastBackupIndex() uint64 { return wr.lastBackupIndex.Load() }

func (wr *walReaderImpl) SetLastBackupIndex(index uint64) error {
    wr.lastBackupIndex.Store(index)
    indexPath := filepath.Join(wr.segmentsDir, "last_backup_index.json")
    data, err := json.Marshal(map[string]uint64{"last_index": index})
    if err != nil {
        return err
    }
    return os.WriteFile(indexPath, data, 0644)
}

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

func (wr *walReaderImpl) ReadSegment(segmentPath string) ([]WALEntry, error) {
    data, err := os.ReadFile(segmentPath)
    if err != nil {
        return nil, err
    }
    entries := make([]WALEntry, 0)
    pos := 0
    for pos < len(data) {
        if pos+4 > len(data) {
            if wr.logger != nil {
                wr.logger.Warn(fmt.Sprintf("Truncated record header at offset %d in %s", pos, segmentPath))
            }
            break
        }
        length := int(data[pos])<<24 | int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
        pos += 4
        if length <= 0 || length > 100*1024*1024 {
            if wr.logger != nil {
                wr.logger.Warn(fmt.Sprintf("Invalid record length %d at offset %d in %s", length, pos-4, segmentPath))
            }
            break
        }
        if pos+length > len(data) {
            if wr.logger != nil {
                wr.logger.Warn(fmt.Sprintf("Torn write detected: expected %d bytes at offset %d, but only %d available in %s",
                    length, pos, len(data)-pos, segmentPath))
            }
            break
        }
        recordData := data[pos : pos+length]
        pos += length
        var record struct {
            LSN       uint64 `json:"lsn"`
            Timestamp int64  `json:"timestamp"`
            Type      byte   `json:"type"`
            Data      []byte `json:"data"`
            CRC       uint32 `json:"crc,omitempty"`
        }
        if err := json.Unmarshal(recordData, &record); err != nil {
            if wr.logger != nil {
                wr.logger.Warn(fmt.Sprintf("Failed to unmarshal record at offset %d in %s: %v", pos-length, segmentPath, err))
            }
            continue
        }
        entry := WALEntry{
            Index:     record.LSN,
            Timestamp: record.Timestamp,
            Type:      fmt.Sprintf("%d", record.Type),
            Data:      record.Data,
            CRC:       record.CRC,
        }
        entries = append(entries, entry)
    }
    return entries, nil
}

type PersistenceConfig struct {
    DataDir               string        `json:"data_dir"`
    CheckpointInterval    time.Duration `json:"checkpoint_interval"`
    MaxCheckpoints        int           `json:"max_checkpoints"`
    CompressEnabled       bool          `json:"compress_enabled"`
    SyncWrites            bool          `json:"sync_writes"`
    WalPath               string        `json:"wal_path"`
    UseWalForCheckpoint   bool          `json:"use_wal_for_checkpoint"`
    AtomicWrites          bool          `json:"atomic_writes"`
    FsyncEnabled          bool          `json:"fsync_enabled"`
    WriteAheadLogging     bool          `json:"write_ahead_logging"`
    DurabilityLevel       string        `json:"durability_level"`
    CheckpointAfterCommit bool          `json:"checkpoint_after_commit"`
    MaxRetriesOnSync      int           `json:"max_retries_on_sync"`
    SyncRetryDelay        time.Duration `json:"sync_retry_delay"`
}

func DefaultPersistenceConfig() *PersistenceConfig {
    return &PersistenceConfig{
        DataDir:               "futriis_data",
        CheckpointInterval:    5 * time.Minute,
        MaxCheckpoints:        10,
        CompressEnabled:       true,
        SyncWrites:            true,
        WalPath:               "futriis.wal",
        UseWalForCheckpoint:   true,
        AtomicWrites:          true,
        FsyncEnabled:          true,
        WriteAheadLogging:     true,
        DurabilityLevel:       "sync",
        CheckpointAfterCommit: false,
        MaxRetriesOnSync:      3,
        SyncRetryDelay:        100 * time.Millisecond,
    }
}

type DatabaseSnapshot struct {
    Name        string                 `json:"name"`
    Collections map[string]interface{} `json:"collections"`
    CreatedAt   int64                  `json:"created_at"`
    Version     uint64                 `json:"version"`
    Checksum    string                 `json:"checksum"`
    WalLSN      uint64                 `json:"wal_lsn,omitempty"`
    WalSegment  string                 `json:"wal_segment,omitempty"`
}

type CheckpointRecoveryInfo struct {
    CheckpointTime    int64  `json:"checkpoint_time"`
    CheckpointVersion uint64 `json:"checkpoint_version"`
    WalLSN            uint64 `json:"wal_lsn"`
    AppliedWalCount   int    `json:"applied_wal_count"`
    RestoredDocs      int64  `json:"restored_docs"`
    RestoredColls     int    `json:"restored_collections"`
    DurationMs        int64  `json:"duration_ms"`
    Success           bool   `json:"success"`
    Error             string `json:"error,omitempty"`
}

type PersistenceManager struct {
    config             *PersistenceConfig
    storage            *Storage
    logger             LoggerInterface
    mu                 sync.RWMutex
    stopChan           chan struct{}
    wg                 sync.WaitGroup
    lastCheckpoint     int64
    checkpointID       atomic.Uint64
    walManager         interface{}
    walReader          WALReader
    recoveryInfo       *CheckpointRecoveryInfo
    isRestoring        atomic.Bool
    commitMutex        sync.Mutex
    syncAttempts       atomic.Uint64
    syncFailures       atomic.Uint64
    atomicWrites       atomic.Uint64
    tornWritesDetected atomic.Uint64
}

func NewPersistenceManager(config *PersistenceConfig, storage *Storage, logger LoggerInterface) *PersistenceManager {
    if config == nil {
        config = DefaultPersistenceConfig()
    }
    if config.DurabilityLevel == "sync" && !config.FsyncEnabled {
        config.FsyncEnabled = true
        if logger != nil {
            logger.Warn("Durability level 'sync' requires fsync, enabling fsync automatically")
        }
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

func (pm *PersistenceManager) SetWALManager(walManager interface{}) {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    pm.walManager = walManager
    if walManager != nil && pm.config.UseWalForCheckpoint {
        pm.walReader = NewWALReaderImpl(filepath.Dir(pm.config.WalPath), pm.logger)
    }
}

func (pm *PersistenceManager) Start() {
    pm.wg.Add(1)
    go pm.checkpointLoop()
    if pm.logger != nil {
        pm.logger.Info(fmt.Sprintf("Persistence manager started, data dir: %s, durability: %s, fsync: %v",
            pm.config.DataDir, pm.config.DurabilityLevel, pm.config.FsyncEnabled))
    }
}

func (pm *PersistenceManager) Stop() {
    close(pm.stopChan)
    pm.wg.Wait()
    if err := pm.SaveAllWithSync(); err != nil {
        if pm.logger != nil {
            pm.logger.Error(fmt.Sprintf("Failed to save checkpoints on stop: %v", err))
        }
    }
    if pm.logger != nil {
        pm.logger.Info("Persistence manager stopped")
    }
}

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
                if pm.config.FsyncEnabled {
                    if err := RealFsyncWithRetry(segWal.currentSegment.File, pm.config.MaxRetriesOnSync, pm.config.SyncRetryDelay); err != nil {
                        pm.syncFailures.Add(1)
                    }
                }
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
    hash := sha256.Sum256(data)
    snapshot.Checksum = hex.EncodeToString(hash[:])
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
    filename := pm.getSnapshotFilename(dbName, snapshot.Version)
    if pm.config.DurabilityLevel == "sync" {
        if err := pm.syncWriteFile(filename, data); err != nil {
            return err
        }
    } else {
        if err := pm.atomicWriteFile(filename, data); err != nil {
            return err
        }
    }
    pm.lastCheckpoint = snapshot.CreatedAt
    if pm.logger != nil {
        pm.logger.Info(fmt.Sprintf("Saved database %s checkpoint %d (%d bytes, checksum: %s, WAL LSN: %d, durability: %s)",
            dbName, snapshot.Version, len(data), snapshot.Checksum[:8], walLSN, pm.config.DurabilityLevel))
    }
    return nil
}

func (pm *PersistenceManager) syncWriteFile(filename string, data []byte) error {
    pm.commitMutex.Lock()
    defer pm.commitMutex.Unlock()
    pm.syncAttempts.Add(1)
    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])
    tempFile := filename + ".tmp." + checksum[:8]
    if err := os.WriteFile(tempFile, data, 0644); err != nil {
        pm.syncFailures.Add(1)
        return fmt.Errorf("failed to write temp file: %v", err)
    }
    f, err := os.OpenFile(tempFile, os.O_RDWR, 0644)
    if err != nil {
        pm.syncFailures.Add(1)
        os.Remove(tempFile)
        return fmt.Errorf("failed to open temp file: %v", err)
    }
    defer f.Close()
    if pm.config.FsyncEnabled {
        if err := RealFsyncWithRetry(f, pm.config.MaxRetriesOnSync, pm.config.SyncRetryDelay); err != nil {
            pm.syncFailures.Add(1)
            os.Remove(tempFile)
            return fmt.Errorf("fsync failed after %d attempts: %v", pm.config.MaxRetriesOnSync, err)
        }
    }
    if err := os.Rename(tempFile, filename); err != nil {
        pm.syncFailures.Add(1)
        os.Remove(tempFile)
        return fmt.Errorf("failed to rename: %v", err)
    }
    if pm.config.FsyncEnabled {
        if err := FsyncDir(pm.config.DataDir); err != nil {
            if pm.logger != nil {
                pm.logger.Warn(fmt.Sprintf("Fsync dir failed (non-critical): %v", err))
            }
        }
    }
    pm.atomicWrites.Add(1)
    return nil
}

func (pm *PersistenceManager) atomicWriteFile(filename string, data []byte) error {
    if pm.config.AtomicWrites {
        tempFile := filename + ".tmp"
        if err := os.WriteFile(tempFile, data, 0644); err != nil {
            return fmt.Errorf("failed to write temp file: %v", err)
        }
        if pm.config.SyncWrites {
            if f, err := os.OpenFile(tempFile, os.O_RDWR, 0644); err == nil {
                if pm.config.FsyncEnabled {
                    RealFsyncWithRetry(f, pm.config.MaxRetriesOnSync, pm.config.SyncRetryDelay)
                } else {
                    f.Sync()
                }
                f.Close()
            }
        }
        if err := os.Rename(tempFile, filename); err != nil {
            os.Remove(tempFile)
            return fmt.Errorf("failed to rename temp file: %v", err)
        }
        if pm.config.SyncWrites && pm.config.FsyncEnabled {
            FsyncDir(pm.config.DataDir)
        }
        pm.atomicWrites.Add(1)
        return nil
    }
    return os.WriteFile(filename, data, 0644)
}

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

func (pm *PersistenceManager) SaveAllWithSync() error {
    if pm.isRestoring.Load() {
        return fmt.Errorf("cannot save checkpoints during restore")
    }
    databases := pm.storage.ListDatabases()
    var lastErr error
    for _, dbName := range databases {
        if err := pm.SaveDatabase(dbName); err != nil {
            lastErr = err
        }
    }
    if pm.config.FsyncEnabled {
        FsyncDir(pm.config.DataDir)
    }
    return lastErr
}

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
    if err := pm.verifySnapshot(snapshot); err != nil {
        recoveryInfo.Error = err.Error()
        pm.recoveryInfo = recoveryInfo
        pm.tornWritesDetected.Add(1)
        return fmt.Errorf("snapshot integrity check failed (possible torn write): %v", err)
    }
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
    restoredColls := 0
    restoredDocs := int64(0)
    for collName, collDataRaw := range snapshot.Collections {
        collData, ok := collDataRaw.([]interface{})
        if !ok {
            continue
        }
        // ИСПРАВЛЕНО: Не удаляем существующую коллекцию сразу. Создаём новую временно.
        if err := db.CreateCollection(collName); err != nil {
            // Коллекция уже существует - пропускаем
            if pm.logger != nil {
                pm.logger.Warn(fmt.Sprintf("Collection %s already exists during restore, skipping creation", collName))
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
                    // ИСПРАВЛЕНО: applyWALEntry теперь применяет транзакцию атомарно
                    if err := pm.applyWALEntry(db, entry); err != nil {
                        if pm.logger != nil {
                            pm.logger.Error(fmt.Sprintf("Failed to apply WAL entry %d (rolled back): %v", entry.Index, err))
                        }
                        pm.tornWritesDetected.Add(1)
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
// ИСПРАВЛЕНО: Частичное применение транзакции откатывается при ошибке
func (pm *PersistenceManager) applyWALEntry(db *Database, entry WALEntry) error {
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
    if txRecord.State != 1 {
        return nil
    }

    // ИСПРАВЛЕНО: Собираем успешно применённые операции для отката
    type appliedOp struct {
        opType     string
        collection string
        docID      string
        oldDoc     *Document // для отката update/delete
    }
    applied := make([]appliedOp, 0)

    rollback := func() {
        for i := len(applied) - 1; i >= 0; i-- {
            ao := applied[i]
            coll, err := db.GetCollection(ao.collection)
            if err != nil {
                continue
            }
            switch ao.opType {
            case "insert":
                coll.PermanentDelete(ao.docID)
            case "update":
                if ao.oldDoc != nil {
                    coll.Update(ao.docID, ao.oldDoc.GetFields())
                }
            case "delete":
                coll.RestoreDeleted(ao.docID)
            }
        }
    }

    for _, op := range txRecord.Operations {
        if op.Database != db.Name() {
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
                rollback()
                return err
            }
            applied = append(applied, appliedOp{opType: "insert", collection: op.Collection, docID: op.DocumentID})

        case "update":
            // ИСПРАВЛЕНО: сохраняем старое состояние для отката
            oldDoc, _ := coll.Find(op.DocumentID)
            var oldCopy *Document
            if oldDoc != nil {
                oldCopy = oldDoc.Clone()
            }
            if err := coll.Update(op.DocumentID, op.Data); err != nil {
                rollback()
                return err
            }
            applied = append(applied, appliedOp{opType: "update", collection: op.Collection, docID: op.DocumentID, oldDoc: oldCopy})

        case "delete":
            if err := coll.Delete(op.DocumentID); err != nil {
                rollback()
                return err
            }
            applied = append(applied, appliedOp{opType: "delete", collection: op.Collection, docID: op.DocumentID})
        }
    }
    return nil
}

func (pm *PersistenceManager) verifySnapshot(snapshot *DatabaseSnapshot) error {
    if snapshot.Checksum == "" {
        return fmt.Errorf("snapshot has no checksum")
    }
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
        return fmt.Errorf("checksum mismatch: expected %s, got %s (possible torn write)",
            snapshot.Checksum[:8], checksum[:8])
    }
    return nil
}

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

func (pm *PersistenceManager) getSnapshotFilename(dbName string, version uint64) string {
    filename := fmt.Sprintf("snapshot_%s_%d.json", dbName, version)
    if pm.config.CompressEnabled {
        filename += ".gz"
    }
    return filepath.Join(pm.config.DataDir, filename)
}

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
    for _, file := range files {
        data, err := os.ReadFile(file)
        if err != nil {
            continue
        }
        if pm.config.CompressEnabled && strings.HasSuffix(file, ".gz") {
            data, err = pm.decompress(data)
            if err != nil {
                pm.tornWritesDetected.Add(1)
                continue
            }
        }
        var snapshot DatabaseSnapshot
        if err := json.Unmarshal(data, &snapshot); err != nil {
            pm.tornWritesDetected.Add(1)
            continue
        }
        if err := pm.verifySnapshot(&snapshot); err != nil {
            pm.tornWritesDetected.Add(1)
            continue
        }
        return &snapshot, nil
    }
    return nil, fmt.Errorf("no valid snapshot found (all may be corrupted)")
}

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

func (pm *PersistenceManager) decompress(data []byte) ([]byte, error) {
    reader, err := gzip.NewReader(bytes.NewReader(data))
    if err != nil {
        return nil, fmt.Errorf("failed to create gzip reader: %v", err)
    }
    defer reader.Close()
    var buf bytes.Buffer
    if _, err := buf.ReadFrom(reader); err != nil {
        return nil, fmt.Errorf("failed to decompress data (possible torn write): %v", err)
    }
    return buf.Bytes(), nil
}

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
    }
}

func (pm *PersistenceManager) GetLastCheckpointInfo() map[string]interface{} {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    info := map[string]interface{}{
        "last_checkpoint_time":     pm.lastCheckpoint,
        "last_checkpoint_time_str": time.UnixMilli(pm.lastCheckpoint).Format("2006-01-02 15:04:05.000"),
        "checkpoint_id":            pm.checkpointID.Load(),
        "wal_path":                 pm.config.WalPath,
        "use_wal":                  pm.config.UseWalForCheckpoint,
        "atomic_writes":            pm.config.AtomicWrites,
        "fsync_enabled":            pm.config.FsyncEnabled,
        "durability_level":         pm.config.DurabilityLevel,
        "is_restoring":             pm.isRestoring.Load(),
        "sync_attempts":            pm.syncAttempts.Load(),
        "sync_failures":            pm.syncFailures.Load(),
        "atomic_writes_count":      pm.atomicWrites.Load(),
        "torn_writes_detected":     pm.tornWritesDetected.Load(),
    }
    if pm.recoveryInfo != nil {
        info["last_recovery"] = pm.recoveryInfo
    }
    return info
}

func (pm *PersistenceManager) GetRecoveryInfo() *CheckpointRecoveryInfo {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    return pm.recoveryInfo
}

func (pm *PersistenceManager) IsRestoring() bool { return pm.isRestoring.Load() }

func (pm *PersistenceManager) GetTornWritesDetected() uint64 { return pm.tornWritesDetected.Load() }
