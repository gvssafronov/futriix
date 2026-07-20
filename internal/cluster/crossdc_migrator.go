/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/crossdc_migrator.go
// Назначение: Кросс-датацентровая миграция данных
// Алгоритм: Асинхронная репликация с CDC (Change Data Capture) и очередью изменений

package cluster

import (
    "bytes"
    "compress/gzip"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "path/filepath"
    "sort"
    "sync"
    "sync/atomic"
    "time"
    
    "futriis/internal/config"
    "futriis/internal/log"
    "futriis/internal/storage"
)

// =============================================================================
// ТИПЫ ДАННЫХ ДЛЯ МИГРАЦИИ
// =============================================================================

// MigrationStatus представляет статус миграции
type MigrationStatus string

const (
    MigrationStatusIdle       MigrationStatus = "idle"
    MigrationStatusPreparing  MigrationStatus = "preparing"
    MigrationStatusMigrating  MigrationStatus = "migrating"
    MigrationStatusDeltaSync  MigrationStatus = "delta_sync"
    MigrationStatusValidating MigrationStatus = "validating"
    MigrationStatusCompleted  MigrationStatus = "completed"
    MigrationStatusFailed     MigrationStatus = "failed"
    MigrationStatusPaused     MigrationStatus = "paused"
)

// MigrationMode представляет режим миграции
type MigrationMode string

const (
    MigrationModeManual    MigrationMode = "manual"      // Полностью ручной
    MigrationModeSemiAuto  MigrationMode = "semi_auto"   // Полуавтоматический
    MigrationModeAuto      MigrationMode = "auto"        // Полностью автоматический
)

// ChangeType представляет тип изменения
type ChangeType string

const (
    ChangeInsert ChangeType = "insert"
    ChangeUpdate ChangeType = "update"
    ChangeDelete ChangeType = "delete"
)

// ChangeRecord представляет запись об изменении
type ChangeRecord struct {
    ID            string                 `json:"id"`
    Database      string                 `json:"database"`
    Collection    string                 `json:"collection"`
    DocumentID    string                 `json:"document_id"`
    ChangeType    ChangeType             `json:"change_type"`
    Document      map[string]interface{} `json:"document,omitempty"`
    PreviousDoc   map[string]interface{} `json:"previous_doc,omitempty"`
    Timestamp     int64                  `json:"timestamp"`
    Version       uint64                 `json:"version"`
    Checksum      string                 `json:"checksum"`
    Applied       bool                   `json:"applied"`
    AppliedAt     int64                  `json:"applied_at,omitempty"`
    LSN           uint64                 `json:"lsn,omitempty"`
}

// MigrationTask представляет задачу миграции
type MigrationTask struct {
    ID              string                 `json:"id"`
    SourceDC        string                 `json:"source_dc"`
    TargetDC        string                 `json:"target_dc"`
    Databases       []string               `json:"databases"`
    Collections     map[string][]string    `json:"collections"`      // database -> collections
    Status          MigrationStatus        `json:"status"`
    ProgressPercent float64                `json:"progress_percent"`
    TotalDocuments  int64                  `json:"total_documents"`
    MigratedDocs    int64                  `json:"migrated_docs"`
    FailedDocs      int64                  `json:"failed_docs"`
    SkippedDocs     int64                  `json:"skipped_docs"`
    StartTime       int64                  `json:"start_time"`
    EndTime         int64                  `json:"end_time"`
    LastCheckpoint  int64                  `json:"last_checkpoint"`
    CheckpointData  map[string]interface{} `json:"checkpoint_data"`
    Error           string                 `json:"error,omitempty"`
    CreatedAt       int64                  `json:"created_at"`
    UpdatedAt       int64                  `json:"updated_at"`
    mu              sync.RWMutex           `json:"-"`
}

// MigrationStats представляет статистику миграции
type MigrationStats struct {
    TotalChanges    int64 `json:"total_changes"`
    AppliedChanges  int64 `json:"applied_changes"`
    FailedChanges   int64 `json:"failed_changes"`
    SkippedChanges  int64 `json:"skipped_changes"`
    LatencyAvg      int64 `json:"latency_avg_ms"`
    LatencyMax      int64 `json:"latency_max_ms"`
    Throughput      int64 `json:"throughput_docs_per_sec"`
    mu              sync.RWMutex
}

// MigrationCheckpoint представляет чекпоинт миграции
type MigrationCheckpoint struct {
    TaskID          string                 `json:"task_id"`
    LastLSN         uint64                 `json:"last_lsn"`
    ProcessedDocs   map[string]bool        `json:"processed_docs"`
    Timestamp       int64                  `json:"timestamp"`
    Version         uint64                 `json:"version"`
}

// =============================================================================
// CHANGE QUEUE - ОЧЕРЕДЬ ИЗМЕНЕНИЙ С ПЕРСИСТЕНТНОСТЬЮ
// =============================================================================

// ChangeQueue представляет очередь изменений для миграции
type ChangeQueue struct {
    changes   []*ChangeRecord
    mu        sync.RWMutex
    maxSize   int
    persisted bool
    filePath  string
    lastLSN   atomic.Uint64
}

// NewChangeQueue создаёт новую очередь изменений
func NewChangeQueue(maxSize int, filePath string) *ChangeQueue {
    q := &ChangeQueue{
        changes:   make([]*ChangeRecord, 0, maxSize),
        maxSize:   maxSize,
        filePath:  filePath,
        persisted: false,
    }
    
    // Загружаем сохранённую очередь
    if filePath != "" {
        q.load()
    }
    
    return q
}

// load загружает очередь из файла
func (cq *ChangeQueue) load() {
    data, err := os.ReadFile(cq.filePath)
    if err != nil {
        return
    }
    
    var records []*ChangeRecord
    if err := json.Unmarshal(data, &records); err != nil {
        return
    }
    
    cq.mu.Lock()
    defer cq.mu.Unlock()
    cq.changes = records
    
    // Восстанавливаем последний LSN
    for _, r := range records {
        if r.LSN > cq.lastLSN.Load() {
            cq.lastLSN.Store(r.LSN)
        }
    }
}

// save сохраняет очередь в файл
func (cq *ChangeQueue) save() {
    if cq.filePath == "" {
        return
    }
    
    cq.mu.RLock()
    data, err := json.Marshal(cq.changes)
    cq.mu.RUnlock()
    
    if err != nil {
        return
    }
    
    os.WriteFile(cq.filePath, data, 0644)
}

// Add добавляет изменение в очередь
func (cq *ChangeQueue) Add(change *ChangeRecord) bool {
    cq.mu.Lock()
    defer cq.mu.Unlock()
    
    if len(cq.changes) >= cq.maxSize {
        // Удаляем половину старых записей
        cq.changes = cq.changes[len(cq.changes)/2:]
    }
    
    // Устанавливаем LSN
    change.LSN = cq.lastLSN.Add(1)
    
    cq.changes = append(cq.changes, change)
    
    // Асинхронное сохранение
    if cq.filePath != "" {
        go cq.save()
    }
    
    return true
}

// GetAll возвращает все изменения и очищает очередь
func (cq *ChangeQueue) GetAll() []*ChangeRecord {
    cq.mu.Lock()
    defer cq.mu.Unlock()
    
    changes := cq.changes
    cq.changes = make([]*ChangeRecord, 0, cq.maxSize)
    
    if cq.filePath != "" {
        os.WriteFile(cq.filePath, []byte("[]"), 0644)
    }
    
    return changes
}

// GetChangesSince возвращает изменения после указанного LSN
func (cq *ChangeQueue) GetChangesSince(lsn uint64) []*ChangeRecord {
    cq.mu.RLock()
    defer cq.mu.RUnlock()
    
    result := make([]*ChangeRecord, 0)
    for _, change := range cq.changes {
        if change.LSN > lsn {
            result = append(result, change)
        }
    }
    return result
}

// GetChangesAfter возвращает изменения после указанного времени
func (cq *ChangeQueue) GetChangesAfter(timestamp int64) []*ChangeRecord {
    cq.mu.RLock()
    defer cq.mu.RUnlock()
    
    result := make([]*ChangeRecord, 0)
    for _, change := range cq.changes {
        if change.Timestamp > timestamp {
            result = append(result, change)
        }
    }
    return result
}

// GetLastLSN возвращает последний LSN
func (cq *ChangeQueue) GetLastLSN() uint64 {
    return cq.lastLSN.Load()
}

// Clear очищает очередь
func (cq *ChangeQueue) Clear() {
    cq.mu.Lock()
    defer cq.mu.Unlock()
    
    cq.changes = make([]*ChangeRecord, 0, cq.maxSize)
    if cq.filePath != "" {
        os.WriteFile(cq.filePath, []byte("[]"), 0644)
    }
}

// Size возвращает размер очереди
func (cq *ChangeQueue) Size() int {
    cq.mu.RLock()
    defer cq.mu.RUnlock()
    return len(cq.changes)
}

// =============================================================================
// ОСНОВНАЯ СТРУКТУРА МИГРАТОРА
// =============================================================================

// CrossDCMigrator управляет кросс-датацентровой миграцией данных
type CrossDCMigrator struct {
    config          *config.MigrationConfig
    store           *storage.Storage
    logger          *log.Logger
    httpClient      *http.Client
    mu              sync.RWMutex
    tasks           map[string]*MigrationTask
    stats           *MigrationStats
    changeQueue     *ChangeQueue
    active          atomic.Bool
    stopChan        chan struct{}
    wg              sync.WaitGroup
    mode            MigrationMode
    currentTaskID   string
    checkpointPath  string
    resumedFrom     *MigrationTask
    deltaTicker     *time.Ticker
    changeSubscribers []chan *ChangeRecord
    subscriberMu    sync.RWMutex
}

// NewCrossDCMigrator создаёт новый экземпляр мигратора
func NewCrossDCMigrator(cfg *config.MigrationConfig, store *storage.Storage, logger *log.Logger) *CrossDCMigrator {
    if cfg == nil {
        cfg = &config.MigrationConfig{
            Enabled: false,
            Mode:    "semi_auto",
            Source: &config.DatacenterConfig{
                Name:       "dc-primary",
                Endpoint:   "localhost:8080",
                TimeoutSec: 30,
            },
            Target: &config.DatacenterConfig{
                Name:       "dc-secondary",
                Endpoint:   "localhost:8081",
                TimeoutSec: 30,
            },
            Settings: &config.MigrationSettings{
                BatchSize:            1000,
                Workers:              4,
                Compression:          "snappy",
                ResumeEnabled:        true,
                CheckpointIntervalSec: 30,
                MaxRetries:           3,
                RetryBackoffSec:      5,
            },
            Delta: &config.DeltaSyncConfig{
                Enabled:      true,
                IntervalSec:  60,
                MaxLagSec:    300,
            },
            Validation: &config.ValidationConfig{
                Enabled:       true,
                SamplePercent: 10,
                MaxErrors:     100,
            },
        }
    }

    cdm := &CrossDCMigrator{
        config:         cfg,
        store:          store,
        logger:         logger,
        tasks:          make(map[string]*MigrationTask),
        stats:          &MigrationStats{},
        changeQueue:    NewChangeQueue(100000, "futriis/migration/change_queue.json"),
        stopChan:       make(chan struct{}),
        mode:           MigrationMode(cfg.Mode),
        checkpointPath: "futriis/migration/checkpoints",
        changeSubscribers: make([]chan *ChangeRecord, 0),
        httpClient: &http.Client{
            Timeout: time.Duration(cfg.Source.TimeoutSec) * time.Second,
        },
    }

    // Создаём директорию для чекпоинтов
    os.MkdirAll(cdm.checkpointPath, 0755)
    os.MkdirAll("futriis/migration", 0755)

    // Загружаем сохранённые задачи
    cdm.loadTasks()

    return cdm
}

// Start запускает мигратор
func (cdm *CrossDCMigrator) Start() {
    if !cdm.active.CompareAndSwap(false, true) {
        return
    }

    cdm.logger.Info("Cross-datacenter migrator started")

    // Восстанавливаем незавершённые миграции
    cdm.resumePendingMigrations()

    // Запускаем обработку изменений
    cdm.wg.Add(1)
    go cdm.processChanges()

    // Запускаем дельта-синхронизацию если включена
    if cdm.config.Delta.Enabled {
        cdm.wg.Add(1)
        go cdm.deltaSyncLoop()
    }

    // Запускаем сохранение чекпоинтов
    cdm.wg.Add(1)
    go cdm.checkpointLoop()
}

// Stop останавливает мигратор
func (cdm *CrossDCMigrator) Stop() {
    if !cdm.active.Load() {
        return
    }

    close(cdm.stopChan)
    cdm.wg.Wait()
    cdm.active.Store(false)

    cdm.saveTasks()
    cdm.logger.Info("Cross-datacenter migrator stopped")
}

// =============================================================================
// ОСНОВНЫЕ МЕТОДЫ МИГРАЦИИ
// =============================================================================

// StartMigration начинает миграцию данных
func (cdm *CrossDCMigrator) StartMigration(sourceDC, targetDC string, databases []string, collections map[string][]string) (*MigrationTask, error) {
    if !cdm.config.Enabled {
        return nil, fmt.Errorf("migration is disabled in configuration")
    }

    cdm.mu.Lock()
    defer cdm.mu.Unlock()

    // Проверяем, есть ли активная миграция
    if cdm.currentTaskID != "" {
        if task, ok := cdm.tasks[cdm.currentTaskID]; ok {
            task.mu.RLock()
            status := task.Status
            task.mu.RUnlock()
            if status == MigrationStatusMigrating || status == MigrationStatusDeltaSync {
                return nil, fmt.Errorf("a migration is already in progress")
            }
        }
    }

    taskID := fmt.Sprintf("mig_%d_%s", time.Now().UnixNano(), sourceDC)

    task := &MigrationTask{
        ID:              taskID,
        SourceDC:        sourceDC,
        TargetDC:        targetDC,
        Databases:       databases,
        Collections:     collections,
        Status:          MigrationStatusPreparing,
        ProgressPercent: 0,
        StartTime:       time.Now().UnixMilli(),
        CheckpointData:  make(map[string]interface{}),
        CreatedAt:       time.Now().UnixMilli(),
        UpdatedAt:       time.Now().UnixMilli(),
    }

    cdm.tasks[taskID] = task
    cdm.currentTaskID = taskID

    cdm.logger.Info(fmt.Sprintf("Migration task %s created: %s -> %s", taskID, sourceDC, targetDC))

    // Запускаем миграцию асинхронно
    cdm.wg.Add(1)
    go cdm.runMigration(task)

    return task, nil
}

// runMigration выполняет миграцию
func (cdm *CrossDCMigrator) runMigration(task *MigrationTask) {
    defer cdm.wg.Done()
    defer func() {
        if r := recover(); r != nil {
            cdm.logger.Error(fmt.Sprintf("Migration %s panicked: %v", task.ID, r))
            task.mu.Lock()
            task.Status = MigrationStatusFailed
            task.Error = fmt.Sprintf("panic: %v", r)
            task.UpdatedAt = time.Now().UnixMilli()
            task.mu.Unlock()
        }
    }()

    cdm.logger.Info(fmt.Sprintf("Starting migration %s", task.ID))

    // 1. Подготовка - сбор метаданных
    if err := cdm.prepareMigration(task); err != nil {
        cdm.setTaskError(task, err)
        return
    }

    // 2. Основная миграция данных
    if err := cdm.migrateData(task); err != nil {
        cdm.setTaskError(task, err)
        return
    }

    // 3. Дельта-синхронизация (если включена)
    if cdm.config.Delta.Enabled && cdm.mode != MigrationModeManual {
        task.mu.Lock()
        task.Status = MigrationStatusDeltaSync
        task.UpdatedAt = time.Now().UnixMilli()
        task.mu.Unlock()

        if err := cdm.runDeltaSync(task); err != nil {
            cdm.logger.Warn(fmt.Sprintf("Delta sync failed: %v", err))
            // Не считаем это фатальной ошибкой
        }
    }

    // 4. Валидация данных
    if cdm.config.Validation.Enabled {
        task.mu.Lock()
        task.Status = MigrationStatusValidating
        task.UpdatedAt = time.Now().UnixMilli()
        task.mu.Unlock()

        if err := cdm.validateMigration(task); err != nil {
            cdm.logger.Warn(fmt.Sprintf("Validation failed: %v", err))
            // Продолжаем, но отмечаем проблемы
        }
    }

    // 5. Завершение
    task.mu.Lock()
    task.Status = MigrationStatusCompleted
    task.EndTime = time.Now().UnixMilli()
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Info(fmt.Sprintf("Migration %s completed successfully", task.ID))
}

// prepareMigration подготавливает миграцию
func (cdm *CrossDCMigrator) prepareMigration(task *MigrationTask) error {
    cdm.logger.Debug(fmt.Sprintf("Preparing migration %s", task.ID))

    // Получаем список баз данных
    databases := task.Databases
    if len(databases) == 0 {
        databases = cdm.store.ListDatabases()
    }

    totalDocs := int64(0)
    for _, dbName := range databases {
        db, err := cdm.store.GetDatabase(dbName)
        if err != nil {
            continue
        }

        collections := task.Collections[dbName]
        if len(collections) == 0 {
            collections = db.ListCollections()
        }

        for _, collName := range collections {
            coll, err := db.GetCollection(collName)
            if err != nil {
                continue
            }
            totalDocs += coll.Count()
        }
    }

    task.mu.Lock()
    task.TotalDocuments = totalDocs
    task.Status = MigrationStatusMigrating
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Info(fmt.Sprintf("Prepared migration %s: %d documents to migrate", task.ID, totalDocs))
    return nil
}

// migrateData выполняет основную миграцию данных
func (cdm *CrossDCMigrator) migrateData(task *MigrationTask) error {
    cdm.logger.Debug(fmt.Sprintf("Migrating data for task %s", task.ID))

    databases := task.Databases
    if len(databases) == 0 {
        databases = cdm.store.ListDatabases()
    }

    // Проверяем наличие чекпоинта для возобновления
    var checkpoint *MigrationCheckpoint
    if cdm.config.Settings.ResumeEnabled {
        checkpoint = cdm.loadCheckpoint(task.ID)
        if checkpoint != nil {
            // Преобразуем map[string]bool в map[string]interface{} для совместимости
            checkpointData := make(map[string]interface{})
            for k, v := range checkpoint.ProcessedDocs {
                checkpointData[k] = v
            }
            task.mu.Lock()
            task.CheckpointData = checkpointData
            task.mu.Unlock()
            cdm.logger.Info(fmt.Sprintf("Resuming migration %s from checkpoint (LSN: %d)", task.ID, checkpoint.LastLSN))
        }
    }

    for _, dbName := range databases {
        // Проверяем остановку
        if !cdm.active.Load() {
            return fmt.Errorf("migration stopped")
        }

        db, err := cdm.store.GetDatabase(dbName)
        if err != nil {
            cdm.logger.Warn(fmt.Sprintf("Database %s not found: %v", dbName, err))
            continue
        }

        collections := task.Collections[dbName]
        if len(collections) == 0 {
            collections = db.ListCollections()
        }

        // Проверяем исключения
        excludeMap := make(map[string]bool)
        for _, excl := range cdm.config.Settings.ExcludeCollections {
            excludeMap[excl] = true
        }

        for _, collName := range collections {
            if excludeMap[collName] {
                continue
            }

            // Проверяем остановку
            if !cdm.active.Load() {
                return fmt.Errorf("migration stopped")
            }

            // Проверяем, была ли коллекция уже обработана (по чекпоинту)
            checkpointKey := fmt.Sprintf("%s:%s", dbName, collName)
            if checkpoint != nil {
                if done, ok := checkpoint.ProcessedDocs[checkpointKey]; ok && done {
                    cdm.logger.Debug(fmt.Sprintf("Collection %s.%s already migrated, skipping", dbName, collName))
                    continue
                }
            }

            if err := cdm.migrateCollection(task, dbName, collName, checkpoint); err != nil {
                cdm.logger.Error(fmt.Sprintf("Failed to migrate collection %s.%s: %v", dbName, collName, err))
                // Продолжаем с другими коллекциями
            }

            // Сохраняем чекпоинт для коллекции
            if cdm.config.Settings.ResumeEnabled {
                if checkpoint == nil {
                    checkpoint = &MigrationCheckpoint{
                        TaskID:        task.ID,
                        ProcessedDocs: make(map[string]bool),
                        Timestamp:     time.Now().UnixMilli(),
                        Version:       1,
                    }
                }
                checkpoint.ProcessedDocs[checkpointKey] = true
                checkpoint.Timestamp = time.Now().UnixMilli()
                checkpoint.Version++
                cdm.saveCheckpoint(task.ID, checkpoint)
                // Преобразуем для task.CheckpointData
                checkpointData := make(map[string]interface{})
                for k, v := range checkpoint.ProcessedDocs {
                    checkpointData[k] = v
                }
                task.mu.Lock()
                task.CheckpointData = checkpointData
                task.mu.Unlock()
            }
        }
    }

    return nil
}

// migrateCollection мигрирует одну коллекцию
func (cdm *CrossDCMigrator) migrateCollection(task *MigrationTask, dbName, collName string, checkpoint *MigrationCheckpoint) error {
    db, err := cdm.store.GetDatabase(dbName)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    // Получаем все документы
    docs := coll.GetAllDocuments()
    if len(docs) == 0 {
        return nil
    }

    batchSize := cdm.config.Settings.BatchSize
    workers := cdm.config.Settings.Workers

    // Создаём каналы для параллельной обработки
    docChan := make(chan *storage.Document, batchSize)
    resultChan := make(chan error, workers)

    // Запускаем воркеры
    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for doc := range docChan {
                err := cdm.sendDocumentToTarget(task, dbName, collName, doc)
                if err != nil {
                    resultChan <- err
                    task.mu.Lock()
                    task.FailedDocs++
                    task.mu.Unlock()
                } else {
                    task.mu.Lock()
                    task.MigratedDocs++
                    // Обновляем прогресс
                    if task.TotalDocuments > 0 {
                        task.ProgressPercent = float64(task.MigratedDocs) / float64(task.TotalDocuments) * 100
                    }
                    task.UpdatedAt = time.Now().UnixMilli()
                    task.mu.Unlock()
                }
            }
        }()
    }

    // Отправляем документы в канал
    sent := 0
    for _, doc := range docs {
        // Проверяем остановку
        if !cdm.active.Load() {
            close(docChan)
            wg.Wait()
            return fmt.Errorf("migration stopped")
        }

        // Проверяем, был ли документ уже обработан (по чекпоинту)
        if checkpoint != nil {
            if done, ok := checkpoint.ProcessedDocs[doc.ID]; ok && done {
                task.mu.Lock()
                task.SkippedDocs++
                task.mu.Unlock()
                continue
            }
        }

        docChan <- doc
        sent++

        // Сохраняем чекпоинт каждые N документов
        if sent%batchSize == 0 && cdm.config.Settings.ResumeEnabled {
            if checkpoint == nil {
                checkpoint = &MigrationCheckpoint{
                    TaskID:        task.ID,
                    ProcessedDocs: make(map[string]bool),
                    Timestamp:     time.Now().UnixMilli(),
                    Version:       1,
                }
            }
            checkpoint.ProcessedDocs[doc.ID] = true
            checkpoint.Timestamp = time.Now().UnixMilli()
            checkpoint.Version++
            cdm.saveCheckpoint(task.ID, checkpoint)
        }
    }

    close(docChan)
    wg.Wait()

    // Проверяем ошибки
    var lastErr error
    select {
    case err := <-resultChan:
        lastErr = err
    default:
    }

    // Обновляем статистику
    task.mu.Lock()
    task.ProgressPercent = 100.0
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    return lastErr
}

// sendDocumentToTarget отправляет документ на целевой датацентр
func (cdm *CrossDCMigrator) sendDocumentToTarget(task *MigrationTask, dbName, collName string, doc *storage.Document) error {
    // Формируем данные документа
    docData := doc.GetFields()
    
    // Добавляем метаданные
    docData["_meta"] = map[string]interface{}{
        "source_dc":      task.SourceDC,
        "source_db":      dbName,
        "source_collection": collName,
        "migration_id":   task.ID,
        "migrated_at":    time.Now().UnixMilli(),
    }

    // Создаём запись изменения
    change := &ChangeRecord{
        ID:         fmt.Sprintf("%s_%s_%s", task.ID, doc.ID, time.Now().Format("20060102150405")),
        Database:   dbName,
        Collection: collName,
        DocumentID: doc.ID,
        ChangeType: ChangeInsert,
        Document:   docData,
        Timestamp:  time.Now().UnixMilli(),
        Version:    doc.Version,
        Applied:    false,
    }

    // Вычисляем контрольную сумму
    data, _ := json.Marshal(docData)
    hash := sha256.Sum256(data)
    change.Checksum = hex.EncodeToString(hash[:])

    // Добавляем в очередь изменений
    cdm.changeQueue.Add(change)

    // Пытаемся отправить напрямую
    targetEndpoint := cdm.config.Target.Endpoint
    if targetEndpoint == "" {
        return fmt.Errorf("target endpoint not configured")
    }

    // Формируем запрос
    reqData := map[string]interface{}{
        "database":   dbName,
        "collection": collName,
        "document":   docData,
        "migration": map[string]interface{}{
            "id":     task.ID,
            "source": task.SourceDC,
        },
    }

    jsonData, err := json.Marshal(reqData)
    if err != nil {
        return err
    }

    // Сжимаем данные
    compressedData, err := cdm.compressData(jsonData)
    if err != nil {
        return err
    }

    // Отправляем HTTP запрос
    url := fmt.Sprintf("%s/api/migration/receive", targetEndpoint)
    req, err := http.NewRequest("POST", url, bytes.NewReader(compressedData))
    if err != nil {
        return err
    }

    req.Header.Set("Content-Type", "application/octet-stream")
    req.Header.Set("Content-Encoding", cdm.config.Settings.Compression)
    req.Header.Set("X-Migration-ID", task.ID)

    // Отправляем с повторными попытками
    var lastErr error
    for retry := 0; retry < cdm.config.Settings.MaxRetries; retry++ {
        resp, err := cdm.httpClient.Do(req)
        if err == nil {
            resp.Body.Close()
            if resp.StatusCode == http.StatusOK {
                change.Applied = true
                change.AppliedAt = time.Now().UnixMilli()
                return nil
            }
            lastErr = fmt.Errorf("status: %s", resp.Status)
        } else {
            lastErr = err
        }

        time.Sleep(time.Duration(cdm.config.Settings.RetryBackoffSec) * time.Second)
    }

    return lastErr
}

// =============================================================================
// ДЕЛЬТА-СИНХРОНИЗАЦИЯ
// =============================================================================

// runDeltaSync выполняет дельта-синхронизацию
func (cdm *CrossDCMigrator) runDeltaSync(task *MigrationTask) error {
    cdm.logger.Info(fmt.Sprintf("Starting delta sync for migration %s", task.ID))

    // Получаем изменения после последнего чекпоинта
    since := task.LastCheckpoint
    if since == 0 {
        since = task.StartTime
    }

    // Собираем изменения из очереди
    changes := cdm.changeQueue.GetChangesAfter(since)

    batchSize := cdm.config.Settings.BatchSize
    for i := 0; i < len(changes); i += batchSize {
        end := i + batchSize
        if end > len(changes) {
            end = len(changes)
        }

        batch := changes[i:end]
        if err := cdm.applyChangeBatch(task, batch); err != nil {
            cdm.logger.Error(fmt.Sprintf("Failed to apply change batch: %v", err))
            return err
        }

        // Обновляем прогресс
        task.mu.Lock()
        task.MigratedDocs += int64(len(batch))
        task.UpdatedAt = time.Now().UnixMilli()
        task.mu.Unlock()
    }

    task.mu.Lock()
    task.LastCheckpoint = time.Now().UnixMilli()
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Info(fmt.Sprintf("Delta sync completed for migration %s: %d changes applied", task.ID, len(changes)))
    return nil
}

// applyChangeBatch применяет пакет изменений
func (cdm *CrossDCMigrator) applyChangeBatch(task *MigrationTask, changes []*ChangeRecord) error {
    for _, change := range changes {
        if !cdm.active.Load() {
            return fmt.Errorf("migration stopped")
        }

        // Применяем изменение в целевом датацентре
        if err := cdm.applyChange(task, change); err != nil {
            cdm.logger.Error(fmt.Sprintf("Failed to apply change %s: %v", change.ID, err))
            continue
        }
    }
    return nil
}

// applyChange применяет одно изменение
func (cdm *CrossDCMigrator) applyChange(task *MigrationTask, change *ChangeRecord) error {
    // Формируем запрос к целевому датацентру
    reqData := map[string]interface{}{
        "database":      change.Database,
        "collection":    change.Collection,
        "document_id":   change.DocumentID,
        "change_type":   string(change.ChangeType),
        "document":      change.Document,
        "previous_doc":  change.PreviousDoc,
        "migration_id":  task.ID,
        "timestamp":     change.Timestamp,
    }

    jsonData, err := json.Marshal(reqData)
    if err != nil {
        return err
    }

    compressedData, err := cdm.compressData(jsonData)
    if err != nil {
        return err
    }

    url := fmt.Sprintf("%s/api/migration/apply", cdm.config.Target.Endpoint)
    req, err := http.NewRequest("POST", url, bytes.NewReader(compressedData))
    if err != nil {
        return err
    }

    req.Header.Set("Content-Type", "application/octet-stream")
    req.Header.Set("Content-Encoding", cdm.config.Settings.Compression)

    var lastErr error
    for retry := 0; retry < cdm.config.Settings.MaxRetries; retry++ {
        resp, err := cdm.httpClient.Do(req)
        if err == nil {
            resp.Body.Close()
            if resp.StatusCode == http.StatusOK {
                change.Applied = true
                change.AppliedAt = time.Now().UnixMilli()
                cdm.stats.mu.Lock()
                cdm.stats.AppliedChanges++
                cdm.stats.mu.Unlock()
                return nil
            }
            lastErr = fmt.Errorf("status: %s", resp.Status)
        } else {
            lastErr = err
        }

        time.Sleep(time.Duration(cdm.config.Settings.RetryBackoffSec) * time.Second)
    }

    cdm.stats.mu.Lock()
    cdm.stats.FailedChanges++
    cdm.stats.mu.Unlock()

    return lastErr
}

// =============================================================================
// ВАЛИДАЦИЯ
// =============================================================================

// validateMigration выполняет валидацию мигрированных данных
func (cdm *CrossDCMigrator) validateMigration(task *MigrationTask) error {
    cdm.logger.Info(fmt.Sprintf("Validating migration %s", task.ID))

    if cdm.config.Validation.SamplePercent <= 0 {
        return nil
    }

    databases := task.Databases
    if len(databases) == 0 {
        databases = cdm.store.ListDatabases()
    }

    errors := 0

    for _, dbName := range databases {
        db, err := cdm.store.GetDatabase(dbName)
        if err != nil {
            continue
        }

        collections := task.Collections[dbName]
        if len(collections) == 0 {
            collections = db.ListCollections()
        }

        for _, collName := range collections {
            coll, err := db.GetCollection(collName)
            if err != nil {
                continue
            }

            docs := coll.GetAllDocuments()
            sampleSize := len(docs) * cdm.config.Validation.SamplePercent / 100
            if sampleSize < 1 {
                sampleSize = 1
            }

            for i := 0; i < sampleSize && i < len(docs); i++ {
                doc := docs[i]
                if err := cdm.validateDocument(task, dbName, collName, doc); err != nil {
                    errors++
                    if errors >= cdm.config.Validation.MaxErrors {
                        return fmt.Errorf("validation stopped: too many errors (%d)", errors)
                    }
                }
            }
        }
    }

    cdm.logger.Info(fmt.Sprintf("Validation completed for %s: %d errors", task.ID, errors))
    return nil
}

// validateDocument валидирует один документ
func (cdm *CrossDCMigrator) validateDocument(task *MigrationTask, dbName, collName string, doc *storage.Document) error {
    docData := doc.GetFields()
    data, _ := json.Marshal(docData)
    hash := sha256.Sum256(data)
    checksum := hex.EncodeToString(hash[:])

    // Запрашиваем документ из целевого датацентра
    url := fmt.Sprintf("%s/api/migration/validate?database=%s&collection=%s&document_id=%s",
        cdm.config.Target.Endpoint, dbName, collName, doc.ID)

    resp, err := cdm.httpClient.Get(url)
    if err != nil {
        return fmt.Errorf("validation request failed: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("target document not found: %s", doc.ID)
    }

    var result map[string]interface{}
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return fmt.Errorf("failed to decode validation response: %v", err)
    }

    targetChecksum, ok := result["checksum"].(string)
    if !ok {
        return fmt.Errorf("invalid checksum format")
    }

    if targetChecksum != checksum {
        return fmt.Errorf("checksum mismatch for %s: source=%s, target=%s", doc.ID, checksum, targetChecksum)
    }

    return nil
}

// =============================================================================
// ЧЕКПОИНТЫ
// =============================================================================

// saveCheckpoint сохраняет чекпоинт миграции
func (cdm *CrossDCMigrator) saveCheckpoint(taskID string, checkpoint *MigrationCheckpoint) {
    if !cdm.config.Settings.ResumeEnabled {
        return
    }

    cdm.mu.Lock()
    defer cdm.mu.Unlock()

    path := filepath.Join(cdm.checkpointPath, fmt.Sprintf("%s.json", taskID))
    
    checkpoint.Timestamp = time.Now().UnixMilli()
    checkpoint.Version++
    
    jsonData, err := json.MarshalIndent(checkpoint, "", "  ")
    if err != nil {
        cdm.logger.Warn(fmt.Sprintf("Failed to marshal checkpoint: %v", err))
        return
    }

    if err := os.WriteFile(path, jsonData, 0644); err != nil {
        cdm.logger.Warn(fmt.Sprintf("Failed to save checkpoint: %v", err))
        return
    }

    cdm.logger.Debug(fmt.Sprintf("Checkpoint saved for task %s (LSN: %d, docs: %d)", 
        taskID, checkpoint.LastLSN, len(checkpoint.ProcessedDocs)))
}

// loadCheckpoint загружает чекпоинт миграции
func (cdm *CrossDCMigrator) loadCheckpoint(taskID string) *MigrationCheckpoint {
    path := filepath.Join(cdm.checkpointPath, fmt.Sprintf("%s.json", taskID))
    
    data, err := os.ReadFile(path)
    if err != nil {
        if !os.IsNotExist(err) {
            cdm.logger.Warn(fmt.Sprintf("Failed to read checkpoint: %v", err))
        }
        return nil
    }

    var checkpoint MigrationCheckpoint
    if err := json.Unmarshal(data, &checkpoint); err != nil {
        cdm.logger.Warn(fmt.Sprintf("Failed to unmarshal checkpoint: %v", err))
        return nil
    }

    cdm.logger.Debug(fmt.Sprintf("Checkpoint loaded for task %s (LSN: %d, docs: %d)", 
        taskID, checkpoint.LastLSN, len(checkpoint.ProcessedDocs)))
    return &checkpoint
}

// =============================================================================
// ДЕЛЬТА-СИНХРОНИЗАЦИЯ В ЦИКЛЕ
// =============================================================================

// deltaSyncLoop выполняет периодическую дельта-синхронизацию
func (cdm *CrossDCMigrator) deltaSyncLoop() {
    defer cdm.wg.Done()

    interval := time.Duration(cdm.config.Delta.IntervalSec) * time.Second
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-cdm.stopChan:
            return
        case <-ticker.C:
            if !cdm.active.Load() {
                continue
            }
            cdm.runPeriodicDeltaSync()
        }
    }
}

// runPeriodicDeltaSync выполняет периодическую дельта-синхронизацию
func (cdm *CrossDCMigrator) runPeriodicDeltaSync() {
    cdm.mu.RLock()
    taskID := cdm.currentTaskID
    cdm.mu.RUnlock()

    if taskID == "" {
        return
    }

    task, ok := cdm.tasks[taskID]
    if !ok {
        return
    }

    task.mu.RLock()
    status := task.Status
    task.mu.RUnlock()

    if status != MigrationStatusCompleted && status != MigrationStatusDeltaSync {
        return
    }

    cdm.logger.Debug("Running periodic delta sync")
    
    if err := cdm.runDeltaSync(task); err != nil {
        cdm.logger.Warn(fmt.Sprintf("Periodic delta sync failed: %v", err))
    }
}

// =============================================================================
// ОБРАБОТКА ИЗМЕНЕНИЙ В ОЧЕРЕДИ
// =============================================================================

// processChanges обрабатывает изменения из очереди
func (cdm *CrossDCMigrator) processChanges() {
    defer cdm.wg.Done()

    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-cdm.stopChan:
            return
        case <-ticker.C:
            if !cdm.active.Load() {
                continue
            }
            cdm.flushChangeQueue()
        }
    }
}

// flushChangeQueue сбрасывает очередь изменений
func (cdm *CrossDCMigrator) flushChangeQueue() {
    changes := cdm.changeQueue.GetAll()
    if len(changes) == 0 {
        return
    }

    cdm.mu.RLock()
    taskID := cdm.currentTaskID
    cdm.mu.RUnlock()

    if taskID == "" {
        cdm.logger.Warn("No active migration, dropping changes")
        cdm.changeQueue.Clear()
        return
    }

    task, ok := cdm.tasks[taskID]
    if !ok {
        cdm.logger.Warn("Migration task not found, dropping changes")
        cdm.changeQueue.Clear()
        return
    }

    // Применяем изменения
    for _, change := range changes {
        if !cdm.active.Load() {
            return
        }

        if err := cdm.applyChange(task, change); err != nil {
            cdm.logger.Warn(fmt.Sprintf("Failed to apply change: %v", err))
        }
    }

    cdm.logger.Debug(fmt.Sprintf("Flushed %d changes to target", len(changes)))
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
// =============================================================================

// compressData сжимает данные
func (cdm *CrossDCMigrator) compressData(data []byte) ([]byte, error) {
    var buf bytes.Buffer
    
    switch cdm.config.Settings.Compression {
    case "gzip", "":
        w := gzip.NewWriter(&buf)
        defer w.Close()
        if _, err := w.Write(data); err != nil {
            return nil, err
        }
        if err := w.Flush(); err != nil {
            return nil, err
        }
    default:
        return data, nil
    }

    return buf.Bytes(), nil
}

// setTaskError устанавливает ошибку задачи
func (cdm *CrossDCMigrator) setTaskError(task *MigrationTask, err error) {
    task.mu.Lock()
    task.Status = MigrationStatusFailed
    task.Error = err.Error()
    task.EndTime = time.Now().UnixMilli()
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Error(fmt.Sprintf("Migration %s failed: %v", task.ID, err))
}

// loadTasks загружает сохранённые задачи
func (cdm *CrossDCMigrator) loadTasks() {
    path := "futriis/migration/tasks.json"
    data, err := os.ReadFile(path)
    if err != nil {
        return
    }

    var tasks map[string]*MigrationTask
    if err := json.Unmarshal(data, &tasks); err != nil {
        return
    }

    cdm.mu.Lock()
    for id, task := range tasks {
        cdm.tasks[id] = task
    }
    cdm.mu.Unlock()

    cdm.logger.Debug(fmt.Sprintf("Loaded %d migration tasks", len(tasks)))
}

// saveTasks сохраняет задачи
func (cdm *CrossDCMigrator) saveTasks() {
    cdm.mu.RLock()
    tasks := make(map[string]*MigrationTask)
    for id, task := range cdm.tasks {
        tasks[id] = task
    }
    cdm.mu.RUnlock()

    data, err := json.MarshalIndent(tasks, "", "  ")
    if err != nil {
        cdm.logger.Warn(fmt.Sprintf("Failed to marshal tasks: %v", err))
        return
    }

    if err := os.WriteFile("futriis/migration/tasks.json", data, 0644); err != nil {
        cdm.logger.Warn(fmt.Sprintf("Failed to save tasks: %v", err))
    }
}

// resumePendingMigrations восстанавливает незавершённые миграции
func (cdm *CrossDCMigrator) resumePendingMigrations() {
    cdm.mu.RLock()
    var pending []*MigrationTask
    for _, task := range cdm.tasks {
        task.mu.RLock()
        status := task.Status
        task.mu.RUnlock()
        if status == MigrationStatusMigrating || status == MigrationStatusDeltaSync {
            pending = append(pending, task)
        }
    }
    cdm.mu.RUnlock()

    for _, task := range pending {
        if cdm.config.Settings.ResumeEnabled {
            cdm.logger.Info(fmt.Sprintf("Resuming migration %s", task.ID))
            cdm.wg.Add(1)
            go cdm.runMigration(task)
        }
    }
}

// checkpointLoop периодически сохраняет чекпоинты
func (cdm *CrossDCMigrator) checkpointLoop() {
    defer cdm.wg.Done()

    interval := time.Duration(cdm.config.Settings.CheckpointIntervalSec) * time.Second
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-cdm.stopChan:
            return
        case <-ticker.C:
            if !cdm.active.Load() {
                continue
            }
            cdm.mu.RLock()
            taskID := cdm.currentTaskID
            cdm.mu.RUnlock()

            if taskID == "" {
                continue
            }

            task, ok := cdm.tasks[taskID]
            if !ok {
                continue
            }

            task.mu.RLock()
            status := task.Status
            checkpointData := task.CheckpointData
            task.mu.RUnlock()

            if status != MigrationStatusMigrating && status != MigrationStatusDeltaSync {
                continue
            }

            // Сохраняем чекпоинт
            checkpoint := &MigrationCheckpoint{
                TaskID:        taskID,
                LastLSN:       cdm.changeQueue.GetLastLSN(),
                ProcessedDocs: make(map[string]bool),
                Timestamp:     time.Now().UnixMilli(),
                Version:       1,
            }
            
            // Конвертируем checkpointData в map[string]bool
            for k, v := range checkpointData {
                if b, ok := v.(bool); ok {
                    checkpoint.ProcessedDocs[k] = b
                }
            }
            
            cdm.saveCheckpoint(taskID, checkpoint)
        }
    }
}

// =============================================================================
// ПУБЛИЧНЫЕ МЕТОДЫ ДЛЯ REPL
// =============================================================================

// GetMigrationStatus возвращает статус миграции
func (cdm *CrossDCMigrator) GetMigrationStatus(taskID string) (*MigrationTask, error) {
    cdm.mu.RLock()
    defer cdm.mu.RUnlock()

    task, ok := cdm.tasks[taskID]
    if !ok {
        return nil, fmt.Errorf("task %s not found", taskID)
    }

    return task, nil
}

// GetCurrentTaskID возвращает ID текущей задачи
func (cdm *CrossDCMigrator) GetCurrentTaskID() string {
    cdm.mu.RLock()
    defer cdm.mu.RUnlock()
    return cdm.currentTaskID
}

// ListTasks возвращает список всех задач
func (cdm *CrossDCMigrator) ListTasks() []*MigrationTask {
    cdm.mu.RLock()
    defer cdm.mu.RUnlock()

    tasks := make([]*MigrationTask, 0, len(cdm.tasks))
    for _, task := range cdm.tasks {
        tasks = append(tasks, task)
    }
    
    // Сортируем по времени создания (новые сверху)
    sort.Slice(tasks, func(i, j int) bool {
        return tasks[i].CreatedAt > tasks[j].CreatedAt
    })
    
    return tasks
}

// PauseMigration приостанавливает миграцию
func (cdm *CrossDCMigrator) PauseMigration(taskID string) error {
    cdm.mu.RLock()
    task, ok := cdm.tasks[taskID]
    cdm.mu.RUnlock()

    if !ok {
        return fmt.Errorf("task %s not found", taskID)
    }

    task.mu.Lock()
    if task.Status != MigrationStatusMigrating && task.Status != MigrationStatusDeltaSync {
        task.mu.Unlock()
        return fmt.Errorf("task %s is not in a running state", taskID)
    }
    task.Status = MigrationStatusPaused
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Info(fmt.Sprintf("Migration %s paused", taskID))
    return nil
}

// ResumeMigration возобновляет миграцию
func (cdm *CrossDCMigrator) ResumeMigration(taskID string) error {
    cdm.mu.RLock()
    task, ok := cdm.tasks[taskID]
    cdm.mu.RUnlock()

    if !ok {
        return fmt.Errorf("task %s not found", taskID)
    }

    task.mu.Lock()
    if task.Status != MigrationStatusPaused {
        task.mu.Unlock()
        return fmt.Errorf("task %s is not paused", taskID)
    }
    task.Status = MigrationStatusMigrating
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Info(fmt.Sprintf("Migration %s resumed", taskID))
    cdm.wg.Add(1)
    go cdm.runMigration(task)

    return nil
}

// CancelMigration отменяет миграцию
func (cdm *CrossDCMigrator) CancelMigration(taskID string) error {
    cdm.mu.RLock()
    task, ok := cdm.tasks[taskID]
    cdm.mu.RUnlock()

    if !ok {
        return fmt.Errorf("task %s not found", taskID)
    }

    task.mu.Lock()
    task.Status = MigrationStatusFailed
    task.Error = "cancelled by user"
    task.EndTime = time.Now().UnixMilli()
    task.UpdatedAt = time.Now().UnixMilli()
    task.mu.Unlock()

    cdm.logger.Info(fmt.Sprintf("Migration %s cancelled", taskID))
    return nil
}

// GetMigrationStats возвращает статистику миграции
func (cdm *CrossDCMigrator) GetMigrationStats() *MigrationStats {
    cdm.stats.mu.RLock()
    defer cdm.stats.mu.RUnlock()
    return &MigrationStats{
        TotalChanges:   cdm.stats.TotalChanges,
        AppliedChanges: cdm.stats.AppliedChanges,
        FailedChanges:  cdm.stats.FailedChanges,
        SkippedChanges: cdm.stats.SkippedChanges,
        LatencyAvg:     cdm.stats.LatencyAvg,
        LatencyMax:     cdm.stats.LatencyMax,
        Throughput:     cdm.stats.Throughput,
    }
}

// SubscribeChanges подписывается на изменения
func (cdm *CrossDCMigrator) SubscribeChanges() <-chan *ChangeRecord {
    ch := make(chan *ChangeRecord, 1000)
    cdm.subscriberMu.Lock()
    cdm.changeSubscribers = append(cdm.changeSubscribers, ch)
    cdm.subscriberMu.Unlock()
    return ch
}

// UnsubscribeChanges отписывается от изменений
func (cdm *CrossDCMigrator) UnsubscribeChanges(ch <-chan *ChangeRecord) {
    cdm.subscriberMu.Lock()
    defer cdm.subscriberMu.Unlock()
    
    for i, sub := range cdm.changeSubscribers {
        if sub == ch {
            cdm.changeSubscribers = append(cdm.changeSubscribers[:i], cdm.changeSubscribers[i+1:]...)
            close(sub)
            break
        }
    }
}

// GetQueueStats возвращает статистику очереди
func (cdm *CrossDCMigrator) GetQueueStats() map[string]interface{} {
    return map[string]interface{}{
        "size":      cdm.changeQueue.Size(),
        "last_lsn":  cdm.changeQueue.GetLastLSN(),
        "max_size":  100000,
        "file_path": cdm.changeQueue.filePath,
    }
}

// GetConfig возвращает конфигурацию мигратора (публичный метод для доступа из REPL)
func (cdm *CrossDCMigrator) GetConfig() *config.MigrationConfig {
    return cdm.config
}
