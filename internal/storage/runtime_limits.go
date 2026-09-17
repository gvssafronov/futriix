/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/runtime_limits.go
// Назначение: Ограничения на размер коллекции/документа в рантайме
//  Добавлен механизм eviction при нехватке памяти (OOM protection)
//  Eviction теперь проверяет активные транзакции перед удалением

package storage

import (
    "fmt"
    "runtime"
    "sync"
    "sync/atomic"
    "time"
)

// LoggerInterface определяет интерфейс для логирования
type LoggerInterface interface {
    Debug(msg string)
    Info(msg string)
    Error(msg string)
    Warn(msg string)
}

// EvictionPolicy определяет политику вытеснения
type EvictionPolicy int

const (
    EvictionNone     EvictionPolicy = iota
    EvictionLRU
    EvictionTTL
    EvictionOldest
)

// RuntimeLimitsManager управляет runtime-ограничениями
type RuntimeLimitsManager struct {
    mu                   sync.RWMutex
    globalMaxDocSize     int64
    globalMaxCollSize    int64
    globalMaxDocsPerColl int64
    globalMaxMemory      int64
    collectionOverrides  map[string]*CollectionLimits
    metrics              *LimitMetrics
    logger               LoggerInterface
    enabled              bool
    evictionPolicy       EvictionPolicy
    memoryThreshold      float64
    evictionChan         chan string
    stopChan             chan struct{}
    wg                   sync.WaitGroup
}

// CollectionLimits содержит лимиты для конкретной коллекции
type CollectionLimits struct {
    MaxDocSize        int64
    MaxCollectionSize int64
    MaxDocuments      int64
    EvictionPolicy    EvictionPolicy
    LastUpdated       int64
}

// LimitMetrics хранит метрики ограничений
type LimitMetrics struct {
    RejectedBySize     atomic.Uint64
    RejectedByDocCount atomic.Uint64
    RejectedByCollSize atomic.Uint64
    RejectedByMemory   atomic.Uint64
    EvictedDocuments   atomic.Uint64
    EvictedBytes       atomic.Uint64
    LastCheckTime      atomic.Int64
    LastEvictionTime   atomic.Int64
    SkippedEviction    atomic.Uint64 //  Пропущено из-за активных транзакций
}

// RuntimeLimitsConfig содержит конфигурацию ограничений
type RuntimeLimitsConfig struct {
    Enabled              bool           `json:"enabled"`
    GlobalMaxDocSizeMB   int            `json:"global_max_doc_size_mb"`
    GlobalMaxCollSizeMB  int64          `json:"global_max_coll_size_mb"`
    GlobalMaxDocsPerColl int64          `json:"global_max_docs_per_coll"`
    GlobalMaxMemoryMB    int64          `json:"global_max_memory_mb"`
    EvictionPolicy       EvictionPolicy `json:"eviction_policy"`
    MemoryThreshold      float64        `json:"memory_threshold"`
}

// DefaultRuntimeLimitsConfig возвращает конфигурацию по умолчанию
func DefaultRuntimeLimitsConfig() *RuntimeLimitsConfig {
    return &RuntimeLimitsConfig{
        Enabled:              true,
        GlobalMaxDocSizeMB:   16,
        GlobalMaxCollSizeMB:  10240,
        GlobalMaxDocsPerColl: 10000000,
        GlobalMaxMemoryMB:    0,
        EvictionPolicy:       EvictionLRU,
        MemoryThreshold:      0.85,
    }
}

// NewRuntimeLimitsManager создаёт новый менеджер ограничений
func NewRuntimeLimitsManager(cfg *RuntimeLimitsConfig, logger LoggerInterface) *RuntimeLimitsManager {
    if cfg == nil {
        cfg = DefaultRuntimeLimitsConfig()
    }
    
    maxMemory := cfg.GlobalMaxMemoryMB * 1024 * 1024
    if maxMemory <= 0 {
        var memStats runtime.MemStats
        runtime.ReadMemStats(&memStats)
        maxMemory = int64(float64(memStats.Sys) * 0.8)
    }
    
    rlm := &RuntimeLimitsManager{
        globalMaxDocSize:     int64(cfg.GlobalMaxDocSizeMB) * 1024 * 1024,
        globalMaxCollSize:    cfg.GlobalMaxCollSizeMB * 1024 * 1024,
        globalMaxDocsPerColl: cfg.GlobalMaxDocsPerColl,
        globalMaxMemory:      maxMemory,
        collectionOverrides:  make(map[string]*CollectionLimits),
        metrics:              &LimitMetrics{},
        logger:               logger,
        enabled:              cfg.Enabled,
        evictionPolicy:       cfg.EvictionPolicy,
        memoryThreshold:      cfg.MemoryThreshold,
        evictionChan:         make(chan string, 100),
        stopChan:             make(chan struct{}),
    }
    
    rlm.wg.Add(1)
    go rlm.memoryMonitorLoop()
    
    if logger != nil {
        logger.Debug(fmt.Sprintf("Runtime limits manager initialized: maxDoc=%dMB, maxColl=%dMB, maxDocs=%d, maxMemory=%dMB, eviction=%d",
            cfg.GlobalMaxDocSizeMB, cfg.GlobalMaxCollSizeMB, cfg.GlobalMaxDocsPerColl, maxMemory/(1024*1024), cfg.EvictionPolicy))
    }
    
    return rlm
}

func (rlm *RuntimeLimitsManager) memoryMonitorLoop() {
    defer rlm.wg.Done()
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            if !rlm.enabled {
                continue
            }
            var memStats runtime.MemStats
            runtime.ReadMemStats(&memStats)
            currentUsage := int64(memStats.Alloc)
            usageRatio := float64(currentUsage) / float64(rlm.globalMaxMemory)
            
            if usageRatio > rlm.memoryThreshold {
                if rlm.logger != nil {
                    rlm.logger.Warn(fmt.Sprintf("Memory usage %.2f%% exceeds threshold %.2f%%, triggering eviction",
                        usageRatio*100, rlm.memoryThreshold*100))
                }
                rlm.triggerEviction()
            }
        case <-rlm.stopChan:
            return
        }
    }
}

func (rlm *RuntimeLimitsManager) triggerEviction() {
    select {
    case rlm.evictionChan <- "global":
    default:
    }
}

// isDocInActiveTransaction проверяет, участвует ли документ в активной транзакции
//  Защита от удаления данных активных транзакций
func (rlm *RuntimeLimitsManager) isDocInActiveTransaction(docID, dbName, collName string) bool {
    if globalTxManager == nil {
        return false
    }
    
    inTx := false
    globalTxManager.activeTransactions.Range(func(key, value interface{}) bool {
        tx := value.(*Transaction)
        tx.mu.RLock()
        for _, op := range tx.Operations {
            if op.DocumentID == docID && op.Database == dbName && op.Collection == collName {
                inTx = true
                break
            }
        }
        tx.mu.RUnlock()
        if inTx {
            return false
        }
        return true
    })
    return inTx
}

// EvictFromCollection выполняет вытеснение документов из коллекции
//  Проверка активных транзакций перед удалением
func (rlm *RuntimeLimitsManager) EvictFromCollection(coll *Collection, targetBytes int64) (int64, error) {
    if !rlm.enabled {
        return 0, nil
    }
    
    evictedBytes := int64(0)
    evictedCount := int64(0)
    skippedCount := int64(0)
    
    docs := coll.GetAllDocumentsIncludingDeleted()
    
    // Сначала вытесняем удалённые документы
    for _, doc := range docs {
        if evictedBytes >= targetBytes {
            break
        }
        
        if doc.IsDeleted() {
            //  Проверяем активные транзакции
            if rlm.isDocInActiveTransaction(doc.ID, coll.DBName(), coll.Name()) {
                skippedCount++
                continue
            }
            
            size := doc.OriginalSize
            if size == 0 {
                size = 1024
            }
            
            if err := coll.PermanentDelete(doc.ID); err == nil {
                evictedBytes += size
                evictedCount++
            }
        }
    }
    
    // Затем вытесняем самые старые
    if evictedBytes < targetBytes && rlm.evictionPolicy == EvictionLRU {
        for _, doc := range docs {
            if evictedBytes >= targetBytes {
                break
            }
            
            if !doc.IsDeleted() {
                //  Проверяем активные транзакции
                if rlm.isDocInActiveTransaction(doc.ID, coll.DBName(), coll.Name()) {
                    skippedCount++
                    continue
                }
                
                size := doc.OriginalSize
                if size == 0 {
                    size = 1024
                }
                
                if coll.metadata.Settings.SoftDelete {
                    if err := coll.Delete(doc.ID); err == nil {
                        evictedBytes += size
                        evictedCount++
                    }
                } else {
                    if err := coll.PermanentDelete(doc.ID); err == nil {
                        evictedBytes += size
                        evictedCount++
                    }
                }
            }
        }
    }
    
    rlm.metrics.EvictedDocuments.Add(uint64(evictedCount))
    rlm.metrics.EvictedBytes.Add(uint64(evictedBytes))
    if skippedCount > 0 {
        rlm.metrics.SkippedEviction.Add(uint64(skippedCount))
    }
    rlm.metrics.LastEvictionTime.Store(time.Now().UnixMilli())
    
    if rlm.logger != nil {
        rlm.logger.Info(fmt.Sprintf("Evicted %d documents (%d bytes) from collection %s.%s (skipped %d in active transactions)",
            evictedCount, evictedBytes, coll.DBName(), coll.Name(), skippedCount))
    }
    
    return evictedBytes, nil
}

func (rlm *RuntimeLimitsManager) ValidateDocumentSize(dbName, collName string, docSize int64) error {
    if !rlm.enabled {
        return nil
    }
    
    limit := rlm.getCollectionLimit(dbName, collName)
    maxSize := rlm.globalMaxDocSize
    if limit != nil && limit.MaxDocSize > 0 {
        maxSize = limit.MaxDocSize
    }
    
    if docSize > maxSize {
        rlm.metrics.RejectedBySize.Add(1)
        return fmt.Errorf("document size %d bytes exceeds limit %d bytes", docSize, maxSize)
    }
    return nil
}

func (rlm *RuntimeLimitsManager) ValidateCollectionSize(coll *Collection, newDocSize int64) error {
    if !rlm.enabled {
        return nil
    }
    
    limit := rlm.getCollectionLimit(coll.DBName(), coll.Name())
    maxSize := rlm.globalMaxCollSize
    if limit != nil && limit.MaxCollectionSize > 0 {
        maxSize = limit.MaxCollectionSize
    }
    
    currentSize := coll.Size()
    if currentSize+newDocSize > maxSize {
        neededBytes := currentSize + newDocSize - maxSize
        if evicted, err := rlm.EvictFromCollection(coll, neededBytes); err == nil && evicted >= neededBytes {
            return nil
        }
        
        rlm.metrics.RejectedByCollSize.Add(1)
        return fmt.Errorf("collection size would exceed limit %d bytes (current: %d, new: %d)",
            maxSize, currentSize, newDocSize)
    }
    return nil
}

func (rlm *RuntimeLimitsManager) ValidateDocumentCount(coll *Collection) error {
    if !rlm.enabled {
        return nil
    }
    
    limit := rlm.getCollectionLimit(coll.DBName(), coll.Name())
    maxDocs := rlm.globalMaxDocsPerColl
    if limit != nil && limit.MaxDocuments > 0 {
        maxDocs = limit.MaxDocuments
    }
    
    currentCount := coll.Count()
    if currentCount >= maxDocs {
        targetBytes := int64((currentCount - maxDocs + 1) * 1024)
        if evicted, err := rlm.EvictFromCollection(coll, targetBytes); err == nil && evicted > 0 {
            if coll.Count() < maxDocs {
                return nil
            }
        }
        
        rlm.metrics.RejectedByDocCount.Add(1)
        return fmt.Errorf("collection has reached maximum document count %d", maxDocs)
    }
    return nil
}

func (rlm *RuntimeLimitsManager) CheckMemoryUsage() error {
    if !rlm.enabled {
        return nil
    }
    
    var memStats runtime.MemStats
    runtime.ReadMemStats(&memStats)
    
    currentUsage := int64(memStats.Alloc)
    if currentUsage > rlm.globalMaxMemory {
        rlm.metrics.RejectedByMemory.Add(1)
        return fmt.Errorf("memory usage %d bytes exceeds limit %d bytes", currentUsage, rlm.globalMaxMemory)
    }
    return nil
}

func (rlm *RuntimeLimitsManager) getCollectionLimit(dbName, collName string) *CollectionLimits {
    rlm.mu.RLock()
    defer rlm.mu.RUnlock()
    
    key := fmt.Sprintf("%s.%s", dbName, collName)
    if limits, ok := rlm.collectionOverrides[key]; ok {
        return limits
    }
    return nil
}

func (rlm *RuntimeLimitsManager) SetCollectionLimits(dbName, collName string, maxDocSizeMB int, maxCollSizeMB int64, maxDocuments int64) {
    rlm.mu.Lock()
    defer rlm.mu.Unlock()
    
    key := fmt.Sprintf("%s.%s", dbName, collName)
    rlm.collectionOverrides[key] = &CollectionLimits{
        MaxDocSize:        int64(maxDocSizeMB) * 1024 * 1024,
        MaxCollectionSize: maxCollSizeMB * 1024 * 1024,
        MaxDocuments:      maxDocuments,
        EvictionPolicy:    rlm.evictionPolicy,
        LastUpdated:       time.Now().UnixMilli(),
    }
    
    if rlm.logger != nil {
        rlm.logger.Info(fmt.Sprintf("Set limits for %s: maxDoc=%dMB, maxColl=%dMB, maxDocs=%d",
            key, maxDocSizeMB, maxCollSizeMB, maxDocuments))
    }
}

func (rlm *RuntimeLimitsManager) RemoveCollectionLimits(dbName, collName string) {
    rlm.mu.Lock()
    defer rlm.mu.Unlock()
    
    key := fmt.Sprintf("%s.%s", dbName, collName)
    delete(rlm.collectionOverrides, key)
    
    if rlm.logger != nil {
        rlm.logger.Info(fmt.Sprintf("Removed limits override for %s", key))
    }
}

func (rlm *RuntimeLimitsManager) GetMetrics() map[string]interface{} {
    return map[string]interface{}{
        "rejected_by_size":        rlm.metrics.RejectedBySize.Load(),
        "rejected_by_doc_count":   rlm.metrics.RejectedByDocCount.Load(),
        "rejected_by_coll_size":   rlm.metrics.RejectedByCollSize.Load(),
        "rejected_by_memory":      rlm.metrics.RejectedByMemory.Load(),
        "evicted_documents":       rlm.metrics.EvictedDocuments.Load(),
        "evicted_bytes":           rlm.metrics.EvictedBytes.Load(),
        "skipped_eviction":        rlm.metrics.SkippedEviction.Load(),
        "last_eviction_time":      rlm.metrics.LastEvictionTime.Load(),
        "global_max_doc_size_mb":  rlm.globalMaxDocSize / (1024 * 1024),
        "global_max_coll_size_mb": rlm.globalMaxCollSize / (1024 * 1024),
        "global_max_docs_per_coll": rlm.globalMaxDocsPerColl,
        "global_max_memory_mb":    rlm.globalMaxMemory / (1024 * 1024),
        "eviction_policy":         rlm.evictionPolicy,
        "memory_threshold":        rlm.memoryThreshold,
        "enabled":                 rlm.enabled,
    }
}

func (rlm *RuntimeLimitsManager) Stop() {
    close(rlm.stopChan)
    rlm.wg.Wait()
}
