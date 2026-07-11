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

package storage

import (
    "fmt"
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

// RuntimeLimitsManager управляет runtime-ограничениями
type RuntimeLimitsManager struct {
    mu                   sync.RWMutex
    globalMaxDocSize     int64              // Максимальный размер документа (байт)
    globalMaxCollSize    int64              // Максимальный размер коллекции (байт)
    globalMaxDocsPerColl int64              // Максимальное количество документов в коллекции
    collectionOverrides  map[string]*CollectionLimits // Переопределения для коллекций
    metrics              *LimitMetrics
    logger               LoggerInterface
    enabled              bool
}

// CollectionLimits содержит лимиты для конкретной коллекции
type CollectionLimits struct {
    MaxDocSize        int64
    MaxCollectionSize int64
    MaxDocuments      int64
    LastUpdated       int64
}

// LimitMetrics хранит метрики ограничений
type LimitMetrics struct {
    RejectedBySize      atomic.Uint64
    RejectedByDocCount  atomic.Uint64
    RejectedByCollSize  atomic.Uint64
    LastCheckTime       atomic.Int64
}

// RuntimeLimitsConfig содержит конфигурацию ограничений
type RuntimeLimitsConfig struct {
    Enabled              bool   `json:"enabled"`
    GlobalMaxDocSizeMB   int    `json:"global_max_doc_size_mb"`
    GlobalMaxCollSizeMB  int64  `json:"global_max_coll_size_mb"`
    GlobalMaxDocsPerColl int64  `json:"global_max_docs_per_coll"`
}

// DefaultRuntimeLimitsConfig возвращает конфигурацию по умолчанию
func DefaultRuntimeLimitsConfig() *RuntimeLimitsConfig {
    return &RuntimeLimitsConfig{
        Enabled:              true,
        GlobalMaxDocSizeMB:   16,      // 16 MB на документ
        GlobalMaxCollSizeMB:  10240,   // 10 GB на коллекцию
        GlobalMaxDocsPerColl: 10000000, // 10 млн документов
    }
}

// NewRuntimeLimitsManager создаёт новый менеджер ограничений
func NewRuntimeLimitsManager(cfg *RuntimeLimitsConfig, logger LoggerInterface) *RuntimeLimitsManager {
    if cfg == nil {
        cfg = DefaultRuntimeLimitsConfig()
    }
    
    rlm := &RuntimeLimitsManager{
        globalMaxDocSize:     int64(cfg.GlobalMaxDocSizeMB) * 1024 * 1024,
        globalMaxCollSize:    cfg.GlobalMaxCollSizeMB * 1024 * 1024,
        globalMaxDocsPerColl: cfg.GlobalMaxDocsPerColl,
        collectionOverrides:  make(map[string]*CollectionLimits),
        metrics:              &LimitMetrics{},
        logger:               logger,
        enabled:              cfg.Enabled,
    }
    
    if logger != nil {
        logger.Debug(fmt.Sprintf("Runtime limits manager initialized: maxDoc=%dMB, maxColl=%dMB, maxDocs=%d",
            cfg.GlobalMaxDocSizeMB, cfg.GlobalMaxCollSizeMB, cfg.GlobalMaxDocsPerColl))
    }
    
    return rlm
}

// ValidateDocumentSize проверяет размер документа
func (rlm *RuntimeLimitsManager) ValidateDocumentSize(dbName, collName string, docSize int64) error {
    if !rlm.enabled {
        return nil
    }
    
    // Проверяем переопределение для коллекции
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

// ValidateCollectionSize проверяет размер коллекции
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
        rlm.metrics.RejectedByCollSize.Add(1)
        return fmt.Errorf("collection size would exceed limit %d bytes (current: %d, new: %d)",
            maxSize, currentSize, newDocSize)
    }
    
    return nil
}

// ValidateDocumentCount проверяет количество документов в коллекции
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
        rlm.metrics.RejectedByDocCount.Add(1)
        return fmt.Errorf("collection has reached maximum document count %d", maxDocs)
    }
    
    return nil
}

// getCollectionLimit возвращает лимиты для коллекции
func (rlm *RuntimeLimitsManager) getCollectionLimit(dbName, collName string) *CollectionLimits {
    rlm.mu.RLock()
    defer rlm.mu.RUnlock()
    
    key := fmt.Sprintf("%s.%s", dbName, collName)
    if limits, ok := rlm.collectionOverrides[key]; ok {
        return limits
    }
    return nil
}

// SetCollectionLimits устанавливает лимиты для коллекции
func (rlm *RuntimeLimitsManager) SetCollectionLimits(dbName, collName string, maxDocSizeMB int, maxCollSizeMB int64, maxDocuments int64) {
    rlm.mu.Lock()
    defer rlm.mu.Unlock()
    
    key := fmt.Sprintf("%s.%s", dbName, collName)
    rlm.collectionOverrides[key] = &CollectionLimits{
        MaxDocSize:        int64(maxDocSizeMB) * 1024 * 1024,
        MaxCollectionSize: maxCollSizeMB * 1024 * 1024,
        MaxDocuments:      maxDocuments,
        LastUpdated:       time.Now().UnixMilli(),
    }
    
    if rlm.logger != nil {
        rlm.logger.Info(fmt.Sprintf("Set limits for %s: maxDoc=%dMB, maxColl=%dMB, maxDocs=%d",
            key, maxDocSizeMB, maxCollSizeMB, maxDocuments))
    }
}

// RemoveCollectionLimits удаляет переопределения для коллекции
func (rlm *RuntimeLimitsManager) RemoveCollectionLimits(dbName, collName string) {
    rlm.mu.Lock()
    defer rlm.mu.Unlock()
    
    key := fmt.Sprintf("%s.%s", dbName, collName)
    delete(rlm.collectionOverrides, key)
    
    if rlm.logger != nil {
        rlm.logger.Info(fmt.Sprintf("Removed limits override for %s", key))
    }
}

// GetMetrics возвращает метрики
func (rlm *RuntimeLimitsManager) GetMetrics() map[string]interface{} {
    return map[string]interface{}{
        "rejected_by_size":        rlm.metrics.RejectedBySize.Load(),
        "rejected_by_doc_count":   rlm.metrics.RejectedByDocCount.Load(),
        "rejected_by_coll_size":   rlm.metrics.RejectedByCollSize.Load(),
        "global_max_doc_size_mb":  rlm.globalMaxDocSize / (1024 * 1024),
        "global_max_coll_size_mb": rlm.globalMaxCollSize / (1024 * 1024),
        "global_max_docs_per_coll": rlm.globalMaxDocsPerColl,
        "enabled":                 rlm.enabled,
    }
}
