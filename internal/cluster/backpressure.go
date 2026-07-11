/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/backpressure.go
// Назначение: Backpressure при перегрузке системы

package cluster

import (
    "fmt"
    "sync"
    "sync/atomic"
    "time"
)

// BackpressureLevel представляет уровень перегрузки
type BackpressureLevel int

const (
    LevelNone   BackpressureLevel = iota // Нет перегрузки
    LevelLow                             // Низкая перегрузка - задержки
    LevelMedium                          // Средняя перегрузка - отклонение части запросов
    LevelHigh                            // Высокая перегрузка - отклонение большинства
    LevelCritical                        // Критическая - только чтение
)

// BackpressureManager управляет backpressure
type BackpressureManager struct {
    mu                   sync.RWMutex
    currentLevel         BackpressureLevel
    cpuThreshold         float64
    memoryThreshold      float64
    queueSizeThreshold   int
    connectionThreshold  int
    currentCPU           atomic.Uint64
    currentMemory        atomic.Uint64
    currentQueueSize     atomic.Int64
    currentConnections   atomic.Int64
    rejectedCount        atomic.Uint64
    delayedCount         atomic.Uint64
    lastCheck            time.Time
    checkInterval        time.Duration
    logger               LoggerInterface // Используем LoggerInterface из node.go
    enabled              bool
    writeAllowed         bool
    readAllowed          bool
    rejectProbability    atomic.Uint32
    delayDuration        atomic.Int64
}

// BackpressureConfig содержит настройки backpressure
type BackpressureConfig struct {
    Enabled              bool          `json:"enabled"`
    CPUThreshold         float64       `json:"cpu_threshold"`
    MemoryThreshold      float64       `json:"memory_threshold"`
    QueueSizeThreshold   int           `json:"queue_size_threshold"`
    ConnectionThreshold  int           `json:"connection_threshold"`
    CheckIntervalMs      int           `json:"check_interval_ms"`
    LowDelayMs           int64         `json:"low_delay_ms"`
    MediumRejectProb     uint32        `json:"medium_reject_prob"`
    HighRejectProb       uint32        `json:"high_reject_prob"`
}

// DefaultBackpressureConfig возвращает конфигурацию по умолчанию
func DefaultBackpressureConfig() *BackpressureConfig {
    return &BackpressureConfig{
        Enabled:             true,
        CPUThreshold:        0.8,
        MemoryThreshold:     0.85,
        QueueSizeThreshold:  10000,
        ConnectionThreshold: 5000,
        CheckIntervalMs:     1000,
        LowDelayMs:          100,
        MediumRejectProb:    30,
        HighRejectProb:      70,
    }
}

// NewBackpressureManager создаёт новый менеджер backpressure
func NewBackpressureManager(cfg *BackpressureConfig, logger LoggerInterface) *BackpressureManager {
    if cfg == nil {
        cfg = DefaultBackpressureConfig()
    }
    
    bpm := &BackpressureManager{
        currentLevel:        LevelNone,
        cpuThreshold:        cfg.CPUThreshold,
        memoryThreshold:     cfg.MemoryThreshold,
        queueSizeThreshold:  cfg.QueueSizeThreshold,
        connectionThreshold: cfg.ConnectionThreshold,
        checkInterval:       time.Duration(cfg.CheckIntervalMs) * time.Millisecond,
        logger:              logger,
        enabled:             cfg.Enabled,
        writeAllowed:        true,
        readAllowed:         true,
        rejectProbability:   atomic.Uint32{},
        delayDuration:       atomic.Int64{},
    }
    
    bpm.rejectProbability.Store(0)
    bpm.delayDuration.Store(0)
    
    if cfg.Enabled {
        go bpm.monitorLoop()
    }
    
    if logger != nil {
        logger.Debug("Backpressure manager initialized")
    }
    
    return bpm
}

// monitorLoop периодически проверяет метрики
func (bpm *BackpressureManager) monitorLoop() {
    ticker := time.NewTicker(bpm.checkInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        bpm.updateLevel()
    }
}

// updateLevel обновляет уровень перегрузки
func (bpm *BackpressureManager) updateLevel() {
    cpu := float64(bpm.currentCPU.Load()) / 100.0
    memory := float64(bpm.currentMemory.Load()) / 100.0
    queueSize := bpm.currentQueueSize.Load()
    connections := bpm.currentConnections.Load()
    
    newLevel := LevelNone
    
    if cpu >= bpm.cpuThreshold || memory >= bpm.memoryThreshold {
        newLevel = LevelHigh
    } else if queueSize > int64(bpm.queueSizeThreshold) {
        if queueSize > int64(bpm.queueSizeThreshold*2) {
            newLevel = LevelCritical
        } else {
            newLevel = LevelMedium
        }
    } else if connections > int64(bpm.connectionThreshold) {
        newLevel = LevelLow
    }
    
    bpm.mu.Lock()
    oldLevel := bpm.currentLevel
    bpm.currentLevel = newLevel
    bpm.mu.Unlock()
    
    // Применяем политики в зависимости от уровня
    bpm.applyPolicies(newLevel)
    
    if oldLevel != newLevel && bpm.logger != nil {
        bpm.logger.Info(fmt.Sprintf("Backpressure level changed from %v to %v (cpu=%.2f%%, mem=%.2f%%, queue=%d, conns=%d)",
            bpm.levelToString(oldLevel), bpm.levelToString(newLevel), cpu*100, memory*100, queueSize, connections))
    }
}

// applyPolicies применяет политики в зависимости от уровня
func (bpm *BackpressureManager) applyPolicies(level BackpressureLevel) {
    bpm.mu.Lock()
    defer bpm.mu.Unlock()
    
    switch level {
    case LevelNone:
        bpm.writeAllowed = true
        bpm.readAllowed = true
        bpm.rejectProbability.Store(0)
        bpm.delayDuration.Store(0)
        
    case LevelLow:
        bpm.writeAllowed = true
        bpm.readAllowed = true
        bpm.rejectProbability.Store(0)
        bpm.delayDuration.Store(100) // 100ms задержка
        
    case LevelMedium:
        bpm.writeAllowed = true
        bpm.readAllowed = true
        bpm.rejectProbability.Store(30) // 30% отклонение
        bpm.delayDuration.Store(200)
        
    case LevelHigh:
        bpm.writeAllowed = false // Запись запрещена
        bpm.readAllowed = true
        bpm.rejectProbability.Store(70) // 70% отклонение
        bpm.delayDuration.Store(500)
        
    case LevelCritical:
        bpm.writeAllowed = false
        bpm.readAllowed = true // Только чтение
        bpm.rejectProbability.Store(90)
        bpm.delayDuration.Store(1000)
    }
}

// BeforeRequest вызывается перед обработкой запроса
func (bpm *BackpressureManager) BeforeRequest(isWrite bool) error {
    if !bpm.enabled {
        return nil
    }
    
    bpm.mu.RLock()
    level := bpm.currentLevel
    writeAllowed := bpm.writeAllowed
    readAllowed := bpm.readAllowed
    rejectProb := bpm.rejectProbability.Load()
    delayDur := bpm.delayDuration.Load()
    bpm.mu.RUnlock()
    
    // Проверяем разрешение на операцию
    if isWrite && !writeAllowed {
        bpm.rejectedCount.Add(1)
        return fmt.Errorf("write operations rejected due to backpressure (level: %v)", bpm.levelToString(level))
    }
    if !isWrite && !readAllowed {
        bpm.rejectedCount.Add(1)
        return fmt.Errorf("read operations rejected due to backpressure (level: %v)", bpm.levelToString(level))
    }
    
    // Вероятностное отклонение
    if rejectProb > 0 {
        // Простая вероятностная проверка
        if uint32(time.Now().UnixNano()%100) < rejectProb {
            bpm.rejectedCount.Add(1)
            return fmt.Errorf("request rejected due to backpressure (probability: %d%%)", rejectProb)
        }
    }
    
    // Добавляем задержку если нужно
    if delayDur > 0 {
        bpm.delayedCount.Add(1)
        time.Sleep(time.Duration(delayDur) * time.Millisecond)
    }
    
    return nil
}

// AfterRequest вызывается после обработки запроса
func (bpm *BackpressureManager) AfterRequest(duration time.Duration, success bool) {
    // Можно использовать для дополнительной статистики
}

// UpdateMetrics обновляет метрики для backpressure
func (bpm *BackpressureManager) UpdateMetrics(cpuPercent, memoryPercent uint64, queueSize, connections int64) {
    bpm.currentCPU.Store(cpuPercent)
    bpm.currentMemory.Store(memoryPercent)
    bpm.currentQueueSize.Store(queueSize)
    bpm.currentConnections.Store(connections)
}

// GetCurrentLevel возвращает текущий уровень перегрузки
func (bpm *BackpressureManager) GetCurrentLevel() BackpressureLevel {
    bpm.mu.RLock()
    defer bpm.mu.RUnlock()
    return bpm.currentLevel
}

// GetStats возвращает статистику backpressure
func (bpm *BackpressureManager) GetStats() map[string]interface{} {
    bpm.mu.RLock()
    defer bpm.mu.RUnlock()
    
    return map[string]interface{}{
        "current_level":      bpm.levelToString(bpm.currentLevel),
        "write_allowed":      bpm.writeAllowed,
        "read_allowed":       bpm.readAllowed,
        "reject_probability": bpm.rejectProbability.Load(),
        "delay_ms":           bpm.delayDuration.Load(),
        "rejected_count":     bpm.rejectedCount.Load(),
        "delayed_count":      bpm.delayedCount.Load(),
        "cpu_threshold":      bpm.cpuThreshold,
        "memory_threshold":   bpm.memoryThreshold,
        "queue_threshold":    bpm.queueSizeThreshold,
        "conn_threshold":     bpm.connectionThreshold,
    }
}

func (bpm *BackpressureManager) levelToString(level BackpressureLevel) string {
    switch level {
    case LevelNone:
        return "none"
    case LevelLow:
        return "low"
    case LevelMedium:
        return "medium"
    case LevelHigh:
        return "high"
    case LevelCritical:
        return "critical"
    default:
        return "unknown"
    }
}
