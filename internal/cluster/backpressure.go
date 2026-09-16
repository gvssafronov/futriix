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
// Реализован алгоритм "buffer ring" (кольцевой буфер) для хранения
// истории изменений уровня перегрузки и равномерного вероятностного отклонения.

package cluster

import (
    "fmt"
    "math/rand"
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

// =============================================================================
// BUFFER RING - КОЛЬЦЕВОЙ БУФЕР ДЛЯ ИСТОРИИ УРОВНЕЙ ПЕРЕГРУЗКИ
// =============================================================================

// ringEntry представляет одну запись в кольцевом буфере
type ringEntry struct {
    level     BackpressureLevel
    timestamp int64
    cpu       float64
    memory    float64
    queueSize int64
    conns     int64
}

// BufferRing представляет кольцевой буфер фиксированного размера
// для хранения истории изменений уровня перегрузки.
// Потокобезопасен, использует атомарные операции для индексов.
type BufferRing struct {
    entries  []ringEntry
    capacity int64
    head     atomic.Int64 // индекс для записи
    tail     atomic.Int64 // индекс для чтения
    count    atomic.Int64 // текущее количество записей
    mu       sync.RWMutex // защита для операций, требующих согласованности
}

// NewBufferRing создаёт новый кольцевой буфер заданной ёмкости
func NewBufferRing(capacity int) *BufferRing {
    if capacity <= 0 {
        capacity = 100
    }
    return &BufferRing{
        entries:  make([]ringEntry, capacity),
        capacity: int64(capacity),
    }
}

// Push добавляет новую запись в кольцевой буфер.
// Если буфер полон, самая старая запись перезаписывается.
func (br *BufferRing) Push(level BackpressureLevel, cpu, memory float64, queueSize, conns int64) {
    br.mu.Lock()
    defer br.mu.Unlock()

    idx := br.head.Load() % br.capacity
    br.entries[idx] = ringEntry{
        level:     level,
        timestamp: time.Now().UnixMilli(),
        cpu:       cpu,
        memory:    memory,
        queueSize: queueSize,
        conns:     conns,
    }
    br.head.Add(1)

    // Обновляем tail и count
    if br.count.Load() < br.capacity {
        br.count.Add(1)
    } else {
        br.tail.Store(br.head.Load() - br.capacity)
    }
}

// GetAll возвращает все записи в кольцевом буфере в порядке добавления.
func (br *BufferRing) GetAll() []ringEntry {
    br.mu.RLock()
    defer br.mu.RUnlock()

    count := br.count.Load()
    if count == 0 {
        return nil
    }

    result := make([]ringEntry, 0, count)
    tail := br.tail.Load()
    head := br.head.Load()

    for i := tail; i < head; i++ {
        idx := i % br.capacity
        result = append(result, br.entries[idx])
    }
    return result
}

// GetLastN возвращает последние N записей из буфера.
func (br *BufferRing) GetLastN(n int) []ringEntry {
    br.mu.RLock()
    defer br.mu.RUnlock()

    count := br.count.Load()
    if count == 0 || n <= 0 {
        return nil
    }
    if int64(n) > count {
        n = int(count)
    }

    result := make([]ringEntry, 0, n)
    head := br.head.Load()
    start := head - int64(n)
    if start < 0 {
        start = 0
    }

    for i := start; i < head; i++ {
        idx := i % br.capacity
        result = append(result, br.entries[idx])
    }
    return result
}

// Size возвращает текущее количество записей в буфере.
func (br *BufferRing) Size() int64 {
    return br.count.Load()
}

// Capacity возвращает ёмкость буфера.
func (br *BufferRing) Capacity() int64 {
    return br.capacity
}

// Clear очищает буфер.
func (br *BufferRing) Clear() {
    br.mu.Lock()
    defer br.mu.Unlock()
    br.head.Store(0)
    br.tail.Store(0)
    br.count.Store(0)
    for i := range br.entries {
        br.entries[i] = ringEntry{}
    }
}

// =============================================================================
// BACKPRESSURE MANAGER
// =============================================================================

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
    historyRing          *BufferRing // Кольцевой буфер истории уровней
    rand                 *rand.Rand  // Генератор случайных чисел для равномерного отклонения
    randMu               sync.Mutex  // Защита для генератора случайных чисел
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
    HistoryRingSize      int           `json:"history_ring_size"` // Размер кольцевого буфера
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
        HistoryRingSize:     256,
    }
}

// NewBackpressureManager создаёт новый менеджер backpressure
func NewBackpressureManager(cfg *BackpressureConfig, logger LoggerInterface) *BackpressureManager {
    if cfg == nil {
        cfg = DefaultBackpressureConfig()
    }

    // Определяем размер кольцевого буфера
    ringSize := cfg.HistoryRingSize
    if ringSize <= 0 {
        ringSize = 256
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
        historyRing:         NewBufferRing(ringSize),
        rand:                rand.New(rand.NewSource(time.Now().UnixNano())),
    }

    bpm.rejectProbability.Store(0)
    bpm.delayDuration.Store(0)

    if cfg.Enabled {
        go bpm.monitorLoop()
    }

    if logger != nil {
        logger.Debug("Backpressure manager initialized with buffer ring")
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

    // Записываем в кольцевой буфер
    bpm.historyRing.Push(newLevel, cpu, memory, queueSize, connections)

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

// shouldReject определяет, нужно ли отклонить запрос на основе вероятности.
// Использует равномерное распределение через rand.Float64().
func (bpm *BackpressureManager) shouldReject(rejectProb uint32) bool {
    if rejectProb == 0 {
        return false
    }
    if rejectProb >= 100 {
        return true
    }

    bpm.randMu.Lock()
    r := bpm.rand.Float64()
    bpm.randMu.Unlock()

    return r*100 < float64(rejectProb)
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

    // Вероятностное отклонение с использованием равномерного распределения
    if bpm.shouldReject(rejectProb) {
        bpm.rejectedCount.Add(1)
        return fmt.Errorf("request rejected due to backpressure (probability: %d%%)", rejectProb)
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
        "history_ring_size":  bpm.historyRing.Size(),
        "history_ring_cap":   bpm.historyRing.Capacity(),
    }
}

// GetHistory возвращает историю изменений уровня перегрузки из кольцевого буфера
func (bpm *BackpressureManager) GetHistory() []map[string]interface{} {
    entries := bpm.historyRing.GetAll()
    result := make([]map[string]interface{}, 0, len(entries))
    for _, e := range entries {
        result = append(result, map[string]interface{}{
            "level":      bpm.levelToString(e.level),
            "timestamp":  e.timestamp,
            "cpu":        e.cpu,
            "memory":     e.memory,
            "queue_size": e.queueSize,
            "conns":      e.conns,
        })
    }
    return result
}

// GetLastNHistory возвращает последние N записей истории
func (bpm *BackpressureManager) GetLastNHistory(n int) []map[string]interface{} {
    entries := bpm.historyRing.GetLastN(n)
    result := make([]map[string]interface{}, 0, len(entries))
    for _, e := range entries {
        result = append(result, map[string]interface{}{
            "level":      bpm.levelToString(e.level),
            "timestamp":  e.timestamp,
            "cpu":        e.cpu,
            "memory":     e.memory,
            "queue_size": e.queueSize,
            "conns":      e.conns,
        })
    }
    return result
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
