/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

package autoscaling

import (
    "fmt"
    "sync"
    "sync/atomic"
    "time"

    "futriis/internal/config"
    "futriis/internal/log"
)

// ClusterAutoscaler управляет автоматическим масштабированием кластера
type ClusterAutoscaler struct {
    config     *config.AutoscalingConfig
    logger     *log.Logger
    mu         sync.RWMutex
    stopChan   chan struct{}
    wg         sync.WaitGroup
    running    atomic.Bool
    stats      *AutoscalingStats
    
    // Метрики нагрузки
    cpuLoad    atomic.Float64
    memLoad    atomic.Float64
    qps        atomic.Uint64
    connections atomic.Uint64
    
    // Состояние кластера
    currentNodeCount int
    targetNodeCount  int
    
    // Коллбэки для реального масштабирования
    scaleUpCallback   func(count int) error
    scaleDownCallback func(count int) error
    getNodeCountCallback func() int
}

// AutoscalingStats статистика автомасштабирования
type AutoscalingStats struct {
    TotalScaleUps   atomic.Uint64
    TotalScaleDowns atomic.Uint64
    LastScaleUpAt   atomic.Int64
    LastScaleDownAt atomic.Int64
    LastEvaluationAt atomic.Int64
    CurrentNodes    int
    TargetNodes     int
    mu              sync.RWMutex
}

// NewClusterAutoscaler создаёт новый экземпляр автомасштабирования
func NewClusterAutoscaler(cfg *config.AutoscalingConfig, logger *log.Logger) *ClusterAutoscaler {
    if cfg == nil {
        return nil
    }
    
    a := &ClusterAutoscaler{
        config:          cfg,
        logger:          logger,
        stopChan:        make(chan struct{}),
        currentNodeCount: cfg.MinNodes,
        targetNodeCount:  cfg.MinNodes,
        stats:           &AutoscalingStats{},
    }
    
    return a
}

// SetCallbacks устанавливает коллбэки для управления кластером
func (a *ClusterAutoscaler) SetCallbacks(
    scaleUp func(count int) error,
    scaleDown func(count int) error,
    getNodeCount func() int,
) {
    a.mu.Lock()
    defer a.mu.Unlock()
    a.scaleUpCallback = scaleUp
    a.scaleDownCallback = scaleDown
    a.getNodeCountCallback = getNodeCount
}

// Start запускает автомасштабирование
func (a *ClusterAutoscaler) Start() {
    if !a.config.Enabled {
        if a.logger != nil {
            a.logger.Info("Autoscaling is disabled in configuration")
        }
        return
    }
    
    if !a.running.CompareAndSwap(false, true) {
        return
    }
    
    a.wg.Add(1)
    go a.scalingLoop()
    
    if a.logger != nil {
        a.logger.Info(fmt.Sprintf("ClusterAutoscaler started: min_nodes=%d, max_nodes=%d, threshold_up=%.2f, threshold_down=%.2f",
            a.config.MinNodes, a.config.MaxNodes, a.config.ScaleUpThreshold, a.config.ScaleDownThreshold))
    }
}

// Stop останавливает автомасштабирование
func (a *ClusterAutoscaler) Stop() {
    if !a.running.Load() {
        return
    }
    
    close(a.stopChan)
    a.wg.Wait()
    a.running.Store(false)
    
    if a.logger != nil {
        a.logger.Info("ClusterAutoscaler stopped")
    }
}

// ReloadConfig обновляет конфигурацию автомасштабирования
func (a *ClusterAutoscaler) ReloadConfig(cfg *config.AutoscalingConfig) {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    oldEnabled := a.config.Enabled
    a.config = cfg
    
    if a.logger != nil {
        a.logger.Info(fmt.Sprintf("ClusterAutoscaler configuration reloaded: enabled=%v, min_nodes=%d, max_nodes=%d",
            cfg.Enabled, cfg.MinNodes, cfg.MaxNodes))
    }
    
    if oldEnabled != cfg.Enabled {
        if cfg.Enabled {
            a.Start()
        } else {
            a.Stop()
        }
    }
    
    if a.currentNodeCount < cfg.MinNodes {
        a.currentNodeCount = cfg.MinNodes
        a.targetNodeCount = cfg.MinNodes
    }
    if a.currentNodeCount > cfg.MaxNodes {
        a.currentNodeCount = cfg.MaxNodes
        a.targetNodeCount = cfg.MaxNodes
    }
}

// UpdateMetrics обновляет метрики нагрузки
func (a *ClusterAutoscaler) UpdateMetrics(cpuLoad, memLoad float64, qps, conns uint64) {
    a.cpuLoad.Store(cpuLoad)
    a.memLoad.Store(memLoad)
    a.qps.Store(qps)
    a.connections.Store(conns)
}

// SetCurrentNodes устанавливает текущее количество узлов
func (a *ClusterAutoscaler) SetCurrentNodes(count int) {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    if count < a.config.MinNodes {
        count = a.config.MinNodes
    }
    if count > a.config.MaxNodes {
        count = a.config.MaxNodes
    }
    a.currentNodeCount = count
    a.targetNodeCount = count
    
    a.stats.mu.Lock()
    a.stats.CurrentNodes = count
    a.stats.TargetNodes = count
    a.stats.mu.Unlock()
}

// GetCurrentNodes возвращает текущее количество узлов
func (a *ClusterAutoscaler) GetCurrentNodes() int {
    a.mu.RLock()
    defer a.mu.RUnlock()
    return a.currentNodeCount
}

// GetTargetNodes возвращает целевое количество узлов
func (a *ClusterAutoscaler) GetTargetNodes() int {
    a.mu.RLock()
    defer a.mu.RUnlock()
    return a.targetNodeCount
}

// GetStats возвращает статистику автомасштабирования
func (a *ClusterAutoscaler) GetStats() map[string]interface{} {
    a.mu.RLock()
    defer a.mu.RUnlock()
    
    a.stats.mu.RLock()
    defer a.stats.mu.RUnlock()
    
    return map[string]interface{}{
        "enabled":                 a.config.Enabled,
        "min_nodes":               a.config.MinNodes,
        "max_nodes":               a.config.MaxNodes,
        "current_nodes":           a.currentNodeCount,
        "target_nodes":            a.targetNodeCount,
        "scale_up_threshold":      a.config.ScaleUpThreshold,
        "scale_down_threshold":    a.config.ScaleDownThreshold,
        "cpu_load":                a.cpuLoad.Load(),
        "memory_load":             a.memLoad.Load(),
        "qps":                     a.qps.Load(),
        "connections":             a.connections.Load(),
        "total_scale_ups":         a.stats.TotalScaleUps.Load(),
        "total_scale_downs":       a.stats.TotalScaleDowns.Load(),
        "last_scale_up_at":        a.stats.LastScaleUpAt.Load(),
        "last_scale_down_at":      a.stats.LastScaleDownAt.Load(),
        "last_evaluation_at":      a.stats.LastEvaluationAt.Load(),
        "predictive_enabled":      a.config.PredictiveEnabled,
        "max_scale_up_nodes":      a.config.MaxScaleUpNodes,
        "max_scale_down_nodes":    a.config.MaxScaleDownNodes,
        "scale_up_cooldown_sec":   a.config.ScaleUpCooldownSec,
        "scale_down_cooldown_sec": a.config.ScaleDownCooldownSec,
        "evaluation_interval_sec": a.config.EvaluationIntervalSec,
    }
}

// scalingLoop основной цикл оценки и масштабирования
func (a *ClusterAutoscaler) scalingLoop() {
    defer a.wg.Done()
    
    ticker := time.NewTicker(a.config.GetEvaluationInterval())
    defer ticker.Stop()
    
    for {
        select {
        case <-a.stopChan:
            return
        case <-ticker.C:
            a.evaluateAndScale()
        }
    }
}

// evaluateAndScale оценивает нагрузку и выполняет масштабирование
func (a *ClusterAutoscaler) evaluateAndScale() {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    if !a.config.Enabled {
        return
    }
    
    a.stats.LastEvaluationAt.Store(time.Now().UnixMilli())
    
    // Получаем текущие метрики
    cpu := a.cpuLoad.Load()
    mem := a.memLoad.Load()
    qps := a.qps.Load()
    conns := a.connections.Load()
    
    // Обновляем текущее количество узлов из внешнего источника
    if a.getNodeCountCallback != nil {
        actualCount := a.getNodeCountCallback()
        if actualCount > 0 {
            a.currentNodeCount = actualCount
        }
    }
    
    needScaleUp := false
    needScaleDown := false
    reason := ""
    
    // Оценка масштабирования вверх
    if cpu > a.config.ScaleUpThreshold || mem > a.config.ScaleUpThreshold {
        needScaleUp = true
        reason = fmt.Sprintf("high resource usage: cpu=%.2f, mem=%.2f", cpu, mem)
    } else if qps > 0 && conns > 0 {
        expectedLoad := float64(qps) / float64(10000)
        if expectedLoad > a.config.ScaleUpThreshold {
            needScaleUp = true
            reason = fmt.Sprintf("high load: qps=%d, connections=%d", qps, conns)
        }
    }
    
    // Оценка масштабирования вниз
    if cpu < a.config.ScaleDownThreshold && mem < a.config.ScaleDownThreshold && 
       float64(qps) < 100 && float64(conns) < 100 {
        needScaleDown = true
        reason = "low resource usage"
    }
    
    // Проверяем cooldown периоды
    now := time.Now().UnixMilli()
    lastUp := a.stats.LastScaleUpAt.Load()
    lastDown := a.stats.LastScaleDownAt.Load()
    
    if needScaleUp {
        if now-lastUp < int64(a.config.GetScaleUpCooldown().Seconds())*1000 {
            needScaleUp = false
        }
    }
    
    if needScaleDown {
        if now-lastDown < int64(a.config.GetScaleDownCooldown().Seconds())*1000 {
            needScaleDown = false
        }
    }
    
    // Выполняем масштабирование
    if needScaleUp && a.currentNodeCount < a.config.MaxNodes {
        a.scaleUp(reason)
    } else if needScaleDown && a.currentNodeCount > a.config.MinNodes {
        a.scaleDown(reason)
    }
}

// scaleUp выполняет масштабирование вверх
func (a *ClusterAutoscaler) scaleUp(reason string) {
    count := a.config.MaxScaleUpNodes
    if a.currentNodeCount+count > a.config.MaxNodes {
        count = a.config.MaxNodes - a.currentNodeCount
    }
    
    if count <= 0 {
        return
    }
    
    a.targetNodeCount = a.currentNodeCount + count
    
    if a.logger != nil {
        a.logger.Info(fmt.Sprintf("Scaling up: %d -> %d nodes (adding %d) reason: %s", 
            a.currentNodeCount, a.targetNodeCount, count, reason))
    }
    
    if a.scaleUpCallback != nil {
        if err := a.scaleUpCallback(count); err != nil {
            if a.logger != nil {
                a.logger.Error(fmt.Sprintf("Scale up failed: %v", err))
            }
            return
        }
    }
    
    a.currentNodeCount = a.targetNodeCount
    a.stats.TotalScaleUps.Add(1)
    a.stats.LastScaleUpAt.Store(time.Now().UnixMilli())
    
    a.stats.mu.Lock()
    a.stats.CurrentNodes = a.currentNodeCount
    a.stats.TargetNodes = a.targetNodeCount
    a.stats.mu.Unlock()
}

// scaleDown выполняет масштабирование вниз
func (a *ClusterAutoscaler) scaleDown(reason string) {
    count := a.config.MaxScaleDownNodes
    if a.currentNodeCount-count < a.config.MinNodes {
        count = a.currentNodeCount - a.config.MinNodes
    }
    
    if count <= 0 {
        return
    }
    
    a.targetNodeCount = a.currentNodeCount - count
    
    if a.logger != nil {
        a.logger.Info(fmt.Sprintf("Scaling down: %d -> %d nodes (removing %d) reason: %s", 
            a.currentNodeCount, a.targetNodeCount, count, reason))
    }
    
    if a.scaleDownCallback != nil {
        if err := a.scaleDownCallback(count); err != nil {
            if a.logger != nil {
                a.logger.Error(fmt.Sprintf("Scale down failed: %v", err))
            }
            return
        }
    }
    
    a.currentNodeCount = a.targetNodeCount
    a.stats.TotalScaleDowns.Add(1)
    a.stats.LastScaleDownAt.Store(time.Now().UnixMilli())
    
    a.stats.mu.Lock()
    a.stats.CurrentNodes = a.currentNodeCount
    a.stats.TargetNodes = a.targetNodeCount
    a.stats.mu.Unlock()
}

// GetStatus возвращает текущий статус автомасштабирования
func (a *ClusterAutoscaler) GetStatus() *AutoscalingStatus {
    a.mu.RLock()
    defer a.mu.RUnlock()
    
    return &AutoscalingStatus{
        Enabled:           a.config.Enabled,
        MinNodes:          a.config.MinNodes,
        MaxNodes:          a.config.MaxNodes,
        CurrentNodes:      a.currentNodeCount,
        TargetNodes:       a.targetNodeCount,
        CPU:               a.cpuLoad.Load(),
        Memory:            a.memLoad.Load(),
        QPS:               a.qps.Load(),
        Connections:       a.connections.Load(),
        LastEvaluationAt:  a.stats.LastEvaluationAt.Load(),
        ScaleUpThreshold:  a.config.ScaleUpThreshold,
        ScaleDownThreshold: a.config.ScaleDownThreshold,
        ScaleUpCooldown:   a.config.GetScaleUpCooldown(),
        ScaleDownCooldown: a.config.GetScaleDownCooldown(),
        TotalScaleUps:     a.stats.TotalScaleUps.Load(),
        TotalScaleDowns:   a.stats.TotalScaleDowns.Load(),
    }
}

// AutoscalingStatus статус автомасштабирования
type AutoscalingStatus struct {
    Enabled           bool          `json:"enabled"`
    MinNodes          int           `json:"min_nodes"`
    MaxNodes          int           `json:"max_nodes"`
    CurrentNodes      int           `json:"current_nodes"`
    TargetNodes       int           `json:"target_nodes"`
    CPU               float64       `json:"cpu_load"`
    Memory            float64       `json:"memory_load"`
    QPS               uint64        `json:"qps"`
    Connections       uint64        `json:"connections"`
    LastEvaluationAt  int64         `json:"last_evaluation_at"`
    ScaleUpThreshold  float64       `json:"scale_up_threshold"`
    ScaleDownThreshold float64      `json:"scale_down_threshold"`
    ScaleUpCooldown   time.Duration `json:"scale_up_cooldown"`
    ScaleDownCooldown time.Duration `json:"scale_down_cooldown"`
    TotalScaleUps     uint64        `json:"total_scale_ups"`
    TotalScaleDowns   uint64        `json:"total_scale_downs"`
}
