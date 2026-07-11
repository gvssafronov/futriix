/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/panic_recovery.go
// Назначение: Автоматическое восстановление после паники по всему коду

package cluster

import (
    "fmt"
    "runtime"
    "runtime/debug"
    "sync"
    "sync/atomic"
    "time"
)

// PanicInfo содержит информацию о панике
type PanicInfo struct {
    ID          string
    GoroutineID int64
    PanicValue  interface{}
    StackTrace  string
    Timestamp   int64
    Recovered   bool
    RecoveryTime int64
}

// PanicRecoveryManager управляет восстановлением после паник
type PanicRecoveryManager struct {
    mu            sync.RWMutex
    panics        map[string]*PanicInfo
    maxPanics     int
    recoveryFuncs map[string]func(interface{}) error
    logger        LoggerInterface
    stopChan      chan struct{}
    wg            sync.WaitGroup
    totalPanics   atomic.Uint64
    recoveredFrom atomic.Uint64
    failedRecoveries atomic.Uint64
}

// NewPanicRecoveryManager создаёт новый менеджер восстановления
func NewPanicRecoveryManager(logger LoggerInterface) *PanicRecoveryManager {
    prm := &PanicRecoveryManager{
        panics:        make(map[string]*PanicInfo),
        maxPanics:     1000,
        recoveryFuncs: make(map[string]func(interface{}) error),
        logger:        logger,
        stopChan:      make(chan struct{}),
    }
    
    prm.registerDefaultRecoveryFuncs()
    
    // Запускаем периодическую очистку старых паник
    prm.wg.Add(1)
    go prm.cleanupOldPanics()
    
    if logger != nil {
        logger.Info("Panic recovery manager initialized")
    }
    
    return prm
}

// getGoroutineID возвращает ID текущей горутины
func getGoroutineID() int64 {
    var buf [64]byte
    n := runtime.Stack(buf[:], false)
    var id int64
    fmt.Sscanf(string(buf[:n]), "goroutine %d", &id)
    return id
}

// RegisterRecoveryFunc регистрирует функцию восстановления для компонента
func (prm *PanicRecoveryManager) RegisterRecoveryFunc(component string, fn func(interface{}) error) {
    prm.mu.Lock()
    defer prm.mu.Unlock()
    prm.recoveryFuncs[component] = fn
}

// registerDefaultRecoveryFuncs регистрирует стандартные функции восстановления
func (prm *PanicRecoveryManager) registerDefaultRecoveryFuncs() {
    prm.recoveryFuncs["TCPServer"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["ConnectionHandler"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["Replication"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["WAL"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["Raft"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["PipelineReplicator"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["BatchCommitManager"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["ReshardingManager"] = func(p interface{}) error {
        return nil
    }
    prm.recoveryFuncs["RecoveryManager"] = func(p interface{}) error {
        return nil
    }
}

// Recover обрабатывает панику и пытается восстановиться
func (prm *PanicRecoveryManager) Recover(component string, context map[string]interface{}) {
    if r := recover(); r != nil {
        prm.totalPanics.Add(1)
        
        panicInfo := &PanicInfo{
            ID:          fmt.Sprintf("panic_%d_%d", time.Now().UnixNano(), prm.totalPanics.Load()),
            GoroutineID: getGoroutineID(),
            PanicValue:  r,
            StackTrace:  string(debug.Stack()),
            Timestamp:   time.Now().UnixMilli(),
            Recovered:   false,
        }
        
        prm.mu.Lock()
        if len(prm.panics) >= prm.maxPanics {
            for k := range prm.panics {
                delete(prm.panics, k)
                break
            }
        }
        prm.panics[panicInfo.ID] = panicInfo
        prm.mu.Unlock()
        
        if prm.logger != nil {
            prm.logger.Error(fmt.Sprintf("PANIC in component %s: %v\n%s", component, r, panicInfo.StackTrace))
        }
        
        // Пытаемся восстановиться
        if recoveryFn, ok := prm.recoveryFuncs[component]; ok {
            if err := recoveryFn(r); err != nil {
                prm.failedRecoveries.Add(1)
                if prm.logger != nil {
                    prm.logger.Error(fmt.Sprintf("Failed to recover from panic in %s: %v", component, err))
                }
            } else {
                panicInfo.Recovered = true
                panicInfo.RecoveryTime = time.Now().UnixMilli()
                prm.recoveredFrom.Add(1)
                
                if prm.logger != nil {
                    prm.logger.Info(fmt.Sprintf("Successfully recovered from panic in %s", component))
                }
            }
        } else {
            if prm.logger != nil {
                prm.logger.Error(fmt.Sprintf("No recovery function registered for component %s", component))
            }
        }
    }
}

// RecoverWithRetry пытается восстановиться с повторными попытками
func (prm *PanicRecoveryManager) RecoverWithRetry(component string, retries int, fn func() error) error {
    var lastErr error
    
    for i := 0; i < retries; i++ {
        func() {
            defer prm.Recover(component, map[string]interface{}{
                "attempt": i + 1,
                "retries": retries,
            })
            if fn != nil {
                lastErr = fn()
            }
        }()
        
        if lastErr == nil {
            return nil
        }
        
        if i < retries-1 {
            time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
        }
    }
    
    return lastErr
}

// SafeGo безопасно запускает горутину с восстановлением
func (prm *PanicRecoveryManager) SafeGo(component string, fn func()) {
    go func() {
        defer prm.Recover(component, nil)
        fn()
    }()
}

// SafeGoWithContext безопасно запускает горутину с контекстом
func (prm *PanicRecoveryManager) SafeGoWithContext(component string, ctx map[string]interface{}, fn func()) {
    go func() {
        defer prm.Recover(component, ctx)
        fn()
    }()
}

// cleanupOldPanics периодически очищает старые записи о паниках
func (prm *PanicRecoveryManager) cleanupOldPanics() {
    defer prm.wg.Done()
    
    ticker := time.NewTicker(1 * time.Hour)
    defer ticker.Stop()
    
    for {
        select {
        case <-prm.stopChan:
            return
        case <-ticker.C:
            prm.mu.Lock()
            now := time.Now().UnixMilli()
            for id, info := range prm.panics {
                if now-info.Timestamp > 24*3600*1000 {
                    delete(prm.panics, id)
                }
            }
            prm.mu.Unlock()
        }
    }
}

// GetPanicInfo возвращает информацию о панике
func (prm *PanicRecoveryManager) GetPanicInfo(id string) *PanicInfo {
    prm.mu.RLock()
    defer prm.mu.RUnlock()
    return prm.panics[id]
}

// GetAllPanics возвращает все паники
func (prm *PanicRecoveryManager) GetAllPanics() []*PanicInfo {
    prm.mu.RLock()
    defer prm.mu.RUnlock()
    
    result := make([]*PanicInfo, 0, len(prm.panics))
    for _, info := range prm.panics {
        result = append(result, info)
    }
    return result
}

// GetStats возвращает статистику
func (prm *PanicRecoveryManager) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "total_panics":      prm.totalPanics.Load(),
        "recovered_from":    prm.recoveredFrom.Load(),
        "failed_recoveries": prm.failedRecoveries.Load(),
        "active_panics":     len(prm.panics),
        "max_panics":        prm.maxPanics,
    }
}

// Stop останавливает менеджер
func (prm *PanicRecoveryManager) Stop() {
    close(prm.stopChan)
    prm.wg.Wait()
}

// RecoverableRoutine обёртка для восстанавливаемых горутин
type RecoverableRoutine struct {
    name       string
    fn         func() error
    mgr        *PanicRecoveryManager
    maxRetries int
    stopChan   chan struct{}
    running    atomic.Bool
    mu         sync.Mutex
}

// NewRecoverableRoutine создаёт новую восстанавливаемую горутину
func NewRecoverableRoutine(name string, fn func() error, mgr *PanicRecoveryManager, maxRetries int) *RecoverableRoutine {
    return &RecoverableRoutine{
        name:       name,
        fn:         fn,
        mgr:        mgr,
        maxRetries: maxRetries,
        stopChan:   make(chan struct{}),
    }
}

// Start запускает горутину с автоматическим восстановлением
func (rr *RecoverableRoutine) Start() {
    if !rr.running.CompareAndSwap(false, true) {
        return
    }
    
    go rr.run()
}

// Stop останавливает горутину
func (rr *RecoverableRoutine) Stop() {
    close(rr.stopChan)
    rr.running.Store(false)
}

// run запускает основной цикл с восстановлением
func (rr *RecoverableRoutine) run() {
    defer rr.running.Store(false)
    
    retries := 0
    
    for {
        select {
        case <-rr.stopChan:
            return
        default:
            err := rr.runWithRecovery()
            if err == nil {
                retries = 0
            } else {
                retries++
                if retries > rr.maxRetries {
                    if rr.mgr.logger != nil {
                        rr.mgr.logger.Error(fmt.Sprintf("Routine %s exceeded max retries (%d)", rr.name, rr.maxRetries))
                    }
                    return
                }
                
                backoff := time.Duration(100*retries) * time.Millisecond
                if backoff > 5*time.Second {
                    backoff = 5 * time.Second
                }
                
                select {
                case <-rr.stopChan:
                    return
                case <-time.After(backoff):
                }
            }
        }
    }
}

// runWithRecovery запускает функцию с защитой от паники
func (rr *RecoverableRoutine) runWithRecovery() (err error) {
    defer func() {
        if r := recover(); r != nil {
            rr.mgr.Recover(rr.name, map[string]interface{}{
                "routine": rr.name,
            })
            err = fmt.Errorf("panic recovered: %v", r)
        }
    }()
    
    if rr.fn != nil {
        return rr.fn()
    }
    return nil
}

// IsRunning возвращает статус работы
func (rr *RecoverableRoutine) IsRunning() bool {
    return rr.running.Load()
}
