/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/worker_pool.go
// Назначение: Lock-free пул воркеров для ограничения количества горутин.
// Использует атомарные операции и lock-free структуры данных.

package cluster

import (
    "fmt"
    "runtime/debug"
    "sync"
    "sync/atomic"
    "time"
    
    "futriis/internal/log"
)

// WorkerPool представляет lock-free пул воркеров для выполнения задач
type WorkerPool struct {
    maxWorkers    int32           // Максимальное количество воркеров
    activeWorkers atomic.Int32    // Текущее количество активных воркеров
    tasks         *LockFreeQueue  // Lock-free очередь задач
    stopChan      chan struct{}
    wg            sync.WaitGroup
    logger        *log.Logger
    
    // Статистика (атомарные счётчики)
    submittedTasks   atomic.Uint64
    completedTasks   atomic.Uint64
    failedTasks      atomic.Uint64
    rejectedTasks    atomic.Uint64
    lastSubmitTime   atomic.Int64
    lastCompleteTime atomic.Int64
}

// Task представляет задачу для выполнения в пуле
type Task struct {
    ID         string
    Execute    func() error
    CreatedAt  int64
    RetryCount int32
}

// LockFreeQueue представляет lock-free очередь на основе CAS операций
type LockFreeQueue struct {
    head atomic.Value // *queueNode
    tail atomic.Value // *queueNode
    size atomic.Int64
}

type queueNode struct {
    value *Task
    next  atomic.Value // *queueNode
}

// NewLockFreeQueue создаёт новую lock-free очередь
func NewLockFreeQueue() *LockFreeQueue {
    q := &LockFreeQueue{}
    dummy := &queueNode{}
    q.head.Store(dummy)
    q.tail.Store(dummy)
    return q
}

// Enqueue добавляет задачу в очередь (lock-free)
func (q *LockFreeQueue) Enqueue(task *Task) bool {
    newNode := &queueNode{value: task}
    
    for {
        tailVal := q.tail.Load()
        if tailVal == nil {
            continue
        }
        tail := tailVal.(*queueNode)
        
        nextVal := tail.next.Load()
        var next *queueNode
        if nextVal != nil {
            next = nextVal.(*queueNode)
        }
        
        if tailVal != q.tail.Load() {
            continue
        }
        
        if next != nil {
            q.tail.CompareAndSwap(tailVal, next)
            continue
        }
        
        if tail.next.CompareAndSwap(nil, newNode) {
            q.tail.CompareAndSwap(tailVal, newNode)
            q.size.Add(1)
            return true
        }
    }
}

// Dequeue извлекает задачу из очереди (lock-free)
func (q *LockFreeQueue) Dequeue() *Task {
    for {
        headVal := q.head.Load()
        if headVal == nil {
            return nil
        }
        head := headVal.(*queueNode)
        
        tailVal := q.tail.Load()
        if tailVal == nil {
            return nil
        }
        tail := tailVal.(*queueNode)
        
        nextVal := head.next.Load()
        var next *queueNode
        if nextVal != nil {
            next = nextVal.(*queueNode)
        }
        
        if headVal != q.head.Load() {
            continue
        }
        
        if head == tail {
            if next == nil {
                return nil
            }
            q.tail.CompareAndSwap(tailVal, next)
            continue
        }
        
        if next == nil {
            return nil
        }
        
        task := next.value
        if q.head.CompareAndSwap(headVal, next) {
            q.size.Add(-1)
            return task
        }
    }
}

// Size возвращает текущий размер очереди (lock-free)
func (q *LockFreeQueue) Size() int64 {
    return q.size.Load()
}

// NewWorkerPool создаёт новый lock-free пул воркеров
func NewWorkerPool(maxWorkers int, logger *log.Logger) *WorkerPool {
    if maxWorkers <= 0 {
        maxWorkers = 500
    }
    
    wp := &WorkerPool{
        maxWorkers: int32(maxWorkers),
        tasks:      NewLockFreeQueue(),
        stopChan:   make(chan struct{}),
        logger:     logger,
    }
    
    // Запускаем диспетчер задач
    go wp.dispatcher()
    
    if logger != nil {
        logger.Debug(fmt.Sprintf("Lock-free worker pool created: maxWorkers=%d", maxWorkers))
    }
    
    return wp
}

// dispatcher управляет воркерами
func (wp *WorkerPool) dispatcher() {
    defer func() {
        if r := recover(); r != nil {
            if wp.logger != nil {
                wp.logger.Error(fmt.Sprintf("Worker pool dispatcher panicked: %v\n%s", r, debug.Stack()))
            }
            // Перезапускаем диспетчер
            go wp.dispatcher()
        }
    }()
    
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()
    
    for {
        select {
        case <-wp.stopChan:
            return
        case <-ticker.C:
            // Динамически регулируем количество воркеров
            wp.adjustWorkers()
        }
    }
}

// adjustWorkers динамически регулирует количество воркеров
func (wp *WorkerPool) adjustWorkers() {
    queueSize := wp.tasks.Size()
    activeWorkers := wp.activeWorkers.Load()
    
    // Если есть задачи и есть место для новых воркеров
    if queueSize > 0 && activeWorkers < wp.maxWorkers {
        if activeWorkers == 0 || queueSize > int64(activeWorkers)*2 {
            if wp.activeWorkers.CompareAndSwap(activeWorkers, activeWorkers+1) {
                wp.wg.Add(1)
                go wp.worker()
                if wp.logger != nil {
                    wp.logger.Debug(fmt.Sprintf("Added worker, active: %d/%d", activeWorkers+1, wp.maxWorkers))
                }
            }
        }
    }
    
    // Уменьшаем количество воркеров, если нет задач
    if queueSize == 0 && activeWorkers > 0 {
        if activeWorkers > 1 {
            lastComplete := wp.lastCompleteTime.Load()
            if time.Now().UnixMilli()-lastComplete > 5000 {
                task := &Task{
                    ID:        fmt.Sprintf("stop_worker_%d", time.Now().UnixNano()),
                    Execute:   func() error { return nil },
                    CreatedAt: time.Now().UnixMilli(),
                }
                if wp.tasks.Enqueue(task) {
                    if wp.logger != nil {
                        wp.logger.Debug(fmt.Sprintf("Stop signal sent to worker, active: %d/%d", activeWorkers-1, wp.maxWorkers))
                    }
                }
            }
        }
    }
}

// worker выполняет задачи из очереди
func (wp *WorkerPool) worker() {
    defer func() {
        if r := recover(); r != nil {
            if wp.logger != nil {
                wp.logger.Error(fmt.Sprintf("Worker panicked: %v\n%s", r, debug.Stack()))
            }
            wp.activeWorkers.Add(-1)
            wp.wg.Done()
            // Создаём нового воркера вместо упавшего
            if wp.activeWorkers.Load() < wp.maxWorkers {
                wp.activeWorkers.Add(1)
                wp.wg.Add(1)
                go wp.worker()
            }
        }
    }()
    
    for {
        select {
        case <-wp.stopChan:
            wp.activeWorkers.Add(-1)
            wp.wg.Done()
            return
        default:
            task := wp.tasks.Dequeue()
            if task == nil {
                wp.activeWorkers.Add(-1)
                wp.wg.Done()
                return
            }
            
            // Проверяем специальную задачу остановки
            if len(task.ID) >= 12 && task.ID[:12] == "stop_worker_" {
                wp.activeWorkers.Add(-1)
                wp.wg.Done()
                return
            }
            
            // Выполняем задачу
            startTime := time.Now().UnixMilli()
            err := task.Execute()
            duration := time.Now().UnixMilli() - startTime
            
            if err != nil {
                wp.failedTasks.Add(1)
                if wp.logger != nil && duration > 100 {
                    wp.logger.Warn(fmt.Sprintf("Task %s failed after %dms: %v", task.ID, duration, err))
                }
            } else {
                wp.completedTasks.Add(1)
                wp.lastCompleteTime.Store(time.Now().UnixMilli())
                if wp.logger != nil && duration > 1000 {
                    wp.logger.Debug(fmt.Sprintf("Task %s completed in %dms", task.ID, duration))
                }
            }
        }
    }
}

// Submit отправляет задачу в пул
func (wp *WorkerPool) Submit(task *Task) error {
    if task.Execute == nil {
        return fmt.Errorf("task execute function is nil")
    }
    
    task.CreatedAt = time.Now().UnixMilli()
    
    if !wp.tasks.Enqueue(task) {
        wp.rejectedTasks.Add(1)
        return fmt.Errorf("failed to enqueue task (queue full)")
    }
    
    wp.submittedTasks.Add(1)
    wp.lastSubmitTime.Store(task.CreatedAt)
    
    // Асинхронно добавляем воркера при необходимости
    if wp.activeWorkers.Load() == 0 {
        if wp.activeWorkers.CompareAndSwap(0, 1) {
            wp.wg.Add(1)
            go wp.worker()
        }
    }
    
    return nil
}

// SubmitFunc отправляет функцию как задачу
func (wp *WorkerPool) SubmitFunc(id string, fn func() error) error {
    return wp.Submit(&Task{
        ID:      id,
        Execute: fn,
    })
}

// GetStats возвращает статистику пула
func (wp *WorkerPool) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "max_workers":        wp.maxWorkers,
        "active_workers":     wp.activeWorkers.Load(),
        "queue_size":         wp.tasks.Size(),
        "submitted_tasks":    wp.submittedTasks.Load(),
        "completed_tasks":    wp.completedTasks.Load(),
        "failed_tasks":       wp.failedTasks.Load(),
        "rejected_tasks":     wp.rejectedTasks.Load(),
        "last_submit_time":   wp.lastSubmitTime.Load(),
        "last_complete_time": wp.lastCompleteTime.Load(),
    }
}

// Stop останавливает пул воркеров
func (wp *WorkerPool) Stop() {
    close(wp.stopChan)
    
    done := make(chan struct{})
    go func() {
        wp.wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        if wp.logger != nil {
            wp.logger.Debug("Worker pool stopped gracefully")
        }
    case <-time.After(10 * time.Second):
        if wp.logger != nil {
            wp.logger.Warn("Worker pool stop timeout, forcing shutdown")
        }
    }
}
