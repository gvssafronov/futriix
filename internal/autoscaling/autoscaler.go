/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/api/rate_limiter.go (НОВЫЙ ФАЙЛ)
// Назначение: Rate limiting для API

package api

import (
    "fmt"
    "net/http"
    "sync"
    "sync/atomic"
    "time"
)

// RateLimiterConfig содержит конфигурацию rate limiter'а
type RateLimiterConfig struct {
    RequestsPerSecond int
    BurstSize         int
    CleanupInterval   time.Duration
    MaxEntries        int
}

// DefaultRateLimiterConfig возвращает конфигурацию по умолчанию
func DefaultRateLimiterConfig() *RateLimiterConfig {
    return &RateLimiterConfig{
        RequestsPerSecond: 100,
        BurstSize:         20,
        CleanupInterval:   5 * time.Minute,
        MaxEntries:        10000,
    }
}

// TokenBucket реализует алгоритм token bucket
type TokenBucket struct {
    tokens        float64
    maxTokens     float64
    refillRate    float64
    lastRefill    time.Time
    mu            sync.Mutex
}

// NewTokenBucket создаёт новый token bucket
func NewTokenBucket(ratePerSecond float64, burst int) *TokenBucket {
    return &TokenBucket{
        tokens:     float64(burst),
        maxTokens:  float64(burst),
        refillRate: ratePerSecond,
        lastRefill: time.Now(),
    }
}

// Allow проверяет, разрешён ли запрос
func (tb *TokenBucket) Allow() bool {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    
    now := time.Now()
    elapsed := now.Sub(tb.lastRefill).Seconds()
    tb.tokens += elapsed * tb.refillRate
    if tb.tokens > tb.maxTokens {
        tb.tokens = tb.maxTokens
    }
    tb.lastRefill = now
    
    if tb.tokens >= 1.0 {
        tb.tokens -= 1.0
        return true
    }
    return false
}

// RateLimiter управляет rate limiting для API
type RateLimiter struct {
    config      *RateLimiterConfig
    limiters    sync.Map
    rejected    atomic.Uint64
    allowed     atomic.Uint64
    stopChan    chan struct{}
    wg          sync.WaitGroup
}

// NewRateLimiter создаёт новый rate limiter
func NewRateLimiter(config *RateLimiterConfig) *RateLimiter {
    if config == nil {
        config = DefaultRateLimiterConfig()
    }
    
    rl := &RateLimiter{
        config:   config,
        stopChan: make(chan struct{}),
    }
    
    rl.wg.Add(1)
    go rl.cleanupLoop()
    
    return rl
}

// getLimiter возвращает или создаёт лимитер для ключа
func (rl *RateLimiter) getLimiter(key string) *TokenBucket {
    if val, ok := rl.limiters.Load(key); ok {
        return val.(*TokenBucket)
    }
    
    limiter := NewTokenBucket(float64(rl.config.RequestsPerSecond), rl.config.BurstSize)
    actual, _ := rl.limiters.LoadOrStore(key, limiter)
    return actual.(*TokenBucket)
}

// Allow проверяет, разрешён ли запрос для ключа
func (rl *RateLimiter) Allow(key string) bool {
    limiter := rl.getLimiter(key)
    allowed := limiter.Allow()
    
    if allowed {
        rl.allowed.Add(1)
    } else {
        rl.rejected.Add(1)
    }
    
    return allowed
}

// Middleware возвращает middleware для HTTP
func (rl *RateLimiter) Middleware(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Используем IP + User-Agent как ключ
        key := r.RemoteAddr
        if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
            key = forwarded
        }
        
        if !rl.Allow(key) {
            w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.config.RequestsPerSecond))
            w.Header().Set("X-RateLimit-Remaining", "0")
            w.Header().Set("Retry-After", "1")
            http.Error(w, "Rate limit exceeded. Please try again later.", http.StatusTooManyRequests)
            return
        }
        
        next(w, r)
    }
}

// cleanupLoop периодически очищает старые лимитеры
func (rl *RateLimiter) cleanupLoop() {
    defer rl.wg.Done()
    
    ticker := time.NewTicker(rl.config.CleanupInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            rl.cleanup()
        case <-rl.stopChan:
            return
        }
    }
}

// cleanup удаляет старые лимитеры
func (rl *RateLimiter) cleanup() {
    count := 0
    rl.limiters.Range(func(key, value interface{}) bool {
        count++
        if count > rl.config.MaxEntries {
            rl.limiters.Delete(key)
        }
        return true
    })
}

// GetStats возвращает статистику rate limiter'а
func (rl *RateLimiter) GetStats() map[string]interface{} {
    count := 0
    rl.limiters.Range(func(key, value interface{}) bool {
        count++
        return true
    })
    
    return map[string]interface{}{
        "active_limiters": count,
        "allowed_requests": rl.allowed.Load(),
        "rejected_requests": rl.rejected.Load(),
        "requests_per_second": rl.config.RequestsPerSecond,
        "burst_size": rl.config.BurstSize,
    }
}

// Stop останавливает rate limiter
func (rl *RateLimiter) Stop() {
    close(rl.stopChan)
    rl.wg.Wait()
}
