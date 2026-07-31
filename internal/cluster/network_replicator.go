/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/network_replicator.go
// Назначение: Сетевой репликатор для отправки данных между узлами кластера.
// Реализует надёжную доставку с повторными попытками, экспоненциальным backoff,
// джиттером и асинхронной отправкой через пул воркеров.

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"futriis/internal/log"
)

// =============================================================================
// КОНФИГУРАЦИЯ РЕПЛИКАЦИИ
// =============================================================================

// ReplicationRetryConfig определяет конфигурацию повторных попыток для репликации.
type ReplicationRetryConfig struct {
	MaxRetries     int           // Максимальное количество попыток
	InitialBackoff time.Duration // Начальная задержка между попытками
	MaxBackoff     time.Duration // Максимальная задержка
	BackoffFactor  float64       // Множитель для экспоненциального увеличения задержки
	JitterEnabled  bool          // Включение случайного джиттера
	JitterPercent  float64       // Процент джиттера (0.0 - 1.0)
}

// DefaultReplicationRetryConfig возвращает конфигурацию по умолчанию.
func DefaultReplicationRetryConfig() *ReplicationRetryConfig {
	return &ReplicationRetryConfig{
		MaxRetries:     5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		BackoffFactor:  2.0,
		JitterEnabled:  true,
		JitterPercent:  0.3,
	}
}

// =============================================================================
// СТАТИСТИКА РЕПЛИКАЦИИ
// =============================================================================

// ReplicationStats содержит статистику работы репликатора.
type ReplicationStats struct {
	mu                  sync.RWMutex
	TotalReplications   uint64            // Всего репликаций
	SuccessfulReplicas  uint64            // Успешных репликаций
	FailedReplicas      uint64            // Неудачных репликаций
	RetriedReplicas     uint64            // Репликаций с повторными попытками
	ReplicationDuration time.Duration     // Общая длительность репликаций
	TargetStats         map[string]*TargetStats // Статистика по целевым узлам
}

// TargetStats содержит статистику для конкретного узла.
type TargetStats struct {
	mu              sync.RWMutex
	TotalAttempts   uint64
	Successful      uint64
	Failed          uint64
	LastFailure     int64
	LastSuccess     int64
	FailureCount    uint64
	SuccessiveFails uint64
}

// NewReplicationStats создаёт новую статистику.
func NewReplicationStats() *ReplicationStats {
	return &ReplicationStats{
		TargetStats: make(map[string]*TargetStats),
	}
}

// GetOrCreateTargetStats возвращает или создаёт статистику для целевого узла.
func (rs *ReplicationStats) GetOrCreateTargetStats(targetID string) *TargetStats {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if stats, ok := rs.TargetStats[targetID]; ok {
		return stats
	}

	stats := &TargetStats{}
	rs.TargetStats[targetID] = stats
	return stats
}

// =============================================================================
// РЕПЛИКАЦИОННОЕ СОЕДИНЕНИЕ
// =============================================================================

// ReplicationConnection представляет соединение для репликации.
type ReplicationConnection struct {
	targetID   string
	targetAddr string
	conn       net.Conn
	mu         sync.Mutex
	lastUsed   int64
	closed     bool
}

// NewReplicationConnection создаёт новое соединение.
func NewReplicationConnection(targetID, targetAddr string) *ReplicationConnection {
	return &ReplicationConnection{
		targetID:   targetID,
		targetAddr: targetAddr,
		lastUsed:   time.Now().UnixMilli(),
	}
}

// Connect устанавливает соединение.
func (rc *ReplicationConnection) Connect(timeout time.Duration) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.closed {
		return fmt.Errorf("connection already closed")
	}

	if rc.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", rc.targetAddr, timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", rc.targetAddr, err)
	}

	rc.conn = conn
	rc.lastUsed = time.Now().UnixMilli()
	return nil
}

// Send отправляет данные через соединение.
func (rc *ReplicationConnection) Send(data []byte, timeout time.Duration) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.closed {
		return fmt.Errorf("connection already closed")
	}

	if rc.conn == nil {
		return fmt.Errorf("connection not established")
	}

	if err := rc.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	// Отправляем длину сообщения (4 байта, big-endian)
	length := uint32(len(data))
	lenBuf := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}

	if _, err := rc.conn.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to send length: %v", err)
	}

	// Отправляем данные
	_, err := rc.conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send data: %v", err)
	}

	rc.lastUsed = time.Now().UnixMilli()
	return nil
}

// Close закрывает соединение.
func (rc *ReplicationConnection) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.closed {
		return nil
	}

	rc.closed = true
	if rc.conn != nil {
		return rc.conn.Close()
	}
	return nil
}

// IsActive проверяет, активно ли соединение.
func (rc *ReplicationConnection) IsActive() bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.closed {
		return false
	}
	if rc.conn == nil {
		return false
	}

	// Проверка соединения
	rc.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1)
	_, err := rc.conn.Read(buf)
	rc.conn.SetReadDeadline(time.Time{})

	return err == nil
}

// =============================================================================
// ОСНОВНОЙ РЕПЛИКАТОР
// =============================================================================

// NetworkReplicator реализует сетевую репликацию данных между узлами кластера.
// Поддерживает:
//   - Асинхронную отправку через пул воркеров
//   - Экспоненциальный backoff с джиттером
//   - Повторные попытки при ошибках
//   - Управление соединениями с keep-alive
//   - Подробную статистику
type NetworkReplicator struct {
	config         *ReplicationRetryConfig
	workerPool     *WorkerPool
	logger         *log.Logger
	stats          *ReplicationStats
	connections    sync.Map // map[string]*ReplicationConnection
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	enabled        atomic.Bool
	connectionTTL  time.Duration
	cleanupTicker  *time.Ticker
	requestTimeout time.Duration

	// Счётчики для метрик
	bytesSent    atomic.Uint64
	bytesReceived atomic.Uint64
}

// NewNetworkReplicator создаёт новый экземпляр сетевого репликатора.
func NewNetworkReplicator(config *ReplicationRetryConfig, workerPool *WorkerPool, logger *log.Logger) *NetworkReplicator {
	if config == nil {
		config = DefaultReplicationRetryConfig()
	}

	// Ограничиваем максимальное количество попыток
	if config.MaxRetries < 1 {
		config.MaxRetries = 1
	}

	// Ограничиваем начальную задержку
	if config.InitialBackoff < 10*time.Millisecond {
		config.InitialBackoff = 10 * time.Millisecond
	}

	// Ограничиваем максимальную задержку
	if config.MaxBackoff < config.InitialBackoff {
		config.MaxBackoff = config.InitialBackoff * 10
	}

	// Ограничиваем фактор backoff
	if config.BackoffFactor < 1.0 {
		config.BackoffFactor = 1.5
	}
	if config.BackoffFactor > 10.0 {
		config.BackoffFactor = 10.0
	}

	// Ограничиваем процент джиттера
	if config.JitterPercent < 0 {
		config.JitterPercent = 0
	}
	if config.JitterPercent > 1.0 {
		config.JitterPercent = 1.0
	}

	nr := &NetworkReplicator{
		config:         config,
		workerPool:     workerPool,
		logger:         logger,
		stats:          NewReplicationStats(),
		stopChan:       make(chan struct{}),
		connectionTTL:  30 * time.Second,
		requestTimeout: 10 * time.Second,
		cleanupTicker:  time.NewTicker(60 * time.Second),
	}

	nr.enabled.Store(true)

	// Запускаем горутину для очистки неактивных соединений
	nr.wg.Add(1)
	go nr.cleanupLoop()

	if logger != nil {
		logger.Info(fmt.Sprintf("Network replicator created: max_retries=%d, initial_backoff=%v, max_backoff=%v",
			config.MaxRetries, config.InitialBackoff, config.MaxBackoff))
	}

	return nr
}

// =============================================================================
// ОСНОВНЫЕ МЕТОДЫ
// =============================================================================

// Replicate выполняет репликацию данных на целевой узел.
// Использует механизм повторных попыток с экспоненциальным backoff.
func (nr *NetworkReplicator) Replicate(targetNodeID, targetAddress string, data []byte) error {
	if !nr.enabled.Load() {
		return fmt.Errorf("replicator is disabled")
	}

	if targetAddress == "" {
		return fmt.Errorf("target address is empty")
	}

	if len(data) == 0 {
		return fmt.Errorf("data is empty")
	}

	startTime := time.Now()

	// Обновляем статистику
	nr.stats.mu.Lock()
	nr.stats.TotalReplications++
	nr.stats.mu.Unlock()

	// Получаем или создаём статистику для целевого узла
	targetStats := nr.stats.GetOrCreateTargetStats(targetNodeID)

	// Выполняем репликацию с повторными попытками
	var lastErr error
	var attempt int

	for attempt = 0; attempt < nr.config.MaxRetries; attempt++ {
		targetStats.mu.Lock()
		targetStats.TotalAttempts++
		targetStats.mu.Unlock()

		// Проверяем, не остановлен ли репликатор
		select {
		case <-nr.stopChan:
			return fmt.Errorf("replicator stopped")
		default:
		}

		// Выполняем попытку репликации
		err := nr.doReplicate(targetNodeID, targetAddress, data)
		if err == nil {
			// Успешно
			targetStats.mu.Lock()
			targetStats.Successful++
			targetStats.LastSuccess = time.Now().UnixMilli()
			targetStats.SuccessiveFails = 0
			targetStats.mu.Unlock()

			nr.stats.mu.Lock()
			nr.stats.SuccessfulReplicas++
			nr.stats.ReplicationDuration += time.Since(startTime)
			nr.stats.mu.Unlock()

			nr.bytesSent.Add(uint64(len(data)))

			if nr.logger != nil && attempt > 0 {
				nr.logger.Debug(fmt.Sprintf("Replication to %s succeeded after %d attempts", targetNodeID, attempt+1))
			}

			return nil
		}

		lastErr = err

		targetStats.mu.Lock()
		targetStats.Failed++
		targetStats.LastFailure = time.Now().UnixMilli()
		targetStats.SuccessiveFails++
		targetStats.mu.Unlock()

		nr.stats.mu.Lock()
		nr.stats.FailedReplicas++
		nr.stats.mu.Unlock()

		// Если это была последняя попытка, выходим
		if attempt == nr.config.MaxRetries-1 {
			break
		}

		// Вычисляем задержку перед следующей попыткой с джиттером
		delay := nr.calculateBackoff(attempt)

		if nr.logger != nil && attempt < 3 {
			nr.logger.Debug(fmt.Sprintf("Replication to %s failed (attempt %d/%d): %v, retrying in %v",
				targetNodeID, attempt+1, nr.config.MaxRetries, err, delay))
		}

		// Ожидаем перед следующей попыткой
		select {
		case <-nr.stopChan:
			return fmt.Errorf("replicator stopped during retry")
		case <-time.After(delay):
			// Продолжаем
		}
	}

	// Все попытки исчерпаны
	if nr.logger != nil {
		nr.logger.Error(fmt.Sprintf("Replication to %s failed after %d attempts: %v",
			targetNodeID, nr.config.MaxRetries, lastErr))
	}

	return fmt.Errorf("replication failed after %d attempts: %v", nr.config.MaxRetries, lastErr)
}

// doReplicate выполняет одну попытку репликации.
func (nr *NetworkReplicator) doReplicate(targetNodeID, targetAddress string, data []byte) error {
	// Получаем или создаём соединение
	conn, err := nr.getOrCreateConnection(targetNodeID, targetAddress)
	if err != nil {
		return err
	}

	// Создаём запрос
	req := ReplicationRequest{
		Type:      "replicate",
		FromNode:  targetNodeID,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	// Отправляем данные
	if err := conn.Send(reqData, nr.requestTimeout); err != nil {
		// Закрываем соединение при ошибке
		conn.Close()
		nr.connections.Delete(targetNodeID)
		return fmt.Errorf("send failed: %v", err)
	}

	// Читаем ответ
	response, err := nr.readResponse(conn)
	if err != nil {
		conn.Close()
		nr.connections.Delete(targetNodeID)
		return fmt.Errorf("response failed: %v", err)
	}

	// Проверяем ответ
	if response.Status != "success" {
		return fmt.Errorf("remote error: %s", response.Error)
	}

	return nil
}

// readResponse читает ответ от удалённого узла.
func (nr *NetworkReplicator) readResponse(conn *ReplicationConnection) (*ReplicationResponse, error) {
	conn.mu.Lock()
	rawConn := conn.conn
	conn.mu.Unlock()

	if rawConn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Устанавливаем таймаут на чтение
	if err := rawConn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %v", err)
	}
	defer rawConn.SetReadDeadline(time.Time{})

	// Читаем длину сообщения (4 байта)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(rawConn, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read message length: %v", err)
	}

	length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if length > 10*1024*1024 { // 10 MB лимит
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}

	// Читаем данные
	data := make([]byte, length)
	if _, err := io.ReadFull(rawConn, data); err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	nr.bytesReceived.Add(uint64(len(data)))

	// Декодируем ответ
	var response ReplicationResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &response, nil
}

// =============================================================================
// УПРАВЛЕНИЕ СОЕДИНЕНИЯМИ
// =============================================================================

// getOrCreateConnection получает существующее или создаёт новое соединение.
func (nr *NetworkReplicator) getOrCreateConnection(targetID, targetAddr string) (*ReplicationConnection, error) {
	// Пытаемся получить существующее соединение
	if val, ok := nr.connections.Load(targetID); ok {
		conn := val.(*ReplicationConnection)
		if conn.IsActive() {
			return conn, nil
		}
		// Соединение неактивно, удаляем и создаём новое
		nr.connections.Delete(targetID)
		conn.Close()
	}

	// Создаём новое соединение
	conn := NewReplicationConnection(targetID, targetAddr)
	if err := conn.Connect(5 * time.Second); err != nil {
		return nil, err
	}

	nr.connections.Store(targetID, conn)
	return conn, nil
}

// cleanupLoop периодически очищает неактивные соединения.
func (nr *NetworkReplicator) cleanupLoop() {
	defer nr.wg.Done()

	for {
		select {
		case <-nr.stopChan:
			return
		case <-nr.cleanupTicker.C:
			nr.cleanupConnections()
		}
	}
}

// cleanupConnections удаляет неактивные и устаревшие соединения.
func (nr *NetworkReplicator) cleanupConnections() {
	now := time.Now().UnixMilli()
	ttl := int64(nr.connectionTTL.Milliseconds())

	var toDelete []string

	nr.connections.Range(func(key, value interface{}) bool {
		targetID := key.(string)
		conn := value.(*ReplicationConnection)

		// Проверяем время последнего использования
		if now-conn.lastUsed > ttl {
			toDelete = append(toDelete, targetID)
			return true
		}

		// Проверяем активность
		if !conn.IsActive() {
			toDelete = append(toDelete, targetID)
		}

		return true
	})

	for _, targetID := range toDelete {
		if val, ok := nr.connections.Load(targetID); ok {
			conn := val.(*ReplicationConnection)
			conn.Close()
			nr.connections.Delete(targetID)
			if nr.logger != nil {
				nr.logger.Debug(fmt.Sprintf("Cleaned up connection to %s", targetID))
			}
		}
	}
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
// =============================================================================

// calculateBackoff вычисляет задержку для повторной попытки.
// Использует экспоненциальный backoff с джиттером.
func (nr *NetworkReplicator) calculateBackoff(attempt int) time.Duration {
	// Экспоненциальный backoff: initial * factor^attempt
	backoff := float64(nr.config.InitialBackoff) * math.Pow(nr.config.BackoffFactor, float64(attempt))

	// Ограничиваем максимальной задержкой
	if backoff > float64(nr.config.MaxBackoff) {
		backoff = float64(nr.config.MaxBackoff)
	}

	// Применяем джиттер
	if nr.config.JitterEnabled {
		jitter := float64(time.Duration(backoff)) * nr.config.JitterPercent
		backoff += (rand.Float64()*2 - 1) * jitter
	}

	// Ограничиваем минимальной задержкой
	if backoff < float64(nr.config.InitialBackoff) {
		backoff = float64(nr.config.InitialBackoff)
	}

	return time.Duration(backoff)
}

// =============================================================================
// УПРАВЛЕНИЕ СОСТОЯНИЕМ
// =============================================================================

// Enable включает репликатор.
func (nr *NetworkReplicator) Enable() {
	nr.enabled.Store(true)
	if nr.logger != nil {
		nr.logger.Info("Network replicator enabled")
	}
}

// Disable отключает репликатор.
func (nr *NetworkReplicator) Disable() {
	nr.enabled.Store(false)
	if nr.logger != nil {
		nr.logger.Info("Network replicator disabled")
	}
}

// IsEnabled возвращает состояние репликатора.
func (nr *NetworkReplicator) IsEnabled() bool {
	return nr.enabled.Load()
}

// Close закрывает репликатор и освобождает ресурсы.
func (nr *NetworkReplicator) Close() error {
	nr.enabled.Store(false)

	close(nr.stopChan)

	// Закрываем все соединения
	nr.connections.Range(func(key, value interface{}) bool {
		conn := value.(*ReplicationConnection)
		conn.Close()
		return true
	})

	nr.wg.Wait()

	if nr.logger != nil {
		nr.logger.Info("Network replicator closed")
	}

	return nil
}

// =============================================================================
// СТАТИСТИКА
// =============================================================================

// GetStats возвращает статистику работы репликатора.
func (nr *NetworkReplicator) GetStats() map[string]interface{} {
	nr.stats.mu.RLock()
	defer nr.stats.mu.RUnlock()

	targetStats := make(map[string]interface{})
	for targetID, stats := range nr.stats.TargetStats {
		stats.mu.RLock()
		targetStats[targetID] = map[string]interface{}{
			"total_attempts":   stats.TotalAttempts,
			"successful":       stats.Successful,
			"failed":           stats.Failed,
			"last_failure":     stats.LastFailure,
			"last_success":     stats.LastSuccess,
			"successive_fails": stats.SuccessiveFails,
		}
		stats.mu.RUnlock()
	}

	return map[string]interface{}{
		"enabled":             nr.enabled.Load(),
		"total_replications":  nr.stats.TotalReplications,
		"successful_replicas": nr.stats.SuccessfulReplicas,
		"failed_replicas":     nr.stats.FailedReplicas,
		"retried_replicas":    nr.stats.RetriedReplicas,
		"bytes_sent":          nr.bytesSent.Load(),
		"bytes_received":      nr.bytesReceived.Load(),
		"active_connections":  nr.getActiveConnectionsCount(),
		"target_stats":        targetStats,
		"config": map[string]interface{}{
			"max_retries":      nr.config.MaxRetries,
			"initial_backoff":  nr.config.InitialBackoff.String(),
			"max_backoff":      nr.config.MaxBackoff.String(),
			"backoff_factor":   nr.config.BackoffFactor,
			"jitter_enabled":   nr.config.JitterEnabled,
			"connection_ttl":   nr.connectionTTL.String(),
			"request_timeout":  nr.requestTimeout.String(),
		},
	}
}

// getActiveConnectionsCount возвращает количество активных соединений.
func (nr *NetworkReplicator) getActiveConnectionsCount() int {
	count := 0
	nr.connections.Range(func(key, value interface{}) bool {
		conn := value.(*ReplicationConnection)
		if conn.IsActive() {
			count++
		}
		return true
	})
	return count
}

// GetTargetStats возвращает статистику для конкретного целевого узла.
func (nr *NetworkReplicator) GetTargetStats(targetID string) map[string]interface{} {
	stats := nr.stats.GetOrCreateTargetStats(targetID)

	stats.mu.RLock()
	defer stats.mu.RUnlock()

	return map[string]interface{}{
		"total_attempts":   stats.TotalAttempts,
		"successful":       stats.Successful,
		"failed":           stats.Failed,
		"last_failure":     stats.LastFailure,
		"last_success":     stats.LastSuccess,
		"successive_fails": stats.SuccessiveFails,
	}
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ СТРУКТУРЫ
// =============================================================================

// ReplicationRequest представляет запрос репликации.
type ReplicationRequest struct {
	Type      string          `json:"type"`
	FromNode  string          `json:"from_node"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

// ReplicationResponse представляет ответ на запрос репликации.
type ReplicationResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// =============================================================================
// БЫСТРАЯ РЕПЛИКАЦИЯ (СИНХРОННАЯ)
// =============================================================================

// ReplicateSync выполняет синхронную репликацию с подтверждением от целевого узла.
// Используется для критически важных операций, требующих гарантированной доставки.
func (nr *NetworkReplicator) ReplicateSync(targetNodeID, targetAddress string, data []byte, timeout time.Duration) error {
	if !nr.enabled.Load() {
		return fmt.Errorf("replicator is disabled")
	}

	if targetAddress == "" {
		return fmt.Errorf("target address is empty")
	}

	if len(data) == 0 {
		return fmt.Errorf("data is empty")
	}

	// Создаём контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Канал для результата
	resultChan := make(chan error, 1)

	// Выполняем репликацию в горутине
	go func() {
		resultChan <- nr.Replicate(targetNodeID, targetAddress, data)
	}()

	// Ожидаем результат или таймаут
	select {
	case err := <-resultChan:
		return err
	case <-ctx.Done():
		return fmt.Errorf("replication timeout after %v", timeout)
	}
}

// =============================================================================
// АСИНХРОННАЯ РЕПЛИКАЦИЯ (НЕБЛОКИРУЮЩАЯ)
// =============================================================================

// ReplicateAsync выполняет асинхронную репликацию.
// Возвращает сразу, не дожидаясь подтверждения.
func (nr *NetworkReplicator) ReplicateAsync(targetNodeID, targetAddress string, data []byte, callback func(error)) error {
	if !nr.enabled.Load() {
		return fmt.Errorf("replicator is disabled")
	}

	if targetAddress == "" {
		return fmt.Errorf("target address is empty")
	}

	if len(data) == 0 {
		return fmt.Errorf("data is empty")
	}

	// Создаём задачу для пула воркеров
	taskID := fmt.Sprintf("replicate_async_%s_%s_%d", targetNodeID, targetAddress, time.Now().UnixNano())

	err := nr.workerPool.SubmitFunc(taskID, func() error {
		startTime := time.Now()

		err := nr.Replicate(targetNodeID, targetAddress, data)

		duration := time.Since(startTime)
		if nr.logger != nil && duration > 5*time.Second {
			nr.logger.Warn(fmt.Sprintf("Slow async replication to %s took %v", targetNodeID, duration))
		}

		if callback != nil {
			callback(err)
		}
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to submit async replication: %v", err)
	}

	nr.stats.mu.Lock()
	nr.stats.RetriedReplicas++
	nr.stats.mu.Unlock()

	return nil
}

// =============================================================================
// ПАКЕТНАЯ РЕПЛИКАЦИЯ
// =============================================================================

// BatchReplicationData содержит данные для пакетной репликации.
type BatchReplicationData struct {
	Documents   []map[string]interface{} `json:"documents"`
	Database    string                   `json:"database"`
	Collection  string                   `json:"collection"`
	BatchID     string                   `json:"batch_id"`
	TotalCount  int                      `json:"total_count"`
}

// ReplicateBatch выполняет пакетную репликацию нескольких документов.
func (nr *NetworkReplicator) ReplicateBatch(targetNodeID, targetAddress string, batch *BatchReplicationData) error {
	if !nr.enabled.Load() {
		return fmt.Errorf("replicator is disabled")
	}

	if batch == nil || len(batch.Documents) == 0 {
		return fmt.Errorf("empty batch")
	}

	// Сериализуем пакет
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}

	// Создаём запрос с типом "batch_replicate"
	req := ReplicationRequest{
		Type:      "batch_replicate",
		FromNode:  targetNodeID,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal batch request: %v", err)
	}

	// Выполняем репликацию
	return nr.Replicate(targetNodeID, targetAddress, reqData)
}
