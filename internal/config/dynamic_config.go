/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/config/dynamic_config.go
// Назначение: Централизованное управление конфигурацией через Raft

package config

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"futriis/internal/log"
)

// =============================================================================
// СОВМЕСТИМЫЕ АТОМАРНЫЕ ОБЁРТКИ (Go 1.13+)
// =============================================================================

type atomicBool struct{ v int32 }

func (b *atomicBool) Load() bool     { return atomic.LoadInt32(&b.v) != 0 }
func (b *atomicBool) Store(v bool)   { atomic.StoreInt32(&b.v, boolToInt32(v)) }
func (b *atomicBool) CAS(old, new bool) bool {
	return atomic.CompareAndSwapInt32(&b.v, boolToInt32(old), boolToInt32(new))
}

func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

type atomicUint64 struct{ v uint64 }

func (a *atomicUint64) Load() uint64        { return atomic.LoadUint64(&a.v) }
func (a *atomicUint64) Store(v uint64)      { atomic.StoreUint64(&a.v, v) }
func (a *atomicUint64) Add(d uint64) uint64 { return atomic.AddUint64(&a.v, d) }

// =============================================================================
// ТИПЫ ДЛЯ ДИНАМИЧЕСКОЙ КОНФИГУРАЦИИ
// =============================================================================

// DynamicConfigChange представляет изменение конфигурации
type DynamicConfigChange struct {
	ID          string                 `json:"id"`
	Version     uint64                 `json:"version"`
	Timestamp   int64                  `json:"timestamp"`
	ChangedBy   string                 `json:"changed_by"`
	Description string                 `json:"description"`
	Changes     map[string]interface{} `json:"changes"`
}

// DynamicConfigSnapshot представляет снэпшот конфигурации
type DynamicConfigSnapshot struct {
	Version       uint64                  `json:"version"`
	UpdatedAt     int64                   `json:"updated_at"`
	Config        *Config                 `json:"config"`
	ChangeHistory []*DynamicConfigChange  `json:"change_history"`
}

// ConfigChangeListener представляет слушатель изменений конфигурации
type ConfigChangeListener func(change *DynamicConfigChange)

// =============================================================================
// DYNAMIC CONFIG MANAGER
// =============================================================================

// DynamicConfigManager управляет динамической конфигурацией через Raft
type DynamicConfigManager struct {
	config        *Config
	logger        *log.Logger
	raft          *raft.Raft
	fsm           *ConfigFSM
	mu            sync.RWMutex
	version       atomicUint64
	changeHistory []*DynamicConfigChange
	listeners     map[string]ConfigChangeListener
	listenerMu    sync.RWMutex
	stopChan      chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	isLeader      atomicBool
}

// ConfigFSM реализует конечный автомат для конфигурации
type ConfigFSM struct {
	config *DynamicConfigSnapshot
	mu     sync.RWMutex
	logger *log.Logger
	// onChange вызывается после успешного применения изменения.
	// Может быть установлен менеджером для уведомления слушателей.
	onChange func(change *DynamicConfigChange)
	onChangeMu sync.RWMutex
}

// ConfigSnapshot реализует raft.FSMSnapshot
type ConfigSnapshot struct {
	config *DynamicConfigSnapshot
}

// NewDynamicConfigManager создаёт новый менеджер динамической конфигурации
func NewDynamicConfigManager(cfg *Config, logger *log.Logger) *DynamicConfigManager {
	if cfg == nil {
		cfg = &Config{}
	}

	// Создаём начальный снэпшот
	initialSnapshot := &DynamicConfigSnapshot{
		Version:       1,
		UpdatedAt:     time.Now().UnixMilli(),
		Config:        cfg,
		ChangeHistory: make([]*DynamicConfigChange, 0),
	}

	fsm := &ConfigFSM{
		config: initialSnapshot,
		logger: logger,
	}

	dm := &DynamicConfigManager{
		config:        cfg,
		logger:        logger,
		fsm:           fsm,
		changeHistory: make([]*DynamicConfigChange, 0),
		listeners:     make(map[string]ConfigChangeListener),
		stopChan:      make(chan struct{}),
	}
	dm.version.Store(1)

	// Подключаем FSM к менеджеру для уведомления слушателей
	fsm.onChangeMu.Lock()
	fsm.onChange = func(change *DynamicConfigChange) {
		dm.version.Store(change.Version)
		dm.mu.Lock()
		dm.changeHistory = append(dm.changeHistory, change)
		// Ограничиваем историю
		if len(dm.changeHistory) > 1000 {
			dm.changeHistory = dm.changeHistory[len(dm.changeHistory)-1000:]
		}
		dm.mu.Unlock()
		dm.notifyListeners(change)
	}
	fsm.onChangeMu.Unlock()

	return dm
}

// SetRaft устанавливает Raft для управления конфигурацией
func (dm *DynamicConfigManager) SetRaft(r *raft.Raft) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.raft = r
}

// GetFSM возвращает FSM для конфигурации
func (dm *DynamicConfigManager) GetFSM() *ConfigFSM {
	return dm.fsm
}

// Start запускает менеджер динамической конфигурации
func (dm *DynamicConfigManager) Start() {
	if dm.logger != nil {
		dm.logger.Info("Dynamic config manager started")
	}
}

// Stop останавливает менеджер динамической конфигурации.
// Безопасен для многократного вызова.
func (dm *DynamicConfigManager) Stop() {
	dm.stopOnce.Do(func() {
		close(dm.stopChan)
	})
	dm.wg.Wait()
	if dm.logger != nil {
		dm.logger.Info("Dynamic config manager stopped")
	}
}

// GetConfig возвращает текущую конфигурацию.
// Возвращает копию, чтобы вызывающий не мог случайно мутировать состояние FSM.
func (dm *DynamicConfigManager) GetConfig() *Config {
	dm.fsm.mu.RLock()
	defer dm.fsm.mu.RUnlock()

	if dm.fsm.config == nil || dm.fsm.config.Config == nil {
		return nil
	}
	return deepCopyConfig(dm.fsm.config.Config)
}

// GetVersion возвращает текущую версию конфигурации
func (dm *DynamicConfigManager) GetVersion() uint64 {
	// Синхронизируем с FSM, чтобы значение было актуальным
	dm.fsm.mu.RLock()
	defer dm.fsm.mu.RUnlock()
	if dm.fsm.config != nil {
		return dm.fsm.config.Version
	}
	return dm.version.Load()
}

// GetChangeHistory возвращает историю изменений
func (dm *DynamicConfigManager) GetChangeHistory() []*DynamicConfigChange {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	history := make([]*DynamicConfigChange, len(dm.changeHistory))
	copy(history, dm.changeHistory)
	return history
}

// ApplyChange применяет изменение конфигурации через Raft
func (dm *DynamicConfigManager) ApplyChange(changes map[string]interface{}, description string, changedBy string) error {
	dm.mu.RLock()
	r := dm.raft
	dm.mu.RUnlock()

	if r == nil {
		return fmt.Errorf("raft not initialized")
	}

	// Проверяем, что мы лидер — через сам Raft, а не через внешний флаг
	if r.State() != raft.Leader {
		return fmt.Errorf("not the leader (current state: %s)", r.State())
	}

	currentVersion := dm.GetVersion()

	// Создаём команду изменения
	change := &DynamicConfigChange{
		ID:          fmt.Sprintf("config_%d_%d", currentVersion+1, time.Now().UnixNano()),
		Version:     currentVersion + 1,
		Timestamp:   time.Now().UnixMilli(),
		ChangedBy:   changedBy,
		Description: description,
		Changes:     changes,
	}

	// Сериализуем и применяем через Raft
	data, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("failed to marshal config change: %v", err)
	}

	future := r.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %v", err)
	}

	// Проверяем ошибку из FSM.Apply
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return fmt.Errorf("fsm apply failed: %v", err)
		}
	}

	if dm.logger != nil {
		dm.logger.Info(fmt.Sprintf("Applied config change: %s (version: %d)", description, change.Version))
	}

	return nil
}

// UpdateConfig обновляет конфигурацию
func (dm *DynamicConfigManager) UpdateConfig(updates map[string]interface{}, description string, changedBy string) error {
	return dm.ApplyChange(updates, description, changedBy)
}

// ApplyConfigChange применяет изменение к FSM (вызывается из Raft)
func (fsm *ConfigFSM) ApplyConfigChange(change *DynamicConfigChange) error {
	if change == nil {
		return fmt.Errorf("nil config change")
	}

	fsm.mu.Lock()

	if fsm.config == nil {
		fsm.config = &DynamicConfigSnapshot{
			Version:       1,
			UpdatedAt:     time.Now().UnixMilli(),
			Config:        &Config{},
			ChangeHistory: make([]*DynamicConfigChange, 0),
		}
	}
	if fsm.config.Config == nil {
		fsm.config.Config = &Config{}
	}

	config := fsm.config.Config

	// Применяем изменения к конфигурации
	for path, value := range change.Changes {
		if err := setConfigField(config, path, value); err != nil {
			fsm.mu.Unlock()
			return err
		}
	}

	// Обновляем метаданные
	fsm.config.Version = change.Version
	fsm.config.UpdatedAt = time.Now().UnixMilli()
	fsm.config.ChangeHistory = append(fsm.config.ChangeHistory, change)
	// Ограничиваем историю
	if len(fsm.config.ChangeHistory) > 1000 {
		fsm.config.ChangeHistory = fsm.config.ChangeHistory[len(fsm.config.ChangeHistory)-1000:]
	}

	fsm.mu.Unlock()

	if fsm.logger != nil {
		fsm.logger.Debug(fmt.Sprintf("Applied config change to FSM: %s", change.Description))
	}

	// Уведомляем слушателей (через callback, установленный менеджером)
	fsm.onChangeMu.RLock()
	cb := fsm.onChange
	fsm.onChangeMu.RUnlock()
	if cb != nil {
		cb(change)
	}

	return nil
}

// Apply применяет команду к FSM (реализация raft.FSM)
func (fsm *ConfigFSM) Apply(log *raft.Log) interface{} {
	var change DynamicConfigChange
	if err := json.Unmarshal(log.Data, &change); err != nil {
		return fmt.Errorf("failed to unmarshal config change: %v", err)
	}

	if err := fsm.ApplyConfigChange(&change); err != nil {
		return err
	}

	return nil
}

// Snapshot создаёт снэпшот состояния FSM
func (fsm *ConfigFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	if fsm.config == nil {
		return &ConfigSnapshot{config: &DynamicConfigSnapshot{
			Version:       1,
			UpdatedAt:     time.Now().UnixMilli(),
			Config:        &Config{},
			ChangeHistory: make([]*DynamicConfigChange, 0),
		}}, nil
	}

	// Глубокая копия через JSON, чтобы снэпшот не зависел от мутаций FSM
	data, err := json.Marshal(fsm.config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot: %v", err)
	}
	var snapshotData DynamicConfigSnapshot
	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %v", err)
	}
	if snapshotData.ChangeHistory == nil {
		snapshotData.ChangeHistory = make([]*DynamicConfigChange, 0)
	}
	if snapshotData.Config == nil {
		snapshotData.Config = &Config{}
	}

	return &ConfigSnapshot{config: &snapshotData}, nil
}

// Restore восстанавливает состояние FSM из снэпшота
func (fsm *ConfigFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var data DynamicConfigSnapshot
	decoder := json.NewDecoder(snapshot)
	if err := decoder.Decode(&data); err != nil {
		return fmt.Errorf("failed to decode snapshot: %v", err)
	}

	if data.Config == nil {
		data.Config = &Config{}
	}
	if data.ChangeHistory == nil {
		data.ChangeHistory = make([]*DynamicConfigChange, 0)
	}
	if data.Version == 0 {
		data.Version = 1
	}

	fsm.mu.Lock()
	fsm.config = &data
	fsm.mu.Unlock()

	return nil
}

// Persist сохраняет снэпшот
func (s *ConfigSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.config)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release освобождает ресурсы снэпшота
func (s *ConfigSnapshot) Release() {}

// RegisterListener регистрирует слушатель изменений конфигурации
func (dm *DynamicConfigManager) RegisterListener(name string, listener ConfigChangeListener) {
	dm.listenerMu.Lock()
	defer dm.listenerMu.Unlock()
	dm.listeners[name] = listener
}

// UnregisterListener удаляет слушатель
func (dm *DynamicConfigManager) UnregisterListener(name string) {
	dm.listenerMu.Lock()
	defer dm.listenerMu.Unlock()
	delete(dm.listeners, name)
}

// notifyListeners уведомляет слушателей об изменении.
// Каждый слушатель вызывается в отдельной горутине; паника в слушателе
// не должна ронять процесс.
func (dm *DynamicConfigManager) notifyListeners(change *DynamicConfigChange) {
	dm.listenerMu.RLock()
	defer dm.listenerMu.RUnlock()

	for name, listener := range dm.listeners {
		go func(n string, l ConfigChangeListener) {
			defer func() {
				if r := recover(); r != nil && dm.logger != nil {
					dm.logger.Error(fmt.Sprintf("panic in config listener %s: %v", n, r))
				}
			}()
			l(change)
		}(name, listener)
	}
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// deepCopyConfig создаёт глубокую копию конфигурации через JSON.
// Если src == nil, возвращает &Config{} (не nil), чтобы избежать паник.
func deepCopyConfig(src *Config) *Config {
	if src == nil {
		return &Config{}
	}

	data, err := json.Marshal(src)
	if err != nil {
		// Возвращаем пустую копию, а не nil, чтобы не сломать вызывающий код
		return &Config{}
	}

	var dst Config
	if err := json.Unmarshal(data, &dst); err != nil {
		return &Config{}
	}

	return &dst
}

// coerceInt пытается привести значение к int.
// Поддерживает int, int32, int64, uint, uint32, uint64, float32, float64, json.Number.
func coerceInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i), true
		}
		if f, err := v.Float64(); err == nil {
			return int(f), true
		}
	}
	return 0, false
}

// coerceInt64 пытается привести значение к int64.
func coerceInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i, true
		}
		if f, err := v.Float64(); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

// coerceFloat64 пытается привести значение к float64.
func coerceFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// coerceBool пытается привести значение к bool.
func coerceBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		s := strings.ToLower(strings.TrimSpace(v))
		return s == "true" || s == "1" || s == "yes" || s == "on", true
	case int:
		return v != 0, true
	case int64:
		return v != 0, true
	case float64:
		return v != 0, true
	}
	return false, false
}

// coerceString пытается привести значение к string.
func coerceString(value interface{}) (string, bool) {
	if s, ok := value.(string); ok {
		return s, true
	}
	return "", false
}

// setConfigField устанавливает значение поля конфигурации по пути.
// Поддерживает основные секции: cluster, storage, replication, wal, mvcc,
// saga, backpressure, api, log, plugins, autoscaling, monitoring, transactions.
func setConfigField(config *Config, path string, value interface{}) error {
	if path == "" {
		return fmt.Errorf("empty config path")
	}

	switch path {
	// ===== cluster =====
	case "cluster.name":
		if v, ok := coerceString(value); ok {
			config.Cluster.Name = v
			return nil
		}
		return fmt.Errorf("cluster.name: expected string, got %T", value)
	case "cluster.node_ip":
		if v, ok := coerceString(value); ok {
			config.Cluster.NodeIP = v
			return nil
		}
		return fmt.Errorf("cluster.node_ip: expected string, got %T", value)
	case "cluster.node_port":
		if v, ok := coerceInt(value); ok {
			config.Cluster.NodePort = v
			return nil
		}
		return fmt.Errorf("cluster.node_port: expected int, got %T", value)
	case "cluster.raft_port":
		if v, ok := coerceInt(value); ok {
			config.Cluster.RaftPort = v
			return nil
		}
		return fmt.Errorf("cluster.raft_port: expected int, got %T", value)
	case "cluster.heartbeat_timeout_ms":
		if v, ok := coerceInt(value); ok {
			config.Cluster.HeartbeatTimeoutMs = v
			return nil
		}
		return fmt.Errorf("cluster.heartbeat_timeout_ms: expected int, got %T", value)
	case "cluster.election_timeout_ms":
		if v, ok := coerceInt(value); ok {
			config.Cluster.ElectionTimeoutMs = v
			return nil
		}
		return fmt.Errorf("cluster.election_timeout_ms: expected int, got %T", value)
	case "cluster.commit_timeout_ms":
		if v, ok := coerceInt(value); ok {
			config.Cluster.CommitTimeoutMs = v
			return nil
		}
		return fmt.Errorf("cluster.commit_timeout_ms: expected int, got %T", value)
	case "cluster.snapshot_interval_min":
		if v, ok := coerceInt(value); ok {
			config.Cluster.SnapshotIntervalMin = v
			return nil
		}
		return fmt.Errorf("cluster.snapshot_interval_min: expected int, got %T", value)
	case "cluster.snapshot_threshold":
		if v, ok := coerceInt(value); ok {
			config.Cluster.SnapshotThreshold = v
			return nil
		}
		return fmt.Errorf("cluster.snapshot_threshold: expected int, got %T", value)
	case "cluster.recovery_timeout_sec":
		if v, ok := coerceInt(value); ok {
			config.Cluster.RecoveryTimeoutSec = v
			return nil
		}
		return fmt.Errorf("cluster.recovery_timeout_sec: expected int, got %T", value)
	case "cluster.split_brain_prevention":
		if v, ok := coerceBool(value); ok {
			config.Cluster.SplitBrainPrevention = v
			return nil
		}
		return fmt.Errorf("cluster.split_brain_prevention: expected bool, got %T", value)
	case "cluster.region":
		if v, ok := coerceString(value); ok {
			config.Cluster.Region = v
			return nil
		}
		return fmt.Errorf("cluster.region: expected string, got %T", value)
	case "cluster.priority_zone":
		if v, ok := coerceInt(value); ok {
			config.Cluster.PriorityZone = v
			return nil
		}
		return fmt.Errorf("cluster.priority_zone: expected int, got %T", value)

	// ===== storage =====
	case "storage.page_size_mb":
		if v, ok := coerceInt(value); ok {
			config.Storage.PageSizeMB = v
			return nil
		}
		return fmt.Errorf("storage.page_size_mb: expected int, got %T", value)
	case "storage.max_collections":
		if v, ok := coerceInt(value); ok {
			config.Storage.MaxCollections = v
			return nil
		}
		return fmt.Errorf("storage.max_collections: expected int, got %T", value)
	case "storage.max_documents_per_collection":
		if v, ok := coerceInt(value); ok {
			config.Storage.MaxDocumentsPerCollection = v
			return nil
		}
		return fmt.Errorf("storage.max_documents_per_collection: expected int, got %T", value)
	case "storage.default_engine":
		if v, ok := coerceString(value); ok {
			config.Storage.DefaultEngine = v
			return nil
		}
		return fmt.Errorf("storage.default_engine: expected string, got %T", value)
	case "storage.enable_custom_engines":
		if v, ok := coerceBool(value); ok {
			config.Storage.EnableCustomEngines = v
			return nil
		}
		return fmt.Errorf("storage.enable_custom_engines: expected bool, got %T", value)

	// ===== replication =====
	case "replication.enabled":
		if v, ok := coerceBool(value); ok {
			config.Replication.Enabled = v
			return nil
		}
		return fmt.Errorf("replication.enabled: expected bool, got %T", value)
	case "replication.sync_replication":
		if v, ok := coerceBool(value); ok {
			config.Replication.SyncReplication = v
			return nil
		}
		return fmt.Errorf("replication.sync_replication: expected bool, got %T", value)
	case "replication.replication_timeout_ms":
		if v, ok := coerceInt(value); ok {
			config.Replication.ReplicationTimeoutMs = v
			return nil
		}
		return fmt.Errorf("replication.replication_timeout_ms: expected int, got %T", value)
	case "replication.max_replica_lag_ms":
		if v, ok := coerceInt(value); ok {
			config.Replication.MaxReplicaLagMs = v
			return nil
		}
		return fmt.Errorf("replication.max_replica_lag_ms: expected int, got %T", value)

	// ===== wal =====
	case "wal.segment_size_mb":
		if v, ok := coerceInt(value); ok {
			config.WAL.SegmentSizeMB = v
			return nil
		}
		return fmt.Errorf("wal.segment_size_mb: expected int, got %T", value)
	case "wal.sync_interval_sec":
		if v, ok := coerceInt(value); ok {
			config.WAL.SyncIntervalSec = v
			return nil
		}
		return fmt.Errorf("wal.sync_interval_sec: expected int, got %T", value)
	case "wal.batch_size":
		if v, ok := coerceInt(value); ok {
			config.WAL.BatchSize = v
			return nil
		}
		return fmt.Errorf("wal.batch_size: expected int, got %T", value)
	case "wal.enabled":
		if v, ok := coerceBool(value); ok {
			config.WAL.Enabled = v
			return nil
		}
		return fmt.Errorf("wal.enabled: expected bool, got %T", value)
	case "wal.async_recovery":
		if v, ok := coerceBool(value); ok {
			config.WAL.AsyncRecovery = v
			return nil
		}
		return fmt.Errorf("wal.async_recovery: expected bool, got %T", value)
	case "wal.recovery_workers":
		if v, ok := coerceInt(value); ok {
			config.WAL.RecoveryWorkers = v
			return nil
		}
		return fmt.Errorf("wal.recovery_workers: expected int, got %T", value)
	case "wal.async_recovery_workers":
		if v, ok := coerceInt(value); ok {
			config.WAL.AsyncRecoveryWorkers = v
			return nil
		}
		return fmt.Errorf("wal.async_recovery_workers: expected int, got %T", value)
	case "wal.async_recovery_buffer":
		if v, ok := coerceInt(value); ok {
			config.WAL.AsyncRecoveryBuffer = v
			return nil
		}
		return fmt.Errorf("wal.async_recovery_buffer: expected int, got %T", value)

	// ===== mvcc =====
	case "mvcc.max_versions_per_doc":
		if v, ok := coerceInt(value); ok {
			config.MVCC.MaxVersionsPerDoc = v
			return nil
		}
		return fmt.Errorf("mvcc.max_versions_per_doc: expected int, got %T", value)
	case "mvcc.visibility_map_size":
		if v, ok := coerceInt(value); ok {
			config.MVCC.VisibilityMapSize = v
			return nil
		}
		return fmt.Errorf("mvcc.visibility_map_size: expected int, got %T", value)
	case "mvcc.prune_interval_min":
		if v, ok := coerceInt(value); ok {
			config.MVCC.PruneIntervalMin = v
			return nil
		}
		return fmt.Errorf("mvcc.prune_interval_min: expected int, got %T", value)
	case "mvcc.retention_days":
		if v, ok := coerceInt(value); ok {
			config.MVCC.RetentionDays = v
			return nil
		}
		return fmt.Errorf("mvcc.retention_days: expected int, got %T", value)
	case "mvcc.read_cache_size":
		if v, ok := coerceInt(value); ok {
			config.MVCC.ReadCacheSize = v
			return nil
		}
		return fmt.Errorf("mvcc.read_cache_size: expected int, got %T", value)
	case "mvcc.read_cache_ttl_sec":
		if v, ok := coerceInt(value); ok {
			config.MVCC.ReadCacheTTLSec = v
			return nil
		}
		return fmt.Errorf("mvcc.read_cache_ttl_sec: expected int, got %T", value)

	// ===== saga =====
	case "saga.enabled":
		if v, ok := coerceBool(value); ok {
			config.Saga.Enabled = v
			return nil
		}
		return fmt.Errorf("saga.enabled: expected bool, got %T", value)
	case "saga.coordinator_count":
		if v, ok := coerceInt(value); ok {
			config.Saga.CoordinatorCount = v
			return nil
		}
		return fmt.Errorf("saga.coordinator_count: expected int, got %T", value)
	case "saga.max_retries":
		if v, ok := coerceInt(value); ok {
			config.Saga.MaxRetries = v
			return nil
		}
		return fmt.Errorf("saga.max_retries: expected int, got %T", value)
	case "saga.saga_timeout_sec":
		if v, ok := coerceInt(value); ok {
			config.Saga.SagaTimeoutSec = v
			return nil
		}
		return fmt.Errorf("saga.saga_timeout_sec: expected int, got %T", value)
	case "saga.retry_backoff_ms":
		if v, ok := coerceInt(value); ok {
			config.Saga.RetryBackoffMs = v
			return nil
		}
		return fmt.Errorf("saga.retry_backoff_ms: expected int, got %T", value)
	case "saga.stuck_check_interval_sec":
		if v, ok := coerceInt(value); ok {
			config.Saga.StuckCheckIntervalSec = v
			return nil
		}
		return fmt.Errorf("saga.stuck_check_interval_sec: expected int, got %T", value)

	// ===== backpressure =====
	case "backpressure.enabled":
		if v, ok := coerceBool(value); ok {
			config.Backpressure.Enabled = v
			return nil
		}
		return fmt.Errorf("backpressure.enabled: expected bool, got %T", value)
	case "backpressure.cpu_threshold":
		if v, ok := coerceFloat64(value); ok {
			config.Backpressure.CPUThreshold = v
			return nil
		}
		return fmt.Errorf("backpressure.cpu_threshold: expected float64, got %T", value)
	case "backpressure.memory_threshold":
		if v, ok := coerceFloat64(value); ok {
			config.Backpressure.MemoryThreshold = v
			return nil
		}
		return fmt.Errorf("backpressure.memory_threshold: expected float64, got %T", value)
	case "backpressure.queue_size_threshold":
		if v, ok := coerceInt(value); ok {
			config.Backpressure.QueueSizeThreshold = v
			return nil
		}
		return fmt.Errorf("backpressure.queue_size_threshold: expected int, got %T", value)
	case "backpressure.connection_threshold":
		if v, ok := coerceInt(value); ok {
			config.Backpressure.ConnectionThreshold = v
			return nil
		}
		return fmt.Errorf("backpressure.connection_threshold: expected int, got %T", value)

	// ===== api =====
	case "api.port":
		if v, ok := coerceInt(value); ok {
			config.API.Port = v
			return nil
		}
		return fmt.Errorf("api.port: expected int, got %T", value)

	// ===== log =====
	case "log.log_level":
		if v, ok := coerceString(value); ok {
			config.Log.LogLevel = v
			return nil
		}
		return fmt.Errorf("log.log_level: expected string, got %T", value)
	case "log.log_file":
		if v, ok := coerceString(value); ok {
			config.Log.LogFile = v
			return nil
		}
		return fmt.Errorf("log.log_file: expected string, got %T", value)

	// ===== plugins =====
	case "plugins.enabled":
		if v, ok := coerceBool(value); ok {
			config.Plugins.Enabled = v
			return nil
		}
		return fmt.Errorf("plugins.enabled: expected bool, got %T", value)
	case "plugins.max_cpu_time_ms":
		if v, ok := coerceInt(value); ok {
			config.Plugins.MaxCPUTimeMs = v
			return nil
		}
		return fmt.Errorf("plugins.max_cpu_time_ms: expected int, got %T", value)
	case "plugins.max_memory_mb":
		if v, ok := coerceInt(value); ok {
			config.Plugins.MaxMemoryMB = v
			return nil
		}
		return fmt.Errorf("plugins.max_memory_mb: expected int, got %T", value)
	case "plugins.hot_reload_interval_sec":
		if v, ok := coerceInt(value); ok {
			config.Plugins.HotReloadIntervalSec = v
			return nil
		}
		return fmt.Errorf("plugins.hot_reload_interval_sec: expected int, got %T", value)
	case "plugins.max_lua_states":
		if v, ok := coerceInt(value); ok {
			config.Plugins.MaxLuaStates = v
			return nil
		}
		return fmt.Errorf("plugins.max_lua_states: expected int, got %T", value)

	// ===== autoscaling =====
	case "autoscaling.enabled":
		if v, ok := coerceBool(value); ok {
			config.Autoscaling.Enabled = v
			return nil
		}
		return fmt.Errorf("autoscaling.enabled: expected bool, got %T", value)
	case "autoscaling.min_nodes":
		if v, ok := coerceInt(value); ok {
			config.Autoscaling.MinNodes = v
			return nil
		}
		return fmt.Errorf("autoscaling.min_nodes: expected int, got %T", value)
	case "autoscaling.max_nodes":
		if v, ok := coerceInt(value); ok {
			config.Autoscaling.MaxNodes = v
			return nil
		}
		return fmt.Errorf("autoscaling.max_nodes: expected int, got %T", value)
	case "autoscaling.scale_up_threshold":
		if v, ok := coerceFloat64(value); ok {
			config.Autoscaling.ScaleUpThreshold = v
			return nil
		}
		return fmt.Errorf("autoscaling.scale_up_threshold: expected float64, got %T", value)
	case "autoscaling.scale_down_threshold":
		if v, ok := coerceFloat64(value); ok {
			config.Autoscaling.ScaleDownThreshold = v
			return nil
		}
		return fmt.Errorf("autoscaling.scale_down_threshold: expected float64, got %T", value)
	case "autoscaling.evaluation_interval_sec":
		if v, ok := coerceInt(value); ok {
			config.Autoscaling.EvaluationIntervalSec = v
			return nil
		}
		return fmt.Errorf("autoscaling.evaluation_interval_sec: expected int, got %T", value)
	case "autoscaling.scale_up_cooldown_sec":
		if v, ok := coerceInt(value); ok {
			config.Autoscaling.ScaleUpCooldownSec = v
			return nil
		}
		return fmt.Errorf("autoscaling.scale_up_cooldown_sec: expected int, got %T", value)
	case "autoscaling.scale_down_cooldown_sec":
		if v, ok := coerceInt(value); ok {
			config.Autoscaling.ScaleDownCooldownSec = v
			return nil
		}
		return fmt.Errorf("autoscaling.scale_down_cooldown_sec: expected int, got %T", value)

	// ===== monitoring =====
	case "monitoring.enable_metrics":
		if v, ok := coerceBool(value); ok {
			config.Monitoring.EnableMetrics = v
			return nil
		}
		return fmt.Errorf("monitoring.enable_metrics: expected bool, got %T", value)
	case "monitoring.metrics_port":
		if v, ok := coerceInt(value); ok {
			config.Monitoring.MetricsPort = v
			return nil
		}
		return fmt.Errorf("monitoring.metrics_port: expected int, got %T", value)
	case "monitoring.enable_tracing":
		if v, ok := coerceBool(value); ok {
			config.Monitoring.EnableTracing = v
			return nil
		}
		return fmt.Errorf("monitoring.enable_tracing: expected bool, got %T", value)
	case "monitoring.trace_sample_rate":
		if v, ok := coerceFloat64(value); ok {
			config.Monitoring.TraceSampleRate = v
			return nil
		}
		return fmt.Errorf("monitoring.trace_sample_rate: expected float64, got %T", value)

	// ===== transactions =====
	case "transactions.default_timeout_sec":
		if v, ok := coerceInt(value); ok {
			config.Transactions.DefaultTimeoutSec = v
			return nil
		}
		return fmt.Errorf("transactions.default_timeout_sec: expected int, got %T", value)
	case "transactions.enabled":
		if v, ok := coerceBool(value); ok {
			config.Transactions.Enabled = v
			return nil
		}
		return fmt.Errorf("transactions.enabled: expected bool, got %T", value)
	case "transactions.deadlock_check_interval_sec":
		if v, ok := coerceInt(value); ok {
			config.Transactions.DeadlockCheckIntervalSec = v
			return nil
		}
		return fmt.Errorf("transactions.deadlock_check_interval_sec: expected int, got %T", value)
	case "transactions.max_savepoints_per_tx":
		if v, ok := coerceInt(value); ok {
			config.Transactions.MaxSavepointsPerTx = v
			return nil
		}
		return fmt.Errorf("transactions.max_savepoints_per_tx: expected int, got %T", value)

	default:
		return fmt.Errorf("unknown config path: %s", path)
	}
}

// GetLeaderStatus возвращает статус лидера
func (dm *DynamicConfigManager) GetLeaderStatus() bool {
	dm.mu.RLock()
	r := dm.raft
	dm.mu.RUnlock()

	if r != nil {
		return r.State() == raft.Leader
	}
	return dm.isLeader.Load()
}

// SetLeaderStatus устанавливает статус лидера.
// Оставлено для совместимости; предпочтительнее использовать Raft.State().
func (dm *DynamicConfigManager) SetLeaderStatus(isLeader bool) {
	dm.isLeader.Store(isLeader)
}

// GetListeners возвращает список зарегистрированных слушателей
func (dm *DynamicConfigManager) GetListeners() []string {
	dm.listenerMu.RLock()
	defer dm.listenerMu.RUnlock()

	names := make([]string, 0, len(dm.listeners))
	for name := range dm.listeners {
		names = append(names, name)
	}
	return names
}
