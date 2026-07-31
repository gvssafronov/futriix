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
	Version      uint64                   `json:"version"`
	UpdatedAt    int64                    `json:"updated_at"`
	Config       *Config                  `json:"config"`
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
	version       atomic.Uint64
	changeHistory []*DynamicConfigChange
	listeners     map[string]ConfigChangeListener
	listenerMu    sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
	isLeader      atomic.Bool
}

// ConfigFSM реализует конечный автомат для конфигурации
type ConfigFSM struct {
	config *DynamicConfigSnapshot
	mu     sync.RWMutex
	logger *log.Logger
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
		Version:      1,
		UpdatedAt:    time.Now().UnixMilli(),
		Config:       cfg,
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

	return dm
}

// SetRaft устанавливает Raft для управления конфигурацией
func (dm *DynamicConfigManager) SetRaft(raft *raft.Raft) {
	dm.raft = raft
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

// Stop останавливает менеджер динамической конфигурации
func (dm *DynamicConfigManager) Stop() {
	close(dm.stopChan)
	dm.wg.Wait()
	if dm.logger != nil {
		dm.logger.Info("Dynamic config manager stopped")
	}
}

// GetConfig возвращает текущую конфигурацию
func (dm *DynamicConfigManager) GetConfig() *Config {
	dm.fsm.mu.RLock()
	defer dm.fsm.mu.RUnlock()

	// Возвращаем копию конфигурации
	return deepCopyConfig(dm.fsm.config.Config)
}

// GetVersion возвращает текущую версию конфигурации
func (dm *DynamicConfigManager) GetVersion() uint64 {
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
	if dm.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	if !dm.isLeader.Load() {
		return fmt.Errorf("not the leader")
	}

	// Создаём команду изменения
	change := &DynamicConfigChange{
		ID:          fmt.Sprintf("config_%d_%d", dm.version.Load()+1, time.Now().UnixNano()),
		Version:     dm.version.Load() + 1,
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

	future := dm.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %v", err)
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
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// Применяем изменения к конфигурации
	config := fsm.config.Config

	// Рекурсивно обновляем поля
	for path, value := range change.Changes {
		if err := setConfigField(config, path, value); err != nil {
			return err
		}
	}

	// Обновляем метаданные
	fsm.config.Version = change.Version
	fsm.config.UpdatedAt = time.Now().UnixMilli()
	fsm.config.ChangeHistory = append(fsm.config.ChangeHistory, change)

	if fsm.logger != nil {
		fsm.logger.Debug(fmt.Sprintf("Applied config change to FSM: %s", change.Description))
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

	// Создаём копию снэпшота
	snapshot := &ConfigSnapshot{
		config: &DynamicConfigSnapshot{
			Version:       fsm.config.Version,
			UpdatedAt:     fsm.config.UpdatedAt,
			Config:        deepCopyConfig(fsm.config.Config),
			ChangeHistory: make([]*DynamicConfigChange, len(fsm.config.ChangeHistory)),
		},
	}

	copy(snapshot.config.ChangeHistory, fsm.config.ChangeHistory)

	return snapshot, nil
}

// Restore восстанавливает состояние FSM из снэпшота
func (fsm *ConfigFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var data DynamicConfigSnapshot
	decoder := json.NewDecoder(snapshot)
	if err := decoder.Decode(&data); err != nil {
		return err
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.config = &data

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

// notifyListeners уведомляет слушателей об изменении
func (dm *DynamicConfigManager) notifyListeners(change *DynamicConfigChange) {
	dm.listenerMu.RLock()
	defer dm.listenerMu.RUnlock()

	for _, listener := range dm.listeners {
		go listener(change)
	}
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// deepCopyConfig создаёт глубокую копию конфигурации
func deepCopyConfig(src *Config) *Config {
	if src == nil {
		return nil
	}

	// Используем JSON для глубокого копирования
	data, err := json.Marshal(src)
	if err != nil {
		return nil
	}

	var dst Config
	if err := json.Unmarshal(data, &dst); err != nil {
		return nil
	}

	return &dst
}

// setConfigField устанавливает значение поля конфигурации по пути
func setConfigField(config *Config, path string, value interface{}) error {
	// Разбиваем путь на части
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return fmt.Errorf("empty path")
	}

	// Простая реализация для основных путей
	// В реальной production-версии следует использовать рефлексию
	switch path {
	case "cluster.name":
		if v, ok := value.(string); ok {
			config.Cluster.Name = v
		}
	case "cluster.heartbeat_timeout_ms":
		if v, ok := value.(int); ok {
			config.Cluster.HeartbeatTimeoutMs = v
		} else if v, ok := value.(int64); ok {
			config.Cluster.HeartbeatTimeoutMs = int(v)
		} else if v, ok := value.(float64); ok {
			config.Cluster.HeartbeatTimeoutMs = int(v)
		}
	case "cluster.election_timeout_ms":
		if v, ok := value.(int); ok {
			config.Cluster.ElectionTimeoutMs = v
		} else if v, ok := value.(int64); ok {
			config.Cluster.ElectionTimeoutMs = int(v)
		} else if v, ok := value.(float64); ok {
			config.Cluster.ElectionTimeoutMs = int(v)
		}
	case "cluster.commit_timeout_ms":
		if v, ok := value.(int); ok {
			config.Cluster.CommitTimeoutMs = v
		} else if v, ok := value.(int64); ok {
			config.Cluster.CommitTimeoutMs = int(v)
		} else if v, ok := value.(float64); ok {
			config.Cluster.CommitTimeoutMs = int(v)
		}
	case "cluster.snapshot_interval_min":
		if v, ok := value.(int); ok {
			config.Cluster.SnapshotIntervalMin = v
		} else if v, ok := value.(int64); ok {
			config.Cluster.SnapshotIntervalMin = int(v)
		} else if v, ok := value.(float64); ok {
			config.Cluster.SnapshotIntervalMin = int(v)
		}
	case "cluster.snapshot_threshold":
		if v, ok := value.(int); ok {
			config.Cluster.SnapshotThreshold = v
		} else if v, ok := value.(int64); ok {
			config.Cluster.SnapshotThreshold = int(v)
		} else if v, ok := value.(float64); ok {
			config.Cluster.SnapshotThreshold = int(v)
		}
	case "cluster.recovery_timeout_sec":
		if v, ok := value.(int); ok {
			config.Cluster.RecoveryTimeoutSec = v
		} else if v, ok := value.(int64); ok {
			config.Cluster.RecoveryTimeoutSec = int(v)
		} else if v, ok := value.(float64); ok {
			config.Cluster.RecoveryTimeoutSec = int(v)
		}
	case "cluster.split_brain_prevention":
		if v, ok := value.(bool); ok {
			config.Cluster.SplitBrainPrevention = v
		}
	case "storage.page_size_mb":
		if v, ok := value.(int); ok {
			config.Storage.PageSizeMB = v
		} else if v, ok := value.(int64); ok {
			config.Storage.PageSizeMB = int(v)
		} else if v, ok := value.(float64); ok {
			config.Storage.PageSizeMB = int(v)
		}
	case "storage.max_collections":
		if v, ok := value.(int); ok {
			config.Storage.MaxCollections = v
		} else if v, ok := value.(int64); ok {
			config.Storage.MaxCollections = int(v)
		} else if v, ok := value.(float64); ok {
			config.Storage.MaxCollections = int(v)
		}
	case "storage.max_documents_per_collection":
		if v, ok := value.(int); ok {
			config.Storage.MaxDocumentsPerCollection = v
		} else if v, ok := value.(int64); ok {
			config.Storage.MaxDocumentsPerCollection = int(v)
		} else if v, ok := value.(float64); ok {
			config.Storage.MaxDocumentsPerCollection = int(v)
		}
	case "replication.enabled":
		if v, ok := value.(bool); ok {
			config.Replication.Enabled = v
		}
	case "replication.sync_replication":
		if v, ok := value.(bool); ok {
			config.Replication.SyncReplication = v
		}
	case "replication.replication_timeout_ms":
		if v, ok := value.(int); ok {
			config.Replication.ReplicationTimeoutMs = v
		} else if v, ok := value.(int64); ok {
			config.Replication.ReplicationTimeoutMs = int(v)
		} else if v, ok := value.(float64); ok {
			config.Replication.ReplicationTimeoutMs = int(v)
		}
	case "replication.max_replica_lag_ms":
		if v, ok := value.(int); ok {
			config.Replication.MaxReplicaLagMs = v
		} else if v, ok := value.(int64); ok {
			config.Replication.MaxReplicaLagMs = int(v)
		} else if v, ok := value.(float64); ok {
			config.Replication.MaxReplicaLagMs = int(v)
		}
	case "wal.segment_size_mb":
		if v, ok := value.(int); ok {
			config.WAL.SegmentSizeMB = v
		} else if v, ok := value.(int64); ok {
			config.WAL.SegmentSizeMB = int(v)
		} else if v, ok := value.(float64); ok {
			config.WAL.SegmentSizeMB = int(v)
		}
	case "wal.sync_interval_sec":
		if v, ok := value.(int); ok {
			config.WAL.SyncIntervalSec = v
		} else if v, ok := value.(int64); ok {
			config.WAL.SyncIntervalSec = int(v)
		} else if v, ok := value.(float64); ok {
			config.WAL.SyncIntervalSec = int(v)
		}
	case "wal.batch_size":
		if v, ok := value.(int); ok {
			config.WAL.BatchSize = v
		} else if v, ok := value.(int64); ok {
			config.WAL.BatchSize = int(v)
		} else if v, ok := value.(float64); ok {
			config.WAL.BatchSize = int(v)
		}
	case "wal.enabled":
		if v, ok := value.(bool); ok {
			config.WAL.Enabled = v
		}
	case "wal.async_recovery":
		if v, ok := value.(bool); ok {
			config.WAL.AsyncRecovery = v
		}
	case "mvcc.max_versions_per_doc":
		if v, ok := value.(int); ok {
			config.MVCC.MaxVersionsPerDoc = v
		} else if v, ok := value.(int64); ok {
			config.MVCC.MaxVersionsPerDoc = int(v)
		} else if v, ok := value.(float64); ok {
			config.MVCC.MaxVersionsPerDoc = int(v)
		}
	case "mvcc.retention_days":
		if v, ok := value.(int); ok {
			config.MVCC.RetentionDays = v
		} else if v, ok := value.(int64); ok {
			config.MVCC.RetentionDays = int(v)
		} else if v, ok := value.(float64); ok {
			config.MVCC.RetentionDays = int(v)
		}
	case "saga.enabled":
		if v, ok := value.(bool); ok {
			config.Saga.Enabled = v
		}
	case "saga.coordinator_count":
		if v, ok := value.(int); ok {
			config.Saga.CoordinatorCount = v
		} else if v, ok := value.(int64); ok {
			config.Saga.CoordinatorCount = int(v)
		} else if v, ok := value.(float64); ok {
			config.Saga.CoordinatorCount = int(v)
		}
	case "saga.max_retries":
		if v, ok := value.(int); ok {
			config.Saga.MaxRetries = v
		} else if v, ok := value.(int64); ok {
			config.Saga.MaxRetries = int(v)
		} else if v, ok := value.(float64); ok {
			config.Saga.MaxRetries = int(v)
		}
	case "saga.saga_timeout_sec":
		if v, ok := value.(int); ok {
			config.Saga.SagaTimeoutSec = v
		} else if v, ok := value.(int64); ok {
			config.Saga.SagaTimeoutSec = int(v)
		} else if v, ok := value.(float64); ok {
			config.Saga.SagaTimeoutSec = int(v)
		}
	case "backpressure.enabled":
		if v, ok := value.(bool); ok {
			config.Backpressure.Enabled = v
		}
	case "backpressure.cpu_threshold":
		if v, ok := value.(float64); ok {
			config.Backpressure.CPUThreshold = v
		}
	case "backpressure.memory_threshold":
		if v, ok := value.(float64); ok {
			config.Backpressure.MemoryThreshold = v
		}
	case "api.port":
		if v, ok := value.(int); ok {
			config.API.Port = v
		} else if v, ok := value.(int64); ok {
			config.API.Port = int(v)
		} else if v, ok := value.(float64); ok {
			config.API.Port = int(v)
		}
	case "log.log_level":
		if v, ok := value.(string); ok {
			config.Log.LogLevel = v
		}
	case "log.log_file":
		if v, ok := value.(string); ok {
			config.Log.LogFile = v
		}
	default:
		// Для неизвестных путей логируем предупреждение
		// В production-версии следует использовать рефлексию для полного покрытия
		return fmt.Errorf("unknown config path: %s", path)
	}

	return nil
}

// GetLeaderStatus возвращает статус лидера
func (dm *DynamicConfigManager) GetLeaderStatus() bool {
	return dm.isLeader.Load()
}

// SetLeaderStatus устанавливает статус лидера
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
