/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/config/config.go
// Назначение: Загрузка и парсинг TOML-конфигурации, валидация параметров,
// предоставление доступа к настройкам кластера, хранилища и REPL.

package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Cluster      ClusterConfig      `toml:"cluster"`
	Storage      StorageConfig      `toml:"storage"`
	Repl         ReplConfig         `toml:"repl"`
	Log          LogConfig          `toml:"log"`
	API          APIConfig          `toml:"api"`
	Replication  ReplicationConfig  `toml:"replication"`
	Plugins      PluginsConfig      `toml:"plugins"`
	Compression  CompressionConfig  `toml:"compression"`
	WebUI        WebUIConfig        `toml:"webui"`
	Performance  PerformanceConfig  `toml:"performance"`
	Security     SecurityConfig     `toml:"security"`
	Monitoring   MonitoringConfig   `toml:"monitoring"`
	Recovery     RecoveryConfig     `toml:"recovery"`
	WAL          WALConfig          `toml:"wal"`
	MVCC         MVCCConfig         `toml:"mvcc"`
	Transactions TransactionsConfig `toml:"transactions"`
	ACL          ACLConfig          `toml:"acl"`
	ClusterTLS   TLSConfig          `toml:"cluster_tls"`
	Backpressure BackpressureConfig `toml:"backpressure"`
	RuntimeLimits RuntimeLimitsConfig `toml:"runtime_limits"`
	Autoscaling  AutoscalingConfig  `toml:"autoscaling"`
	SchemaMigration SchemaMigrationConfig `toml:"schema_migration"`
	Backup       BackupConfig       `toml:"backup"`
	Engines      EnginesConfig      `toml:"engines"`
}

type ClusterConfig struct {
	Name        string   `toml:"name"`
	NodeIP      string   `toml:"node_ip"`
	NodePort    int      `toml:"node_port"`
	RaftPort    int      `toml:"raft_port"`
	RaftDataDir string   `toml:"raft_data_dir"`
	Bootstrap   bool     `toml:"bootstrap"`
	Nodes       []string `toml:"nodes"`

	HeartbeatTimeoutMs int `toml:"heartbeat_timeout_ms"`
	ElectionTimeoutMs  int `toml:"election_timeout_ms"`
	CommitTimeoutMs    int `toml:"commit_timeout_ms"`

	SnapshotIntervalMin int `toml:"snapshot_interval_min"`
	SnapshotThreshold   int `toml:"snapshot_threshold"`

	SplitBrainPrevention bool `toml:"split_brain_prevention"`
	RecoveryTimeoutSec   int  `toml:"recovery_timeout_sec"`

	Region       string `toml:"region"`
	PriorityZone int    `toml:"priority_zone"`
}

type StorageConfig struct {
	PageSizeMB               int    `toml:"page_size_mb"`
	MaxCollections           int    `toml:"max_collections"`
	MaxDocumentsPerCollection int    `toml:"max_documents_per_collection"`
	DefaultEngine            string `toml:"default_engine"`
	EnableCustomEngines      bool   `toml:"enable_custom_engines"`
}

type ReplConfig struct {
	PromptColor  string `toml:"prompt_color"`
	HistorySize  int    `toml:"history_size"`
}

type LogConfig struct {
	LogFile  string `toml:"log_file"`
	LogLevel string `toml:"log_level"`
}

type APIConfig struct {
	Port int `toml:"port"`
}

type ReplicationConfig struct {
	Enabled              bool `toml:"enabled"`
	SyncReplication      bool `toml:"sync_replication"`
	ReplicationTimeoutMs int  `toml:"replication_timeout_ms"`
	MaxReplicaLagMs      int  `toml:"max_replica_lag_ms"`
}

type PluginsConfig struct {
	Enabled               bool     `toml:"enabled"`
	ScriptDir             string   `toml:"script_dir"`
	AllowList             []string `toml:"allow_list"`
	MaxCPUTimeMs          int      `toml:"max_cpu_time_ms"`
	MaxMemoryMB           int      `toml:"max_memory_mb"`
	MaxExecutionTimeSec   int      `toml:"max_execution_time_sec"`
	MaxInstructions       int64    `toml:"max_instructions"`
	HotReloadIntervalSec  int      `toml:"hot_reload_interval_sec"`
	MaxEventLogSize       int      `toml:"max_event_log_size"`
	LoadTimeoutSec        int      `toml:"load_timeout_sec"`
	MaxLuaStates          int      `toml:"max_lua_states"`
	LuaStateTTLSec        int      `toml:"lua_state_ttl_sec"`
	EnginePluginDir       string   `toml:"engine_plugin_dir"`
}

type CompressionConfig struct {
	Enabled   bool   `toml:"enabled"`
	Algorithm string `toml:"algorithm"`
	Level     int    `toml:"level"`
	MinSize   int    `toml:"min_size"`
}

type WebUIConfig struct {
	Enabled bool   `toml:"enabled"`
	Port    int    `toml:"port"`
	Theme   string `toml:"theme"`
}

type PerformanceConfig struct {
	EnablePipeline   bool `toml:"enable_pipeline"`
	BatchSize        int  `toml:"batch_size"`
	ReadFromFollower bool `toml:"read_from_follower"`
	MaxConnections   int  `toml:"max_connections"`
	ReadReplicaDelayMs int `toml:"read_replica_delay_ms"`
}

type SecurityConfig struct {
	EnableTLS  bool   `toml:"enable_tls"`
	CertFile   string `toml:"cert_file"`
	KeyFile    string `toml:"key_file"`
	CAFile     string `toml:"ca_file"`
	MinVersion string `toml:"min_version"`
}

type MonitoringConfig struct {
	EnableMetrics   bool    `toml:"enable_metrics"`
	MetricsPort     int     `toml:"metrics_port"`
	EnableTracing   bool    `toml:"enable_tracing"`
	TraceSampleRate float64 `toml:"trace_sample_rate"`
}

type RecoveryConfig struct {
	AutoRejoin                bool `toml:"auto_rejoin"`
	MaxRetrySec               int  `toml:"max_retry_sec"`
	DataReplicationTimeoutSec int  `toml:"data_replication_timeout_sec"`
	StaleReadTimeoutSec       int  `toml:"stale_read_timeout_sec"`
}

type WALConfig struct {
	SegmentSizeMB         int  `toml:"segment_size_mb"`
	SyncIntervalSec       int  `toml:"sync_interval_sec"`
	BatchSize             int  `toml:"batch_size"`
	RecoveryWorkers       int  `toml:"recovery_workers"`
	Enabled               bool `toml:"enabled"`
	AsyncRecovery         bool `toml:"async_recovery"`
	AsyncRecoveryWorkers  int  `toml:"async_recovery_workers"`
	AsyncRecoveryBuffer   int  `toml:"async_recovery_buffer"`
}

type MVCCConfig struct {
	MaxVersionsPerDoc   int `toml:"max_versions_per_doc"`
	VisibilityMapSize   int `toml:"visibility_map_size"`
	PruneIntervalMin    int `toml:"prune_interval_min"`
	RetentionDays       int `toml:"retention_days"`
	ReadCacheSize       int `toml:"read_cache_size"`
	ReadCacheTTLSec     int `toml:"read_cache_ttl_sec"`
}

type TransactionsConfig struct {
	DefaultTimeoutSec        int  `toml:"default_timeout_sec"`
	DeadlockCheckIntervalSec int  `toml:"deadlock_check_interval_sec"`
	MaxSavepointsPerTx       int  `toml:"max_savepoints_per_tx"`
	Enabled                  bool `toml:"enabled"`
	CheckpointIntervalSec    int  `toml:"checkpoint_interval_sec"`
}

type ACLConfig struct {
	MaxDeniedLogSize      int  `toml:"max_denied_log_size"`
	TemporaryGrantTTLHours int  `toml:"temporary_grant_ttl_hours"`
	EnableRoleHierarchy   bool `toml:"enable_role_hierarchy"`
	CacheTTLSec           int  `toml:"cache_ttl_sec"`
}

type TLSConfig struct {
	Enabled            bool   `toml:"enabled"`
	CertFile           string `toml:"cert_file"`
	KeyFile            string `toml:"key_file"`
	CAFile             string `toml:"ca_file"`
	MinVersion         string `toml:"min_version"`
	MutualAuth         bool   `toml:"mutual_auth"`
	KeyRotationDays    int    `toml:"key_rotation_days"`
	AutoGenerate       bool   `toml:"auto_generate"`
}

type BackpressureConfig struct {
	Enabled              bool    `toml:"enabled"`
	CPUThreshold         float64 `toml:"cpu_threshold"`
	MemoryThreshold      float64 `toml:"memory_threshold"`
	QueueSizeThreshold   int     `toml:"queue_size_threshold"`
	ConnectionThreshold  int     `toml:"connection_threshold"`
	CheckIntervalMs      int     `toml:"check_interval_ms"`
	LowDelayMs           int64   `toml:"low_delay_ms"`
	MediumRejectProb     uint32  `toml:"medium_reject_prob"`
	HighRejectProb       uint32  `toml:"high_reject_prob"`
}

type RuntimeLimitsConfig struct {
	Enabled              bool  `toml:"enabled"`
	GlobalMaxDocSizeMB   int   `toml:"global_max_doc_size_mb"`
	GlobalMaxCollSizeMB  int64 `toml:"global_max_coll_size_mb"`
	GlobalMaxDocsPerColl int64 `toml:"global_max_docs_per_coll"`
}

type AutoscalingConfig struct {
	Enabled               bool    `toml:"enabled"`
	MinNodes              int     `toml:"min_nodes"`
	MaxNodes              int     `toml:"max_nodes"`
	ScaleUpThreshold      float64 `toml:"scale_up_threshold"`
	ScaleDownThreshold    float64 `toml:"scale_down_threshold"`
	ScaleUpCooldownSec    int     `toml:"scale_up_cooldown_sec"`
	ScaleDownCooldownSec  int     `toml:"scale_down_cooldown_sec"`
	EvaluationIntervalSec int     `toml:"evaluation_interval_sec"`
	PredictiveEnabled     bool    `toml:"predictive_enabled"`
	MaxScaleUpNodes       int     `toml:"max_scale_up_nodes"`
	MaxScaleDownNodes     int     `toml:"max_scale_down_nodes"`
}

type SchemaMigrationConfig struct {
	Enabled        bool   `toml:"enabled"`
	MigrationDir   string `toml:"migration_dir"`
	AutoMigrate    bool   `toml:"auto_migrate"`
	TargetVersion  string `toml:"target_version"`
}

type BackupConfig struct {
	Enabled          bool   `toml:"enabled"`
	BackupDir        string `toml:"backup_dir"`
	MaxConcurrent    int    `toml:"max_concurrent"`
	CompressEnabled  bool   `toml:"compress_enabled"`
	RetentionDays    int    `toml:"retention_days"`
}

type EnginesConfig struct {
	Row      EngineConfig `toml:"row"`
	Columnar EngineConfig `toml:"columnar"`
	Document EngineConfig `toml:"document"`
	KV       EngineConfig `toml:"kv"`
	TS       EngineConfig `toml:"ts"`
	Graph    EngineConfig `toml:"graph"`
}

type EngineConfig struct {
	Enabled     bool                   `toml:"enabled"`
	Description string                 `toml:"description"`
	Config      map[string]interface{} `toml:"config"`
}

type ValidationResult struct {
	Valid    bool
	Errors   []error
	Warnings []error
}

// ========== Методы для ReplicationConfig ==========

func (r *ReplicationConfig) GetReplicationTimeout() time.Duration {
	if r.ReplicationTimeoutMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(r.ReplicationTimeoutMs) * time.Millisecond
}

func (r *ReplicationConfig) IsReplicationEnabled() bool {
	return r.Enabled
}

func (r *ReplicationConfig) IsSyncReplicationEnabled() bool {
	return r.SyncReplication
}

func (r *ReplicationConfig) GetMaxReplicaLag() time.Duration {
	if r.MaxReplicaLagMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(r.MaxReplicaLagMs) * time.Millisecond
}

// ========== Методы для ClusterConfig ==========

func (c *ClusterConfig) GetHeartbeatTimeout() time.Duration {
	if c.HeartbeatTimeoutMs <= 0 {
		return 1000 * time.Millisecond
	}
	return time.Duration(c.HeartbeatTimeoutMs) * time.Millisecond
}

func (c *ClusterConfig) GetElectionTimeout() time.Duration {
	if c.ElectionTimeoutMs <= 0 {
		return 1000 * time.Millisecond
	}
	return time.Duration(c.ElectionTimeoutMs) * time.Millisecond
}

func (c *ClusterConfig) GetCommitTimeout() time.Duration {
	if c.CommitTimeoutMs <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(c.CommitTimeoutMs) * time.Millisecond
}

func (c *ClusterConfig) GetSnapshotInterval() time.Duration {
	if c.SnapshotIntervalMin <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(c.SnapshotIntervalMin) * time.Minute
}

func (c *ClusterConfig) GetSnapshotThreshold() uint64 {
	if c.SnapshotThreshold <= 0 {
		return 1000
	}
	return uint64(c.SnapshotThreshold)
}

func (c *ClusterConfig) IsSplitBrainPreventionEnabled() bool {
	return c.SplitBrainPrevention
}

func (c *ClusterConfig) GetRecoveryTimeout() time.Duration {
	if c.RecoveryTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.RecoveryTimeoutSec) * time.Second
}

func (c *ClusterConfig) GetRegion() string {
	if c.Region == "" {
		return "default"
	}
	return c.Region
}

func (c *ClusterConfig) GetPriorityZone() int {
	if c.PriorityZone < 0 {
		return 0
	}
	if c.PriorityZone > 9 {
		return 9
	}
	return c.PriorityZone
}

// ========== Методы для PluginsConfig ==========

func (p *PluginsConfig) GetMaxCPUTime() time.Duration {
	if p.MaxCPUTimeMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(p.MaxCPUTimeMs) * time.Millisecond
}

func (p *PluginsConfig) GetMaxMemory() int64 {
	if p.MaxMemoryMB <= 0 {
		return 50 * 1024 * 1024
	}
	return int64(p.MaxMemoryMB) * 1024 * 1024
}

func (p *PluginsConfig) GetMaxExecutionTime() time.Duration {
	if p.MaxExecutionTimeSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(p.MaxExecutionTimeSec) * time.Second
}

func (p *PluginsConfig) GetMaxInstructions() int64 {
	if p.MaxInstructions <= 0 {
		return 1000000
	}
	return p.MaxInstructions
}

func (p *PluginsConfig) GetHotReloadInterval() time.Duration {
	if p.HotReloadIntervalSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(p.HotReloadIntervalSec) * time.Second
}

func (p *PluginsConfig) GetMaxEventLogSize() int {
	if p.MaxEventLogSize <= 0 {
		return 1000
	}
	return p.MaxEventLogSize
}

func (p *PluginsConfig) GetLoadTimeout() time.Duration {
	if p.LoadTimeoutSec <= 0 {
		return 10 * time.Second
	}
	return time.Duration(p.LoadTimeoutSec) * time.Second
}

func (p *PluginsConfig) GetMaxLuaStates() int {
	if p.MaxLuaStates <= 0 {
		return 100
	}
	return p.MaxLuaStates
}

func (p *PluginsConfig) GetLuaStateTTL() time.Duration {
	if p.LuaStateTTLSec <= 0 {
		return 10 * time.Minute
	}
	return time.Duration(p.LuaStateTTLSec) * time.Second
}

func (p *PluginsConfig) GetEnginePluginDir() string {
	if p.EnginePluginDir == "" {
		return "engines"
	}
	return p.EnginePluginDir
}

// ========== Методы для WALConfig ==========

func (w *WALConfig) GetSegmentSize() int64 {
	if w.SegmentSizeMB <= 0 {
		return 64 * 1024 * 1024
	}
	return int64(w.SegmentSizeMB) * 1024 * 1024
}

func (w *WALConfig) GetSyncInterval() time.Duration {
	if w.SyncIntervalSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(w.SyncIntervalSec) * time.Second
}

func (w *WALConfig) GetBatchSize() int {
	if w.BatchSize <= 0 {
		return 100
	}
	return w.BatchSize
}

func (w *WALConfig) GetRecoveryWorkers() int {
	if w.RecoveryWorkers <= 0 {
		return 4
	}
	return w.RecoveryWorkers
}

func (w *WALConfig) IsWALEnabled() bool {
	return w.Enabled
}

func (w *WALConfig) IsAsyncRecoveryEnabled() bool {
	return w.AsyncRecovery
}

func (w *WALConfig) GetAsyncRecoveryWorkers() int {
	if w.AsyncRecoveryWorkers <= 0 {
		return 4
	}
	return w.AsyncRecoveryWorkers
}

func (w *WALConfig) GetAsyncRecoveryBuffer() int {
	if w.AsyncRecoveryBuffer <= 0 {
		return 10000
	}
	return w.AsyncRecoveryBuffer
}

// ========== Методы для MVCCConfig ==========

func (m *MVCCConfig) GetMaxVersionsPerDoc() int {
	if m.MaxVersionsPerDoc <= 0 {
		return 10
	}
	return m.MaxVersionsPerDoc
}

func (m *MVCCConfig) GetVisibilityMapSize() int {
	if m.VisibilityMapSize <= 0 {
		return 1024 * 1024
	}
	return m.VisibilityMapSize
}

func (m *MVCCConfig) GetPruneInterval() time.Duration {
	if m.PruneIntervalMin <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(m.PruneIntervalMin) * time.Minute
}

func (m *MVCCConfig) GetRetentionDays() int {
	if m.RetentionDays <= 0 {
		return 7
	}
	return m.RetentionDays
}

func (m *MVCCConfig) GetReadCacheSize() int {
	if m.ReadCacheSize <= 0 {
		return 10000
	}
	return m.ReadCacheSize
}

func (m *MVCCConfig) GetReadCacheTTL() time.Duration {
	if m.ReadCacheTTLSec <= 0 {
		return 300 * time.Second
	}
	return time.Duration(m.ReadCacheTTLSec) * time.Second
}

// ========== Методы для TransactionsConfig ==========

func (t *TransactionsConfig) GetDefaultTimeout() time.Duration {
	if t.DefaultTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(t.DefaultTimeoutSec) * time.Second
}

func (t *TransactionsConfig) GetDeadlockCheckInterval() time.Duration {
	if t.DeadlockCheckIntervalSec <= 0 {
		return 1 * time.Second
	}
	return time.Duration(t.DeadlockCheckIntervalSec) * time.Second
}

func (t *TransactionsConfig) GetMaxSavepointsPerTx() int {
	if t.MaxSavepointsPerTx <= 0 {
		return 100
	}
	return t.MaxSavepointsPerTx
}

func (t *TransactionsConfig) IsTransactionsEnabled() bool {
	return t.Enabled
}

func (t *TransactionsConfig) GetCheckpointInterval() int64 {
	if t.CheckpointIntervalSec <= 0 {
		return 300
	}
	return int64(t.CheckpointIntervalSec)
}

// ========== Методы для ACLConfig ==========

func (a *ACLConfig) GetMaxDeniedLogSize() int {
	if a.MaxDeniedLogSize <= 0 {
		return 10000
	}
	return a.MaxDeniedLogSize
}

func (a *ACLConfig) GetTemporaryGrantTTL() time.Duration {
	if a.TemporaryGrantTTLHours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(a.TemporaryGrantTTLHours) * time.Hour
}

func (a *ACLConfig) IsRoleHierarchyEnabled() bool {
	return a.EnableRoleHierarchy
}

func (a *ACLConfig) GetCacheTTL() time.Duration {
	if a.CacheTTLSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(a.CacheTTLSec) * time.Second
}

// ========== Методы для TLSConfig ==========

func (t *TLSConfig) GetTLSMinVersion() string {
	if t.MinVersion == "" {
		return "1.2"
	}
	return t.MinVersion
}

func (t *TLSConfig) IsTLSEnabled() bool {
	return t.Enabled
}

func (t *TLSConfig) IsMutualAuthEnabled() bool {
	return t.MutualAuth
}

func (t *TLSConfig) GetKeyRotationDays() int {
	if t.KeyRotationDays <= 0 {
		return 30
	}
	return t.KeyRotationDays
}

func (t *TLSConfig) IsAutoGenerateEnabled() bool {
	return t.AutoGenerate
}

// ========== Методы для BackpressureConfig ==========

func (b *BackpressureConfig) IsBackpressureEnabled() bool {
	return b.Enabled
}

func (b *BackpressureConfig) GetCPUThreshold() float64 {
	if b.CPUThreshold <= 0 {
		return 0.8
	}
	if b.CPUThreshold > 1.0 {
		return 1.0
	}
	return b.CPUThreshold
}

func (b *BackpressureConfig) GetMemoryThreshold() float64 {
	if b.MemoryThreshold <= 0 {
		return 0.85
	}
	if b.MemoryThreshold > 1.0 {
		return 1.0
	}
	return b.MemoryThreshold
}

func (b *BackpressureConfig) GetQueueSizeThreshold() int {
	if b.QueueSizeThreshold <= 0 {
		return 10000
	}
	return b.QueueSizeThreshold
}

func (b *BackpressureConfig) GetConnectionThreshold() int {
	if b.ConnectionThreshold <= 0 {
		return 5000
	}
	return b.ConnectionThreshold
}

func (b *BackpressureConfig) GetCheckInterval() time.Duration {
	if b.CheckIntervalMs <= 0 {
		return 1 * time.Second
	}
	return time.Duration(b.CheckIntervalMs) * time.Millisecond
}

func (b *BackpressureConfig) GetLowDelay() time.Duration {
	if b.LowDelayMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(b.LowDelayMs) * time.Millisecond
}

func (b *BackpressureConfig) GetMediumRejectProb() uint32 {
	if b.MediumRejectProb > 100 {
		return 100
	}
	return b.MediumRejectProb
}

func (b *BackpressureConfig) GetHighRejectProb() uint32 {
	if b.HighRejectProb > 100 {
		return 100
	}
	return b.HighRejectProb
}

// ========== Методы для RuntimeLimitsConfig ==========

func (r *RuntimeLimitsConfig) IsRuntimeLimitsEnabled() bool {
	return r.Enabled
}

func (r *RuntimeLimitsConfig) GetGlobalMaxDocSizeMB() int {
	if r.GlobalMaxDocSizeMB <= 0 {
		return 16
	}
	return r.GlobalMaxDocSizeMB
}

func (r *RuntimeLimitsConfig) GetGlobalMaxCollSizeMB() int64 {
	if r.GlobalMaxCollSizeMB <= 0 {
		return 10240
	}
	return r.GlobalMaxCollSizeMB
}

func (r *RuntimeLimitsConfig) GetGlobalMaxDocsPerColl() int64 {
	if r.GlobalMaxDocsPerColl <= 0 {
		return 10000000
	}
	return r.GlobalMaxDocsPerColl
}

// ========== Методы для AutoscalingConfig ==========

func (a *AutoscalingConfig) IsAutoscalingEnabled() bool {
	return a.Enabled
}

func (a *AutoscalingConfig) GetMinNodes() int {
	if a.MinNodes <= 0 {
		return 1
	}
	return a.MinNodes
}

func (a *AutoscalingConfig) GetMaxNodes() int {
	if a.MaxNodes <= 0 {
		return 10
	}
	return a.MaxNodes
}

func (a *AutoscalingConfig) GetScaleUpThreshold() float64 {
	if a.ScaleUpThreshold <= 0 {
		return 0.75
	}
	if a.ScaleUpThreshold > 1.0 {
		return 1.0
	}
	return a.ScaleUpThreshold
}

func (a *AutoscalingConfig) GetScaleDownThreshold() float64 {
	if a.ScaleDownThreshold <= 0 {
		return 0.30
	}
	return a.ScaleDownThreshold
}

func (a *AutoscalingConfig) GetScaleUpCooldown() time.Duration {
	if a.ScaleUpCooldownSec <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(a.ScaleUpCooldownSec) * time.Second
}

func (a *AutoscalingConfig) GetScaleDownCooldown() time.Duration {
	if a.ScaleDownCooldownSec <= 0 {
		return 10 * time.Minute
	}
	return time.Duration(a.ScaleDownCooldownSec) * time.Second
}

func (a *AutoscalingConfig) GetEvaluationInterval() time.Duration {
	if a.EvaluationIntervalSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(a.EvaluationIntervalSec) * time.Second
}

func (a *AutoscalingConfig) IsPredictiveEnabled() bool {
	return a.PredictiveEnabled
}

func (a *AutoscalingConfig) GetMaxScaleUpNodes() int {
	if a.MaxScaleUpNodes <= 0 {
		return 3
	}
	return a.MaxScaleUpNodes
}

func (a *AutoscalingConfig) GetMaxScaleDownNodes() int {
	if a.MaxScaleDownNodes <= 0 {
		return 2
	}
	return a.MaxScaleDownNodes
}

// ========== Методы для SchemaMigrationConfig ==========

func (s *SchemaMigrationConfig) IsSchemaMigrationEnabled() bool {
	return s.Enabled
}

func (s *SchemaMigrationConfig) GetMigrationDir() string {
	if s.MigrationDir == "" {
		return "migrations"
	}
	return s.MigrationDir
}

func (s *SchemaMigrationConfig) IsAutoMigrateEnabled() bool {
	return s.AutoMigrate
}

func (s *SchemaMigrationConfig) GetTargetVersion() string {
	if s.TargetVersion == "" {
		return "latest"
	}
	return s.TargetVersion
}

// ========== Методы для BackupConfig ==========

func (b *BackupConfig) IsBackupEnabled() bool {
	return b.Enabled
}

func (b *BackupConfig) GetBackupDir() string {
	if b.BackupDir == "" {
		return "backups"
	}
	return b.BackupDir
}

func (b *BackupConfig) GetMaxConcurrentBackups() int {
	if b.MaxConcurrent <= 0 {
		return 1
	}
	return b.MaxConcurrent
}

func (b *BackupConfig) IsCompressEnabled() bool {
	return b.CompressEnabled
}

func (b *BackupConfig) GetBackupRetentionDays() int {
	if b.RetentionDays <= 0 {
		return 7
	}
	return b.RetentionDays
}

// ========== Методы для StorageConfig ==========

func (s *StorageConfig) GetDefaultEngine() string {
	if s.DefaultEngine == "" {
		return "row"
	}
	return s.DefaultEngine
}

func (s *StorageConfig) IsCustomEnginesEnabled() bool {
	return s.EnableCustomEngines
}

// ========== Методы для EnginesConfig ==========

func (e *EnginesConfig) IsEngineEnabled(name string) bool {
	switch name {
	case "row":
		return e.Row.Enabled
	case "columnar":
		return e.Columnar.Enabled
	case "document":
		return e.Document.Enabled
	case "kv":
		return e.KV.Enabled
	case "ts":
		return e.TS.Enabled
	case "graph":
		return e.Graph.Enabled
	default:
		return false
	}
}

func (e *EnginesConfig) GetEngineConfig(name string) map[string]interface{} {
	switch name {
	case "row":
		return e.Row.Config
	case "columnar":
		return e.Columnar.Config
	case "document":
		return e.Document.Config
	case "kv":
		return e.KV.Config
	case "ts":
		return e.TS.Config
	case "graph":
		return e.Graph.Config
	default:
		return make(map[string]interface{})
	}
}

func (e *EnginesConfig) GetEngineDescription(name string) string {
	switch name {
	case "row":
		return e.Row.Description
	case "columnar":
		return e.Columnar.Description
	case "document":
		return e.Document.Description
	case "kv":
		return e.KV.Description
	case "ts":
		return e.TS.Description
	case "graph":
		return e.Graph.Description
	default:
		return ""
	}
}

// ========== Методы для RecoveryConfig ==========

func (r *RecoveryConfig) GetMaxRetryDuration() time.Duration {
	if r.MaxRetrySec <= 0 {
		return 300 * time.Second
	}
	return time.Duration(r.MaxRetrySec) * time.Second
}

func (r *RecoveryConfig) GetDataReplicationTimeout() time.Duration {
	if r.DataReplicationTimeoutSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(r.DataReplicationTimeoutSec) * time.Second
}

func (r *RecoveryConfig) GetStaleReadTimeout() time.Duration {
	if r.StaleReadTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(r.StaleReadTimeoutSec) * time.Second
}

func (r *RecoveryConfig) IsAutoRejoinEnabled() bool {
	return r.AutoRejoin
}

// ========== Методы для PerformanceConfig ==========

func (p *PerformanceConfig) GetBatchSize() int {
	if p.BatchSize <= 0 {
		return 100
	}
	return p.BatchSize
}

func (p *PerformanceConfig) GetMaxConnections() int {
	if p.MaxConnections <= 0 {
		return 1000
	}
	return p.MaxConnections
}

func (p *PerformanceConfig) IsReadFromFollowerEnabled() bool {
	return p.ReadFromFollower
}

func (p *PerformanceConfig) IsPipelineEnabled() bool {
	return p.EnablePipeline
}

func (p *PerformanceConfig) GetReadReplicaDelay() time.Duration {
	if p.ReadReplicaDelayMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(p.ReadReplicaDelayMs) * time.Millisecond
}

// ========== Методы для MonitoringConfig ==========

func (m *MonitoringConfig) GetMetricsPort() int {
	if m.MetricsPort <= 0 {
		return 9090
	}
	return m.MetricsPort
}

func (m *MonitoringConfig) GetTraceSampleRate() float64 {
	if m.TraceSampleRate <= 0 || m.TraceSampleRate > 1 {
		return 0.01
	}
	return m.TraceSampleRate
}

func (m *MonitoringConfig) IsMetricsEnabled() bool {
	return m.EnableMetrics
}

func (m *MonitoringConfig) IsTracingEnabled() bool {
	return m.EnableTracing
}

// ========== Методы для SecurityConfig ==========

func (s *SecurityConfig) GetTLSMinVersion() uint16 {
	switch s.MinVersion {
	case "1.0":
		return 0x0301
	case "1.1":
		return 0x0302
	case "1.2":
		return 0x0303
	case "1.3":
		return 0x0304
	default:
		return 0x0303
	}
}

func (s *SecurityConfig) IsTLSEnabled() bool {
	return s.EnableTLS && s.CertFile != "" && s.KeyFile != ""
}

func (s *SecurityConfig) GetCertFile() string {
	return s.CertFile
}

func (s *SecurityConfig) GetKeyFile() string {
	return s.KeyFile
}

func (s *SecurityConfig) GetCAFile() string {
	return s.CAFile
}

// ========== Методы для CompressionConfig ==========

func (c *CompressionConfig) IsCompressionEnabled() bool {
	return c.Enabled
}

func (c *CompressionConfig) GetAlgorithm() string {
	if c.Algorithm == "" {
		return "snappy"
	}
	return c.Algorithm
}

func (c *CompressionConfig) GetLevel() int {
	if c.Level < 1 {
		return 3
	}
	if c.Level > 9 {
		return 9
	}
	return c.Level
}

func (c *CompressionConfig) GetMinSize() int {
	if c.MinSize <= 0 {
		return 1024
	}
	return c.MinSize
}

// ========== Методы для WebUIConfig ==========

func (w *WebUIConfig) IsWebUIEnabled() bool {
	return w.Enabled
}

func (w *WebUIConfig) GetWebUIPort() int {
	if w.Port <= 0 {
		return 8080
	}
	return w.Port
}

func (w *WebUIConfig) GetTheme() string {
	if w.Theme == "" {
		return "dark"
	}
	return w.Theme
}

// ========== Загрузка и валидация конфигурации ==========

// LoadConfig загружает и валидирует конфигурацию из TOML файла
func LoadConfig(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %v", err)
	}

	// Значения по умолчанию для существующих настроек
	if cfg.Cluster.RaftPort == 0 {
		cfg.Cluster.RaftPort = 9878
	}
	if cfg.Cluster.RaftDataDir == "" {
		cfg.Cluster.RaftDataDir = "raft_data"
	}
	if cfg.Replication.ReplicationTimeoutMs == 0 {
		cfg.Replication.ReplicationTimeoutMs = 5000
	}
	if cfg.Replication.MaxReplicaLagMs == 0 {
		cfg.Replication.MaxReplicaLagMs = 5000
	}
	if cfg.Plugins.ScriptDir == "" {
		cfg.Plugins.ScriptDir = "plugins"
	}
	if cfg.Plugins.EnginePluginDir == "" {
		cfg.Plugins.EnginePluginDir = "engines"
	}
	if cfg.API.Port == 0 {
		cfg.API.Port = 8080
	}
	if cfg.Compression.Algorithm == "" {
		cfg.Compression.Algorithm = "snappy"
	}
	if cfg.Compression.MinSize == 0 {
		cfg.Compression.MinSize = 1024
	}
	if cfg.Compression.Level == 0 {
		cfg.Compression.Level = 3
	}
	if cfg.WebUI.Port == 0 {
		cfg.WebUI.Port = 9080
	}
	if cfg.WebUI.Theme == "" {
		cfg.WebUI.Theme = "dark"
	}
	if cfg.Cluster.HeartbeatTimeoutMs == 0 {
		cfg.Cluster.HeartbeatTimeoutMs = 1000
	}
	if cfg.Cluster.ElectionTimeoutMs == 0 {
		cfg.Cluster.ElectionTimeoutMs = 1000
	}
	if cfg.Cluster.CommitTimeoutMs == 0 {
		cfg.Cluster.CommitTimeoutMs = 500
	}
	if cfg.Cluster.SnapshotIntervalMin == 0 {
		cfg.Cluster.SnapshotIntervalMin = 30
	}
	if cfg.Cluster.SnapshotThreshold == 0 {
		cfg.Cluster.SnapshotThreshold = 1000
	}
	if cfg.Cluster.RecoveryTimeoutSec == 0 {
		cfg.Cluster.RecoveryTimeoutSec = 30
	}
	if cfg.Cluster.Region == "" {
		cfg.Cluster.Region = "default"
	}
	if cfg.Performance.BatchSize == 0 {
		cfg.Performance.BatchSize = 100
	}
	if cfg.Performance.MaxConnections == 0 {
		cfg.Performance.MaxConnections = 1000
	}
	if cfg.Performance.ReadReplicaDelayMs == 0 {
		cfg.Performance.ReadReplicaDelayMs = 100
	}
	if cfg.Monitoring.MetricsPort == 0 {
		cfg.Monitoring.MetricsPort = 9090
	}
	if cfg.Monitoring.TraceSampleRate == 0 {
		cfg.Monitoring.TraceSampleRate = 0.01
	}
	if cfg.Recovery.MaxRetrySec == 0 {
		cfg.Recovery.MaxRetrySec = 300
	}
	if cfg.Recovery.DataReplicationTimeoutSec == 0 {
		cfg.Recovery.DataReplicationTimeoutSec = 60
	}
	if cfg.Recovery.StaleReadTimeoutSec == 0 {
		cfg.Recovery.StaleReadTimeoutSec = 30
	}
	if cfg.Security.MinVersion == "" {
		cfg.Security.MinVersion = "1.2"
	}
	if cfg.Storage.DefaultEngine == "" {
		cfg.Storage.DefaultEngine = "row"
	}

	// Значения по умолчанию для WAL
	if cfg.WAL.SegmentSizeMB == 0 {
		cfg.WAL.SegmentSizeMB = 64
	}
	if cfg.WAL.SyncIntervalSec == 0 {
		cfg.WAL.SyncIntervalSec = 5
	}
	if cfg.WAL.BatchSize == 0 {
		cfg.WAL.BatchSize = 100
	}
	if cfg.WAL.RecoveryWorkers == 0 {
		cfg.WAL.RecoveryWorkers = 4
	}
	if cfg.WAL.AsyncRecoveryWorkers == 0 {
		cfg.WAL.AsyncRecoveryWorkers = 4
	}
	if cfg.WAL.AsyncRecoveryBuffer == 0 {
		cfg.WAL.AsyncRecoveryBuffer = 10000
	}

	// Значения по умолчанию для MVCC
	if cfg.MVCC.MaxVersionsPerDoc == 0 {
		cfg.MVCC.MaxVersionsPerDoc = 10
	}
	if cfg.MVCC.VisibilityMapSize == 0 {
		cfg.MVCC.VisibilityMapSize = 1024 * 1024
	}
	if cfg.MVCC.PruneIntervalMin == 0 {
		cfg.MVCC.PruneIntervalMin = 5
	}
	if cfg.MVCC.RetentionDays == 0 {
		cfg.MVCC.RetentionDays = 7
	}
	if cfg.MVCC.ReadCacheSize == 0 {
		cfg.MVCC.ReadCacheSize = 10000
	}
	if cfg.MVCC.ReadCacheTTLSec == 0 {
		cfg.MVCC.ReadCacheTTLSec = 300
	}

	// Значения по умолчанию для транзакций
	if cfg.Transactions.DefaultTimeoutSec == 0 {
		cfg.Transactions.DefaultTimeoutSec = 30
	}
	if cfg.Transactions.DeadlockCheckIntervalSec == 0 {
		cfg.Transactions.DeadlockCheckIntervalSec = 1
	}
	if cfg.Transactions.MaxSavepointsPerTx == 0 {
		cfg.Transactions.MaxSavepointsPerTx = 100
	}
	if cfg.Transactions.CheckpointIntervalSec == 0 {
		cfg.Transactions.CheckpointIntervalSec = 300
	}

	// Значения по умолчанию для ACL
	if cfg.ACL.MaxDeniedLogSize == 0 {
		cfg.ACL.MaxDeniedLogSize = 10000
	}
	if cfg.ACL.TemporaryGrantTTLHours == 0 {
		cfg.ACL.TemporaryGrantTTLHours = 24
	}
	if cfg.ACL.CacheTTLSec == 0 {
		cfg.ACL.CacheTTLSec = 60
	}

	// Значения по умолчанию для расширенных настроек плагинов
	if cfg.Plugins.MaxCPUTimeMs == 0 {
		cfg.Plugins.MaxCPUTimeMs = 100
	}
	if cfg.Plugins.MaxMemoryMB == 0 {
		cfg.Plugins.MaxMemoryMB = 50
	}
	if cfg.Plugins.MaxExecutionTimeSec == 0 {
		cfg.Plugins.MaxExecutionTimeSec = 5
	}
	if cfg.Plugins.MaxInstructions == 0 {
		cfg.Plugins.MaxInstructions = 1000000
	}
	if cfg.Plugins.HotReloadIntervalSec == 0 {
		cfg.Plugins.HotReloadIntervalSec = 30
	}
	if cfg.Plugins.MaxEventLogSize == 0 {
		cfg.Plugins.MaxEventLogSize = 1000
	}
	if cfg.Plugins.LoadTimeoutSec == 0 {
		cfg.Plugins.LoadTimeoutSec = 10
	}
	if cfg.Plugins.MaxLuaStates == 0 {
		cfg.Plugins.MaxLuaStates = 100
	}
	if cfg.Plugins.LuaStateTTLSec == 0 {
		cfg.Plugins.LuaStateTTLSec = 600
	}

	// Значения по умолчанию для новых конфигураций
	if cfg.ClusterTLS.MinVersion == "" {
		cfg.ClusterTLS.MinVersion = "1.2"
	}
	if cfg.ClusterTLS.KeyRotationDays == 0 {
		cfg.ClusterTLS.KeyRotationDays = 30
	}
	if cfg.Backpressure.CheckIntervalMs == 0 {
		cfg.Backpressure.CheckIntervalMs = 1000
	}
	if cfg.Backpressure.LowDelayMs == 0 {
		cfg.Backpressure.LowDelayMs = 100
	}
	if cfg.RuntimeLimits.GlobalMaxDocSizeMB == 0 {
		cfg.RuntimeLimits.GlobalMaxDocSizeMB = 16
	}
	if cfg.RuntimeLimits.GlobalMaxCollSizeMB == 0 {
		cfg.RuntimeLimits.GlobalMaxCollSizeMB = 10240
	}
	if cfg.RuntimeLimits.GlobalMaxDocsPerColl == 0 {
		cfg.RuntimeLimits.GlobalMaxDocsPerColl = 10000000
	}
	if cfg.Autoscaling.MinNodes == 0 {
		cfg.Autoscaling.MinNodes = 1
	}
	if cfg.Autoscaling.MaxNodes == 0 {
		cfg.Autoscaling.MaxNodes = 10
	}
	if cfg.Autoscaling.ScaleUpCooldownSec == 0 {
		cfg.Autoscaling.ScaleUpCooldownSec = 300
	}
	if cfg.Autoscaling.ScaleDownCooldownSec == 0 {
		cfg.Autoscaling.ScaleDownCooldownSec = 600
	}
	if cfg.Autoscaling.EvaluationIntervalSec == 0 {
		cfg.Autoscaling.EvaluationIntervalSec = 30
	}
	if cfg.SchemaMigration.MigrationDir == "" {
		cfg.SchemaMigration.MigrationDir = "migrations"
	}
	if cfg.Backup.BackupDir == "" {
		cfg.Backup.BackupDir = "backups"
	}
	if cfg.Backup.RetentionDays == 0 {
		cfg.Backup.RetentionDays = 7
	}

	// Значения по умолчанию для движков
	if !cfg.Engines.Row.Enabled {
		cfg.Engines.Row.Enabled = true
		cfg.Engines.Row.Description = "Row-based storage engine (default)"
	}
	if cfg.Engines.Row.Config == nil {
		cfg.Engines.Row.Config = make(map[string]interface{})
	}
	if cfg.Engines.Columnar.Config == nil {
		cfg.Engines.Columnar.Config = make(map[string]interface{})
	}
	if cfg.Engines.Document.Config == nil {
		cfg.Engines.Document.Config = make(map[string]interface{})
	}
	if cfg.Engines.KV.Config == nil {
		cfg.Engines.KV.Config = make(map[string]interface{})
	}
	if cfg.Engines.TS.Config == nil {
		cfg.Engines.TS.Config = make(map[string]interface{})
	}
	if cfg.Engines.Graph.Config == nil {
		cfg.Engines.Graph.Config = make(map[string]interface{})
	}

	// Валидация конфигурации
	validationResult := ValidateConfigFull(&cfg)
	if !validationResult.Valid {
		var errMsg strings.Builder
		errMsg.WriteString("config validation failed:\n")
		for _, err := range validationResult.Errors {
			errMsg.WriteString(fmt.Sprintf("  - %s\n", err.Error()))
		}
		if len(validationResult.Warnings) > 0 {
			errMsg.WriteString("Warnings:\n")
			for _, warn := range validationResult.Warnings {
				errMsg.WriteString(fmt.Sprintf("  - %s\n", warn.Error()))
			}
		}
		return nil, fmt.Errorf("%s", errMsg.String())
	}

	if len(validationResult.Warnings) > 0 {
		fmt.Println("Configuration warnings:")
		for _, warn := range validationResult.Warnings {
			fmt.Printf("  - %s\n", warn.Error())
		}
	}

	return &cfg, nil
}

// ValidateConfigFull выполняет полную валидацию конфигурации
func ValidateConfigFull(cfg *Config) *ValidationResult {
	result := &ValidationResult{
		Valid:    true,
		Errors:   make([]error, 0),
		Warnings: make([]error, 0),
	}

	// Валидация кластерной конфигурации
	if cfg.Cluster.NodePort <= 0 || cfg.Cluster.NodePort > 65535 {
		result.Errors = append(result.Errors, fmt.Errorf("invalid node_port: %d", cfg.Cluster.NodePort))
	}
	if cfg.Cluster.RaftPort <= 0 || cfg.Cluster.RaftPort > 65535 {
		result.Errors = append(result.Errors, fmt.Errorf("invalid raft_port: %d", cfg.Cluster.RaftPort))
	}
	if cfg.Cluster.PriorityZone < 0 || cfg.Cluster.PriorityZone > 9 {
		result.Errors = append(result.Errors, fmt.Errorf("priority_zone must be between 0 and 9, got %d", cfg.Cluster.PriorityZone))
	}

	// Валидация производительности
	if cfg.Performance.BatchSize < 1 {
		result.Errors = append(result.Errors, fmt.Errorf("batch_size must be at least 1, got %d", cfg.Performance.BatchSize))
	}
	if cfg.Performance.MaxConnections < 1 {
		result.Errors = append(result.Errors, fmt.Errorf("max_connections must be at least 1, got %d", cfg.Performance.MaxConnections))
	}

	// Валидация мониторинга
	if cfg.Monitoring.TraceSampleRate < 0 || cfg.Monitoring.TraceSampleRate > 1 {
		result.Errors = append(result.Errors, fmt.Errorf("trace_sample_rate must be between 0 and 1, got %f", cfg.Monitoring.TraceSampleRate))
	}

	// Проверка конфликтов портов
	if cfg.API.Port == cfg.Cluster.NodePort {
		result.Errors = append(result.Errors, fmt.Errorf("API port %d conflicts with cluster node port", cfg.API.Port))
	}
	if cfg.WebUI.Port == cfg.Cluster.NodePort {
		result.Errors = append(result.Errors, fmt.Errorf("WebUI port %d conflicts with cluster node port", cfg.WebUI.Port))
	}
	if cfg.Monitoring.MetricsPort == cfg.Cluster.NodePort {
		result.Errors = append(result.Errors, fmt.Errorf("metrics port %d conflicts with cluster node port", cfg.Monitoring.MetricsPort))
	}

	// Валидация TLS
	if cfg.Security.IsTLSEnabled() {
		if cfg.Security.CertFile == "" {
			result.Errors = append(result.Errors, fmt.Errorf("cert_file is required when TLS is enabled"))
		}
		if cfg.Security.KeyFile == "" {
			result.Errors = append(result.Errors, fmt.Errorf("key_file is required when TLS is enabled"))
		}
	}

	// Валидация сжатия
	if cfg.Compression.Enabled {
		switch cfg.Compression.Algorithm {
		case "snappy", "lz4", "zstd":
		default:
			result.Errors = append(result.Errors, fmt.Errorf("unsupported compression algorithm: %s, supported: snappy, lz4, zstd", cfg.Compression.Algorithm))
		}
	}

	// Валидация WAL
	if cfg.WAL.RecoveryWorkers < 1 {
		result.Errors = append(result.Errors, fmt.Errorf("wal.recovery_workers must be at least 1, got %d", cfg.WAL.RecoveryWorkers))
	}
	if cfg.WAL.AsyncRecoveryWorkers < 1 && cfg.WAL.AsyncRecovery {
		result.Warnings = append(result.Warnings, fmt.Errorf("wal.async_recovery_workers is less than 1, will use default 4"))
	}
	if cfg.WAL.AsyncRecoveryBuffer < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("wal.async_recovery_buffer cannot be negative, got %d", cfg.WAL.AsyncRecoveryBuffer))
	}

	// Валидация MVCC
	if cfg.MVCC.MaxVersionsPerDoc < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("mvcc.max_versions_per_doc cannot be negative, got %d", cfg.MVCC.MaxVersionsPerDoc))
	}

	// Валидация плагинов
	if cfg.Plugins.MaxCPUTimeMs < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("plugins.max_cpu_time_ms cannot be negative, got %d", cfg.Plugins.MaxCPUTimeMs))
	}
	if cfg.Plugins.MaxMemoryMB < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("plugins.max_memory_mb cannot be negative, got %d", cfg.Plugins.MaxMemoryMB))
	}
	if cfg.Plugins.MaxLuaStates < 1 {
		result.Warnings = append(result.Warnings, fmt.Errorf("plugins.max_lua_states is less than 1, will use default 100"))
	}

	// Валидация backpressure
	if cfg.Backpressure.Enabled {
		if cfg.Backpressure.CPUThreshold < 0 || cfg.Backpressure.CPUThreshold > 1 {
			result.Errors = append(result.Errors, fmt.Errorf("backpressure.cpu_threshold must be between 0 and 1, got %f", cfg.Backpressure.CPUThreshold))
		}
		if cfg.Backpressure.MemoryThreshold < 0 || cfg.Backpressure.MemoryThreshold > 1 {
			result.Errors = append(result.Errors, fmt.Errorf("backpressure.memory_threshold must be between 0 and 1, got %f", cfg.Backpressure.MemoryThreshold))
		}
		if cfg.Backpressure.MediumRejectProb > 100 {
			result.Warnings = append(result.Warnings, fmt.Errorf("backpressure.medium_reject_prob is > 100, will be capped to 100"))
		}
		if cfg.Backpressure.HighRejectProb > 100 {
			result.Warnings = append(result.Warnings, fmt.Errorf("backpressure.high_reject_prob is > 100, will be capped to 100"))
		}
	}

	// Валидация runtime limits
	if cfg.RuntimeLimits.Enabled {
		if cfg.RuntimeLimits.GlobalMaxDocSizeMB < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("runtime_limits.global_max_doc_size_mb cannot be negative, got %d", cfg.RuntimeLimits.GlobalMaxDocSizeMB))
		}
		if cfg.RuntimeLimits.GlobalMaxCollSizeMB < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("runtime_limits.global_max_coll_size_mb cannot be negative, got %d", cfg.RuntimeLimits.GlobalMaxCollSizeMB))
		}
		if cfg.RuntimeLimits.GlobalMaxDocsPerColl < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("runtime_limits.global_max_docs_per_coll cannot be negative, got %d", cfg.RuntimeLimits.GlobalMaxDocsPerColl))
		}
	}

	// Валидация автомасштабирования
	if cfg.Autoscaling.Enabled {
		if cfg.Autoscaling.MinNodes < 1 {
			result.Errors = append(result.Errors, fmt.Errorf("autoscaling.min_nodes must be at least 1, got %d", cfg.Autoscaling.MinNodes))
		}
		if cfg.Autoscaling.MaxNodes < cfg.Autoscaling.MinNodes {
			result.Errors = append(result.Errors, fmt.Errorf("autoscaling.max_nodes (%d) must be >= min_nodes (%d)", cfg.Autoscaling.MaxNodes, cfg.Autoscaling.MinNodes))
		}
		if cfg.Autoscaling.ScaleUpThreshold <= 0 || cfg.Autoscaling.ScaleUpThreshold > 1 {
			result.Warnings = append(result.Warnings, fmt.Errorf("autoscaling.scale_up_threshold is invalid, will use default 0.75"))
		}
		if cfg.Autoscaling.ScaleDownThreshold <= 0 || cfg.Autoscaling.ScaleDownThreshold >= cfg.Autoscaling.ScaleUpThreshold {
			result.Warnings = append(result.Warnings, fmt.Errorf("autoscaling.scale_down_threshold should be less than scale_up_threshold"))
		}
	}

	// Валидация движков
	if cfg.Storage.EnableCustomEngines {
		defaultEngine := cfg.Storage.GetDefaultEngine()
		switch defaultEngine {
		case "row", "columnar", "document", "kv", "ts", "graph":
		default:
			result.Warnings = append(result.Warnings, fmt.Errorf("default_engine '%s' is not a built-in engine, ensure plugin is loaded", defaultEngine))
		}
	}

	result.Valid = len(result.Errors) == 0
	return result
}
