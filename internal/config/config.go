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
//
// Данный пакет отвечает за:
//   - Чтение конфигурационного файла в формате TOML
//   - Установку значений по умолчанию для всех параметров
//   - Валидацию корректности настроек
//   - Предоставление типобезопасного доступа к конфигурации через методы-геттеры

package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// =============================================================================
// ОСНОВНАЯ СТРУКТУРА КОНФИГУРАЦИИ
// =============================================================================

// Config представляет собой корневую структуру конфигурации,
// объединяющую все подсистемы приложения.
// Каждое поле соответствует секции в TOML-файле.
type Config struct {
	Cluster         ClusterConfig         `toml:"cluster"`          // Настройки кластеризации (Raft, узлы)
	Storage         StorageConfig         `toml:"storage"`          // Настройки хранилища (движки, ограничения)
	Repl            ReplConfig            `toml:"repl"`             // Настройки REPL (интерактивной оболочки)
	Log             LogConfig             `toml:"log"`              // Настройки логирования
	API             APIConfig             `toml:"api"`              // Настройки HTTP API
	Replication     ReplicationConfig     `toml:"replication"`      // Настройки репликации
	Plugins         PluginsConfig         `toml:"plugins"`          // Настройки системы плагинов (Lua)
	Compression     CompressionConfig     `toml:"compression"`      // Настройки сжатия данных
	WebUI           WebUIConfig           `toml:"webui"`            // Настройки веб-интерфейса
	Performance     PerformanceConfig     `toml:"performance"`      // Настройки производительности
	Security        SecurityConfig        `toml:"security"`         // Настройки безопасности (TLS)
	Monitoring      MonitoringConfig      `toml:"monitoring"`       // Настройки мониторинга (метрики, трассировка)
	Recovery        RecoveryConfig        `toml:"recovery"`         // Настройки восстановления после сбоев
	WAL             WALConfig             `toml:"wal"`              // Настройки WAL (Write-Ahead Log)
	MVCC            MVCCConfig            `toml:"mvcc"`             // Настройки MVCC (Multi-Version Concurrency Control)
	Transactions    TransactionsConfig    `toml:"transactions"`     // Настройки транзакций
	Saga            SagaConfig            `toml:"saga"`             // Настройки SAGA оркестратора
	ACL             ACLConfig             `toml:"acl"`              // Настройки ACL (Access Control List)
	ClusterTLS      TLSConfig             `toml:"cluster_tls"`      // Настройки TLS для кластерного взаимодействия
	Backpressure    BackpressureConfig    `toml:"backpressure"`     // Настройки механизма обратного давления
	RuntimeLimits   RuntimeLimitsConfig   `toml:"runtime_limits"`   // Ограничения времени выполнения
	Autoscaling     AutoscalingConfig     `toml:"autoscaling"`      // Настройки автоматического масштабирования
	SchemaMigration SchemaMigrationConfig `toml:"schema_migration"` // Настройки миграции схемы
	Backup          BackupConfig          `toml:"backup"`           // Настройки резервного копирования
	Engines         EnginesConfig         `toml:"engines"`          // Настройки движков хранения
}

// =============================================================================
// КОНФИГУРАЦИЯ КЛАСТЕРА
// =============================================================================

// ClusterConfig содержит настройки для работы в кластерном режиме
// на основе протокола Raft для обеспечения согласованности.
type ClusterConfig struct {
	// Имя кластера - идентификатор для группировки узлов
	Name string `toml:"name"`
	
	// Сетевые настройки узла
	NodeIP   string `toml:"node_ip"`   // IP-адрес для прослушивания
	NodePort int    `toml:"node_port"` // Порт для клиентских подключений
	RaftPort int    `toml:"raft_port"` // Порт для Raft-коммуникации
	
	// Настройки Raft
	RaftDataDir string   `toml:"raft_data_dir"` // Директория для хранения Raft-лога
	Bootstrap   bool     `toml:"bootstrap"`     // Является ли узел инициализатором кластера
	Nodes       []string `toml:"nodes"`         // Список адресов других узлов кластера

	// Таймауты Raft (в миллисекундах)
	HeartbeatTimeoutMs int `toml:"heartbeat_timeout_ms"` // Интервал отправки heartbeat
	ElectionTimeoutMs  int `toml:"election_timeout_ms"`  // Таймаут для начала выборов
	CommitTimeoutMs    int `toml:"commit_timeout_ms"`    // Таймаут коммита записей

	// Настройки снэпшотов
	SnapshotIntervalMin int `toml:"snapshot_interval_min"` // Интервал создания снэпшотов (минуты)
	SnapshotThreshold   int `toml:"snapshot_threshold"`    // Количество записей для создания снэпшота

	// Защита от split-brain и восстановление
	SplitBrainPrevention bool `toml:"split_brain_prevention"` // Включить защиту от разделения кластера
	RecoveryTimeoutSec   int  `toml:"recovery_timeout_sec"`   // Таймаут восстановления после сбоя

	// Географические настройки для оптимизации маршрутизации
	Region       string `toml:"region"`        // Регион расположения узла
	PriorityZone int    `toml:"priority_zone"` // Приоритетная зона (0-9, где 9 - наивысший)
}

// =============================================================================
// КОНФИГУРАЦИЯ SAGA ОРКЕСТРАТОРА
// =============================================================================

// SagaConfig содержит настройки отказоустойчивого оркестратора распределённых транзакций.
// Обеспечивает выполнение длительных транзакций с компенсацией шагов по протоколу SAGA.
type SagaConfig struct {
	// Основные настройки
	Enabled          bool   `toml:"enabled"`            // Включить SAGA оркестратор
	CoordinatorCount int    `toml:"coordinator_count"`  // Количество координаторов (>= 3 для отказоустойчивости)
	StateDir         string `toml:"state_dir"`          // Директория для хранения состояний SAGA

	// Настройки выполнения
	MaxRetries           int `toml:"max_retries"`              // Максимальное количество попыток выполнения шага
	RetryBackoffMs       int `toml:"retry_backoff_ms"`         // Базовая задержка между повторными попытками (мс)
	SagaTimeoutSec       int `toml:"saga_timeout_sec"`         // Таймаут выполнения SAGA (сек)
	StuckCheckIntervalSec int `toml:"stuck_check_interval_sec"` // Интервал проверки зависших SAGA (сек)

	// Настройки оркестрации
	LeaderElectionIntervalSec int `toml:"leader_election_interval_sec"` // Интервал выбора лидера (сек)
	RecoveryIntervalSec       int `toml:"recovery_interval_sec"`        // Интервал восстановления незавершённых SAGA (сек)
	MetricsIntervalSec        int `toml:"metrics_interval_sec"`         // Интервал сбора метрик (сек)

	// Настройки очистки и кэширования
	CleanupPeriodHours     int `toml:"cleanup_period_hours"`      // Период очистки старых SAGA (часы)
	MaxCacheSize           int `toml:"max_cache_size"`            // Максимальное количество состояний в кэше
	OperationRetentionDays int `toml:"operation_retention_days"`  // Время жизни выполненных операций (дни)

	// Настройки производительности
	ChannelBufferSize        int `toml:"channel_buffer_size"`          // Размер буфера каналов
	AsyncRecoveryWorkers     int `toml:"async_recovery_workers"`       // Количество воркеров для асинхронного восстановления
	AsyncRecoveryTimeoutSec  int `toml:"async_recovery_timeout_sec"`   // Таймаут асинхронного восстановления (сек)

	// Настройки синхронизации с диском
	FsyncEnabled      bool `toml:"fsync_enabled"`         // Принудительная синхронизация с диском
	FsyncMaxRetries   int  `toml:"fsync_max_retries"`     // Количество повторных попыток синхронизации
	FsyncRetryDelayMs int  `toml:"fsync_retry_delay_ms"`  // Задержка между повторными попытками (мс)
}

// =============================================================================
// КОНФИГУРАЦИЯ ХРАНИЛИЩА
// =============================================================================

// StorageConfig содержит настройки системы хранения данных.
type StorageConfig struct {
	PageSizeMB               int    `toml:"page_size_mb"`                  // Размер страницы памяти в МБ
	MaxCollections           int    `toml:"max_collections"`               // Максимальное количество коллекций
	MaxDocumentsPerCollection int    `toml:"max_documents_per_collection"` // Макс. документов в коллекции
	DefaultEngine            string `toml:"default_engine"`               // Движок по умолчанию
	EnableCustomEngines      bool   `toml:"enable_custom_engines"`        // Включить пользовательские движки
}

// =============================================================================
// КОНФИГУРАЦИЯ REPL (ИНТЕРАКТИВНАЯ ОБОЛОЧКА)
// =============================================================================

// ReplConfig содержит настройки интерактивной командной оболочки.
type ReplConfig struct {
	PromptColor string `toml:"prompt_color"` // Цвет приглашения ввода
	HistorySize int    `toml:"history_size"` // Размер истории команд
}

// =============================================================================
// КОНФИГУРАЦИЯ ЛОГИРОВАНИЯ
// =============================================================================

// LogConfig содержит настройки системы логирования.
type LogConfig struct {
	LogFile  string `toml:"log_file"`  // Путь к файлу лога (пусто = stdout)
	LogLevel string `toml:"log_level"` // Уровень логирования (debug, info, warn, error)
}

// =============================================================================
// КОНФИГУРАЦИЯ HTTP API
// =============================================================================

// APIConfig содержит настройки HTTP API сервера.
type APIConfig struct {
	Port int `toml:"port"` // Порт для HTTP API
}

// =============================================================================
// КОНФИГУРАЦИЯ РЕПЛИКАЦИИ
// =============================================================================

// ReplicationConfig содержит настройки репликации данных между узлами.
type ReplicationConfig struct {
	Enabled              bool `toml:"enabled"`                 // Включена ли репликация
	SyncReplication      bool `toml:"sync_replication"`       // Синхронная репликация
	ReplicationTimeoutMs int  `toml:"replication_timeout_ms"` // Таймаут репликации (мс)
	MaxReplicaLagMs      int  `toml:"max_replica_lag_ms"`     // Максимальное отставание реплики (мс)
}

// =============================================================================
// КОНФИГУРАЦИЯ ПЛАГИНОВ (LUA)
// =============================================================================

// PluginsConfig содержит настройки системы выполнения Lua-скриптов.
type PluginsConfig struct {
	Enabled               bool     `toml:"enabled"`                   // Включена ли поддержка плагинов
	ScriptDir             string   `toml:"script_dir"`               // Директория со скриптами
	AllowList             []string `toml:"allow_list"`               // Белый список разрешенных скриптов
	MaxCPUTimeMs          int      `toml:"max_cpu_time_ms"`          // Макс. время CPU (мс)
	MaxMemoryMB           int      `toml:"max_memory_mb"`            // Макс. память (МБ)
	MaxExecutionTimeSec   int      `toml:"max_execution_time_sec"`   // Макс. время выполнения (сек)
	MaxInstructions       int64    `toml:"max_instructions"`         // Макс. количество инструкций
	HotReloadIntervalSec  int      `toml:"hot_reload_interval_sec"`  // Интервал горячей перезагрузки
	MaxEventLogSize       int      `toml:"max_event_log_size"`       // Макс. размер лога событий
	LoadTimeoutSec        int      `toml:"load_timeout_sec"`         // Таймаут загрузки скрипта
	MaxLuaStates          int      `toml:"max_lua_states"`           // Макс. количество Lua-состояний
	LuaStateTTLSec        int      `toml:"lua_state_ttl_sec"`        // TTL Lua-состояния
	EnginePluginDir       string   `toml:"engine_plugin_dir"`        // Директория плагинов движков
}

// =============================================================================
// КОНФИГУРАЦИЯ СЖАТИЯ
// =============================================================================

// CompressionConfig содержит настройки сжатия данных.
type CompressionConfig struct {
	Enabled   bool   `toml:"enabled"`   // Включено ли сжатие
	Algorithm string `toml:"algorithm"` // Алгоритм сжатия (snappy, lz4, zstd)
	Level     int    `toml:"level"`     // Уровень сжатия (1-9)
	MinSize   int    `toml:"min_size"`  // Минимальный размер для сжатия (байт)
}

// =============================================================================
// КОНФИГУРАЦИЯ ВЕБ-ИНТЕРФЕЙСА
// =============================================================================

// WebUIConfig содержит настройки встроенного веб-интерфейса.
type WebUIConfig struct {
	Enabled bool   `toml:"enabled"` // Включен ли веб-интерфейс
	Port    int    `toml:"port"`    // Порт для веб-интерфейса
	Theme   string `toml:"theme"`   // Тема оформления (dark, light)
}

// =============================================================================
// КОНФИГУРАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ
// =============================================================================

// PerformanceConfig содержит настройки, влияющие на производительность системы.
type PerformanceConfig struct {
	EnablePipeline   bool `toml:"enable_pipeline"`      // Включить конвейерную обработку
	BatchSize        int  `toml:"batch_size"`           // Размер пакета для пакетных операций
	ReadFromFollower bool `toml:"read_from_follower"`   // Читать с ведомых узлов
	MaxConnections   int  `toml:"max_connections"`      // Максимальное количество соединений
	ReadReplicaDelayMs int `toml:"read_replica_delay_ms"` // Задержка чтения с реплики (мс)
}

// =============================================================================
// КОНФИГУРАЦИЯ БЕЗОПАСНОСТИ
// =============================================================================

// SecurityConfig содержит настройки безопасности (TLS).
type SecurityConfig struct {
	EnableTLS  bool   `toml:"enable_tls"`   // Включен ли TLS
	CertFile   string `toml:"cert_file"`    // Путь к сертификату
	KeyFile    string `toml:"key_file"`     // Путь к приватному ключу
	CAFile     string `toml:"ca_file"`      // Путь к корневому сертификату CA
	MinVersion string `toml:"min_version"`  // Минимальная версия TLS (1.0, 1.1, 1.2, 1.3)
}

// =============================================================================
// КОНФИГУРАЦИЯ МОНИТОРИНГА
// =============================================================================

// MonitoringConfig содержит настройки мониторинга и наблюдаемости.
type MonitoringConfig struct {
	EnableMetrics   bool    `toml:"enable_metrics"`    // Включить сбор метрик
	MetricsPort     int     `toml:"metrics_port"`      // Порт для экспорта метрик (Prometheus)
	EnableTracing   bool    `toml:"enable_tracing"`    // Включить распределённую трассировку
	TraceSampleRate float64 `toml:"trace_sample_rate"` // Частота сэмплирования трассировки (0-1)
}

// =============================================================================
// КОНФИГУРАЦИЯ ВОССТАНОВЛЕНИЯ
// =============================================================================

// RecoveryConfig содержит настройки восстановления после сбоев.
type RecoveryConfig struct {
	AutoRejoin                bool `toml:"auto_rejoin"`                   // Автоматическое переподключение
	MaxRetrySec               int  `toml:"max_retry_sec"`                // Макс. время повторных попыток
	DataReplicationTimeoutSec int  `toml:"data_replication_timeout_sec"` // Таймаут репликации данных
	StaleReadTimeoutSec       int  `toml:"stale_read_timeout_sec"`       // Таймаут устаревшего чтения
}

// =============================================================================
// КОНФИГУРАЦИЯ WAL (WRITE-AHEAD LOG)
// =============================================================================

// WALConfig содержит настройки журнала упреждающей записи.
// WAL обеспечивает durability и используется для восстановления после сбоев.
type WALConfig struct {
	SegmentSizeMB         int  `toml:"segment_size_mb"`          // Размер сегмента WAL (МБ)
	SyncIntervalSec       int  `toml:"sync_interval_sec"`        // Интервал синхронизации на диск
	BatchSize             int  `toml:"batch_size"`               // Размер пакета для записи
	RecoveryWorkers       int  `toml:"recovery_workers"`         // Количество потоков восстановления
	Enabled               bool `toml:"enabled"`                  // Включен ли WAL
	AsyncRecovery         bool `toml:"async_recovery"`           // Асинхронное восстановление
	AsyncRecoveryWorkers  int  `toml:"async_recovery_workers"`   // Потоков асинхронного восстановления
	AsyncRecoveryBuffer   int  `toml:"async_recovery_buffer"`    // Размер буфера асинхронного восстановления
}

// =============================================================================
// КОНФИГУРАЦИЯ MVCC (MULTI-VERSION CONCURRENCY CONTROL)
// =============================================================================

// MVCCConfig содержит настройки механизма многоверсионного контроля параллелизма.
// MVCC позволяет читать согласованные снимки данных без блокировок записи.
type MVCCConfig struct {
	MaxVersionsPerDoc   int `toml:"max_versions_per_doc"`   // Макс. версий на документ
	VisibilityMapSize   int `toml:"visibility_map_size"`    // Размер карты видимости
	PruneIntervalMin    int `toml:"prune_interval_min"`     // Интервал очистки старых версий (минуты)
	RetentionDays       int `toml:"retention_days"`         // Срок хранения старых версий (дни)
	ReadCacheSize       int `toml:"read_cache_size"`        // Размер кэша чтения
	ReadCacheTTLSec     int `toml:"read_cache_ttl_sec"`     // TTL кэша чтения (секунды)
}

// =============================================================================
// КОНФИГУРАЦИЯ ТРАНЗАКЦИЙ
// =============================================================================

// TransactionsConfig содержит настройки транзакционной подсистемы.
type TransactionsConfig struct {
	DefaultTimeoutSec        int  `toml:"default_timeout_sec"`         // Таймаут транзакции по умолчанию
	DeadlockCheckIntervalSec int  `toml:"deadlock_check_interval_sec"` // Интервал проверки deadlock
	MaxSavepointsPerTx       int  `toml:"max_savepoints_per_tx"`       // Макс. точек сохранения
	Enabled                  bool `toml:"enabled"`                     // Включены ли транзакции
	CheckpointIntervalSec    int  `toml:"checkpoint_interval_sec"`     // Интервал контрольных точек
}

// =============================================================================
// КОНФИГУРАЦИЯ ACL (ACCESS CONTROL LIST)
// =============================================================================

// ACLConfig содержит настройки контроля доступа.
type ACLConfig struct {
	MaxDeniedLogSize      int  `toml:"max_denied_log_size"`        // Макс. размер лога отказов
	TemporaryGrantTTLHours int  `toml:"temporary_grant_ttl_hours"` // TTL временных прав (часы)
	EnableRoleHierarchy   bool `toml:"enable_role_hierarchy"`      // Иерархия ролей
	CacheTTLSec           int  `toml:"cache_ttl_sec"`              // TTL кэша ACL (сек)
}

// =============================================================================
// КОНФИГУРАЦИЯ TLS ДЛЯ КЛАСТЕРА
// =============================================================================

// TLSConfig содержит настройки TLS для кластерного взаимодействия.
type TLSConfig struct {
	Enabled            bool   `toml:"enabled"`              // Включен ли TLS
	CertFile           string `toml:"cert_file"`            // Путь к сертификату
	KeyFile            string `toml:"key_file"`             // Путь к приватному ключу
	CAFile             string `toml:"ca_file"`              // Путь к корневому сертификату CA
	MinVersion         string `toml:"min_version"`          // Минимальная версия TLS
	MutualAuth         bool   `toml:"mutual_auth"`          // Взаимная аутентификация
	KeyRotationDays    int    `toml:"key_rotation_days"`    // Интервал ротации ключей
	AutoGenerate       bool   `toml:"auto_generate"`        // Автоматическая генерация сертификатов
}

// =============================================================================
// КОНФИГУРАЦИЯ ОБРАТНОГО ДАВЛЕНИЯ (BACKPRESSURE)
// =============================================================================

// BackpressureConfig содержит настройки механизма обратного давления.
// Обратное давление защищает систему от перегрузки, ограничивая поступление запросов.
type BackpressureConfig struct {
	Enabled              bool    `toml:"enabled"`                // Включен ли механизм
	CPUThreshold         float64 `toml:"cpu_threshold"`          // Порог CPU (0-1)
	MemoryThreshold      float64 `toml:"memory_threshold"`       // Порог памяти (0-1)
	QueueSizeThreshold   int     `toml:"queue_size_threshold"`   // Порог размера очереди
	ConnectionThreshold  int     `toml:"connection_threshold"`   // Порог количества соединений
	CheckIntervalMs      int     `toml:"check_interval_ms"`      // Интервал проверки (мс)
	LowDelayMs           int64   `toml:"low_delay_ms"`           // Задержка для низкой нагрузки (мс)
	MediumRejectProb     uint32  `toml:"medium_reject_prob"`     // Вероятность отказа при средней нагрузке (%)
	HighRejectProb       uint32  `toml:"high_reject_prob"`       // Вероятность отказа при высокой нагрузке (%)
}

// =============================================================================
// КОНФИГУРАЦИЯ ОГРАНИЧЕНИЙ ВРЕМЕНИ ВЫПОЛНЕНИЯ
// =============================================================================

// RuntimeLimitsConfig содержит глобальные ограничения для защиты от недобросовестных запросов.
type RuntimeLimitsConfig struct {
	Enabled              bool  `toml:"enabled"`                      // Включены ли ограничения
	GlobalMaxDocSizeMB   int   `toml:"global_max_doc_size_mb"`      // Макс. размер документа (МБ)
	GlobalMaxCollSizeMB  int64 `toml:"global_max_coll_size_mb"`     // Макс. размер коллекции (МБ)
	GlobalMaxDocsPerColl int64 `toml:"global_max_docs_per_coll"`    // Макс. документов в коллекции
}

// =============================================================================
// КОНФИГУРАЦИЯ АВТОМАСШТАБИРОВАНИЯ
// =============================================================================

// AutoscalingConfig содержит настройки автоматического масштабирования кластера.
type AutoscalingConfig struct {
	Enabled               bool    `toml:"enabled"`                  // Включено ли автомасштабирование
	MinNodes              int     `toml:"min_nodes"`               // Минимальное количество узлов
	MaxNodes              int     `toml:"max_nodes"`               // Максимальное количество узлов
	ScaleUpThreshold      float64 `toml:"scale_up_threshold"`      // Порог для увеличения (0-1)
	ScaleDownThreshold    float64 `toml:"scale_down_threshold"`    // Порог для уменьшения (0-1)
	ScaleUpCooldownSec    int     `toml:"scale_up_cooldown_sec"`   // Задержка перед масштабированием вверх
	ScaleDownCooldownSec  int     `toml:"scale_down_cooldown_sec"` // Задержка перед масштабированием вниз
	EvaluationIntervalSec int     `toml:"evaluation_interval_sec"` // Интервал оценки нагрузки
	PredictiveEnabled     bool    `toml:"predictive_enabled"`      // Прогнозирующее масштабирование
	MaxScaleUpNodes       int     `toml:"max_scale_up_nodes"`      // Макс. узлов при масштабировании вверх
	MaxScaleDownNodes     int     `toml:"max_scale_down_nodes"`    // Макс. узлов при масштабировании вниз
}

// =============================================================================
// КОНФИГУРАЦИЯ МИГРАЦИИ СХЕМЫ
// =============================================================================

// SchemaMigrationConfig содержит настройки миграции схемы данных.
type SchemaMigrationConfig struct {
	Enabled        bool   `toml:"enabled"`          // Включена ли миграция
	MigrationDir   string `toml:"migration_dir"`    // Директория с миграциями
	AutoMigrate    bool   `toml:"auto_migrate"`     // Автоматическая миграция при старте
	TargetVersion  string `toml:"target_version"`   // Целевая версия схемы
}

// =============================================================================
// КОНФИГУРАЦИЯ РЕЗЕРВНОГО КОПИРОВАНИЯ
// =============================================================================

// BackupConfig содержит настройки резервного копирования.
type BackupConfig struct {
	Enabled          bool   `toml:"enabled"`            // Включено ли резервное копирование
	BackupDir        string `toml:"backup_dir"`         // Директория для бэкапов
	MaxConcurrent    int    `toml:"max_concurrent"`     // Макс. параллельных бэкапов
	CompressEnabled  bool   `toml:"compress_enabled"`   // Сжатие бэкапов
	RetentionDays    int    `toml:"retention_days"`     // Срок хранения бэкапов (дни)
}

// =============================================================================
// КОНФИГУРАЦИЯ ДВИЖКОВ ХРАНЕНИЯ
// =============================================================================

// EnginesConfig содержит настройки всех движков хранения данных.
// Каждый движок оптимизирован для определённого типа нагрузки.
type EnginesConfig struct {
	Row      EngineConfig `toml:"row"`       // Строчный движок (по умолчанию)
	Columnar EngineConfig `toml:"columnar"`  // Колоночный движок (аналитика)
	Document EngineConfig `toml:"document"`  // Документоориентированный движок
	KV       EngineConfig `toml:"kv"`        // Key-Value движок
	TS       EngineConfig `toml:"ts"`        // Time-Series движок (временные ряды)
	Graph    EngineConfig `toml:"graph"`     // Графовый движок
}

// EngineConfig содержит настройки отдельного движка хранения.
type EngineConfig struct {
	Enabled     bool                   `toml:"enabled"`     // Включен ли движок
	Description string                 `toml:"description"` // Описание движка
	Config      map[string]interface{} `toml:"config"`     // Дополнительные параметры движка
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ СТРУКТУРЫ
// =============================================================================

// ValidationResult содержит результаты валидации конфигурации.
type ValidationResult struct {
	Valid    bool     // Корректна ли конфигурация
	Errors   []error  // Критические ошибки
	Warnings []error  // Предупреждения (некритичные)
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ SagaConfig
// =============================================================================

// IsSagaEnabled возвращает флаг включения SAGA оркестратора.
func (s *SagaConfig) IsSagaEnabled() bool {
	return s.Enabled
}

// GetCoordinatorCount возвращает количество координаторов.
// По умолчанию 3.
func (s *SagaConfig) GetCoordinatorCount() int {
	if s.CoordinatorCount < 1 {
		return 3
	}
	return s.CoordinatorCount
}

// GetStateDir возвращает директорию для хранения состояний SAGA.
// По умолчанию "saga_states".
func (s *SagaConfig) GetStateDir() string {
	if s.StateDir == "" {
		return "saga_states"
	}
	return s.StateDir
}

// GetMaxRetries возвращает максимальное количество попыток выполнения шага.
// По умолчанию 5.
func (s *SagaConfig) GetMaxRetries() int {
	if s.MaxRetries <= 0 {
		return 5
	}
	return s.MaxRetries
}

// GetRetryBackoff возвращает базовую задержку между повторными попытками.
// По умолчанию 100 миллисекунд.
func (s *SagaConfig) GetRetryBackoff() time.Duration {
	if s.RetryBackoffMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(s.RetryBackoffMs) * time.Millisecond
}

// GetSagaTimeout возвращает таймаут выполнения SAGA.
// По умолчанию 300 секунд (5 минут).
func (s *SagaConfig) GetSagaTimeout() time.Duration {
	if s.SagaTimeoutSec <= 0 {
		return 300 * time.Second
	}
	return time.Duration(s.SagaTimeoutSec) * time.Second
}

// GetStuckCheckInterval возвращает интервал проверки зависших SAGA.
// По умолчанию 10 секунд.
func (s *SagaConfig) GetStuckCheckInterval() time.Duration {
	if s.StuckCheckIntervalSec <= 0 {
		return 10 * time.Second
	}
	return time.Duration(s.StuckCheckIntervalSec) * time.Second
}

// GetLeaderElectionInterval возвращает интервал выбора лидера.
// По умолчанию 5 секунд.
func (s *SagaConfig) GetLeaderElectionInterval() time.Duration {
	if s.LeaderElectionIntervalSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(s.LeaderElectionIntervalSec) * time.Second
}

// GetRecoveryInterval возвращает интервал восстановления незавершённых SAGA.
// По умолчанию 30 секунд.
func (s *SagaConfig) GetRecoveryInterval() time.Duration {
	if s.RecoveryIntervalSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(s.RecoveryIntervalSec) * time.Second
}

// GetMetricsInterval возвращает интервал сбора метрик.
// По умолчанию 60 секунд.
func (s *SagaConfig) GetMetricsInterval() time.Duration {
	if s.MetricsIntervalSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(s.MetricsIntervalSec) * time.Second
}

// GetCleanupPeriod возвращает период очистки старых SAGA.
// По умолчанию 24 часа.
func (s *SagaConfig) GetCleanupPeriod() time.Duration {
	if s.CleanupPeriodHours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(s.CleanupPeriodHours) * time.Hour
}

// GetMaxCacheSize возвращает максимальное количество состояний в кэше.
// По умолчанию 10000.
func (s *SagaConfig) GetMaxCacheSize() int {
	if s.MaxCacheSize <= 0 {
		return 10000
	}
	return s.MaxCacheSize
}

// GetOperationRetentionDuration возвращает время жизни выполненных операций.
// По умолчанию 7 дней.
func (s *SagaConfig) GetOperationRetentionDuration() time.Duration {
	if s.OperationRetentionDays <= 0 {
		return 7 * 24 * time.Hour
	}
	return time.Duration(s.OperationRetentionDays) * 24 * time.Hour
}

// GetChannelBufferSize возвращает размер буфера каналов.
// По умолчанию 10000.
func (s *SagaConfig) GetChannelBufferSize() int {
	if s.ChannelBufferSize <= 0 {
		return 10000
	}
	return s.ChannelBufferSize
}

// GetAsyncRecoveryWorkers возвращает количество воркеров для асинхронного восстановления.
// По умолчанию 4.
func (s *SagaConfig) GetAsyncRecoveryWorkers() int {
	if s.AsyncRecoveryWorkers <= 0 {
		return 4
	}
	return s.AsyncRecoveryWorkers
}

// GetAsyncRecoveryTimeout возвращает таймаут асинхронного восстановления.
// По умолчанию 30 секунд.
func (s *SagaConfig) GetAsyncRecoveryTimeout() time.Duration {
	if s.AsyncRecoveryTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(s.AsyncRecoveryTimeoutSec) * time.Second
}

// IsFsyncEnabled возвращает флаг принудительной синхронизации с диском.
func (s *SagaConfig) IsFsyncEnabled() bool {
	return s.FsyncEnabled
}

// GetFsyncMaxRetries возвращает количество повторных попыток синхронизации.
// По умолчанию 3.
func (s *SagaConfig) GetFsyncMaxRetries() int {
	if s.FsyncMaxRetries <= 0 {
		return 3
	}
	return s.FsyncMaxRetries
}

// GetFsyncRetryDelay возвращает задержку между повторными попытками синхронизации.
// По умолчанию 100 миллисекунд.
func (s *SagaConfig) GetFsyncRetryDelay() time.Duration {
	if s.FsyncRetryDelayMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(s.FsyncRetryDelayMs) * time.Millisecond
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ ReplicationConfig
// =============================================================================

// GetReplicationTimeout возвращает таймаут репликации как time.Duration.
// Если значение не задано (≤ 0), возвращается значение по умолчанию 5 секунд.
func (r *ReplicationConfig) GetReplicationTimeout() time.Duration {
	if r.ReplicationTimeoutMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(r.ReplicationTimeoutMs) * time.Millisecond
}

// IsReplicationEnabled возвращает флаг включения репликации.
func (r *ReplicationConfig) IsReplicationEnabled() bool {
	return r.Enabled
}

// IsSyncReplicationEnabled возвращает флаг синхронной репликации.
func (r *ReplicationConfig) IsSyncReplicationEnabled() bool {
	return r.SyncReplication
}

// GetMaxReplicaLag возвращает максимальное допустимое отставание реплики.
// Если значение не задано (≤ 0), возвращается 5 секунд.
func (r *ReplicationConfig) GetMaxReplicaLag() time.Duration {
	if r.MaxReplicaLagMs <= 0 {
		return 5 * time.Second
	}
	return time.Duration(r.MaxReplicaLagMs) * time.Millisecond
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ ClusterConfig
// =============================================================================

// GetHeartbeatTimeout возвращает таймаут heartbeat-сообщений.
// По умолчанию 1 секунда.
func (c *ClusterConfig) GetHeartbeatTimeout() time.Duration {
	if c.HeartbeatTimeoutMs <= 0 {
		return 1000 * time.Millisecond
	}
	return time.Duration(c.HeartbeatTimeoutMs) * time.Millisecond
}

// GetElectionTimeout возвращает таймаут для начала выборов лидера.
// По умолчанию 1 секунда.
func (c *ClusterConfig) GetElectionTimeout() time.Duration {
	if c.ElectionTimeoutMs <= 0 {
		return 1000 * time.Millisecond
	}
	return time.Duration(c.ElectionTimeoutMs) * time.Millisecond
}

// GetCommitTimeout возвращает таймаут коммита записей.
// По умолчанию 500 миллисекунд.
func (c *ClusterConfig) GetCommitTimeout() time.Duration {
	if c.CommitTimeoutMs <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(c.CommitTimeoutMs) * time.Millisecond
}

// GetSnapshotInterval возвращает интервал создания снэпшотов.
// По умолчанию 30 минут.
func (c *ClusterConfig) GetSnapshotInterval() time.Duration {
	if c.SnapshotIntervalMin <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(c.SnapshotIntervalMin) * time.Minute
}

// GetSnapshotThreshold возвращает порог количества записей для создания снэпшота.
// По умолчанию 1000 записей.
func (c *ClusterConfig) GetSnapshotThreshold() uint64 {
	if c.SnapshotThreshold <= 0 {
		return 1000
	}
	return uint64(c.SnapshotThreshold)
}

// IsSplitBrainPreventionEnabled возвращает флаг защиты от split-brain.
func (c *ClusterConfig) IsSplitBrainPreventionEnabled() bool {
	return c.SplitBrainPrevention
}

// GetRecoveryTimeout возвращает таймаут восстановления после сбоя.
// По умолчанию 30 секунд.
func (c *ClusterConfig) GetRecoveryTimeout() time.Duration {
	if c.RecoveryTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(c.RecoveryTimeoutSec) * time.Second
}

// GetRegion возвращает регион узла.
// Если не указан, возвращает "default".
func (c *ClusterConfig) GetRegion() string {
	if c.Region == "" {
		return "default"
	}
	return c.Region
}

// GetPriorityZone возвращает приоритетную зону узла.
// Значение нормализуется в диапазон [0, 9].
func (c *ClusterConfig) GetPriorityZone() int {
	if c.PriorityZone < 0 {
		return 0
	}
	if c.PriorityZone > 9 {
		return 9
	}
	return c.PriorityZone
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ PluginsConfig
// =============================================================================

// GetMaxCPUTime возвращает максимальное время CPU для выполнения скрипта.
// По умолчанию 100 миллисекунд.
func (p *PluginsConfig) GetMaxCPUTime() time.Duration {
	if p.MaxCPUTimeMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(p.MaxCPUTimeMs) * time.Millisecond
}

// GetMaxMemory возвращает максимальный объем памяти для скрипта в байтах.
// По умолчанию 50 МБ.
func (p *PluginsConfig) GetMaxMemory() int64 {
	if p.MaxMemoryMB <= 0 {
		return 50 * 1024 * 1024
	}
	return int64(p.MaxMemoryMB) * 1024 * 1024
}

// GetMaxExecutionTime возвращает максимальное время выполнения скрипта.
// По умолчанию 5 секунд.
func (p *PluginsConfig) GetMaxExecutionTime() time.Duration {
	if p.MaxExecutionTimeSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(p.MaxExecutionTimeSec) * time.Second
}

// GetMaxInstructions возвращает максимальное количество инструкций для скрипта.
// По умолчанию 1 000 000.
func (p *PluginsConfig) GetMaxInstructions() int64 {
	if p.MaxInstructions <= 0 {
		return 1000000
	}
	return p.MaxInstructions
}

// GetHotReloadInterval возвращает интервал горячей перезагрузки скриптов.
// По умолчанию 30 секунд.
func (p *PluginsConfig) GetHotReloadInterval() time.Duration {
	if p.HotReloadIntervalSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(p.HotReloadIntervalSec) * time.Second
}

// GetMaxEventLogSize возвращает максимальный размер лога событий.
// По умолчанию 1000 записей.
func (p *PluginsConfig) GetMaxEventLogSize() int {
	if p.MaxEventLogSize <= 0 {
		return 1000
	}
	return p.MaxEventLogSize
}

// GetLoadTimeout возвращает таймаут загрузки скрипта.
// По умолчанию 10 секунд.
func (p *PluginsConfig) GetLoadTimeout() time.Duration {
	if p.LoadTimeoutSec <= 0 {
		return 10 * time.Second
	}
	return time.Duration(p.LoadTimeoutSec) * time.Second
}

// GetMaxLuaStates возвращает максимальное количество Lua-состояний.
// По умолчанию 100.
func (p *PluginsConfig) GetMaxLuaStates() int {
	if p.MaxLuaStates <= 0 {
		return 100
	}
	return p.MaxLuaStates
}

// GetLuaStateTTL возвращает время жизни Lua-состояния.
// По умолчанию 10 минут.
func (p *PluginsConfig) GetLuaStateTTL() time.Duration {
	if p.LuaStateTTLSec <= 0 {
		return 10 * time.Minute
	}
	return time.Duration(p.LuaStateTTLSec) * time.Second
}

// GetEnginePluginDir возвращает директорию с плагинами движков.
// По умолчанию "engines".
func (p *PluginsConfig) GetEnginePluginDir() string {
	if p.EnginePluginDir == "" {
		return "engines"
	}
	return p.EnginePluginDir
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ WALConfig
// =============================================================================

// GetSegmentSize возвращает размер сегмента WAL в байтах.
// По умолчанию 64 МБ.
func (w *WALConfig) GetSegmentSize() int64 {
	if w.SegmentSizeMB <= 0 {
		return 64 * 1024 * 1024
	}
	return int64(w.SegmentSizeMB) * 1024 * 1024
}

// GetSyncInterval возвращает интервал синхронизации WAL с диском.
// По умолчанию 5 секунд.
func (w *WALConfig) GetSyncInterval() time.Duration {
	if w.SyncIntervalSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(w.SyncIntervalSec) * time.Second
}

// GetBatchSize возвращает размер пакета для записи в WAL.
// По умолчанию 100 записей.
func (w *WALConfig) GetBatchSize() int {
	if w.BatchSize <= 0 {
		return 100
	}
	return w.BatchSize
}

// GetRecoveryWorkers возвращает количество потоков для восстановления из WAL.
// По умолчанию 4 потока.
func (w *WALConfig) GetRecoveryWorkers() int {
	if w.RecoveryWorkers <= 0 {
		return 4
	}
	return w.RecoveryWorkers
}

// IsWALEnabled возвращает флаг включения WAL.
func (w *WALConfig) IsWALEnabled() bool {
	return w.Enabled
}

// IsAsyncRecoveryEnabled возвращает флаг асинхронного восстановления.
func (w *WALConfig) IsAsyncRecoveryEnabled() bool {
	return w.AsyncRecovery
}

// GetAsyncRecoveryWorkers возвращает количество потоков асинхронного восстановления.
// По умолчанию 4 потока.
func (w *WALConfig) GetAsyncRecoveryWorkers() int {
	if w.AsyncRecoveryWorkers <= 0 {
		return 4
	}
	return w.AsyncRecoveryWorkers
}

// GetAsyncRecoveryBuffer возвращает размер буфера асинхронного восстановления.
// По умолчанию 10 000 записей.
func (w *WALConfig) GetAsyncRecoveryBuffer() int {
	if w.AsyncRecoveryBuffer <= 0 {
		return 10000
	}
	return w.AsyncRecoveryBuffer
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ MVCCConfig
// =============================================================================

// GetMaxVersionsPerDoc возвращает максимальное количество версий на документ.
// По умолчанию 10 версий.
func (m *MVCCConfig) GetMaxVersionsPerDoc() int {
	if m.MaxVersionsPerDoc <= 0 {
		return 10
	}
	return m.MaxVersionsPerDoc
}

// GetVisibilityMapSize возвращает размер карты видимости.
// По умолчанию 1 048 576 элементов (1 МБ).
func (m *MVCCConfig) GetVisibilityMapSize() int {
	if m.VisibilityMapSize <= 0 {
		return 1024 * 1024
	}
	return m.VisibilityMapSize
}

// GetPruneInterval возвращает интервал очистки старых версий.
// По умолчанию 5 минут.
func (m *MVCCConfig) GetPruneInterval() time.Duration {
	if m.PruneIntervalMin <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(m.PruneIntervalMin) * time.Minute
}

// GetRetentionDays возвращает срок хранения старых версий в днях.
// По умолчанию 7 дней.
func (m *MVCCConfig) GetRetentionDays() int {
	if m.RetentionDays <= 0 {
		return 7
	}
	return m.RetentionDays
}

// GetReadCacheSize возвращает размер кэша чтения.
// По умолчанию 10 000 записей.
func (m *MVCCConfig) GetReadCacheSize() int {
	if m.ReadCacheSize <= 0 {
		return 10000
	}
	return m.ReadCacheSize
}

// GetReadCacheTTL возвращает TTL кэша чтения.
// По умолчанию 300 секунд (5 минут).
func (m *MVCCConfig) GetReadCacheTTL() time.Duration {
	if m.ReadCacheTTLSec <= 0 {
		return 300 * time.Second
	}
	return time.Duration(m.ReadCacheTTLSec) * time.Second
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ TransactionsConfig
// =============================================================================

// GetDefaultTimeout возвращает таймаут транзакции по умолчанию.
// По умолчанию 30 секунд.
func (t *TransactionsConfig) GetDefaultTimeout() time.Duration {
	if t.DefaultTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(t.DefaultTimeoutSec) * time.Second
}

// GetDeadlockCheckInterval возвращает интервал проверки взаимоблокировок.
// По умолчанию 1 секунда.
func (t *TransactionsConfig) GetDeadlockCheckInterval() time.Duration {
	if t.DeadlockCheckIntervalSec <= 0 {
		return 1 * time.Second
	}
	return time.Duration(t.DeadlockCheckIntervalSec) * time.Second
}

// GetMaxSavepointsPerTx возвращает максимальное количество точек сохранения.
// По умолчанию 100.
func (t *TransactionsConfig) GetMaxSavepointsPerTx() int {
	if t.MaxSavepointsPerTx <= 0 {
		return 100
	}
	return t.MaxSavepointsPerTx
}

// IsTransactionsEnabled возвращает флаг включения транзакций.
func (t *TransactionsConfig) IsTransactionsEnabled() bool {
	return t.Enabled
}

// GetCheckpointInterval возвращает интервал контрольных точек в секундах.
// По умолчанию 300 секунд (5 минут).
func (t *TransactionsConfig) GetCheckpointInterval() int64 {
	if t.CheckpointIntervalSec <= 0 {
		return 300
	}
	return int64(t.CheckpointIntervalSec)
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ ACLConfig
// =============================================================================

// GetMaxDeniedLogSize возвращает максимальный размер лога отказов в доступе.
// По умолчанию 10 000 записей.
func (a *ACLConfig) GetMaxDeniedLogSize() int {
	if a.MaxDeniedLogSize <= 0 {
		return 10000
	}
	return a.MaxDeniedLogSize
}

// GetTemporaryGrantTTL возвращает TTL временных прав доступа.
// По умолчанию 24 часа.
func (a *ACLConfig) GetTemporaryGrantTTL() time.Duration {
	if a.TemporaryGrantTTLHours <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(a.TemporaryGrantTTLHours) * time.Hour
}

// IsRoleHierarchyEnabled возвращает флаг включения иерархии ролей.
func (a *ACLConfig) IsRoleHierarchyEnabled() bool {
	return a.EnableRoleHierarchy
}

// GetCacheTTL возвращает TTL кэша ACL.
// По умолчанию 60 секунд.
func (a *ACLConfig) GetCacheTTL() time.Duration {
	if a.CacheTTLSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(a.CacheTTLSec) * time.Second
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ TLSConfig
// =============================================================================

// GetTLSMinVersion возвращает минимальную версию TLS как строку.
// По умолчанию "1.2".
func (t *TLSConfig) GetTLSMinVersion() string {
	if t.MinVersion == "" {
		return "1.2"
	}
	return t.MinVersion
}

// IsTLSEnabled возвращает флаг включения TLS.
func (t *TLSConfig) IsTLSEnabled() bool {
	return t.Enabled
}

// IsMutualAuthEnabled возвращает флаг взаимной аутентификации.
func (t *TLSConfig) IsMutualAuthEnabled() bool {
	return t.MutualAuth
}

// GetKeyRotationDays возвращает интервал ротации ключей в днях.
// По умолчанию 30 дней.
func (t *TLSConfig) GetKeyRotationDays() int {
	if t.KeyRotationDays <= 0 {
		return 30
	}
	return t.KeyRotationDays
}

// IsAutoGenerateEnabled возвращает флаг автоматической генерации сертификатов.
func (t *TLSConfig) IsAutoGenerateEnabled() bool {
	return t.AutoGenerate
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ BackpressureConfig
// =============================================================================

// IsBackpressureEnabled возвращает флаг включения механизма обратного давления.
func (b *BackpressureConfig) IsBackpressureEnabled() bool {
	return b.Enabled
}

// GetCPUThreshold возвращает порог загрузки CPU.
// По умолчанию 0.8 (80%).
// Значение нормализуется в диапазон [0, 1].
func (b *BackpressureConfig) GetCPUThreshold() float64 {
	if b.CPUThreshold <= 0 {
		return 0.8
	}
	if b.CPUThreshold > 1.0 {
		return 1.0
	}
	return b.CPUThreshold
}

// GetMemoryThreshold возвращает порог использования памяти.
// По умолчанию 0.85 (85%).
// Значение нормализуется в диапазон [0, 1].
func (b *BackpressureConfig) GetMemoryThreshold() float64 {
	if b.MemoryThreshold <= 0 {
		return 0.85
	}
	if b.MemoryThreshold > 1.0 {
		return 1.0
	}
	return b.MemoryThreshold
}

// GetQueueSizeThreshold возвращает порог размера очереди.
// По умолчанию 10 000 записей.
func (b *BackpressureConfig) GetQueueSizeThreshold() int {
	if b.QueueSizeThreshold <= 0 {
		return 10000
	}
	return b.QueueSizeThreshold
}

// GetConnectionThreshold возвращает порог количества соединений.
// По умолчанию 5 000.
func (b *BackpressureConfig) GetConnectionThreshold() int {
	if b.ConnectionThreshold <= 0 {
		return 5000
	}
	return b.ConnectionThreshold
}

// GetCheckInterval возвращает интервал проверки нагрузки.
// По умолчанию 1 секунда.
func (b *BackpressureConfig) GetCheckInterval() time.Duration {
	if b.CheckIntervalMs <= 0 {
		return 1 * time.Second
	}
	return time.Duration(b.CheckIntervalMs) * time.Millisecond
}

// GetLowDelay возвращает задержку при низкой нагрузке.
// По умолчанию 100 миллисекунд.
func (b *BackpressureConfig) GetLowDelay() time.Duration {
	if b.LowDelayMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(b.LowDelayMs) * time.Millisecond
}

// GetMediumRejectProb возвращает вероятность отказа при средней нагрузке.
// Значение нормализуется в диапазон [0, 100].
func (b *BackpressureConfig) GetMediumRejectProb() uint32 {
	if b.MediumRejectProb > 100 {
		return 100
	}
	return b.MediumRejectProb
}

// GetHighRejectProb возвращает вероятность отказа при высокой нагрузке.
// Значение нормализуется в диапазон [0, 100].
func (b *BackpressureConfig) GetHighRejectProb() uint32 {
	if b.HighRejectProb > 100 {
		return 100
	}
	return b.HighRejectProb
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ RuntimeLimitsConfig
// =============================================================================

// IsRuntimeLimitsEnabled возвращает флаг включения ограничений времени выполнения.
func (r *RuntimeLimitsConfig) IsRuntimeLimitsEnabled() bool {
	return r.Enabled
}

// GetGlobalMaxDocSizeMB возвращает максимальный размер документа в МБ.
// По умолчанию 16 МБ.
func (r *RuntimeLimitsConfig) GetGlobalMaxDocSizeMB() int {
	if r.GlobalMaxDocSizeMB <= 0 {
		return 16
	}
	return r.GlobalMaxDocSizeMB
}

// GetGlobalMaxCollSizeMB возвращает максимальный размер коллекции в МБ.
// По умолчанию 10240 МБ (10 ГБ).
func (r *RuntimeLimitsConfig) GetGlobalMaxCollSizeMB() int64 {
	if r.GlobalMaxCollSizeMB <= 0 {
		return 10240
	}
	return r.GlobalMaxCollSizeMB
}

// GetGlobalMaxDocsPerColl возвращает максимальное количество документов в коллекции.
// По умолчанию 10 000 000.
func (r *RuntimeLimitsConfig) GetGlobalMaxDocsPerColl() int64 {
	if r.GlobalMaxDocsPerColl <= 0 {
		return 10000000
	}
	return r.GlobalMaxDocsPerColl
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ AutoscalingConfig
// =============================================================================

// IsAutoscalingEnabled возвращает флаг включения автомасштабирования.
func (a *AutoscalingConfig) IsAutoscalingEnabled() bool {
	return a.Enabled
}

// GetMinNodes возвращает минимальное количество узлов.
// По умолчанию 1.
func (a *AutoscalingConfig) GetMinNodes() int {
	if a.MinNodes <= 0 {
		return 1
	}
	return a.MinNodes
}

// GetMaxNodes возвращает максимальное количество узлов.
// По умолчанию 10.
func (a *AutoscalingConfig) GetMaxNodes() int {
	if a.MaxNodes <= 0 {
		return 10
	}
	return a.MaxNodes
}

// GetScaleUpThreshold возвращает порог для масштабирования вверх.
// По умолчанию 0.75 (75%).
// Значение нормализуется в диапазон [0, 1].
func (a *AutoscalingConfig) GetScaleUpThreshold() float64 {
	if a.ScaleUpThreshold <= 0 {
		return 0.75
	}
	if a.ScaleUpThreshold > 1.0 {
		return 1.0
	}
	return a.ScaleUpThreshold
}

// GetScaleDownThreshold возвращает порог для масштабирования вниз.
// По умолчанию 0.30 (30%).
func (a *AutoscalingConfig) GetScaleDownThreshold() float64 {
	if a.ScaleDownThreshold <= 0 {
		return 0.30
	}
	return a.ScaleDownThreshold
}

// GetScaleUpCooldown возвращает задержку перед масштабированием вверх.
// По умолчанию 5 минут.
func (a *AutoscalingConfig) GetScaleUpCooldown() time.Duration {
	if a.ScaleUpCooldownSec <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(a.ScaleUpCooldownSec) * time.Second
}

// GetScaleDownCooldown возвращает задержку перед масштабированием вниз.
// По умолчанию 10 минут.
func (a *AutoscalingConfig) GetScaleDownCooldown() time.Duration {
	if a.ScaleDownCooldownSec <= 0 {
		return 10 * time.Minute
	}
	return time.Duration(a.ScaleDownCooldownSec) * time.Second
}

// GetEvaluationInterval возвращает интервал оценки нагрузки.
// По умолчанию 30 секунд.
func (a *AutoscalingConfig) GetEvaluationInterval() time.Duration {
	if a.EvaluationIntervalSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(a.EvaluationIntervalSec) * time.Second
}

// IsPredictiveEnabled возвращает флаг прогнозирующего масштабирования.
func (a *AutoscalingConfig) IsPredictiveEnabled() bool {
	return a.PredictiveEnabled
}

// GetMaxScaleUpNodes возвращает максимальное количество узлов для масштабирования вверх.
// По умолчанию 3.
func (a *AutoscalingConfig) GetMaxScaleUpNodes() int {
	if a.MaxScaleUpNodes <= 0 {
		return 3
	}
	return a.MaxScaleUpNodes
}

// GetMaxScaleDownNodes возвращает максимальное количество узлов для масштабирования вниз.
// По умолчанию 2.
func (a *AutoscalingConfig) GetMaxScaleDownNodes() int {
	if a.MaxScaleDownNodes <= 0 {
		return 2
	}
	return a.MaxScaleDownNodes
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ SchemaMigrationConfig
// =============================================================================

// IsSchemaMigrationEnabled возвращает флаг включения миграции схемы.
func (s *SchemaMigrationConfig) IsSchemaMigrationEnabled() bool {
	return s.Enabled
}

// GetMigrationDir возвращает директорию с миграциями.
// По умолчанию "migrations".
func (s *SchemaMigrationConfig) GetMigrationDir() string {
	if s.MigrationDir == "" {
		return "migrations"
	}
	return s.MigrationDir
}

// IsAutoMigrateEnabled возвращает флаг автоматической миграции при старте.
func (s *SchemaMigrationConfig) IsAutoMigrateEnabled() bool {
	return s.AutoMigrate
}

// GetTargetVersion возвращает целевую версию схемы.
// По умолчанию "latest".
func (s *SchemaMigrationConfig) GetTargetVersion() string {
	if s.TargetVersion == "" {
		return "latest"
	}
	return s.TargetVersion
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ BackupConfig
// =============================================================================

// IsBackupEnabled возвращает флаг включения резервного копирования.
func (b *BackupConfig) IsBackupEnabled() bool {
	return b.Enabled
}

// GetBackupDir возвращает директорию для резервных копий.
// По умолчанию "backups".
func (b *BackupConfig) GetBackupDir() string {
	if b.BackupDir == "" {
		return "backups"
	}
	return b.BackupDir
}

// GetMaxConcurrentBackups возвращает максимальное количество параллельных бэкапов.
// По умолчанию 1.
func (b *BackupConfig) GetMaxConcurrentBackups() int {
	if b.MaxConcurrent <= 0 {
		return 1
	}
	return b.MaxConcurrent
}

// IsCompressEnabled возвращает флаг сжатия резервных копий.
func (b *BackupConfig) IsCompressEnabled() bool {
	return b.CompressEnabled
}

// GetBackupRetentionDays возвращает срок хранения резервных копий в днях.
// По умолчанию 7 дней.
func (b *BackupConfig) GetBackupRetentionDays() int {
	if b.RetentionDays <= 0 {
		return 7
	}
	return b.RetentionDays
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ StorageConfig
// =============================================================================

// GetDefaultEngine возвращает движок хранения по умолчанию.
// По умолчанию "row".
func (s *StorageConfig) GetDefaultEngine() string {
	if s.DefaultEngine == "" {
		return "row"
	}
	return s.DefaultEngine
}

// IsCustomEnginesEnabled возвращает флаг включения пользовательских движков.
func (s *StorageConfig) IsCustomEnginesEnabled() bool {
	return s.EnableCustomEngines
}

// =============================================================================
// МЕТОДЫ ДЛЯ EnginesConfig
// =============================================================================

// IsEngineEnabled проверяет, включен ли указанный движок хранения.
// Поддерживаются: row, columnar, document, kv, ts, graph.
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

// GetEngineConfig возвращает конфигурацию указанного движка хранения.
// Возвращает пустую карту, если движок не найден.
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

// GetEngineDescription возвращает описание указанного движка хранения.
// Возвращает пустую строку, если движок не найден.
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

// =============================================================================
// ГЕТТЕРЫ ДЛЯ RecoveryConfig
// =============================================================================

// GetMaxRetryDuration возвращает максимальную длительность повторных попыток.
// По умолчанию 300 секунд (5 минут).
func (r *RecoveryConfig) GetMaxRetryDuration() time.Duration {
	if r.MaxRetrySec <= 0 {
		return 300 * time.Second
	}
	return time.Duration(r.MaxRetrySec) * time.Second
}

// GetDataReplicationTimeout возвращает таймаут репликации данных при восстановлении.
// По умолчанию 60 секунд.
func (r *RecoveryConfig) GetDataReplicationTimeout() time.Duration {
	if r.DataReplicationTimeoutSec <= 0 {
		return 60 * time.Second
	}
	return time.Duration(r.DataReplicationTimeoutSec) * time.Second
}

// GetStaleReadTimeout возвращает таймаут для чтения устаревших данных.
// По умолчанию 30 секунд.
func (r *RecoveryConfig) GetStaleReadTimeout() time.Duration {
	if r.StaleReadTimeoutSec <= 0 {
		return 30 * time.Second
	}
	return time.Duration(r.StaleReadTimeoutSec) * time.Second
}

// IsAutoRejoinEnabled возвращает флаг автоматического переподключения.
func (r *RecoveryConfig) IsAutoRejoinEnabled() bool {
	return r.AutoRejoin
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ PerformanceConfig
// =============================================================================

// GetBatchSize возвращает размер пакета для пакетных операций.
// По умолчанию 100 записей.
func (p *PerformanceConfig) GetBatchSize() int {
	if p.BatchSize <= 0 {
		return 100
	}
	return p.BatchSize
}

// GetMaxConnections возвращает максимальное количество соединений.
// По умолчанию 1000.
func (p *PerformanceConfig) GetMaxConnections() int {
	if p.MaxConnections <= 0 {
		return 1000
	}
	return p.MaxConnections
}

// IsReadFromFollowerEnabled возвращает флаг чтения с ведомых узлов.
func (p *PerformanceConfig) IsReadFromFollowerEnabled() bool {
	return p.ReadFromFollower
}

// IsPipelineEnabled возвращает флаг конвейерной обработки.
func (p *PerformanceConfig) IsPipelineEnabled() bool {
	return p.EnablePipeline
}

// GetReadReplicaDelay возвращает задержку чтения с реплики.
// По умолчанию 100 миллисекунд.
func (p *PerformanceConfig) GetReadReplicaDelay() time.Duration {
	if p.ReadReplicaDelayMs <= 0 {
		return 100 * time.Millisecond
	}
	return time.Duration(p.ReadReplicaDelayMs) * time.Millisecond
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ MonitoringConfig
// =============================================================================

// GetMetricsPort возвращает порт для экспорта метрик.
// По умолчанию 9090.
func (m *MonitoringConfig) GetMetricsPort() int {
	if m.MetricsPort <= 0 {
		return 9090
	}
	return m.MetricsPort
}

// GetTraceSampleRate возвращает частоту сэмплирования трассировки.
// Нормализуется в диапазон (0, 1].
func (m *MonitoringConfig) GetTraceSampleRate() float64 {
	if m.TraceSampleRate <= 0 || m.TraceSampleRate > 1 {
		return 0.01
	}
	return m.TraceSampleRate
}

// IsMetricsEnabled возвращает флаг включения сбора метрик.
func (m *MonitoringConfig) IsMetricsEnabled() bool {
	return m.EnableMetrics
}

// IsTracingEnabled возвращает флаг включения трассировки.
func (m *MonitoringConfig) IsTracingEnabled() bool {
	return m.EnableTracing
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ SecurityConfig
// =============================================================================

// GetTLSMinVersion возвращает минимальную версию TLS как числовой код.
// Используется для настройки TLS-соединений.
// Возвращает 0x0303 (TLS 1.2) по умолчанию.
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

// IsTLSEnabled возвращает флаг включения TLS.
// TLS считается включенным, если EnableTLS=true и указаны файлы сертификата и ключа.
func (s *SecurityConfig) IsTLSEnabled() bool {
	return s.EnableTLS && s.CertFile != "" && s.KeyFile != ""
}

// GetCertFile возвращает путь к файлу сертификата.
func (s *SecurityConfig) GetCertFile() string {
	return s.CertFile
}

// GetKeyFile возвращает путь к файлу приватного ключа.
func (s *SecurityConfig) GetKeyFile() string {
	return s.KeyFile
}

// GetCAFile возвращает путь к файлу корневого сертификата CA.
func (s *SecurityConfig) GetCAFile() string {
	return s.CAFile
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ CompressionConfig
// =============================================================================

// IsCompressionEnabled возвращает флаг включения сжатия.
func (c *CompressionConfig) IsCompressionEnabled() bool {
	return c.Enabled
}

// GetAlgorithm возвращает алгоритм сжатия.
// По умолчанию "snappy".
func (c *CompressionConfig) GetAlgorithm() string {
	if c.Algorithm == "" {
		return "snappy"
	}
	return c.Algorithm
}

// GetLevel возвращает уровень сжатия.
// Нормализуется в диапазон [1, 9].
func (c *CompressionConfig) GetLevel() int {
	if c.Level < 1 {
		return 3
	}
	if c.Level > 9 {
		return 9
	}
	return c.Level
}

// GetMinSize возвращает минимальный размер данных для сжатия в байтах.
// По умолчанию 1024 байта.
func (c *CompressionConfig) GetMinSize() int {
	if c.MinSize <= 0 {
		return 1024
	}
	return c.MinSize
}

// =============================================================================
// ГЕТТЕРЫ ДЛЯ WebUIConfig
// =============================================================================

// IsWebUIEnabled возвращает флаг включения веб-интерфейса.
func (w *WebUIConfig) IsWebUIEnabled() bool {
	return w.Enabled
}

// GetWebUIPort возвращает порт для веб-интерфейса.
// По умолчанию 8080.
func (w *WebUIConfig) GetWebUIPort() int {
	if w.Port <= 0 {
		return 8080
	}
	return w.Port
}

// GetTheme возвращает тему веб-интерфейса.
// По умолчанию "dark".
func (w *WebUIConfig) GetTheme() string {
	if w.Theme == "" {
		return "dark"
	}
	return w.Theme
}

// =============================================================================
// ЗАГРУЗКА И ВАЛИДАЦИЯ КОНФИГУРАЦИИ
// =============================================================================

// LoadConfig загружает и валидирует конфигурацию из TOML-файла.
//
// Алгоритм работы:
// 1. Декодирование TOML-файла в структуру Config
// 2. Применение значений по умолчанию для всех параметров
// 3. Полная валидация конфигурации
// 4. Вывод предупреждений (если есть)
// 5. Возврат заполненной структуры или ошибки
//
// Параметры:
//   - path: путь к TOML-файлу конфигурации
//
// Возвращает:
//   - *Config: заполненная структура конфигурации
//   - error: ошибка загрузки или валидации
func LoadConfig(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %v", err)
	}

	// ===== УСТАНОВКА ЗНАЧЕНИЙ ПО УМОЛЧАНИЮ =====
	// Все значения по умолчанию гарантируют работоспособность
	// даже при отсутствии соответствующих секций в конфиге.

	// Кластерные настройки
	if cfg.Cluster.RaftPort == 0 {
		cfg.Cluster.RaftPort = 9878 // Порт Raft по умолчанию
	}
	if cfg.Cluster.RaftDataDir == "" {
		cfg.Cluster.RaftDataDir = "raft_data" // Директория Raft-лога
	}
	if cfg.Cluster.HeartbeatTimeoutMs == 0 {
		cfg.Cluster.HeartbeatTimeoutMs = 1000 // 1 секунда
	}
	if cfg.Cluster.ElectionTimeoutMs == 0 {
		cfg.Cluster.ElectionTimeoutMs = 1000 // 1 секунда
	}
	if cfg.Cluster.CommitTimeoutMs == 0 {
		cfg.Cluster.CommitTimeoutMs = 500 // 500 мс
	}
	if cfg.Cluster.SnapshotIntervalMin == 0 {
		cfg.Cluster.SnapshotIntervalMin = 30 // 30 минут
	}
	if cfg.Cluster.SnapshotThreshold == 0 {
		cfg.Cluster.SnapshotThreshold = 1000 // 1000 записей
	}
	if cfg.Cluster.RecoveryTimeoutSec == 0 {
		cfg.Cluster.RecoveryTimeoutSec = 30 // 30 секунд
	}
	if cfg.Cluster.Region == "" {
		cfg.Cluster.Region = "default" // Регион по умолчанию
	}

	// Настройки SAGA
	if cfg.Saga.CoordinatorCount == 0 {
		cfg.Saga.CoordinatorCount = 3
	}
	if cfg.Saga.StateDir == "" {
		cfg.Saga.StateDir = "saga_states"
	}
	if cfg.Saga.MaxRetries == 0 {
		cfg.Saga.MaxRetries = 5
	}
	if cfg.Saga.RetryBackoffMs == 0 {
		cfg.Saga.RetryBackoffMs = 100
	}
	if cfg.Saga.SagaTimeoutSec == 0 {
		cfg.Saga.SagaTimeoutSec = 300
	}
	if cfg.Saga.StuckCheckIntervalSec == 0 {
		cfg.Saga.StuckCheckIntervalSec = 10
	}
	if cfg.Saga.LeaderElectionIntervalSec == 0 {
		cfg.Saga.LeaderElectionIntervalSec = 5
	}
	if cfg.Saga.RecoveryIntervalSec == 0 {
		cfg.Saga.RecoveryIntervalSec = 30
	}
	if cfg.Saga.MetricsIntervalSec == 0 {
		cfg.Saga.MetricsIntervalSec = 60
	}
	if cfg.Saga.CleanupPeriodHours == 0 {
		cfg.Saga.CleanupPeriodHours = 24
	}
	if cfg.Saga.MaxCacheSize == 0 {
		cfg.Saga.MaxCacheSize = 10000
	}
	if cfg.Saga.OperationRetentionDays == 0 {
		cfg.Saga.OperationRetentionDays = 7
	}
	if cfg.Saga.ChannelBufferSize == 0 {
		cfg.Saga.ChannelBufferSize = 10000
	}
	if cfg.Saga.AsyncRecoveryWorkers == 0 {
		cfg.Saga.AsyncRecoveryWorkers = 4
	}
	if cfg.Saga.AsyncRecoveryTimeoutSec == 0 {
		cfg.Saga.AsyncRecoveryTimeoutSec = 30
	}
	if cfg.Saga.FsyncMaxRetries == 0 {
		cfg.Saga.FsyncMaxRetries = 3
	}
	if cfg.Saga.FsyncRetryDelayMs == 0 {
		cfg.Saga.FsyncRetryDelayMs = 100
	}

	// Настройки репликации
	if cfg.Replication.ReplicationTimeoutMs == 0 {
		cfg.Replication.ReplicationTimeoutMs = 5000 // 5 секунд
	}
	if cfg.Replication.MaxReplicaLagMs == 0 {
		cfg.Replication.MaxReplicaLagMs = 5000 // 5 секунд
	}

	// Настройки плагинов
	if cfg.Plugins.ScriptDir == "" {
		cfg.Plugins.ScriptDir = "plugins" // Директория плагинов
	}
	if cfg.Plugins.EnginePluginDir == "" {
		cfg.Plugins.EnginePluginDir = "engines" // Директория движков
	}
	if cfg.Plugins.MaxCPUTimeMs == 0 {
		cfg.Plugins.MaxCPUTimeMs = 100 // 100 мс
	}
	if cfg.Plugins.MaxMemoryMB == 0 {
		cfg.Plugins.MaxMemoryMB = 50 // 50 МБ
	}
	if cfg.Plugins.MaxExecutionTimeSec == 0 {
		cfg.Plugins.MaxExecutionTimeSec = 5 // 5 секунд
	}
	if cfg.Plugins.MaxInstructions == 0 {
		cfg.Plugins.MaxInstructions = 1000000 // 1 млн инструкций
	}
	if cfg.Plugins.HotReloadIntervalSec == 0 {
		cfg.Plugins.HotReloadIntervalSec = 30 // 30 секунд
	}
	if cfg.Plugins.MaxEventLogSize == 0 {
		cfg.Plugins.MaxEventLogSize = 1000 // 1000 записей
	}
	if cfg.Plugins.LoadTimeoutSec == 0 {
		cfg.Plugins.LoadTimeoutSec = 10 // 10 секунд
	}
	if cfg.Plugins.MaxLuaStates == 0 {
		cfg.Plugins.MaxLuaStates = 100 // 100 состояний
	}
	if cfg.Plugins.LuaStateTTLSec == 0 {
		cfg.Plugins.LuaStateTTLSec = 600 // 10 минут
	}

	// Настройки API
	if cfg.API.Port == 0 {
		cfg.API.Port = 8080 // Порт API по умолчанию
	}

	// Настройки сжатия
	if cfg.Compression.Algorithm == "" {
		cfg.Compression.Algorithm = "snappy"
	}
	if cfg.Compression.MinSize == 0 {
		cfg.Compression.MinSize = 1024 // 1 КБ
	}
	if cfg.Compression.Level == 0 {
		cfg.Compression.Level = 3 // Уровень сжатия по умолчанию
	}

	// Настройки веб-интерфейса
	if cfg.WebUI.Port == 0 {
		cfg.WebUI.Port = 9080 // Порт WebUI по умолчанию
	}
	if cfg.WebUI.Theme == "" {
		cfg.WebUI.Theme = "dark"
	}

	// Настройки производительности
	if cfg.Performance.BatchSize == 0 {
		cfg.Performance.BatchSize = 100
	}
	if cfg.Performance.MaxConnections == 0 {
		cfg.Performance.MaxConnections = 1000
	}
	if cfg.Performance.ReadReplicaDelayMs == 0 {
		cfg.Performance.ReadReplicaDelayMs = 100 // 100 мс
	}

	// Настройки мониторинга
	if cfg.Monitoring.MetricsPort == 0 {
		cfg.Monitoring.MetricsPort = 9090
	}
	if cfg.Monitoring.TraceSampleRate == 0 {
		cfg.Monitoring.TraceSampleRate = 0.01 // 1%
	}

	// Настройки восстановления
	if cfg.Recovery.MaxRetrySec == 0 {
		cfg.Recovery.MaxRetrySec = 300 // 5 минут
	}
	if cfg.Recovery.DataReplicationTimeoutSec == 0 {
		cfg.Recovery.DataReplicationTimeoutSec = 60 // 1 минута
	}
	if cfg.Recovery.StaleReadTimeoutSec == 0 {
		cfg.Recovery.StaleReadTimeoutSec = 30 // 30 секунд
	}

	// Настройки безопасности
	if cfg.Security.MinVersion == "" {
		cfg.Security.MinVersion = "1.2" // TLS 1.2
	}

	// Настройки хранилища
	if cfg.Storage.DefaultEngine == "" {
		cfg.Storage.DefaultEngine = "row" // Строчный движок по умолчанию
	}

	// Настройки WAL
	if cfg.WAL.SegmentSizeMB == 0 {
		cfg.WAL.SegmentSizeMB = 64 // 64 МБ
	}
	if cfg.WAL.SyncIntervalSec == 0 {
		cfg.WAL.SyncIntervalSec = 5 // 5 секунд
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

	// Настройки MVCC
	if cfg.MVCC.MaxVersionsPerDoc == 0 {
		cfg.MVCC.MaxVersionsPerDoc = 10
	}
	if cfg.MVCC.VisibilityMapSize == 0 {
		cfg.MVCC.VisibilityMapSize = 1024 * 1024
	}
	if cfg.MVCC.PruneIntervalMin == 0 {
		cfg.MVCC.PruneIntervalMin = 5 // 5 минут
	}
	if cfg.MVCC.RetentionDays == 0 {
		cfg.MVCC.RetentionDays = 7 // 7 дней
	}
	if cfg.MVCC.ReadCacheSize == 0 {
		cfg.MVCC.ReadCacheSize = 10000
	}
	if cfg.MVCC.ReadCacheTTLSec == 0 {
		cfg.MVCC.ReadCacheTTLSec = 300 // 5 минут
	}

	// Настройки транзакций
	if cfg.Transactions.DefaultTimeoutSec == 0 {
		cfg.Transactions.DefaultTimeoutSec = 30 // 30 секунд
	}
	if cfg.Transactions.DeadlockCheckIntervalSec == 0 {
		cfg.Transactions.DeadlockCheckIntervalSec = 1 // 1 секунда
	}
	if cfg.Transactions.MaxSavepointsPerTx == 0 {
		cfg.Transactions.MaxSavepointsPerTx = 100
	}
	if cfg.Transactions.CheckpointIntervalSec == 0 {
		cfg.Transactions.CheckpointIntervalSec = 300 // 5 минут
	}

	// Настройки ACL
	if cfg.ACL.MaxDeniedLogSize == 0 {
		cfg.ACL.MaxDeniedLogSize = 10000
	}
	if cfg.ACL.TemporaryGrantTTLHours == 0 {
		cfg.ACL.TemporaryGrantTTLHours = 24 // 24 часа
	}
	if cfg.ACL.CacheTTLSec == 0 {
		cfg.ACL.CacheTTLSec = 60 // 60 секунд
	}

	// Настройки кластерного TLS
	if cfg.ClusterTLS.MinVersion == "" {
		cfg.ClusterTLS.MinVersion = "1.2"
	}
	if cfg.ClusterTLS.KeyRotationDays == 0 {
		cfg.ClusterTLS.KeyRotationDays = 30 // 30 дней
	}

	// Настройки обратного давления
	if cfg.Backpressure.CheckIntervalMs == 0 {
		cfg.Backpressure.CheckIntervalMs = 1000 // 1 секунда
	}
	if cfg.Backpressure.LowDelayMs == 0 {
		cfg.Backpressure.LowDelayMs = 100 // 100 мс
	}

	// Настройки ограничений времени выполнения
	if cfg.RuntimeLimits.GlobalMaxDocSizeMB == 0 {
		cfg.RuntimeLimits.GlobalMaxDocSizeMB = 16 // 16 МБ
	}
	if cfg.RuntimeLimits.GlobalMaxCollSizeMB == 0 {
		cfg.RuntimeLimits.GlobalMaxCollSizeMB = 10240 // 10 ГБ
	}
	if cfg.RuntimeLimits.GlobalMaxDocsPerColl == 0 {
		cfg.RuntimeLimits.GlobalMaxDocsPerColl = 10000000 // 10 млн
	}

	// Настройки автомасштабирования
	if cfg.Autoscaling.MinNodes == 0 {
		cfg.Autoscaling.MinNodes = 1
	}
	if cfg.Autoscaling.MaxNodes == 0 {
		cfg.Autoscaling.MaxNodes = 10
	}
	if cfg.Autoscaling.ScaleUpCooldownSec == 0 {
		cfg.Autoscaling.ScaleUpCooldownSec = 300 // 5 минут
	}
	if cfg.Autoscaling.ScaleDownCooldownSec == 0 {
		cfg.Autoscaling.ScaleDownCooldownSec = 600 // 10 минут
	}
	if cfg.Autoscaling.EvaluationIntervalSec == 0 {
		cfg.Autoscaling.EvaluationIntervalSec = 30 // 30 секунд
	}

	// Настройки миграции схемы
	if cfg.SchemaMigration.MigrationDir == "" {
		cfg.SchemaMigration.MigrationDir = "migrations"
	}

	// Настройки резервного копирования
	if cfg.Backup.BackupDir == "" {
		cfg.Backup.BackupDir = "backups"
	}
	if cfg.Backup.RetentionDays == 0 {
		cfg.Backup.RetentionDays = 7 // 7 дней
	}

	// ===== ИНИЦИАЛИЗАЦИЯ ДВИЖКОВ =====
	// Строчный движок включен по умолчанию
	if !cfg.Engines.Row.Enabled {
		cfg.Engines.Row.Enabled = true
		cfg.Engines.Row.Description = "Row-based storage engine (default)"
	}
	// Инициализация конфигураций движков
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

	// ===== ВАЛИДАЦИЯ КОНФИГУРАЦИИ =====
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

	// Вывод предупреждений (если они есть)
	if len(validationResult.Warnings) > 0 {
		fmt.Println("Configuration warnings:")
		for _, warn := range validationResult.Warnings {
			fmt.Printf("  - %s\n", warn.Error())
		}
	}

	return &cfg, nil
}

// ValidateConfigFull выполняет полную валидацию конфигурации.
//
// Проверяются:
//   - Корректность портов (диапазон 1-65535)
//   - Отсутствие конфликтов портов между службами
//   - Корректность параметров (диапазоны, зависимости)
//   - Совместимость настроек
//
// Возвращает ValidationResult с ошибками и предупреждениями.
func ValidateConfigFull(cfg *Config) *ValidationResult {
	result := &ValidationResult{
		Valid:    true,
		Errors:   make([]error, 0),
		Warnings: make([]error, 0),
	}

	// ===== ВАЛИДАЦИЯ КЛАСТЕРНОЙ КОНФИГУРАЦИИ =====
	if cfg.Cluster.NodePort <= 0 || cfg.Cluster.NodePort > 65535 {
		result.Errors = append(result.Errors, fmt.Errorf("invalid node_port: %d (must be 1-65535)", cfg.Cluster.NodePort))
	}
	if cfg.Cluster.RaftPort <= 0 || cfg.Cluster.RaftPort > 65535 {
		result.Errors = append(result.Errors, fmt.Errorf("invalid raft_port: %d (must be 1-65535)", cfg.Cluster.RaftPort))
	}
	if cfg.Cluster.PriorityZone < 0 || cfg.Cluster.PriorityZone > 9 {
		result.Errors = append(result.Errors, fmt.Errorf("priority_zone must be between 0 and 9, got %d", cfg.Cluster.PriorityZone))
	}

	// ===== ВАЛИДАЦИЯ SAGA КОНФИГУРАЦИИ =====
	if cfg.Saga.Enabled {
		if cfg.Saga.CoordinatorCount < 1 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.coordinator_count must be at least 1, got %d", cfg.Saga.CoordinatorCount))
		}
		if cfg.Saga.CoordinatorCount < 3 {
			result.Warnings = append(result.Warnings, fmt.Errorf("saga.coordinator_count is less than 3, may not be fully fault-tolerant"))
		}
		if cfg.Saga.MaxRetries < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.max_retries cannot be negative, got %d", cfg.Saga.MaxRetries))
		}
		if cfg.Saga.RetryBackoffMs < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.retry_backoff_ms cannot be negative, got %d", cfg.Saga.RetryBackoffMs))
		}
		if cfg.Saga.SagaTimeoutSec < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.saga_timeout_sec cannot be negative, got %d", cfg.Saga.SagaTimeoutSec))
		}
		if cfg.Saga.StuckCheckIntervalSec < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.stuck_check_interval_sec cannot be negative, got %d", cfg.Saga.StuckCheckIntervalSec))
		}
		if cfg.Saga.AsyncRecoveryWorkers < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.async_recovery_workers cannot be negative, got %d", cfg.Saga.AsyncRecoveryWorkers))
		}
		if cfg.Saga.ChannelBufferSize < 0 {
			result.Errors = append(result.Errors, fmt.Errorf("saga.channel_buffer_size cannot be negative, got %d", cfg.Saga.ChannelBufferSize))
		}
	}

	// ===== ВАЛИДАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ =====
	if cfg.Performance.BatchSize < 1 {
		result.Errors = append(result.Errors, fmt.Errorf("batch_size must be at least 1, got %d", cfg.Performance.BatchSize))
	}
	if cfg.Performance.MaxConnections < 1 {
		result.Errors = append(result.Errors, fmt.Errorf("max_connections must be at least 1, got %d", cfg.Performance.MaxConnections))
	}

	// ===== ВАЛИДАЦИЯ МОНИТОРИНГА =====
	if cfg.Monitoring.TraceSampleRate < 0 || cfg.Monitoring.TraceSampleRate > 1 {
		result.Errors = append(result.Errors, fmt.Errorf("trace_sample_rate must be between 0 and 1, got %f", cfg.Monitoring.TraceSampleRate))
	}

	// ===== ПРОВЕРКА КОНФЛИКТОВ ПОРТОВ =====
	// Никакие два сервиса не должны использовать один порт
	if cfg.API.Port == cfg.Cluster.NodePort {
		result.Errors = append(result.Errors, fmt.Errorf("API port %d conflicts with cluster node port", cfg.API.Port))
	}
	if cfg.WebUI.Port == cfg.Cluster.NodePort {
		result.Errors = append(result.Errors, fmt.Errorf("WebUI port %d conflicts with cluster node port", cfg.WebUI.Port))
	}
	if cfg.Monitoring.MetricsPort == cfg.Cluster.NodePort {
		result.Errors = append(result.Errors, fmt.Errorf("metrics port %d conflicts with cluster node port", cfg.Monitoring.MetricsPort))
	}

	// ===== ВАЛИДАЦИЯ TLS =====
	if cfg.Security.IsTLSEnabled() {
		if cfg.Security.CertFile == "" {
			result.Errors = append(result.Errors, fmt.Errorf("cert_file is required when TLS is enabled"))
		}
		if cfg.Security.KeyFile == "" {
			result.Errors = append(result.Errors, fmt.Errorf("key_file is required when TLS is enabled"))
		}
	}

	// ===== ВАЛИДАЦИЯ СЖАТИЯ =====
	if cfg.Compression.Enabled {
		switch cfg.Compression.Algorithm {
		case "snappy", "lz4", "zstd":
			// Поддерживаемые алгоритмы
		default:
			result.Errors = append(result.Errors, fmt.Errorf("unsupported compression algorithm: %s, supported: snappy, lz4, zstd", cfg.Compression.Algorithm))
		}
	}

	// ===== ВАЛИДАЦИЯ WAL =====
	if cfg.WAL.RecoveryWorkers < 1 {
		result.Errors = append(result.Errors, fmt.Errorf("wal.recovery_workers must be at least 1, got %d", cfg.WAL.RecoveryWorkers))
	}
	if cfg.WAL.AsyncRecoveryWorkers < 1 && cfg.WAL.AsyncRecovery {
		result.Warnings = append(result.Warnings, fmt.Errorf("wal.async_recovery_workers is less than 1, will use default 4"))
	}
	if cfg.WAL.AsyncRecoveryBuffer < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("wal.async_recovery_buffer cannot be negative, got %d", cfg.WAL.AsyncRecoveryBuffer))
	}

	// ===== ВАЛИДАЦИЯ MVCC =====
	if cfg.MVCC.MaxVersionsPerDoc < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("mvcc.max_versions_per_doc cannot be negative, got %d", cfg.MVCC.MaxVersionsPerDoc))
	}

	// ===== ВАЛИДАЦИЯ ПЛАГИНОВ =====
	if cfg.Plugins.MaxCPUTimeMs < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("plugins.max_cpu_time_ms cannot be negative, got %d", cfg.Plugins.MaxCPUTimeMs))
	}
	if cfg.Plugins.MaxMemoryMB < 0 {
		result.Errors = append(result.Errors, fmt.Errorf("plugins.max_memory_mb cannot be negative, got %d", cfg.Plugins.MaxMemoryMB))
	}
	if cfg.Plugins.MaxLuaStates < 1 {
		result.Warnings = append(result.Warnings, fmt.Errorf("plugins.max_lua_states is less than 1, will use default 100"))
	}

	// ===== ВАЛИДАЦИЯ ОБРАТНОГО ДАВЛЕНИЯ =====
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

	// ===== ВАЛИДАЦИЯ ОГРАНИЧЕНИЙ ВРЕМЕНИ ВЫПОЛНЕНИЯ =====
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

	// ===== ВАЛИДАЦИЯ АВТОМАСШТАБИРОВАНИЯ =====
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

	// ===== ВАЛИДАЦИЯ ДВИЖКОВ =====
	if cfg.Storage.EnableCustomEngines {
		defaultEngine := cfg.Storage.GetDefaultEngine()
		switch defaultEngine {
		case "row", "columnar", "document", "kv", "ts", "graph":
			// Встроенные движки
		default:
			result.Warnings = append(result.Warnings, fmt.Errorf("default_engine '%s' is not a built-in engine, ensure plugin is loaded", defaultEngine))
		}
	}

	// Конфигурация считается корректной, если нет ни одной ошибки
	result.Valid = len(result.Errors) == 0
	return result
}
