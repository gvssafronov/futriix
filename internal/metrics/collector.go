/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/metrics/collector.go
// Назначение: Периодический сбор метрик из storage и cluster и публикация
// их в Prometheus-реестре. Работает на Linux и OpenIndiana.

package metrics

import (
	"fmt"
	"time"
)

// CollectorDeps зависимости коллектора
type CollectorDeps struct {
	Storage    StorageStatsProvider
	Coordinator CoordinatorStatsProvider
	Logger     Logger
	Interval   time.Duration
}

// StorageStatsProvider интерфейс для получения статистики storage
type StorageStatsProvider interface {
	GetStats() map[string]interface{}
	GetDatabaseCount() int
	GetTotalDocuments() int64
}

// CoordinatorStatsProvider интерфейс для получения статистики cluster
type CoordinatorStatsProvider interface {
	GetClusterStatus() ClusterStatusLike
	GetActiveNodes() []NodeInfoLike
	GetAllNodes() []NodeInfoLike
	IsLeader() bool
	GetCurrentTerm() uint64
}

// ClusterStatusLike минимальный интерфейс статуса кластера
type ClusterStatusLike interface {
	GetTotalNodes() int
	GetActiveNodes() int
	GetFailedNodes() int
	GetLeaderID() string
	GetHealth() string
}

// NodeInfoLike минимальный интерфейс узла
type NodeInfoLike interface {
	GetID() string
	GetIP() string
	GetPort() int
	GetStatus() string
	GetLastSeen() int64
}

// Logger минимальный интерфейс логгера
type Logger interface {
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

// Collector собирает метрики и публикует в реестр
type Collector struct {
	deps     CollectorDeps
	registry *Registry

	// Метрики
	mStorageDatabases    *MetricFamily
	mStorageDocuments    *MetricFamily
	mClusterNodes        *MetricFamily
	mClusterLeader       *MetricFamily
	mClusterTerm         *MetricFamily
	mNodeLastSeen        *MetricFamily
	mHTTPRequests        *MetricFamily
	mHTTPDuration        *MetricFamily
	mReplicationTotal    *MetricFamily
	mReplicationFailed   *MetricFamily
	mBackpressureLevel   *MetricFamily
	mMigrationTasks      *MetricFamily

	stop chan struct{}
}

// NewCollector создаёт новый коллектор
func NewCollector(deps CollectorDeps) *Collector {
	if deps.Interval <= 0 {
		deps.Interval = 15 * time.Second
	}
	r := DefaultRegistry()

	c := &Collector{
		deps:     deps,
		registry: r,
		stop:     make(chan struct{}),
	}

	c.mStorageDatabases = r.RegisterGauge("futriis_storage_databases_total",
		"Total number of databases")
	c.mStorageDocuments = r.RegisterGauge("futriis_storage_documents_total",
		"Total number of documents across all collections")
	c.mClusterNodes = r.RegisterGauge("futriis_cluster_nodes",
		"Number of cluster nodes by status")
	c.mClusterLeader = r.RegisterGauge("futriis_cluster_has_leader",
		"Whether a leader is elected (1) or not (0)")
	c.mClusterTerm = r.RegisterGauge("futriis_cluster_raft_term",
		"Current Raft term")
	c.mNodeLastSeen = r.RegisterGauge("futriis_node_last_seen_seconds",
		"Unix timestamp of last node contact")
	c.mHTTPRequests = r.RegisterCounter("futriis_http_requests_total",
		"Total number of HTTP requests")
	c.mHTTPDuration = r.RegisterHistogram("futriis_http_request_duration_seconds",
		"HTTP request duration in seconds",
		[]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	c.mReplicationTotal = r.RegisterCounter("futriis_replication_total",
		"Total replication operations")
	c.mReplicationFailed = r.RegisterCounter("futriis_replication_failed_total",
		"Failed replication operations")
	c.mBackpressureLevel = r.RegisterGauge("futriis_backpressure_level",
		"Current backpressure level (0=none,4=critical)")
	c.mMigrationTasks = r.RegisterGauge("futriis_migration_tasks",
		"Migration tasks by status")

	return c
}

// Start запускает периодический сбор метрик
func (c *Collector) Start() {
	go c.loop()
	if c.deps.Logger != nil {
		c.deps.Logger.Info(fmt.Sprintf("Prometheus metrics collector started (interval=%v)", c.deps.Interval))
	}
}

// Stop останавливает коллектор
func (c *Collector) Stop() {
	close(c.stop)
}

func (c *Collector) loop() {
	ticker := time.NewTicker(c.deps.Interval)
	defer ticker.Stop()

	c.collect()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

// collect собирает все метрики
func (c *Collector) collect() {
	c.collectStorage()
	c.collectCluster()
}

func (c *Collector) collectStorage() {
	if c.deps.Storage == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil && c.deps.Logger != nil {
			c.deps.Logger.Error(fmt.Sprintf("metrics: storage collect panic: %v", r))
		}
	}()

	c.mStorageDatabases.WithLabels().Set(float64(c.deps.Storage.GetDatabaseCount()))
	c.mStorageDocuments.WithLabels().Set(float64(c.deps.Storage.GetTotalDocuments()))

	stats := c.deps.Storage.GetStats()
	if dbs, ok := stats["databases"].([]map[string]interface{}); ok {
		for _, db := range dbs {
			name, _ := db["name"].(string)
			docs, _ := db["documents"].(int64)
			size, _ := db["size_bytes"].(int64)
			c.registry.RegisterGauge("futriis_database_documents_total",
				"Documents per database").WithLabels(LabelPair{"database", name}).Set(float64(docs))
			c.registry.RegisterGauge("futriis_database_size_bytes",
				"Size of database in bytes").WithLabels(LabelPair{"database", name}).Set(float64(size))
		}
	}
}

func (c *Collector) collectCluster() {
	if c.deps.Coordinator == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil && c.deps.Logger != nil {
			c.deps.Logger.Error(fmt.Sprintf("metrics: cluster collect panic: %v", r))
		}
	}()

	status := c.deps.Coordinator.GetClusterStatus()
	if status != nil {
		c.mClusterNodes.WithLabels(LabelPair{"status", "total"}).Set(float64(status.GetTotalNodes()))
		c.mClusterNodes.WithLabels(LabelPair{"status", "active"}).Set(float64(status.GetActiveNodes()))
		c.mClusterNodes.WithLabels(LabelPair{"status", "failed"}).Set(float64(status.GetFailedNodes()))
		if status.GetLeaderID() != "" {
			c.mClusterLeader.WithLabels().Set(1)
		} else {
			c.mClusterLeader.WithLabels().Set(0)
		}
		c.registry.RegisterGauge("futriis_cluster_health",
			"Cluster health status (1=healthy, 0.5=degraded, 0=critical)").
			WithLabels(LabelPair{"health", status.GetHealth()}).Set(1)
	}

	if c.deps.Coordinator.IsLeader() {
		c.mClusterLeader.WithLabels(LabelPair{"role", "self"}).Set(1)
	} else {
		c.mClusterLeader.WithLabels(LabelPair{"role", "self"}).Set(0)
	}
	c.mClusterTerm.WithLabels().Set(float64(c.deps.Coordinator.GetCurrentTerm()))

	for _, node := range c.deps.Coordinator.GetActiveNodes() {
		c.mNodeLastSeen.WithLabels(
			LabelPair{"node_id", node.GetID()},
			LabelPair{"ip", node.GetIP()},
		).Set(float64(node.GetLastSeen()) / 1000.0)
	}
}

// HTTPRequestObserved регистрирует факт HTTP-запроса
func (c *Collector) HTTPRequestObserved(method, path string, status int, duration time.Duration) {
	c.mHTTPRequests.WithLabels(
		LabelPair{"method", method},
		LabelPair{"path", path},
		LabelPair{"status", fmt.Sprintf("%d", status)},
	).Inc()
	c.mHTTPDuration.WithLabels(
		LabelPair{"method", method},
		LabelPair{"path", path},
	).Observe(duration.Seconds())
}

// ReplicationObserved регистрирует операцию репликации
func (c *Collector) ReplicationObserved(success bool) {
	c.mReplicationTotal.WithLabels().Inc()
	if !success {
		c.mReplicationFailed.WithLabels().Inc()
	}
}

// SetBackpressureLevel устанавливает уровень backpressure
func (c *Collector) SetBackpressureLevel(level int) {
	c.mBackpressureLevel.WithLabels().Set(float64(level))
}

// SetMigrationTasks устанавливает количество задач миграции
func (c *Collector) SetMigrationTasks(status string, count int) {
	c.mMigrationTasks.WithLabels(LabelPair{"status", status}).Set(float64(count))
}
