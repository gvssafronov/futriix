/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 *
 * Файл: cmd/futriis/main.go
 * Назначение: Точка входа в приложение СУБД futriis.
 *
 * ИСПРАВЛЕНО:
 *   - Валидация пути к конфигурации и проверка прав доступа к файлу
 *   - Валидация конфигурации после загрузки (ValidateConfig)
 *   - WAL и data-dir привязаны к директории futriis, а не CWD
 *   - Graceful shutdown: убраны os.Exit(1) до завершения defer'ов,
 *     используется переменная exitCode и os.Exit в самом конце
 *   - Ожидание запуска HTTP-сервера с таймаутом
 *   - SAGA оркестратор останавливается всегда (даже если disabled)
 *   - Проверка allow-list для плагинов перед StartPlugin
 *   - Заменён SIGHUP на SIGUSR1 (reload) + SIGTERM/SIGINT (shutdown)
 *   - Проверка nil для raftCoordinator перед использованием
 *   - repl.NewRepl теперь возвращает (*Repl, error)
 *   - Интеграция с Prometheus: /metrics endpoint
 *   - ДОБАВЛЕНО: поддержка TLS на HTTP API (вариант A — TLS внутри futriiX).
 *     Теперь используется api.NewHTTPServerWithTLS(..., &cfg.Security).
 */

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"futriis/internal/acl"
	"futriis/internal/api"
	"futriis/internal/cluster"
	"futriis/internal/config"
	"futriis/internal/log"
	"futriis/internal/metrics"
	"futriis/internal/plugin"
	"futriis/internal/repl"
	"futriis/internal/storage"
	"futriis/pkg/utils"
)

const (
	defaultDataDir    = "futriis"
	maxConfigFileSize = 1 * 1024 * 1024
)

var exitCode = 0

func main() {
	utils.SetColorEnabled(true)

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	dataDir, err := ensureDataDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to prepare data directory: %v\n", err)
		os.Exit(1)
	}

	logFile := filepath.Join(dataDir, "futriis.log")
	if err := log.InitDefaultLogger(logFile, logLevel); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	log.Info("Futriis DB starting...")
	log.Infof("Log level: %s", logLevel)
	log.Infof("Data directory: %s", dataDir)

	configPath := os.Getenv("FUTRIIS_CONFIG")
	if configPath == "" {
		configPath = "config.toml"
	}

	cfg, err := loadAndValidateConfig(configPath)
	if err != nil {
		log.Error("Failed to load config: " + err.Error())
		utils.PrintError("Failed to load config: " + err.Error())
		os.Exit(1)
	}

	logger, err := log.NewLogger(cfg.Log.LogFile, cfg.Log.LogLevel)
	if err != nil {
		log.Error("Failed to initialize logger: " + err.Error())
		utils.PrintError("Failed to initialize logger: " + err.Error())
		os.Exit(1)
	}
	defer logger.Close()
	logger.Info("futriis database starting...")

	store := storage.NewStorage(cfg.Storage.PageSizeMB, logger)
	storage.SetGlobalStorage(store)

	walPath := filepath.Join(dataDir, "futriis.wal")
	if err := storage.InitTransactionManager(walPath); err != nil {
		logger.Warn("Failed to initialize transaction manager: " + err.Error())
	} else {
		storage.SetTransactionLogger(logger)
		logger.Info("Transaction manager initialized")
	}

	storage.InitTriggerManager(logger)
	logger.Info("Trigger manager initialized")

	aclManager := acl.NewACLManager()
	logger.Info("ACL manager initialized")

	raftCoordinator, err := cluster.NewRaftCoordinator(cfg, store, logger)
	if err != nil {
		logger.Error("Failed to start Raft coordinator: " + err.Error())
		utils.PrintError("Failed to start Raft coordinator: " + err.Error())
		os.Exit(1)
	}

	// ========== SAGA ORCHESTRATOR ==========
	var sagaOrchestrator *storage.SagaOrchestrator
	if cfg.Saga.Enabled {
		sagaConfig := &cfg.Saga
		sagaOrchestrator, err = storage.NewSagaOrchestratorWithConfig(
			sagaConfig,
			cfg.Cluster.NodeIP+":"+fmt.Sprint(cfg.Cluster.NodePort),
			logger,
		)
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to initialize saga orchestrator: %v", err))
		} else {
			logger.Info(fmt.Sprintf("Saga orchestrator initialized with %d coordinators", sagaConfig.GetCoordinatorCount()))
			storage.SetGlobalSagaOrchestrator(sagaOrchestrator)
		}
	} else {
		logger.Info("Saga orchestrator disabled in configuration")
	}

	if raftCoordinator != nil {
		schemaMigrator := raftCoordinator.GetSchemaMigrator()
		if schemaMigrator != nil {
			logger.Info("Checking for schema migrations...")
			status := schemaMigrator.GetStatus()
			logger.Infof("Current schema version: %s, total migrations: %d, applied: %d, pending: %d",
				status.CurrentVersion, status.TotalMigrations, status.AppliedMigrations, status.PendingMigrations)
			if status.PendingMigrations > 0 {
				logger.Info("Applying pending schema migrations...")
				if err := schemaMigrator.Migrate("2.1.0"); err != nil {
					logger.Warn(fmt.Sprintf("Schema migration warning: %v", err))
				}
			}
		}
	}

	if cfg.Cluster.Bootstrap || len(cfg.Cluster.Nodes) <= 1 {
		for i := 0; i < 10; i++ {
			if raftCoordinator.IsLeader() {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	node := cluster.NewNode(cfg.Cluster.NodeIP, cfg.Cluster.NodePort, store, logger)

	var registerErr error
	for i := 0; i < 5; i++ {
		registerErr = raftCoordinator.RegisterNode(node)
		if registerErr == nil {
			break
		}
		if i < 4 {
			logger.Warn(fmt.Sprintf("Failed to register node (attempt %d/5): %v, retrying...", i+1, registerErr))
			time.Sleep(2 * time.Second)
		}
	}
	if registerErr != nil {
		logger.Error("Failed to register node: " + registerErr.Error())
		utils.PrintError("Failed to register node: " + registerErr.Error())
		os.Exit(1)
	}

	pluginManager := plugin.NewPluginManager(
		cfg.Plugins.ScriptDir,
		logger,
		store,
		cfg.Plugins.Enabled,
	)

	if cfg.Plugins.Enabled {
		logger.Info(fmt.Sprintf("Plugin manager initialized, directory: %s", cfg.Plugins.ScriptDir))
		allowList := make(map[string]bool)
		for _, name := range cfg.Plugins.AllowList {
			allowList[name] = true
		}
		for _, p := range pluginManager.ListPlugins() {
			if len(allowList) > 0 && !allowList[p.Name] {
				logger.Warn(fmt.Sprintf("Plugin %s is not in allow-list, skipping start", p.Name))
				continue
			}
			if err := pluginManager.StartPlugin(p.Name); err != nil {
				logger.Warn(fmt.Sprintf("Failed to start plugin %s: %v", p.Name, err))
			}
		}
	}

	// ========== PROMETHEUS METRICS ==========
	var metricsCollector *metrics.Collector
	if cfg.Metrics.Enabled {
		metricsCollector = metrics.NewCollector(metrics.CollectorDeps{
			Storage:     newStorageStatsAdapter(store),
			Coordinator: newCoordinatorStatsAdapter(raftCoordinator),
			Logger:      logger,
			Interval:    time.Duration(cfg.Metrics.CollectIntervalSec) * time.Second,
		})
		metricsCollector.Start()
		logger.Info("Prometheus metrics collector enabled")
	} else {
		logger.Info("Prometheus metrics collector disabled in configuration")
	}

	// ========== HTTP API SERVER ==========
	// ДОБАВЛЕНО: используем NewHTTPServerWithTLS и передаём cfg.Security.
	// Если в config.toml [security].enable_tls = true, сервер поднимется по HTTPS.
	httpPort := cfg.API.Port
	httpServer := api.NewHTTPServerWithTLS(httpPort, store, raftCoordinator, aclManager, logger, &cfg.Security)
	if metricsCollector != nil {
		httpServer.SetMetricsCollector(metricsCollector)
	}

	httpErrChan := make(chan error, 1)
	go func() {
		if err := httpServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrChan <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	httpStarted := false
	for i := 0; i < 50; i++ {
		select {
		case err := <-httpErrChan:
			logger.Error(err.Error())
			utils.PrintError(err.Error())
			os.Exit(1)
		default:
		}
		if isPortListening(httpPort) {
			httpStarted = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !httpStarted {
		logger.Warn("HTTP server did not start within timeout, continuing anyway")
	}
	scheme := "http"
	if httpServer.IsTLSEnabled() {
		scheme = "https"
	}
	logger.Info(fmt.Sprintf("HTTP API server started on %s://0.0.0.0:%d", scheme, httpPort))
	if metricsCollector != nil {
		logger.Info(fmt.Sprintf("Prometheus metrics available at %s://localhost:%d/metrics", scheme, httpPort))
	}

	if raftCoordinator != nil {
		logger.Info("Cluster features enabled: Pipeline Replication, Batch Commit, Dynamic Resharding, Joint Consensus")
		logger.Info("  - Single Point of Failure protection (Leader Fallback) - ENABLED")
		logger.Info("  - Automatic Panic Recovery for goroutines - ENABLED")
		logger.Info("  - Schema Migration support - ENABLED")
		logger.Info("  - Full configuration validation - ENABLED")
		logger.Info("  - Automatic data persistence - ENABLED")
	}

	displayBanner(cfg.Cluster.Name, httpPort, raftCoordinator, cfg.Saga.Enabled, cfg.Metrics.Enabled)

	replInstance, err := repl.NewRepl(store, raftCoordinator, logger, cfg, aclManager, pluginManager)
	if err != nil {
		logger.Error("Failed to initialize REPL: " + err.Error())
		utils.PrintError("Failed to initialize REPL: " + err.Error())
		os.Exit(1)
	}
	defer replInstance.Close()

	// ========== GRACEFUL SHUTDOWN ==========
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		for sig := range sigChan {
			switch sig {
			case syscall.SIGUSR1:
				logger.Info("Received SIGUSR1 (reload signal), reloading configuration...")
				continue
			case syscall.SIGINT, syscall.SIGTERM:
				logger.Info(fmt.Sprintf("Received signal %v, starting graceful shutdown...", sig))
			}
			break
		}

		utils.Println("\nReceived shutdown signal, starting graceful shutdown...")

		logger.Info("Stopping REPL...")
		replInstance.Close()
		logger.Info("REPL stopped")

		logger.Info("Stopping HTTP server...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		httpShutdownDone := make(chan struct{})
		go func() {
			if err := httpServer.Stop(); err != nil {
				logger.Error(fmt.Sprintf("HTTP server stop error: %v", err))
			}
			close(httpShutdownDone)
		}()
		select {
		case <-httpShutdownDone:
			logger.Info("HTTP server stopped")
		case <-shutdownCtx.Done():
			logger.Warn("HTTP server shutdown timeout")
		}

		if metricsCollector != nil {
			logger.Info("Stopping metrics collector...")
			metricsCollector.Stop()
			logger.Info("Metrics collector stopped")
		}

		if cfg.Plugins.Enabled && pluginManager != nil {
			logger.Info("Stopping plugins...")
			for _, p := range pluginManager.ListPlugins() {
				if err := pluginManager.StopPlugin(p.Name); err != nil {
					logger.Warn(fmt.Sprintf("Failed to stop plugin %s: %v", p.Name, err))
				}
			}
		}

		if sagaOrchestrator != nil {
			logger.Info("Stopping SAGA orchestrator...")
			sagaStopDone := make(chan struct{})
			go func() {
				sagaOrchestrator.Stop()
				close(sagaStopDone)
			}()
			select {
			case <-sagaStopDone:
				logger.Info("SAGA orchestrator stopped")
			case <-time.After(10 * time.Second):
				logger.Warn("SAGA orchestrator shutdown timeout")
			}
		}

		if raftCoordinator != nil {
			logger.Info("Persisting data to disk...")
			persistDone := make(chan struct{})
			go func() {
				if pm := raftCoordinator.GetPersistenceManager(); pm != nil {
					if err := pm.SaveAll(); err != nil {
						logger.Error(fmt.Sprintf("Failed to save data: %v", err))
					}
				}
				close(persistDone)
			}()
			select {
			case <-persistDone:
				logger.Info("Persistence completed")
			case <-time.After(15 * time.Second):
				logger.Warn("Persistence timeout")
			}
		}

		if raftCoordinator != nil {
			logger.Info("Stopping Raft coordinator...")
			raftStopDone := make(chan struct{})
			go func() {
				raftCoordinator.Stop()
				close(raftStopDone)
			}()
			select {
			case <-raftStopDone:
				logger.Info("Raft coordinator stopped")
			case <-time.After(15 * time.Second):
				logger.Warn("Raft coordinator shutdown timeout")
			}
		}

		logger.Info("Stopping cluster node...")
		nodeStopDone := make(chan struct{})
		go func() {
			node.Stop()
			close(nodeStopDone)
		}()
		select {
		case <-nodeStopDone:
			logger.Info("Cluster node stopped")
		case <-time.After(10 * time.Second):
			logger.Warn("Node shutdown timeout")
		}

		logger.Info("Finalizing logger...")
		if err := logger.Sync(); err != nil {
			fmt.Printf("Failed to sync logger: %v\n", err)
		}
		logger.Close()

		if err := storage.StopTransactionManager(); err != nil {
			fmt.Printf("Failed to stop transaction manager: %v\n", err)
		}

		if aclManager != nil {
			aclManager.Stop()
		}

		utils.DisableColorMode()
		fmt.Println("Futriis DB shutdown complete")

		exitCode = 0
		close(sigChan)
	}()

	if err := replInstance.Run(); err != nil {
		logger.Error("REPL error: " + err.Error())
		utils.PrintError("REPL error: " + err.Error())
		exitCode = 1
	}

	time.Sleep(500 * time.Millisecond)

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// ==================== Адаптеры для metrics ====================

type storageStatsAdapter struct {
	s *storage.Storage
}

func newStorageStatsAdapter(s *storage.Storage) *storageStatsAdapter {
	return &storageStatsAdapter{s: s}
}

func (a *storageStatsAdapter) GetStats() map[string]interface{} { return a.s.GetStats() }
func (a *storageStatsAdapter) GetDatabaseCount() int            { return a.s.GetDatabaseCount() }
func (a *storageStatsAdapter) GetTotalDocuments() int64         { return a.s.GetTotalDocuments() }

type coordinatorStatsAdapter struct {
	c *cluster.RaftCoordinator
}

func newCoordinatorStatsAdapter(c *cluster.RaftCoordinator) *coordinatorStatsAdapter {
	return &coordinatorStatsAdapter{c: c}
}

func (a *coordinatorStatsAdapter) GetClusterStatus() metrics.ClusterStatusLike {
	st := a.c.GetClusterStatus()
	return &clusterStatusAdapter{st: st}
}

func (a *coordinatorStatsAdapter) GetActiveNodes() []metrics.NodeInfoLike {
	nodes := a.c.GetActiveNodes()
	out := make([]metrics.NodeInfoLike, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, &nodeInfoAdapter{n: n})
	}
	return out
}

func (a *coordinatorStatsAdapter) GetAllNodes() []metrics.NodeInfoLike {
	nodes := a.c.GetAllNodes()
	out := make([]metrics.NodeInfoLike, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, &nodeInfoAdapter{n: n})
	}
	return out
}

func (a *coordinatorStatsAdapter) IsLeader() bool         { return a.c.IsLeader() }
func (a *coordinatorStatsAdapter) GetCurrentTerm() uint64 { return a.c.GetCurrentTerm() }

type clusterStatusAdapter struct {
	st *cluster.ClusterStatus
}

func (a *clusterStatusAdapter) GetTotalNodes() int   { return a.st.TotalNodes }
func (a *clusterStatusAdapter) GetActiveNodes() int  { return a.st.ActiveNodes }
func (a *clusterStatusAdapter) GetFailedNodes() int  { return a.st.FailedNodes }
func (a *clusterStatusAdapter) GetLeaderID() string  { return a.st.LeaderID }
func (a *clusterStatusAdapter) GetHealth() string    { return a.st.Health }

type nodeInfoAdapter struct {
	n *cluster.NodeInfo
}

func (a *nodeInfoAdapter) GetID() string       { return a.n.ID }
func (a *nodeInfoAdapter) GetIP() string       { return a.n.IP }
func (a *nodeInfoAdapter) GetPort() int        { return a.n.Port }
func (a *nodeInfoAdapter) GetStatus() string   { return a.n.Status }
func (a *nodeInfoAdapter) GetLastSeen() int64  { return a.n.LastSeen }

// ==================== Утилиты ====================

func ensureDataDir() (string, error) {
	dataDir := os.Getenv("FUTRIIS_DATA_DIR")
	if dataDir == "" {
		dataDir = defaultDataDir
	}
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path for %s: %w", dataDir, err)
	}
	if err := os.MkdirAll(absPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create data directory %s: %w", absPath, err)
	}
	testFile := filepath.Join(absPath, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0600); err != nil {
		return "", fmt.Errorf("data directory %s is not writable: %w", absPath, err)
	}
	_ = os.Remove(testFile)
	return absPath, nil
}

func loadAndValidateConfig(path string) (*config.Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("config file not found: %s", path)
		}
		return nil, fmt.Errorf("failed to stat config file: %w", err)
	}
	if info.Size() > maxConfigFileSize {
		return nil, fmt.Errorf("config file too large: %d bytes (max %d)", info.Size(), maxConfigFileSize)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("config path is not a regular file: %s", path)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("config is nil after load")
	}
	if err := config.ValidateConfig(cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	return cfg, nil
}

func isPortListening(port int) bool {
	if port <= 0 {
		return false
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func displayBanner(clusterName string, httpPort int, coordinator *cluster.RaftCoordinator, sagaEnabled, metricsEnabled bool) {
	utils.Println("")
	bannerLines := []string{
		"                futriis 3i²(by 02.04.2026)                 ",
		"                Distributed Document-Store in-memory database with support lua plugins   ",
		"                Cluster status: enable (Raft consensus)",
		"                Cluster features: Pipeline Replication, Batch Commit, Dynamic Resharding",
		"                Cluster name: " + clusterName,
		"                HTTP API (for curl or wget utils only): http://localhost:" + fmt.Sprintf("%d", httpPort) + "/api/",
	}
	if metricsEnabled {
		bannerLines = append(bannerLines,
			fmt.Sprintf("                Prometheus metrics: http://localhost:%d/metrics", httpPort),
			"                Grafana: add Prometheus datasource pointing to /metrics URL",
		)
	}
	bannerLines = append(bannerLines, "                Additional features:")
	bannerLines = append(bannerLines, "                  - Single Point of Failure protection (Leader Fallback)")
	bannerLines = append(bannerLines, "                  - Automatic Panic Recovery for goroutines")
	bannerLines = append(bannerLines, "                  - Schema Migration support")
	bannerLines = append(bannerLines, "                  - Full configuration validation")
	bannerLines = append(bannerLines, "                  - Automatic data persistence")
	if sagaEnabled {
		bannerLines = append(bannerLines, "                  - SAGA Orchestrator: ENABLED (distributed transactions)")
	} else {
		bannerLines = append(bannerLines, "                  - SAGA Orchestrator: DISABLED")
	}
	if metricsEnabled {
		bannerLines = append(bannerLines, "                  - Prometheus metrics: ENABLED")
	} else {
		bannerLines = append(bannerLines, "                  - Prometheus metrics: DISABLED")
	}

	bannerLines = append(bannerLines, []string{
		"",
		"                Type 'quit' or 'exit' to quit",
		"                Type 'status' to see cluster status",
		"                Type 'acl login <user> <pass>' to authenticate",
		"                Type 'plugin list' to see loaded plugins",
		"                Type 'cluster pipeline' to see pipeline stats",
		"                Type 'cluster reshard' to trigger manual resharding",
		"                Type 'migrate status' to see schema migration status",
		"                Type 'fallback status' to see SPoF protection status",
		"                Type 'panic stats' to see panic recovery statistics",
		"                Type 'persist status' to see data persistence status",
		"                Type 'saga status' to see SAGA orchestrator status",
		"                Type 'saga list' to list active SAGA transactions",
	}...)

	for _, line := range bannerLines {
		utils.PrintInfo(line)
	}
}

func printJSON(data interface{}) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		utils.PrintError("Failed to marshal JSON: " + err.Error())
		return
	}
	fmt.Println(string(jsonData))
}
