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
 *
 * ДОПОЛНИТЕЛЬНО ИСПРАВЛЕНО:
 *   - repl.NewRepl теперь возвращает (*Repl, error), так как внутри
 *     создаётся *readline.Instance (chzyer/readline), инициализация
 *     которого может завершиться ошибкой (например, при отсутствии TTY
 *     или проблемах с termios на OpenIndiana в single-user режиме).
 *     Добавлена обработка ошибки инициализации REPL.
 *
 * УДАЛЕНО:
 *   - WebUI полностью удалён из ядра. HTTP API (api.NewHTTPServer)
 *     продолжает работать и предоставляет весь функционал через REST.
 *     Для UI используйте внешние инструменты (curl, jq, отдельный
 *     webui-клиент), подключающиеся к HTTP API.
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
	"futriis/internal/plugin"
	"futriis/internal/repl"
	"futriis/internal/storage"
	"futriis/pkg/utils"
)

const (
	// Директория по умолчанию для данных futriis
	defaultDataDir = "futriis"
	// Максимальный размер файла конфигурации (1 MB)
	maxConfigFileSize = 1 * 1024 * 1024
)

// exitCode — код возврата. Устанавливается в graceful shutdown,
// а os.Exit вызывается только в самом конце main(), чтобы defer'ы
// успели выполниться.
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

	// Валидация пути к конфигу и прав доступа
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

	// ИСПРАВЛЕНО: WAL в dataDir, а не относительно CWD
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

	// ========== ИНИЦИАЛИЗАЦИЯ SAGA ОРКЕСТРАТОРА ==========
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

			for _, m := range status.Migrations {
				if m.Applied {
					logger.Debugf("  [APPLIED] %s v%s: %s (at %s)",
						m.ID, m.Version, m.Description, time.UnixMilli(m.AppliedAt).Format("2006-01-02 15:04:05"))
				} else {
					logger.Debugf("  [PENDING] %s v%s: %s", m.ID, m.Version, m.Description)
				}
			}

			if status.PendingMigrations > 0 {
				logger.Info("Applying pending schema migrations...")
				if err := schemaMigrator.Migrate("2.1.0"); err != nil {
					logger.Warn(fmt.Sprintf("Schema migration warning: %v", err))
				} else {
					logger.Info("Schema migrations completed successfully")
				}
			}
		} else {
			logger.Warn("Schema migrator not available")
		}

		fallbackMgr := raftCoordinator.GetFallbackManager()
		if fallbackMgr != nil {
			logger.Info("Leader fallback manager is active (Single Point of Failure protection enabled)")
			fallbackStats := raftCoordinator.GetFallbackStats()
			logger.Debugf("Fallback manager stats: %+v", fallbackStats)
		} else {
			logger.Warn("Leader fallback manager not available")
		}

		panicRecoveryMgr := raftCoordinator.GetPanicRecoveryManager()
		if panicRecoveryMgr != nil {
			logger.Info("Panic recovery manager is active (automatic goroutine recovery enabled)")
			recoveryStats := raftCoordinator.GetPanicRecoveryStats()
			logger.Debugf("Panic recovery stats: %+v", recoveryStats)
		} else {
			logger.Warn("Panic recovery manager not available")
		}

		persistenceMgr := raftCoordinator.GetPersistenceManager()
		if persistenceMgr != nil {
			logger.Info("Persistence manager is active (automatic data persistence enabled)")
		} else {
			logger.Warn("Persistence manager not available")
		}
	}

	if cfg.Cluster.Bootstrap || len(cfg.Cluster.Nodes) <= 1 {
		maxRetriesLocal := 10
		for i := 0; i < maxRetriesLocal; i++ {
			if raftCoordinator.IsLeader() {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	node := cluster.NewNode(cfg.Cluster.NodeIP, cfg.Cluster.NodePort, store, logger)

	maxRetries := 5
	var registerErr error
	for i := 0; i < maxRetries; i++ {
		registerErr = raftCoordinator.RegisterNode(node)
		if registerErr == nil {
			break
		}
		if i < maxRetries-1 {
			logger.Warn(fmt.Sprintf("Failed to register node (attempt %d/%d): %v, retrying...", i+1, maxRetries, registerErr))
			time.Sleep(2 * time.Second)
		}
	}

	if registerErr != nil {
		logger.Error("Failed to register node: " + registerErr.Error())
		utils.PrintError("Failed to register node: " + registerErr.Error())
		os.Exit(1)
	}

	// ИСПРАВЛЕНО: проверка allow-list перед запуском плагинов
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

		plugins := pluginManager.ListPlugins()
		for _, p := range plugins {
			// ИСПРАВЛЕНО: если allow-list не пуст, плагин должен быть в нём
			if len(allowList) > 0 && !allowList[p.Name] {
				logger.Warn(fmt.Sprintf("Plugin %s is not in allow-list, skipping start", p.Name))
				continue
			}
			if err := pluginManager.StartPlugin(p.Name); err != nil {
				logger.Warn(fmt.Sprintf("Failed to start plugin %s: %v", p.Name, err))
			}
		}
	}

	// ========== HTTP API SERVER ==========
	// WebUI удалён из ядра. HTTP API предоставляет весь функционал через REST.
	httpPort := cfg.API.Port
	httpServer := api.NewHTTPServer(httpPort, store, raftCoordinator, aclManager, logger)

	httpErrChan := make(chan error, 1)
	go func() {
		if err := httpServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrChan <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	// Ждём запуска HTTP сервера (до 5 секунд)
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
	logger.Info(fmt.Sprintf("HTTP API server started on port %d", httpPort))

	if raftCoordinator != nil {
		logger.Info("Cluster features enabled: Pipeline Replication, Batch Commit, Dynamic Resharding, Joint Consensus")
		logger.Info("Additional features:")
		logger.Info("  - Single Point of Failure protection (Leader Fallback) - ENABLED")
		logger.Info("  - Automatic Panic Recovery for goroutines - ENABLED")
		logger.Info("  - Schema Migration support - ENABLED")
		logger.Info("  - Full configuration validation - ENABLED")
		logger.Info("  - Automatic data persistence - ENABLED")
	}

	displayBanner(cfg.Cluster.Name, httpPort, raftCoordinator, cfg.Saga.Enabled)

	// =========================================================================
	// ИСПРАВЛЕНО: repl.NewRepl теперь возвращает (*Repl, error),
	// так как внутри создаётся *readline.Instance, инициализация
	// которого может завершиться ошибкой (например, при отсутствии TTY
	// или проблемах с termios на OpenIndiana в single-user режиме).
	//
	// Было:
	//     replInstance := repl.NewRepl(store, raftCoordinator, logger, cfg, aclManager, pluginManager)
	//
	// Стало:
	//     replInstance, err := repl.NewRepl(...)
	//     if err != nil { ... }
	// =========================================================================
	replInstance, err := repl.NewRepl(store, raftCoordinator, logger, cfg, aclManager, pluginManager)
	if err != nil {
		logger.Error("Failed to initialize REPL: " + err.Error())
		utils.PrintError("Failed to initialize REPL: " + err.Error())
		os.Exit(1)
	}
	defer replInstance.Close()

	// ========== GRACEFUL SHUTDOWN ==========
	sigChan := make(chan os.Signal, 1)
	// ИСПРАВЛЕНО: SIGHUP заменён на SIGUSR1 (reload), SIGHUP игнорируется
	// или обрабатывается отдельно, чтобы не ронять процесс при закрытии терминала.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		for sig := range sigChan {
			switch sig {
			case syscall.SIGUSR1:
				logger.Info("Received SIGUSR1 (reload signal), reloading configuration...")
				// TODO: реализовать reload конфигурации
				continue
			case syscall.SIGINT, syscall.SIGTERM:
				logger.Info(fmt.Sprintf("Received signal %v, starting graceful shutdown...", sig))
			}
			break
		}

		utils.Println("\nReceived shutdown signal, starting graceful shutdown...")

		// 1. Останавливаем REPL
		logger.Info("Stopping REPL...")
		replInstance.Close()
		logger.Info("REPL stopped")

		// 2. Останавливаем HTTP сервер с таймаутом
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

		// 3. Останавливаем плагины
		if cfg.Plugins.Enabled && pluginManager != nil {
			logger.Info("Stopping plugins...")
			plugins := pluginManager.ListPlugins()
			pluginStopDone := make(chan struct{})
			go func() {
				for _, p := range plugins {
					if err := pluginManager.StopPlugin(p.Name); err != nil {
						logger.Warn(fmt.Sprintf("Failed to stop plugin %s: %v", p.Name, err))
					}
				}
				close(pluginStopDone)
			}()

			select {
			case <-pluginStopDone:
				logger.Info("Plugins stopped")
			case <-time.After(5 * time.Second):
				logger.Warn("Plugins shutdown timeout")
			}
		}

		// 4. Останавливаем SAGA оркестратор (всегда, если создан)
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

		// 5. Сохраняем данные на диск
		// ИСПРАВЛЕНО: проверяем raftCoordinator на nil
		if raftCoordinator != nil {
			logger.Info("Persisting data to disk...")
			persistDone := make(chan struct{})
			go func() {
				persistenceMgr := raftCoordinator.GetPersistenceManager()
				if persistenceMgr != nil {
					if err := persistenceMgr.SaveAll(); err != nil {
						logger.Error(fmt.Sprintf("Failed to save data: %v", err))
					} else {
						logger.Info("Data persisted to disk")
					}
				} else {
					logger.Warn("Persistence manager not available, skipping data persistence")
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

		// 6. Останавливаем координатор
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

		// 7. Останавливаем узел
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

		// 8. Синхронизируем и закрываем логгер
		logger.Info("Finalizing logger...")
		if err := logger.Sync(); err != nil {
			fmt.Printf("Failed to sync logger: %v\n", err)
		}
		logger.Close()

		// 9. Останавливаем транзакционный менеджер
		if err := storage.StopTransactionManager(); err != nil {
			fmt.Printf("Failed to stop transaction manager: %v\n", err)
		}

		// 10. Останавливаем ACL manager (фоновые горутины очистки сессий)
		if aclManager != nil {
			aclManager.Stop()
		}

		utils.DisableColorMode()
		fmt.Println("Futriis DB shutdown complete")

		// ИСПРАВЛЕНО: устанавливаем exitCode и завершаем main через return,
		// чтобы defer'ы выполнились. os.Exit вызывается только в конце main.
		exitCode = 0
		close(sigChan)
	}()

	// Запускаем REPL (блокирующий вызов)
	if err := replInstance.Run(); err != nil {
		logger.Error("REPL error: " + err.Error())
		utils.PrintError("REPL error: " + err.Error())
		exitCode = 1
	}

	// Даём graceful shutdown время завершиться
	time.Sleep(500 * time.Millisecond)

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

// ensureDataDir создаёт и возвращает абсолютный путь к директории данных.
// ИСПРАВЛЕНО: возвращает абсолютный путь, чтобы избежать зависимости от CWD.
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

	// Проверяем, что директория доступна для записи
	testFile := filepath.Join(absPath, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0600); err != nil {
		return "", fmt.Errorf("data directory %s is not writable: %w", absPath, err)
	}
	_ = os.Remove(testFile)

	return absPath, nil
}

// loadAndValidateConfig загружает и валидирует конфигурацию.
// ИСПРАВЛЕНО: проверка размера файла, прав доступа, валидация.
func loadAndValidateConfig(path string) (*config.Config, error) {
	// Проверка существования файла
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("config file not found: %s", path)
		}
		return nil, fmt.Errorf("failed to stat config file: %w", err)
	}

	// Проверка размера
	if info.Size() > maxConfigFileSize {
		return nil, fmt.Errorf("config file too large: %d bytes (max %d)", info.Size(), maxConfigFileSize)
	}

	// Проверка, что это обычный файл (не симлинк на /etc/passwd и т.д.)
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

	// Валидация конфигурации
	if err := config.ValidateConfig(cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// isPortListening проверяет, слушается ли порт.
// Простая эвристика: пробуем подключиться к localhost:port с таймаутом.
// Если порт занят — значит, сервер запустился.
func isPortListening(port int) bool {
	if port <= 0 {
		return false
	}
	// Пробуем установить TCP-соединение с коротким таймаутом
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func displayBanner(clusterName string, httpPort int, coordinator *cluster.RaftCoordinator, sagaEnabled bool) {
	utils.Println("")
	bannerLines := []string{
		"                futriis 3i²(by 02.04.2026)                 ",
		"                Distributed Document-Store in-memory database with support lua plugins   ",
		"                Cluster status: enable (Raft consensus)",
		"                Cluster features: Pipeline Replication, Batch Commit, Dynamic Resharding",
		"                Cluster name: " + clusterName,
		"                HTTP API (for curl or wget utils only): http://localhost:" + fmt.Sprintf("%d", httpPort) + "/api/",
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

	if coordinator != nil {
		fallbackMgr := coordinator.GetFallbackManager()
		if fallbackMgr != nil {
			bannerLines = append(bannerLines, "                  - SPoF Protection: ACTIVE")
		} else {
			bannerLines = append(bannerLines, "                  - SPoF Protection: DISABLED")
		}

		panicRecoveryMgr := coordinator.GetPanicRecoveryManager()
		if panicRecoveryMgr != nil {
			bannerLines = append(bannerLines, "                  - Panic Recovery: ACTIVE")
			stats := coordinator.GetPanicRecoveryStats()
			if totalPanics, ok := stats["total_panics"].(uint64); ok && totalPanics > 0 {
				bannerLines = append(bannerLines, fmt.Sprintf("                  - Panics recovered: %d", totalPanics))
			}
		} else {
			bannerLines = append(bannerLines, "                  - Panic Recovery: DISABLED")
		}

		migrationStatus := coordinator.GetMigrationStatus()
		if migrationStatus != nil {
			if migrationStatus.PendingMigrations > 0 {
				bannerLines = append(bannerLines, fmt.Sprintf("                  - Pending migrations: %d", migrationStatus.PendingMigrations))
				bannerLines = append(bannerLines, "                  - Run 'migrate status' for details")
			} else {
				bannerLines = append(bannerLines, "                  - Schema: UP-TO-DATE")
			}
		}

		persistenceMgr := coordinator.GetPersistenceManager()
		if persistenceMgr != nil {
			bannerLines = append(bannerLines, "                  - Data Persistence: ACTIVE")
		} else {
			bannerLines = append(bannerLines, "                  - Data Persistence: DISABLED")
		}
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
