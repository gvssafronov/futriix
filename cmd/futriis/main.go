/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 
 // Файл: cmd/futriis/main.go
// Назначение: Точка входа в приложение СУБД futriix. 
//  Инициализирует все компоненты системы: логгер, хранилище, транзакции, ACL, Raft координатор, HTTP API, WebUI, плагины и REPL
// Реализует graceful shutdown, проверку и применение миграций схемы данных, защиту от SPoF, автоматическое восстановление после паник горутин и полную валидацию конфигурации
  */

package main

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "os/signal"
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

func main() {
    utils.SetColorEnabled(true)
    
    logLevel := os.Getenv("LOG_LEVEL")
    if logLevel == "" {
        logLevel = "info"
    }
    
    if err := log.InitDefaultLogger("futriis/futriis.log", logLevel); err != nil {
        fmt.Printf("Failed to initialize logger: %v\n", err)
        os.Exit(1)
    }
    
    log.Info("Futriis DB starting...")
    log.Infof("Log level: %s", logLevel)
    
    cfg, err := config.LoadConfig("config.toml")
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
    logger.Info("futriix database starting...")

    store := storage.NewStorage(cfg.Storage.PageSizeMB, logger)
    
    storage.SetGlobalStorage(store)
    
    if err := storage.InitTransactionManager("futriix.wal"); err != nil {
        logger.Warn("Failed to initialize transaction manager: " + err.Error())
    } else {
        storage.SetTransactionLogger(logger)
        logger.Info("Transaction manager initialized")
    }
    
    // Инициализация менеджера триггеров (функция не возвращает ошибку)
    storage.InitTriggerManager(logger)
    logger.Info("Trigger manager initialized")
    
    aclManager := acl.NewACLManager()
    logger.Info("ACL manager initialized")

    // Исправлено: передаём store как второй аргумент
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
            // Сохраняем для использования в других компонентах
            storage.SetGlobalSagaOrchestrator(sagaOrchestrator)
        }
    } else {
        logger.Info("Saga orchestrator disabled in configuration")
    }

    // ========== НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Проверка статуса миграций схемы данных ==========
    if raftCoordinator != nil {
        schemaMigrator := raftCoordinator.GetSchemaMigrator()
        if schemaMigrator != nil {
            logger.Info("Checking for schema migrations...")
            status := schemaMigrator.GetStatus()
            logger.Infof("Current schema version: %s, total migrations: %d, applied: %d, pending: %d", 
                status.CurrentVersion, status.TotalMigrations, status.AppliedMigrations, status.PendingMigrations)
            
            // Выводим список миграций
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
        
        // ========== НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Проверка статуса Fallback менеджера ==========
        fallbackMgr := raftCoordinator.GetFallbackManager()
        if fallbackMgr != nil {
            logger.Info("Leader fallback manager is active (Single Point of Failure protection enabled)")
            fallbackStats := raftCoordinator.GetFallbackStats()
            logger.Debugf("Fallback manager stats: %+v", fallbackStats)
        } else {
            logger.Warn("Leader fallback manager not available")
        }
        
        // ========== НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Проверка статуса Panic Recovery менеджера ==========
        panicRecoveryMgr := raftCoordinator.GetPanicRecoveryManager()
        if panicRecoveryMgr != nil {
            logger.Info("Panic recovery manager is active (automatic goroutine recovery enabled)")
            recoveryStats := raftCoordinator.GetPanicRecoveryStats()
            logger.Debugf("Panic recovery stats: %+v", recoveryStats)
        } else {
            logger.Warn("Panic recovery manager not available")
        }
        
        // ========== НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Проверка статуса Persistence менеджера ==========
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
    
    // Исправлено: объявляем переменную maxRetries здесь, чтобы она была доступна
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

    // Исправлено: передаём отдельные параметры из конфигурации плагинов
    pluginManager := plugin.NewPluginManager(
        cfg.Plugins.ScriptDir,
        logger,
        store,
        cfg.Plugins.Enabled,
    )
    
    if cfg.Plugins.Enabled {
        logger.Info(fmt.Sprintf("Plugin manager initialized, directory: %s", cfg.Plugins.ScriptDir))
        
        plugins := pluginManager.ListPlugins()
        for _, p := range plugins {
            if err := pluginManager.StartPlugin(p.Name); err != nil {
                logger.Warn(fmt.Sprintf("Failed to start plugin %s: %v", p.Name, err))
            }
        }
    }

    httpPort := cfg.API.Port
    httpServer := api.NewHTTPServer(httpPort, store, raftCoordinator, aclManager, logger)
    go func() {
        if err := httpServer.Start(); err != nil {
            logger.Error("HTTP server error: " + err.Error())
            utils.PrintError("HTTP server error: " + err.Error())
        }
    }()
    logger.Info(fmt.Sprintf("HTTP API server started on port %d", httpPort))

    webUIPort := cfg.WebUI.Port
    if webUIPort == 0 {
        webUIPort = 8081
    }
    webUI := api.NewWebUIServer(webUIPort, cfg.WebUI.Enabled, store, raftCoordinator, aclManager, logger)
    go func() {
        if err := webUI.Start(); err != nil && cfg.WebUI.Enabled {
            logger.Error("Web UI error: " + err.Error())
            utils.PrintError("Web UI error: " + err.Error())
        }
    }()
    
    if cfg.WebUI.Enabled {
        logger.Info(fmt.Sprintf("Web UI started on port %d", webUIPort))
    }

    if raftCoordinator != nil {
        logger.Info("Cluster features enabled: Pipeline Replication, Batch Commit, Dynamic Resharding, Joint Consensus")
        logger.Info("Observability metrics and health checks available at /api/webui/metrics and /api/webui/health")
        
        // ========== НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Вывод информации о новых компонентах ==========
        logger.Info("Additional features:")
        logger.Info("  - Single Point of Failure protection (Leader Fallback) - ENABLED")
        logger.Info("  - Automatic Panic Recovery for goroutines - ENABLED")
        logger.Info("  - Schema Migration support - ENABLED")
        logger.Info("  - Full configuration validation - ENABLED")
        logger.Info("  - Automatic data persistence - ENABLED")
    }

    displayBanner(cfg.Cluster.Name, cfg.WebUI.Enabled, webUIPort, httpPort, raftCoordinator, cfg.Saga.Enabled)

    replInstance := repl.NewRepl(store, raftCoordinator, logger, cfg, aclManager, pluginManager)

    // ========== GRACEFUL SHUTDOWN ==========
    // Канал для сигналов завершения
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
    
    // Запускаем graceful shutdown в отдельной горутине
    go func() {
        <-sigChan
        utils.Println("\nReceived shutdown signal, starting graceful shutdown...")
        logger.Info("Received shutdown signal, starting graceful shutdown...")
        
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
        
        // 3. Останавливаем WebUI
        logger.Info("Stopping WebUI...")
        webUIStopDone := make(chan struct{})
        go func() {
            if err := webUI.Stop(); err != nil {
                logger.Error(fmt.Sprintf("WebUI stop error: %v", err))
            }
            close(webUIStopDone)
        }()
        
        select {
        case <-webUIStopDone:
            logger.Info("WebUI stopped")
        case <-time.After(10 * time.Second):
            logger.Warn("WebUI shutdown timeout")
        }
        
        // 4. Останавливаем плагины
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
        
        // 5. Останавливаем SAGA оркестратор
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
        
        // 6. Сохраняем данные на диск
        logger.Info("Persisting data to disk...")
        persistDone := make(chan struct{})
        go func() {
            // Используем публичный метод GetPersistenceManager()
            persistenceMgr := raftCoordinator.GetPersistenceManager()
            if raftCoordinator != nil && persistenceMgr != nil {
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
        
        // 7. Останавливаем координатор
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
        
        // 8. Останавливаем узел
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
        
        // 9. Синхронизируем и закрываем логгер
        logger.Info("Finalizing logger...")
        if err := logger.Sync(); err != nil {
            fmt.Printf("Failed to sync logger: %v\n", err)
        }
        logger.Close()
        
        utils.DisableColorMode()
        fmt.Println("Futriis DB shutdown complete")
        os.Exit(0)
    }()

    // Запускаем REPL (блокирующий вызов)
    if err := replInstance.Run(); err != nil {
        logger.Error("REPL error: " + err.Error())
        utils.PrintError("REPL error: " + err.Error())
        os.Exit(1)
    }
}

func displayBanner(clusterName string, webUIEnabled bool, webUIPort int, httpPort int, coordinator *cluster.RaftCoordinator, sagaEnabled bool) {
    utils.Println("")
    bannerLines := []string{
        "                futriix 3i²(by 02.04.2026)                 ",
        "                Distributed Document-Store in-memory database with support lua plugins   ",
        "                Cluster status: enable (Raft consensus)",
        "                Cluster features: Pipeline Replication, Batch Commit, Dynamic Resharding",
        "                Cluster name: " + clusterName,
        "                HTTP API (for curl or wget utils only): http://localhost:" + fmt.Sprintf("%d", httpPort) + "/api/",
    }
    
    if webUIEnabled {
        bannerLines = append(bannerLines, fmt.Sprintf("                Web UI: http://localhost:%d/", webUIPort))
        bannerLines = append(bannerLines, "                Observability endpoints: /api/webui/metrics, /api/webui/health")
    }
    
    // ========== НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: Добавляем информацию о новых компонентах в баннер ==========
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
    
    // Выводим статус Fallback менеджера, если он активен
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
