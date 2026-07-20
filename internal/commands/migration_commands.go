/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/commands/migration_commands.go
// Назначение: Команды REPL для управления кросс-датацентровой миграцией

package commands

import (
    "fmt"
    "strings"
    "time"

    "futriis/internal/cluster"
    "futriis/internal/storage"
    "futriis/pkg/utils"
)

// MigrationCommandHandler обрабатывает команды миграции
type MigrationCommandHandler struct {
    migrator *cluster.CrossDCMigrator
    store    *storage.Storage
}

// NewMigrationCommandHandler создаёт новый обработчик команд миграции
func NewMigrationCommandHandler(migrator *cluster.CrossDCMigrator, store *storage.Storage) *MigrationCommandHandler {
    return &MigrationCommandHandler{
        migrator: migrator,
        store:    store,
    }
}

// ExecuteMigrationCommand выполняет команду миграции
func (h *MigrationCommandHandler) ExecuteMigrationCommand(cmd string) error {
    if h.migrator == nil {
        return fmt.Errorf("migration system is not initialized")
    }

    parts := strings.Fields(cmd)
    if len(parts) < 2 {
        return fmt.Errorf("usage: migration <subcommand> [options]")
    }

    subcommand := parts[1]

    switch subcommand {
    case "start":
        return h.handleStartMigration(parts[2:])
    case "status":
        return h.handleMigrationStatus(parts[2:])
    case "list":
        return h.handleListMigrations(parts[2:])
    case "pause":
        return h.handlePauseMigration(parts[2:])
    case "resume":
        return h.handleResumeMigration(parts[2:])
    case "cancel":
        return h.handleCancelMigration(parts[2:])
    case "stats":
        return h.handleMigrationStats(parts[2:])
    case "config":
        return h.handleMigrationConfig(parts[2:])
    case "queue":
        return h.handleQueueStatus(parts[2:])
    default:
        return fmt.Errorf("unknown migration subcommand: %s", subcommand)
    }
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД
// =============================================================================

// handleStartMigration обрабатывает команду "migration start"
// Формат: migration start <source_dc> <target_dc> [database] [collection]
func (h *MigrationCommandHandler) handleStartMigration(args []string) error {
    if len(args) < 2 {
        return fmt.Errorf("usage: migration start <source_dc> <target_dc> [database] [collection]")
    }

    sourceDC := args[0]
    targetDC := args[1]

    // Определяем базы данных и коллекции
    var databases []string
    var collections map[string][]string

    if len(args) > 2 {
        databases = []string{args[2]}
        if len(args) > 3 {
            collections = map[string][]string{
                args[2]: {args[3]},
            }
        }
    }

    utils.PrintInfo(fmt.Sprintf("Starting migration from %s to %s at %s...", sourceDC, targetDC, time.Now().Format("2006-01-02 15:04:05")))

    task, err := h.migrator.StartMigration(sourceDC, targetDC, databases, collections)
    if err != nil {
        return fmt.Errorf("failed to start migration: %v", err)
    }

    utils.PrintSuccess(fmt.Sprintf("Migration started: %s", task.ID))
    utils.PrintInfo(fmt.Sprintf("  Source: %s", task.SourceDC))
    utils.PrintInfo(fmt.Sprintf("  Target: %s", task.TargetDC))
    utils.PrintInfo(fmt.Sprintf("  Total documents: %d", task.TotalDocuments))
    utils.PrintInfo(fmt.Sprintf("  Status: %s", task.Status))

    return nil
}

// handleMigrationStatus обрабатывает команду "migration status"
// Формат: migration status [task_id]
func (h *MigrationCommandHandler) handleMigrationStatus(args []string) error {
    var taskID string
    if len(args) > 0 {
        taskID = args[0]
    } else {
        taskID = h.migrator.GetCurrentTaskID()
        if taskID == "" {
            return fmt.Errorf("no active migration task")
        }
    }

    task, err := h.migrator.GetMigrationStatus(taskID)
    if err != nil {
        return err
    }

    h.printTaskStatus(task)
    return nil
}

// handleListMigrations обрабатывает команду "migration list"
func (h *MigrationCommandHandler) handleListMigrations(args []string) error {
    tasks := h.migrator.ListTasks()

    if len(tasks) == 0 {
        utils.PrintInfo("No migration tasks found")
        return nil
    }

    utils.PrintHeader("Migration Tasks")
    utils.PrintInfo("  ID                                   STATUS       PROGRESS    SOURCE -> TARGET")
    utils.Println("  ---------------------------------------------------------------------------------")

    for _, task := range tasks {
        task.mu.RLock()
        id := task.ID
        if len(id) > 20 {
            id = id[:17] + "..."
        }

        statusColor := h.getStatusColor(string(task.Status))
        statusStr := h.colorize(string(task.Status), statusColor)

        progress := fmt.Sprintf("%.1f%%", task.ProgressPercent)
        if task.Status == cluster.MigrationStatusCompleted {
            progress = "100%"
        }

        utils.PrintInfo(fmt.Sprintf("  %-36s %-12s %-10s %s -> %s",
            id, statusStr, progress, task.SourceDC, task.TargetDC))
        task.mu.RUnlock()
    }

    return nil
}

// handlePauseMigration обрабатывает команду "migration pause"
func (h *MigrationCommandHandler) handlePauseMigration(args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("usage: migration pause <task_id>")
    }

    taskID := args[0]
    if err := h.migrator.PauseMigration(taskID); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Migration %s paused", taskID))
    return nil
}

// handleResumeMigration обрабатывает команду "migration resume"
func (h *MigrationCommandHandler) handleResumeMigration(args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("usage: migration resume <task_id>")
    }

    taskID := args[0]
    if err := h.migrator.ResumeMigration(taskID); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Migration %s resumed", taskID))
    return nil
}

// handleCancelMigration обрабатывает команду "migration cancel"
func (h *MigrationCommandHandler) handleCancelMigration(args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("usage: migration cancel <task_id>")
    }

    taskID := args[0]
    if err := h.migrator.CancelMigration(taskID); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Migration %s cancelled", taskID))
    return nil
}

// handleMigrationStats обрабатывает команду "migration stats"
func (h *MigrationCommandHandler) handleMigrationStats(args []string) error {
    stats := h.migrator.GetMigrationStats()

    utils.PrintHeader("Migration Statistics")
    utils.PrintInfo(fmt.Sprintf("  Total Changes:    %d", stats.TotalChanges))
    utils.PrintInfo(fmt.Sprintf("  Applied Changes:  %d", stats.AppliedChanges))
    utils.PrintInfo(fmt.Sprintf("  Failed Changes:   %d", stats.FailedChanges))
    utils.PrintInfo(fmt.Sprintf("  Skipped Changes:  %d", stats.SkippedChanges))
    utils.PrintInfo(fmt.Sprintf("  Avg Latency:      %d ms", stats.LatencyAvg))
    utils.PrintInfo(fmt.Sprintf("  Max Latency:      %d ms", stats.LatencyMax))
    utils.PrintInfo(fmt.Sprintf("  Throughput:       %d docs/sec", stats.Throughput))

    return nil
}

// handleMigrationConfig обрабатывает команду "migration config"
func (h *MigrationCommandHandler) handleMigrationConfig(args []string) error {
    utils.PrintHeader("Migration Configuration")

    if len(args) > 0 {
        // Показать конкретную настройку
        switch args[0] {
        case "mode":
            utils.PrintInfo(fmt.Sprintf("  Mode: %s", h.migrator.mode))
        default:
            return fmt.Errorf("unknown config key: %s", args[0])
        }
        return nil
    }

    // Показать все настройки
    utils.PrintInfo(fmt.Sprintf("  Enabled:          %v", h.migrator.config.Enabled))
    utils.PrintInfo(fmt.Sprintf("  Mode:             %s", h.migrator.mode))
    utils.PrintInfo(fmt.Sprintf("  Source DC:        %s", h.migrator.config.Source.Name))
    utils.PrintInfo(fmt.Sprintf("  Target DC:        %s", h.migrator.config.Target.Name))
    utils.PrintInfo(fmt.Sprintf("  Batch Size:       %d", h.migrator.config.Settings.BatchSize))
    utils.PrintInfo(fmt.Sprintf("  Workers:          %d", h.migrator.config.Settings.Workers))
    utils.PrintInfo(fmt.Sprintf("  Compression:      %s", h.migrator.config.Settings.Compression))
    utils.PrintInfo(fmt.Sprintf("  Resume Enabled:   %v", h.migrator.config.Settings.ResumeEnabled))
    utils.PrintInfo(fmt.Sprintf("  Max Retries:      %d", h.migrator.config.Settings.MaxRetries))
    utils.PrintInfo(fmt.Sprintf("  Delta Sync:       %v", h.migrator.config.Delta.Enabled))
    utils.PrintInfo(fmt.Sprintf("  Delta Interval:   %d sec", h.migrator.config.Delta.IntervalSec))
    utils.PrintInfo(fmt.Sprintf("  Validation:       %v", h.migrator.config.Validation.Enabled))
    utils.PrintInfo(fmt.Sprintf("  Sample Percent:   %d%%", h.migrator.config.Validation.SamplePercent))

    return nil
}

// handleQueueStatus обрабатывает команду "migration queue"
func (h *MigrationCommandHandler) handleQueueStatus(args []string) error {
    stats := h.migrator.GetQueueStats()

    utils.PrintHeader("Migration Queue Status")
    utils.PrintInfo(fmt.Sprintf("  Queue Size:       %d", stats["size"]))
    utils.PrintInfo(fmt.Sprintf("  Last LSN:         %d", stats["last_lsn"]))
    utils.PrintInfo(fmt.Sprintf("  Max Size:         %d", stats["max_size"]))
    utils.PrintInfo(fmt.Sprintf("  File Path:        %s", stats["file_path"]))

    return nil
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
// =============================================================================

// printTaskStatus выводит статус задачи
func (h *MigrationCommandHandler) printTaskStatus(task *cluster.MigrationTask) {
    task.mu.RLock()
    defer task.mu.RUnlock()

    utils.PrintHeader(fmt.Sprintf("Migration Task: %s", task.ID))
    utils.PrintInfo(fmt.Sprintf("  Source DC:     %s", task.SourceDC))
    utils.PrintInfo(fmt.Sprintf("  Target DC:     %s", task.TargetDC))
    utils.PrintInfo(fmt.Sprintf("  Status:        %s", h.colorize(string(task.Status), h.getStatusColor(string(task.Status)))))
    utils.PrintInfo(fmt.Sprintf("  Progress:      %.1f%%", task.ProgressPercent))
    utils.PrintInfo(fmt.Sprintf("  Total Docs:    %d", task.TotalDocuments))
    utils.PrintInfo(fmt.Sprintf("  Migrated:      %d", task.MigratedDocs))
    utils.PrintInfo(fmt.Sprintf("  Failed:        %d", task.FailedDocs))
    utils.PrintInfo(fmt.Sprintf("  Skipped:       %d", task.SkippedDocs))

    if task.StartTime > 0 {
        startTime := time.UnixMilli(task.StartTime).Format("2006-01-02 15:04:05")
        utils.PrintInfo(fmt.Sprintf("  Started:       %s", startTime))
    }

    if task.EndTime > 0 {
        endTime := time.UnixMilli(task.EndTime).Format("2006-01-02 15:04:05")
        utils.PrintInfo(fmt.Sprintf("  Completed:     %s", endTime))
    }

    if task.Error != "" {
        utils.PrintError(fmt.Sprintf("  Error:         %s", task.Error))
    }

    // Показываем базы данных
    if len(task.Databases) > 0 {
        utils.PrintInfo(fmt.Sprintf("  Databases:     %s", strings.Join(task.Databases, ", ")))
    }

    // Показываем коллекции
    if len(task.Collections) > 0 {
        utils.PrintInfo("  Collections:")
        for db, colls := range task.Collections {
            utils.PrintInfo(fmt.Sprintf("    %s: %s", db, strings.Join(colls, ", ")))
        }
    }
}

// colorize возвращает цветной текст
func (h *MigrationCommandHandler) colorize(text, color string) string {
    switch color {
    case "green":
        return "\033[32m" + text + "\033[0m"
    case "red":
        return "\033[31m" + text + "\033[0m"
    case "yellow":
        return "\033[33m" + text + "\033[0m"
    case "blue":
        return "\033[34m" + text + "\033[0m"
    case "cyan":
        return "\033[36m" + text + "\033[0m"
    case "white":
        return "\033[37m" + text + "\033[0m"
    default:
        return text
    }
}

// getStatusColor возвращает цвет для статуса
func (h *MigrationCommandHandler) getStatusColor(status string) string {
    switch status {
    case "idle":
        return "cyan"
    case "preparing":
        return "blue"
    case "migrating":
        return "yellow"
    case "delta_sync":
        return "yellow"
    case "validating":
        return "cyan"
    case "completed":
        return "green"
    case "failed":
        return "red"
    case "paused":
        return "yellow"
    default:
        return "white"
    }
}
