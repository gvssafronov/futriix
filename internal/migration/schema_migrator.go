/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/migration/schema_migrator.go
// Назначение: Миграция схемы данных при обновлении версии СУБД
// Новая функциональность: Прозрачное обновление схемы данных (автоматическая миграция документов)

package migration

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "time"
    
    "futriis/internal/log"
    "futriis/internal/storage"
)

// SchemaDefinition определяет схему коллекции
type SchemaDefinition struct {
    Version     string                       `json:"version"`
    Collections map[string]*CollectionSchema `json:"collections"`
    UpdatedAt   int64                        `json:"updated_at"`
}

// CollectionSchema определяет схему одной коллекции
type CollectionSchema struct {
    Fields     map[string]*FieldDefinition `json:"fields"`
    Required   []string                    `json:"required"`
    Indexes    []IndexDefinition           `json:"indexes"`
    UpdatedAt  int64                       `json:"updated_at"`
}

// FieldDefinition определяет поле в схеме
type FieldDefinition struct {
    Type          string      `json:"type"`          // string, int, float, bool, array, object
    Required      bool        `json:"required"`
    Default       interface{} `json:"default"`
    Validate      string      `json:"validate"`      // regex, min, max, enum
    ValidateValue interface{} `json:"validate_value"`
}

// IndexDefinition определяет индекс
type IndexDefinition struct {
    Name   string   `json:"name"`
    Fields []string `json:"fields"`
    Unique bool     `json:"unique"`
}

// SchemaMigration представляет миграцию схемы
type SchemaMigration struct {
    ID          string
    Version     string
    Description string
    Up          func(tx *storage.Transaction, schema *SchemaDefinition) error
    Down        func(tx *storage.Transaction, schema *SchemaDefinition) error
    CreatedAt   int64
    AppliedAt   int64
}

// SchemaMigrationRecord запись о применённой миграции схемы
type SchemaMigrationRecord struct {
    ID          string `json:"id"`
    Version     string `json:"version"`
    Description string `json:"description"`
    AppliedAt   int64  `json:"applied_at"`
    Success     bool   `json:"success"`
    Error       string `json:"error,omitempty"`
}

// DocumentMigrationStrategy стратегия миграции документа
type DocumentMigrationStrategy int

const (
    StrategyInPlace   DocumentMigrationStrategy = iota // Обновление на месте
    StrategyCopyNew                                    // Копирование в новую коллекцию
    StrategyLazy                                       // Ленивая миграция при доступе
)

// SchemaMigrator управляет миграциями схемы данных
type SchemaMigrator struct {
    store          *storage.Storage
    logger         *log.Logger
    migrations     map[string]*SchemaMigration
    applied        map[string]*SchemaMigrationRecord
    mu             sync.RWMutex
    migrationDir   string
    currentVersion string
    targetVersion  string
    schema         *SchemaDefinition
    strategy       DocumentMigrationStrategy
    migrationStats *MigrationStats
}

// MigrationStats статистика миграции
type MigrationStats struct {
    TotalDocuments    int64
    MigratedDocuments int64
    FailedDocuments   int64
    SkippedDocuments  int64
    StartTime         int64
    EndTime           int64
    mu                sync.RWMutex
}

// NewSchemaMigrator создаёт новый мигратор схемы
func NewSchemaMigrator(store *storage.Storage, logger *log.Logger, migrationDir string) *SchemaMigrator {
    sm := &SchemaMigrator{
        store:          store,
        logger:         logger,
        migrations:     make(map[string]*SchemaMigration),
        applied:        make(map[string]*SchemaMigrationRecord),
        migrationDir:   migrationDir,
        strategy:       StrategyLazy, // Ленивая миграция по умолчанию
        migrationStats: &MigrationStats{},
        schema: &SchemaDefinition{
            Version:     "1.0.0",
            Collections: make(map[string]*CollectionSchema),
            UpdatedAt:   time.Now().UnixMilli(),
        },
    }
    
    // Создаём директорию для миграций
    os.MkdirAll(migrationDir, 0755)
    
    // Загружаем существующую схему
    sm.loadSchema()
    
    // Загружаем применённые миграции
    sm.loadAppliedMigrations()
    
    // Регистрируем встроенные миграции схемы
    sm.registerBuiltinSchemaMigrations()
    
    return sm
}

// loadSchema загружает определение схемы из файла
func (sm *SchemaMigrator) loadSchema() {
    path := filepath.Join(sm.migrationDir, "schema.json")
    
    data, err := os.ReadFile(path)
    if err != nil {
        if sm.logger != nil {
            sm.logger.Debug("No existing schema found, creating default schema")
        }
        sm.createDefaultSchema()
        return
    }
    
    var schema SchemaDefinition
    if err := json.Unmarshal(data, &schema); err != nil {
        if sm.logger != nil {
            sm.logger.Error(fmt.Sprintf("Failed to unmarshal schema: %v", err))
        }
        sm.createDefaultSchema()
        return
    }
    
    sm.mu.Lock()
    sm.schema = &schema
    sm.currentVersion = schema.Version
    sm.mu.Unlock()
    
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Loaded schema version %s with %d collections", schema.Version, len(schema.Collections)))
    }
}

// saveSchema сохраняет определение схемы
func (sm *SchemaMigrator) saveSchema() error {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    data, err := json.MarshalIndent(sm.schema, "", "  ")
    if err != nil {
        return err
    }
    
    path := filepath.Join(sm.migrationDir, "schema.json")
    return os.WriteFile(path, data, 0644)
}

// createDefaultSchema создаёт схему по умолчанию
func (sm *SchemaMigrator) createDefaultSchema() {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    
    sm.schema = &SchemaDefinition{
        Version:     "1.0.0",
        Collections: make(map[string]*CollectionSchema),
        UpdatedAt:   time.Now().UnixMilli(),
    }
    
    // Добавляем базовую схему для всех коллекций
    sm.schema.Collections["_default"] = &CollectionSchema{
        Fields: map[string]*FieldDefinition{
            "_id": {
                Type:     "string",
                Required: true,
            },
            "created_at": {
                Type:    "int64",
                Default: int64(0),
            },
            "updated_at": {
                Type:    "int64",
                Default: int64(0),
            },
        },
        Required:  []string{"_id"},
        Indexes:   []IndexDefinition{},
        UpdatedAt: time.Now().UnixMilli(),
    }
    
    sm.saveSchema()
}

// registerBuiltinSchemaMigrations регистрирует встроенные миграции схемы
func (sm *SchemaMigrator) registerBuiltinSchemaMigrations() {
    // Миграция v1.0.0 -> v1.1.0: Добавление обязательных полей created_at/updated_at
    sm.RegisterSchemaMigration(&SchemaMigration{
        ID:          "schema_001_add_timestamps",
        Version:     "1.1.0",
        Description: "Add created_at and updated_at timestamps to all collections",
        Up: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaAddTimestamps(tx, schema)
        },
        Down: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaRemoveTimestamps(tx, schema)
        },
        CreatedAt: time.Now().UnixMilli(),
    })
    
    // Миграция v1.1.0 -> v1.2.0: Добавление мягкого удаления
    sm.RegisterSchemaMigration(&SchemaMigration{
        ID:          "schema_002_add_soft_delete",
        Version:     "1.2.0",
        Description: "Add soft delete support (deleted_at, deleted fields)",
        Up: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaAddSoftDelete(tx, schema)
        },
        Down: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaRemoveSoftDelete(tx, schema)
        },
        CreatedAt: time.Now().UnixMilli(),
    })
    
    // Миграция v1.2.0 -> v1.3.0: Добавление версионирования
    sm.RegisterSchemaMigration(&SchemaMigration{
        ID:          "schema_003_add_versioning",
        Version:     "1.3.0",
        Description: "Add document versioning (_version field)",
        Up: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaAddVersioning(tx, schema)
        },
        Down: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaRemoveVersioning(tx, schema)
        },
        CreatedAt: time.Now().UnixMilli(),
    })
    
    // Миграция v1.3.0 -> v2.0.0: Добавление индексов
    sm.RegisterSchemaMigration(&SchemaMigration{
        ID:          "schema_004_add_indexes",
        Version:     "2.0.0",
        Description: "Add secondary indexes for performance",
        Up: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaAddIndexes(tx, schema)
        },
        Down: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaRemoveIndexes(tx, schema)
        },
        CreatedAt: time.Now().UnixMilli(),
    })
    
    // Миграция v2.0.0 -> v2.1.0: Добавление валидации полей
    sm.RegisterSchemaMigration(&SchemaMigration{
        ID:          "schema_005_add_validation",
        Version:     "2.1.0",
        Description: "Add field validation rules",
        Up: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaAddValidation(tx, schema)
        },
        Down: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return sm.migrateSchemaRemoveValidation(tx, schema)
        },
        CreatedAt: time.Now().UnixMilli(),
    })
}

// RegisterSchemaMigration регистрирует миграцию схемы
func (sm *SchemaMigrator) RegisterSchemaMigration(m *SchemaMigration) {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    sm.migrations[m.ID] = m
}

// loadAppliedMigrations загружает применённые миграции
func (sm *SchemaMigrator) loadAppliedMigrations() {
    path := filepath.Join(sm.migrationDir, "schema_migrations.json")
    
    data, err := os.ReadFile(path)
    if err != nil {
        return
    }
    
    var records []SchemaMigrationRecord
    if err := json.Unmarshal(data, &records); err != nil {
        if sm.logger != nil {
            sm.logger.Error(fmt.Sprintf("Failed to unmarshal schema migrations: %v", err))
        }
        return
    }
    
    sm.mu.Lock()
    defer sm.mu.Unlock()
    
    for _, record := range records {
        sm.applied[record.ID] = &record
    }
}

// saveAppliedMigrations сохраняет применённые миграции
func (sm *SchemaMigrator) saveAppliedMigrations() error {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    records := make([]SchemaMigrationRecord, 0, len(sm.applied))
    for _, record := range sm.applied {
        records = append(records, *record)
    }
    
    data, err := json.MarshalIndent(records, "", "  ")
    if err != nil {
        return err
    }
    
    path := filepath.Join(sm.migrationDir, "schema_migrations.json")
    return os.WriteFile(path, data, 0644)
}

// MigrateSchema выполняет миграцию схемы до указанной версии
func (sm *SchemaMigrator) MigrateSchema(targetVersion string) error {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Starting schema migration to version %s", targetVersion))
    }
    
    // Получаем отсортированный список миграций
    migrations := sm.getSortedSchemaMigrations()
    
    currentVersion := sm.schema.Version
    
    if currentVersion == targetVersion {
        if sm.logger != nil {
            sm.logger.Info("Already at target version, no migration needed")
        }
        return nil
    }
    
    var toApply []*SchemaMigration
    
    for _, m := range migrations {
        if sm.isVersionGreater(m.Version, currentVersion) && !sm.isVersionGreater(m.Version, targetVersion) {
            if _, applied := sm.applied[m.ID]; !applied {
                toApply = append(toApply, m)
            }
        }
    }
    
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Applying %d schema migrations", len(toApply)))
    }
    
    // Начинаем транзакцию для миграции
    tx := storage.BeginTransaction()
    defer func() {
        if r := recover(); r != nil {
            storage.AbortCurrentTransaction()
            if sm.logger != nil {
                sm.logger.Error(fmt.Sprintf("Panic during schema migration: %v", r))
            }
        }
    }()
    
    for _, m := range toApply {
        if sm.logger != nil {
            sm.logger.Info(fmt.Sprintf("Applying schema migration: %s (%s)", m.ID, m.Description))
        }
        
        startTime := time.Now()
        
        // Применяем миграцию
        if err := m.Up(tx, sm.schema); err != nil {
            storage.AbortCurrentTransaction()
            return fmt.Errorf("schema migration %s failed: %v", m.ID, err)
        }
        
        // Обновляем версию схемы
        sm.schema.Version = m.Version
        sm.schema.UpdatedAt = time.Now().UnixMilli()
        
        // Сохраняем запись
        record := &SchemaMigrationRecord{
            ID:          m.ID,
            Version:     m.Version,
            Description: m.Description,
            AppliedAt:   time.Now().UnixMilli(),
            Success:     true,
        }
        sm.applied[m.ID] = record
        
        duration := time.Since(startTime)
        if sm.logger != nil {
            sm.logger.Info(fmt.Sprintf("Schema migration %s completed in %v", m.ID, duration))
        }
    }
    
    // Сохраняем схему
    if err := sm.saveSchema(); err != nil {
        storage.AbortCurrentTransaction()
        return fmt.Errorf("failed to save schema: %v", err)
    }
    
    // Коммитим транзакцию
    if err := storage.CommitCurrentTransaction(); err != nil {
        return fmt.Errorf("failed to commit schema migration: %v", err)
    }
    
    sm.saveAppliedMigrations()
    sm.currentVersion = targetVersion
    
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Schema migration to version %s completed successfully", targetVersion))
    }
    
    return nil
}

// MigrateDocument прозрачно мигрирует отдельный документ к актуальной схеме
func (sm *SchemaMigrator) MigrateDocument(doc *storage.Document, collectionName string) (*storage.Document, error) {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    // Получаем схему для коллекции
    collectionSchema, exists := sm.schema.Collections[collectionName]
    if !exists {
        // Используем схему по умолчанию
        collectionSchema = sm.schema.Collections["_default"]
        if collectionSchema == nil {
            return doc, nil
        }
    }
    
    // Клонируем документ
    migratedDoc := doc.Clone()
    changed := false
    
    // Проверяем и добавляем недостающие поля с значениями по умолчанию
    for fieldName, fieldDef := range collectionSchema.Fields {
        if _, err := migratedDoc.GetField(fieldName); err != nil {
            if fieldDef.Default != nil {
                migratedDoc.SetField(fieldName, fieldDef.Default)
                changed = true
                if sm.logger != nil {
                    sm.logger.Debug(fmt.Sprintf("Added default value for field %s in document %s", fieldName, doc.ID))
                }
            }
        }
    }
    
    // Проверяем типы полей и преобразуем при необходимости
    for fieldName, fieldDef := range collectionSchema.Fields {
        if value, err := migratedDoc.GetField(fieldName); err == nil {
            converted, needsConversion := sm.convertFieldType(value, fieldDef.Type)
            if needsConversion {
                migratedDoc.SetField(fieldName, converted)
                changed = true
                if sm.logger != nil {
                    sm.logger.Debug(fmt.Sprintf("Converted field %s type for document %s", fieldName, doc.ID))
                }
            }
        }
    }
    
    if changed {
        migratedDoc.UpdatedAt = time.Now().UnixMilli()
        migratedDoc.Version++
        sm.migrationStats.mu.Lock()
        sm.migrationStats.MigratedDocuments++
        sm.migrationStats.mu.Unlock()
    }
    
    return migratedDoc, nil
}

// MigrateCollectionDocuments прозрачно мигрирует все документы коллекции
func (sm *SchemaMigrator) MigrateCollectionDocuments(collection *storage.Collection) error {
    startTime := time.Now()
    
    sm.migrationStats.mu.Lock()
    sm.migrationStats.StartTime = startTime.UnixMilli()
    sm.migrationStats.TotalDocuments = collection.Count()
    sm.migrationStats.MigratedDocuments = 0
    sm.migrationStats.FailedDocuments = 0
    sm.migrationStats.SkippedDocuments = 0
    sm.migrationStats.mu.Unlock()
    
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Starting migration of collection %s with %d documents", 
            collection.Name(), collection.Count()))
    }
    
    docs := collection.GetAllDocuments()
    
    for _, doc := range docs {
        migratedDoc, err := sm.MigrateDocument(doc, collection.Name())
        if err != nil {
            sm.migrationStats.mu.Lock()
            sm.migrationStats.FailedDocuments++
            sm.migrationStats.mu.Unlock()
            if sm.logger != nil {
                sm.logger.Error(fmt.Sprintf("Failed to migrate document %s: %v", doc.ID, err))
            }
            continue
        }
        
        if migratedDoc != doc {
            // Обновляем документ в коллекции
            updates := migratedDoc.GetFields()
            if err := collection.Update(doc.ID, updates); err != nil {
                sm.migrationStats.mu.Lock()
                sm.migrationStats.FailedDocuments++
                sm.migrationStats.mu.Unlock()
                if sm.logger != nil {
                    sm.logger.Error(fmt.Sprintf("Failed to update migrated document %s: %v", doc.ID, err))
                }
            }
        } else {
            sm.migrationStats.mu.Lock()
            sm.migrationStats.SkippedDocuments++
            sm.migrationStats.mu.Unlock()
        }
    }
    
    sm.migrationStats.mu.Lock()
    sm.migrationStats.EndTime = time.Now().UnixMilli()
    sm.migrationStats.mu.Unlock()
    
    duration := time.Since(startTime)
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Migration of collection %s completed in %v: %d migrated, %d failed, %d skipped",
            collection.Name(), duration, 
            sm.migrationStats.MigratedDocuments, 
            sm.migrationStats.FailedDocuments, 
            sm.migrationStats.SkippedDocuments))
    }
    
    return nil
}

// convertFieldType преобразует значение поля к нужному типу
func (sm *SchemaMigrator) convertFieldType(value interface{}, targetType string) (interface{}, bool) {
    switch targetType {
    case "string":
        if _, ok := value.(string); !ok {
            return fmt.Sprintf("%v", value), true
        }
    case "int64":
        switch v := value.(type) {
        case int:
            return int64(v), true
        case float64:
            return int64(v), true
        case string:
            var i int64
            if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
                return i, true
            }
        }
    case "float64":
        switch v := value.(type) {
        case int:
            return float64(v), true
        case int64:
            return float64(v), true
        case string:
            var f float64
            if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
                return f, true
            }
        }
    case "bool":
        if _, ok := value.(bool); !ok {
            if str, ok := value.(string); ok {
                return strings.ToLower(str) == "true" || str == "1", true
            }
            return false, true
        }
    }
    return value, false
}

// getSortedSchemaMigrations возвращает отсортированный список миграций
func (sm *SchemaMigrator) getSortedSchemaMigrations() []*SchemaMigration {
    migrations := make([]*SchemaMigration, 0, len(sm.migrations))
    for _, m := range sm.migrations {
        migrations = append(migrations, m)
    }
    
    sort.Slice(migrations, func(i, j int) bool {
        return sm.isVersionLess(migrations[i].Version, migrations[j].Version)
    })
    
    return migrations
}

// isVersionLess сравнивает версии
func (sm *SchemaMigrator) isVersionLess(v1, v2 string) bool {
    return sm.versionToInt(v1) < sm.versionToInt(v2)
}

// isVersionGreater сравнивает версии
func (sm *SchemaMigrator) isVersionGreater(v1, v2 string) bool {
    return sm.versionToInt(v1) > sm.versionToInt(v2)
}

// versionToInt преобразует версию в число
func (sm *SchemaMigrator) versionToInt(version string) int64 {
    var major, minor, patch int
    fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch)
    return int64(major*1000000 + minor*1000 + patch)
}

// ========== Конкретные реализации миграций схемы ==========

// migrateSchemaAddTimestamps добавляет поля created_at и updated_at
func (sm *SchemaMigrator) migrateSchemaAddTimestamps(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        if _, exists := collSchema.Fields["created_at"]; !exists {
            collSchema.Fields["created_at"] = &FieldDefinition{
                Type:    "int64",
                Required: false,
                Default: int64(0),
            }
        }
        if _, exists := collSchema.Fields["updated_at"]; !exists {
            collSchema.Fields["updated_at"] = &FieldDefinition{
                Type:    "int64",
                Required: false,
                Default: int64(0),
            }
        }
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaRemoveTimestamps удаляет поля created_at и updated_at
func (sm *SchemaMigrator) migrateSchemaRemoveTimestamps(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        delete(collSchema.Fields, "created_at")
        delete(collSchema.Fields, "updated_at")
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaAddSoftDelete добавляет поддержку мягкого удаления
func (sm *SchemaMigrator) migrateSchemaAddSoftDelete(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        if _, exists := collSchema.Fields["deleted_at"]; !exists {
            collSchema.Fields["deleted_at"] = &FieldDefinition{
                Type:    "int64",
                Required: false,
                Default: int64(0),
            }
        }
        if _, exists := collSchema.Fields["deleted"]; !exists {
            collSchema.Fields["deleted"] = &FieldDefinition{
                Type:    "bool",
                Required: false,
                Default: false,
            }
        }
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaRemoveSoftDelete удаляет поддержку мягкого удаления
func (sm *SchemaMigrator) migrateSchemaRemoveSoftDelete(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        delete(collSchema.Fields, "deleted_at")
        delete(collSchema.Fields, "deleted")
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaAddVersioning добавляет версионирование
func (sm *SchemaMigrator) migrateSchemaAddVersioning(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        if _, exists := collSchema.Fields["_version"]; !exists {
            collSchema.Fields["_version"] = &FieldDefinition{
                Type:    "int64",
                Required: false,
                Default: int64(1),
            }
        }
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaRemoveVersioning удаляет версионирование
func (sm *SchemaMigrator) migrateSchemaRemoveVersioning(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        delete(collSchema.Fields, "_version")
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaAddIndexes добавляет индексы
func (sm *SchemaMigrator) migrateSchemaAddIndexes(tx *storage.Transaction, schema *SchemaDefinition) error {
    for collectionName, collSchema := range schema.Collections {
        // Добавляем индексы для часто используемых полей
        defaultIndexes := []IndexDefinition{
            {Name: "idx_created_at", Fields: []string{"created_at"}, Unique: false},
            {Name: "idx_updated_at", Fields: []string{"updated_at"}, Unique: false},
        }
        
        for _, idx := range defaultIndexes {
            found := false
            for _, existing := range collSchema.Indexes {
                if existing.Name == idx.Name {
                    found = true
                    break
                }
            }
            if !found {
                collSchema.Indexes = append(collSchema.Indexes, idx)
            }
        }
        collSchema.UpdatedAt = time.Now().UnixMilli()
        
        if sm.logger != nil {
            sm.logger.Debug(fmt.Sprintf("Added indexes to collection %s", collectionName))
        }
    }
    return nil
}

// migrateSchemaRemoveIndexes удаляет индексы
func (sm *SchemaMigrator) migrateSchemaRemoveIndexes(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        // Оставляем только пользовательские индексы, удаляем автоматические
        newIndexes := make([]IndexDefinition, 0)
        for _, idx := range collSchema.Indexes {
            if !strings.HasPrefix(idx.Name, "idx_") {
                newIndexes = append(newIndexes, idx)
            }
        }
        collSchema.Indexes = newIndexes
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaAddValidation добавляет валидацию полей
func (sm *SchemaMigrator) migrateSchemaAddValidation(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        for fieldName, fieldDef := range collSchema.Fields {
            // Добавляем базовую валидацию для обязательных полей
            if fieldName == "_id" {
                fieldDef.Required = true
            }
        }
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// migrateSchemaRemoveValidation удаляет валидацию
func (sm *SchemaMigrator) migrateSchemaRemoveValidation(tx *storage.Transaction, schema *SchemaDefinition) error {
    for _, collSchema := range schema.Collections {
        for _, fieldDef := range collSchema.Fields {
            fieldDef.Required = false
            fieldDef.Validate = ""
            fieldDef.ValidateValue = nil
        }
        collSchema.UpdatedAt = time.Now().UnixMilli()
    }
    return nil
}

// ========== Публичные методы для управления схемой ==========

// AddCollectionSchema добавляет схему для новой коллекции
func (sm *SchemaMigrator) AddCollectionSchema(collectionName string, schema *CollectionSchema) error {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    
    sm.schema.Collections[collectionName] = schema
    sm.schema.UpdatedAt = time.Now().UnixMilli()
    
    return sm.saveSchema()
}

// GetCollectionSchema возвращает схему коллекции
func (sm *SchemaMigrator) GetCollectionSchema(collectionName string) *CollectionSchema {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    if schema, exists := sm.schema.Collections[collectionName]; exists {
        return schema
    }
    return sm.schema.Collections["_default"]
}

// UpdateFieldSchema обновляет схему поля
func (sm *SchemaMigrator) UpdateFieldSchema(collectionName, fieldName string, fieldDef *FieldDefinition) error {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    
    collSchema, exists := sm.schema.Collections[collectionName]
    if !exists {
        collSchema = &CollectionSchema{
            Fields:    make(map[string]*FieldDefinition),
            Required:  make([]string, 0),
            Indexes:   make([]IndexDefinition, 0),
            UpdatedAt: time.Now().UnixMilli(),
        }
        sm.schema.Collections[collectionName] = collSchema
    }
    
    collSchema.Fields[fieldName] = fieldDef
    collSchema.UpdatedAt = time.Now().UnixMilli()
    sm.schema.UpdatedAt = time.Now().UnixMilli()
    
    return sm.saveSchema()
}

// GetSchema возвращает текущую схему
func (sm *SchemaMigrator) GetSchema() *SchemaDefinition {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    // Возвращаем копию
    data, _ := json.Marshal(sm.schema)
    var copy SchemaDefinition
    json.Unmarshal(data, &copy)
    return &copy
}

// GetSchemaVersion возвращает текущую версию схемы
func (sm *SchemaMigrator) GetSchemaVersion() string {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    return sm.schema.Version
}

// GetMigrationStats возвращает статистику миграции
func (sm *SchemaMigrator) GetMigrationStats() *MigrationStats {
    sm.migrationStats.mu.RLock()
    defer sm.migrationStats.mu.RUnlock()
    
    stats := &MigrationStats{
        TotalDocuments:    sm.migrationStats.TotalDocuments,
        MigratedDocuments: sm.migrationStats.MigratedDocuments,
        FailedDocuments:   sm.migrationStats.FailedDocuments,
        SkippedDocuments:  sm.migrationStats.SkippedDocuments,
        StartTime:         sm.migrationStats.StartTime,
        EndTime:           sm.migrationStats.EndTime,
    }
    return stats
}

// SetMigrationStrategy устанавливает стратегию миграции
func (sm *SchemaMigrator) SetMigrationStrategy(strategy DocumentMigrationStrategy) {
    sm.mu.Lock()
    defer sm.mu.Unlock()
    sm.strategy = strategy
    
    if sm.logger != nil {
        strategyName := "InPlace"
        if strategy == StrategyCopyNew {
            strategyName = "CopyNew"
        } else if strategy == StrategyLazy {
            strategyName = "Lazy"
        }
        sm.logger.Info(fmt.Sprintf("Migration strategy set to %s", strategyName))
    }
}

// ValidateDocumentAgainstSchema проверяет документ на соответствие схеме
func (sm *SchemaMigrator) ValidateDocumentAgainstSchema(doc *storage.Document, collectionName string) error {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    collSchema, exists := sm.schema.Collections[collectionName]
    if !exists {
        collSchema = sm.schema.Collections["_default"]
    }
    
    // Проверяем обязательные поля
    for _, requiredField := range collSchema.Required {
        if _, err := doc.GetField(requiredField); err != nil {
            return fmt.Errorf("required field '%s' is missing", requiredField)
        }
    }
    
    // Проверяем типы полей
    for fieldName, fieldDef := range collSchema.Fields {
        if value, err := doc.GetField(fieldName); err == nil {
            if err := sm.validateFieldType(value, fieldDef.Type); err != nil {
                return fmt.Errorf("field '%s' validation failed: %v", fieldName, err)
            }
            
            // Дополнительная валидация
            if fieldDef.Validate != "" {
                if err := sm.validateFieldValue(value, fieldDef); err != nil {
                    return fmt.Errorf("field '%s' value validation failed: %v", fieldName, err)
                }
            }
        }
    }
    
    return nil
}

// validateFieldType проверяет тип поля
func (sm *SchemaMigrator) validateFieldType(value interface{}, expectedType string) error {
    switch expectedType {
    case "string":
        if _, ok := value.(string); !ok {
            return fmt.Errorf("expected string, got %T", value)
        }
    case "int64":
        switch value.(type) {
        case int, int64, float64:
            // допустимые типы
        default:
            return fmt.Errorf("expected int64, got %T", value)
        }
    case "float64":
        switch value.(type) {
        case float64, int, int64:
            // допустимые типы
        default:
            return fmt.Errorf("expected float64, got %T", value)
        }
    case "bool":
        if _, ok := value.(bool); !ok {
            return fmt.Errorf("expected bool, got %T", value)
        }
    case "array":
        // проверка на слайс
    case "object":
        // проверка на map
    }
    return nil
}

// validateFieldValue проверяет значение поля по правилам валидации
func (sm *SchemaMigrator) validateFieldValue(value interface{}, fieldDef *FieldDefinition) error {
    switch fieldDef.Validate {
    case "min":
        if min, ok := fieldDef.ValidateValue.(float64); ok {
            switch v := value.(type) {
            case float64:
                if v < min {
                    return fmt.Errorf("value %.2f is less than minimum %.2f", v, min)
                }
            case int64:
                if float64(v) < min {
                    return fmt.Errorf("value %d is less than minimum %.2f", v, min)
                }
            }
        }
    case "max":
        if max, ok := fieldDef.ValidateValue.(float64); ok {
            switch v := value.(type) {
            case float64:
                if v > max {
                    return fmt.Errorf("value %.2f exceeds maximum %.2f", v, max)
                }
            case int64:
                if float64(v) > max {
                    return fmt.Errorf("value %d exceeds maximum %.2f", v, max)
                }
            }
        }
    case "enum":
        if enumVals, ok := fieldDef.ValidateValue.([]interface{}); ok {
            found := false
            for _, ev := range enumVals {
                if fmt.Sprintf("%v", value) == fmt.Sprintf("%v", ev) {
                    found = true
                    break
                }
            }
            if !found {
                return fmt.Errorf("value %v not in enum list", value)
            }
        }
    case "regex":
        if pattern, ok := fieldDef.ValidateValue.(string); ok {
            if str, ok := value.(string); ok {
                // Простая проверка (в реальности использовать regexp)
                if !strings.Contains(str, pattern) {
                    return fmt.Errorf("value '%s' does not match pattern '%s'", str, pattern)
                }
            }
        }
    }
    return nil
}

// GetSchemaStatus возвращает статус схемы
func (sm *SchemaMigrator) GetSchemaStatus() *SchemaStatus {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    status := &SchemaStatus{
        CurrentVersion:    sm.schema.Version,
        TotalCollections:  len(sm.schema.Collections),
        TotalMigrations:   len(sm.migrations),
        AppliedMigrations: len(sm.applied),
        PendingMigrations: len(sm.migrations) - len(sm.applied),
        UpdatedAt:         sm.schema.UpdatedAt,
    }
    
    for name, collSchema := range sm.schema.Collections {
        status.Collections = append(status.Collections, CollectionSchemaInfo{
            Name:         name,
            FieldsCount:  len(collSchema.Fields),
            IndexesCount: len(collSchema.Indexes),
            UpdatedAt:    collSchema.UpdatedAt,
        })
    }
    
    return status
}

// SchemaStatus статус схемы
type SchemaStatus struct {
    CurrentVersion     string                  `json:"current_version"`
    TotalCollections   int                     `json:"total_collections"`
    TotalMigrations    int                     `json:"total_migrations"`
    AppliedMigrations  int                     `json:"applied_migrations"`
    PendingMigrations  int                     `json:"pending_migrations"`
    UpdatedAt          int64                   `json:"updated_at"`
    Collections        []CollectionSchemaInfo  `json:"collections"`
}

// CollectionSchemaInfo информация о схеме коллекции
type CollectionSchemaInfo struct {
    Name         string `json:"name"`
    FieldsCount  int    `json:"fields_count"`
    IndexesCount int    `json:"indexes_count"`
    UpdatedAt    int64  `json:"updated_at"`
}

// MigrationStatusItem элемент статуса миграции (совместимость со старым кодом)
type MigrationStatusItem struct {
    ID          string `json:"id"`
    Version     string `json:"version"`
    Description string `json:"description"`
    Applied     bool   `json:"applied"`
    AppliedAt   int64  `json:"applied_at,omitempty"`
    CreatedAt   int64  `json:"created_at"`
    Success     bool   `json:"success"`
}

// MigrationStatus статус миграций (совместимость со старым кодом)
type MigrationStatus struct {
    CurrentVersion     string                 `json:"current_version"`
    TotalMigrations    int                    `json:"total_migrations"`
    AppliedMigrations  int                    `json:"applied_migrations"`
    PendingMigrations  int                    `json:"pending_migrations"`
    Migrations         []MigrationStatusItem  `json:"migrations"`
}

// GetStatus возвращает статус миграций (для совместимости со старым кодом)
func (sm *SchemaMigrator) GetStatus() *MigrationStatus {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    
    status := &MigrationStatus{
        CurrentVersion:    sm.schema.Version,
        TotalMigrations:   len(sm.migrations),
        AppliedMigrations: len(sm.applied),
        PendingMigrations: len(sm.migrations) - len(sm.applied),
        Migrations:        make([]MigrationStatusItem, 0),
    }
    
    for _, m := range sm.getSortedSchemaMigrations() {
        item := MigrationStatusItem{
            ID:          m.ID,
            Version:     m.Version,
            Description: m.Description,
            CreatedAt:   m.CreatedAt,
        }
        
        if record, ok := sm.applied[m.ID]; ok {
            item.Applied = true
            item.AppliedAt = record.AppliedAt
            item.Success = record.Success
        } else {
            item.Applied = false
        }
        
        status.Migrations = append(status.Migrations, item)
    }
    
    return status
}

// ========== Совместимость со старым кодом ==========

// Migration (старая структура для совместимости)
type Migration struct {
    ID          string
    Version     string
    Description string
    Up          func(tx *storage.Transaction) error
    Down        func(tx *storage.Transaction) error
    CreatedAt   int64
    AppliedAt   int64
}

// MigrationRecord (старая структура для совместимости)
type MigrationRecord struct {
    ID          string `json:"id"`
    Version     string `json:"version"`
    Description string `json:"description"`
    AppliedAt   int64  `json:"applied_at"`
    Success     bool   `json:"success"`
    Error       string `json:"error,omitempty"`
}

// RegisterMigration регистрирует миграцию (старый интерфейс)
func (sm *SchemaMigrator) RegisterMigration(m *Migration) {
    // Конвертируем в новую структуру
    schemaMig := &SchemaMigration{
        ID:          m.ID,
        Version:     m.Version,
        Description: m.Description,
        Up: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return m.Up(tx)
        },
        Down: func(tx *storage.Transaction, schema *SchemaDefinition) error {
            return m.Down(tx)
        },
        CreatedAt: m.CreatedAt,
    }
    sm.RegisterSchemaMigration(schemaMig)
}

// Migrate выполняет миграцию (старый интерфейс)
func (sm *SchemaMigrator) Migrate(targetVersion string) error {
    return sm.MigrateSchema(targetVersion)
}
