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
	Fields    map[string]*FieldDefinition `json:"fields"`
	Required  []string                    `json:"required"`
	Indexes   []IndexDefinition           `json:"indexes"`
	UpdatedAt int64                       `json:"updated_at"`
}

// FieldDefinition определяет поле в схеме
type FieldDefinition struct {
	Type          string      `json:"type"` // string, int, float, bool, array, object
	Required      bool        `json:"required"`
	Default       interface{} `json:"default"`
	Validate      string      `json:"validate"` // regex, min, max, enum
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
	StrategyInPlace DocumentMigrationStrategy = iota // Обновление на месте
	StrategyCopyNew                                  // Копирование в новую коллекцию
	StrategyLazy                                     // Ленивая миграция при доступе
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
		strategy:       StrategyLazy,
		migrationStats: &MigrationStats{},
		schema: &SchemaDefinition{
			Version:     "1.0.0",
			Collections: make(map[string]*CollectionSchema),
			UpdatedAt:   time.Now().UnixMilli(),
		},
	}

	// Создаём директорию для миграций (без паники при ошибке)
	if err := os.MkdirAll(migrationDir, 0755); err != nil {
		if logger != nil {
			logger.Error(fmt.Sprintf("Failed to create migration directory %s: %v", migrationDir, err))
		}
	}

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

	if schema.Collections == nil {
		schema.Collections = make(map[string]*CollectionSchema)
	}
	if schema.Version == "" {
		schema.Version = "1.0.0"
	}

	sm.mu.Lock()
	sm.schema = &schema
	sm.currentVersion = schema.Version
	sm.mu.Unlock()

	if sm.logger != nil {
		sm.logger.Info(fmt.Sprintf("Loaded schema version %s with %d collections", schema.Version, len(schema.Collections)))
	}
}

// saveSchema сохраняет определение схемы атомарно (через временный файл + rename).
//
// ВАЖНО: метод НЕ берёт sm.mu, чтобы его можно было вызывать из функций,
// уже удерживающих sm.mu.Lock(). Вызывающий сам отвечает за консистентность
// читаемого состояния sm.schema.
func (sm *SchemaMigrator) saveSchemaLocked() error {
	data, err := json.MarshalIndent(sm.schema, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(sm.migrationDir, "schema.json")
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// saveSchema — публичная обёртка, берущая RLock.
func (sm *SchemaMigrator) saveSchema() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.saveSchemaLocked()
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

	sm.currentVersion = "1.0.0"
	_ = sm.saveSchemaLocked()
}

// registerBuiltinSchemaMigrations регистрирует встроенные миграции схемы
func (sm *SchemaMigrator) registerBuiltinSchemaMigrations() {
	sm.RegisterSchemaMigration(&SchemaMigration{
		ID:          "schema_001_add_timestamps",
		Version:     "1.1.0",
		Description: "Add created_at and updated_at timestamps to all collections",
		Up:          sm.migrateSchemaAddTimestamps,
		Down:        sm.migrateSchemaRemoveTimestamps,
		CreatedAt:   time.Now().UnixMilli(),
	})

	sm.RegisterSchemaMigration(&SchemaMigration{
		ID:          "schema_002_add_soft_delete",
		Version:     "1.2.0",
		Description: "Add soft delete support (deleted_at, deleted fields)",
		Up:          sm.migrateSchemaAddSoftDelete,
		Down:        sm.migrateSchemaRemoveSoftDelete,
		CreatedAt:   time.Now().UnixMilli(),
	})

	sm.RegisterSchemaMigration(&SchemaMigration{
		ID:          "schema_003_add_versioning",
		Version:     "1.3.0",
		Description: "Add document versioning (_version field)",
		Up:          sm.migrateSchemaAddVersioning,
		Down:        sm.migrateSchemaRemoveVersioning,
		CreatedAt:   time.Now().UnixMilli(),
	})

	sm.RegisterSchemaMigration(&SchemaMigration{
		ID:          "schema_004_add_indexes",
		Version:     "2.0.0",
		Description: "Add secondary indexes for performance",
		Up:          sm.migrateSchemaAddIndexes,
		Down:        sm.migrateSchemaRemoveIndexes,
		CreatedAt:   time.Now().UnixMilli(),
	})

	sm.RegisterSchemaMigration(&SchemaMigration{
		ID:          "schema_005_add_validation",
		Version:     "2.1.0",
		Description: "Add field validation rules",
		Up:          sm.migrateSchemaAddValidation,
		Down:        sm.migrateSchemaRemoveValidation,
		CreatedAt:   time.Now().UnixMilli(),
	})
}

// RegisterSchemaMigration регистрирует миграцию схемы
func (sm *SchemaMigrator) RegisterSchemaMigration(m *SchemaMigration) {
	if m == nil || m.ID == "" {
		return
	}
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

	for i := range records {
		sm.applied[records[i].ID] = &records[i]
	}
}

// saveAppliedMigrationsLocked сохраняет применённые миграции.
// Не берёт sm.mu (вызывающий уже держит Lock).
func (sm *SchemaMigrator) saveAppliedMigrationsLocked() error {
	records := make([]SchemaMigrationRecord, 0, len(sm.applied))
	for _, record := range sm.applied {
		records = append(records, *record)
	}

	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(sm.migrationDir, "schema_migrations.json")
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// MigrateSchema выполняет миграцию схемы до указанной версии.
//
// ВАЖНО: этот метод НЕ удерживает sm.mu на всё время выполнения,
// чтобы избежать deadlock с saveSchema/saveAppliedMigrations и
// чтобы не блокировать чтение схемы на время долгих миграций.
// Лок берётся только для доступа к картам migrations/applied и к sm.schema.
func (sm *SchemaMigrator) MigrateSchema(targetVersion string) (err error) {
	if targetVersion == "" {
		return fmt.Errorf("target version is empty")
	}

	if sm.logger != nil {
		sm.logger.Info(fmt.Sprintf("Starting schema migration to version %s", targetVersion))
	}

	// Снимок текущего состояния под RLock
	sm.mu.RLock()
	currentVersion := sm.schema.Version
	allMigrations := make([]*SchemaMigration, 0, len(sm.migrations))
	for _, m := range sm.migrations {
		allMigrations = append(allMigrations, m)
	}
	appliedCopy := make(map[string]bool, len(sm.applied))
	for id := range sm.applied {
		appliedCopy[id] = true
	}
	sm.mu.RUnlock()

	if currentVersion == targetVersion {
		if sm.logger != nil {
			sm.logger.Info("Already at target version, no migration needed")
		}
		return nil
	}

	// Сортируем по версии
	sort.Slice(allMigrations, func(i, j int) bool {
		return sm.isVersionLess(allMigrations[i].Version, allMigrations[j].Version)
	})

	// Отбираем миграции для применения
	var toApply []*SchemaMigration
	for _, m := range allMigrations {
		if sm.isVersionGreater(m.Version, currentVersion) && !sm.isVersionGreater(m.Version, targetVersion) {
			if !appliedCopy[m.ID] {
				toApply = append(toApply, m)
			}
		}
	}

	if sm.logger != nil {
		sm.logger.Info(fmt.Sprintf("Applying %d schema migrations", len(toApply)))
	}

	// Если targetVersion меньше currentVersion — это откат, который не поддерживается.
	if len(toApply) == 0 && sm.isVersionGreater(currentVersion, targetVersion) {
		return fmt.Errorf("downgrade from %s to %s is not supported", currentVersion, targetVersion)
	}

	// Копируем схему, чтобы изменения были атомарны: применяем к копии,
	// а в sm.schema записываем только после успешного коммита.
	sm.mu.RLock()
	schemaCopy := sm.deepCopySchemaLocked()
	sm.mu.RUnlock()

	if schemaCopy == nil {
		return fmt.Errorf("failed to copy schema")
	}

	// Начинаем транзакцию
	tx := storage.BeginTransaction()
	if tx == nil {
		return fmt.Errorf("failed to begin transaction")
	}

	// Обработка паники с именованным возвратом err
	defer func() {
		if r := recover(); r != nil {
			storage.AbortCurrentTransaction()
			if sm.logger != nil {
				sm.logger.Error(fmt.Sprintf("Panic during schema migration: %v", r))
			}
			err = fmt.Errorf("panic during schema migration: %v", r)
		}
	}()

	// Новые applied-записи, которые закоммитим в sm.applied только при успехе
	newApplied := make(map[string]*SchemaMigrationRecord)

	for _, m := range toApply {
		if sm.logger != nil {
			sm.logger.Info(fmt.Sprintf("Applying schema migration: %s (%s)", m.ID, m.Description))
		}

		startTime := time.Now()

		if m.Up == nil {
			storage.AbortCurrentTransaction()
			return fmt.Errorf("migration %s has nil Up function", m.ID)
		}

		if err := m.Up(tx, schemaCopy); err != nil {
			storage.AbortCurrentTransaction()
			return fmt.Errorf("schema migration %s failed: %v", m.ID, err)
		}

		schemaCopy.Version = m.Version
		schemaCopy.UpdatedAt = time.Now().UnixMilli()

		newApplied[m.ID] = &SchemaMigrationRecord{
			ID:          m.ID,
			Version:     m.Version,
			Description: m.Description,
			AppliedAt:   time.Now().UnixMilli(),
			Success:     true,
		}

		if sm.logger != nil {
			sm.logger.Info(fmt.Sprintf("Schema migration %s completed in %v", m.ID, time.Since(startTime)))
		}
	}

	// Коммитим транзакцию ДО записи на диск
	if err := storage.CommitCurrentTransaction(); err != nil {
		return fmt.Errorf("failed to commit schema migration: %v", err)
	}

	// Только теперь применяем изменения в память
	sm.mu.Lock()
	sm.schema = schemaCopy
	for id, rec := range newApplied {
		sm.applied[id] = rec
	}
	sm.currentVersion = targetVersion

	// Сохраняем на диск (под Lock, вызывая Locked-версии)
	if err := sm.saveSchemaLocked(); err != nil {
		sm.mu.Unlock()
		return fmt.Errorf("failed to save schema: %v", err)
	}
	if err := sm.saveAppliedMigrationsLocked(); err != nil {
		sm.mu.Unlock()
		return fmt.Errorf("failed to save applied migrations: %v", err)
	}
	sm.mu.Unlock()

	if sm.logger != nil {
		sm.logger.Info(fmt.Sprintf("Schema migration to version %s completed successfully", targetVersion))
	}

	return nil
}

// deepCopySchemaLocked создаёт глубокую копию sm.schema через JSON.
// Вызывающий должен удерживать sm.mu (RLock).
func (sm *SchemaMigrator) deepCopySchemaLocked() *SchemaDefinition {
	if sm.schema == nil {
		return nil
	}
	data, err := json.Marshal(sm.schema)
	if err != nil {
		return nil
	}
	var copy SchemaDefinition
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil
	}
	if copy.Collections == nil {
		copy.Collections = make(map[string]*CollectionSchema)
	}
	return &copy
}

// MigrateDocument прозрачно мигрирует отдельный документ к актуальной схеме.
// Возвращает копию документа и флаг changed (были ли изменения).
func (sm *SchemaMigrator) MigrateDocument(doc *storage.Document, collectionName string) (*storage.Document, bool, error) {
	if doc == nil {
		return nil, false, fmt.Errorf("nil document")
	}

	sm.mu.RLock()
	collSchema, exists := sm.schema.Collections[collectionName]
	if !exists {
		collSchema = sm.schema.Collections["_default"]
	}
	// Копируем схему, чтобы не держать RLock во время работы с документом
	schemaCopy := copyCollectionSchema(collSchema)
	sm.mu.RUnlock()

	if schemaCopy == nil {
		// Нет схемы — возвращаем документ без изменений
		return doc.Clone(), false, nil
	}

	migratedDoc := doc.Clone()
	changed := false

	// Добавляем недостающие поля с значениями по умолчанию
	for fieldName, fieldDef := range schemaCopy.Fields {
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
	for fieldName, fieldDef := range schemaCopy.Fields {
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
	}

	return migratedDoc, changed, nil
}

// copyCollectionSchema создаёт глубокую копию CollectionSchema через JSON.
func copyCollectionSchema(src *CollectionSchema) *CollectionSchema {
	if src == nil {
		return nil
	}
	data, err := json.Marshal(src)
	if err != nil {
		return nil
	}
	var copy CollectionSchema
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil
	}
	if copy.Fields == nil {
		copy.Fields = make(map[string]*FieldDefinition)
	}
	return &copy
}

// MigrateCollectionDocuments прозрачно мигрирует все документы коллекции
func (sm *SchemaMigrator) MigrateCollectionDocuments(collection *storage.Collection) error {
	if collection == nil {
		return fmt.Errorf("nil collection")
	}

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
		migratedDoc, changed, err := sm.MigrateDocument(doc, collection.Name())
		if err != nil {
			sm.migrationStats.mu.Lock()
			sm.migrationStats.FailedDocuments++
			sm.migrationStats.mu.Unlock()
			if sm.logger != nil {
				sm.logger.Error(fmt.Sprintf("Failed to migrate document %s: %v", doc.ID, err))
			}
			continue
		}

		if !changed {
			sm.migrationStats.mu.Lock()
			sm.migrationStats.SkippedDocuments++
			sm.migrationStats.mu.Unlock()
			continue
		}

		updates := migratedDoc.GetFields()
		if err := collection.Update(doc.ID, updates); err != nil {
			sm.migrationStats.mu.Lock()
			sm.migrationStats.FailedDocuments++
			sm.migrationStats.mu.Unlock()
			if sm.logger != nil {
				sm.logger.Error(fmt.Sprintf("Failed to update migrated document %s: %v", doc.ID, err))
			}
			continue
		}

		sm.migrationStats.mu.Lock()
		sm.migrationStats.MigratedDocuments++
		sm.migrationStats.mu.Unlock()
	}

	sm.migrationStats.mu.Lock()
	sm.migrationStats.EndTime = time.Now().UnixMilli()
	total := sm.migrationStats.TotalDocuments
	migrated := sm.migrationStats.MigratedDocuments
	failed := sm.migrationStats.FailedDocuments
	skipped := sm.migrationStats.SkippedDocuments
	sm.migrationStats.mu.Unlock()

	if sm.logger != nil {
		sm.logger.Info(fmt.Sprintf("Migration of collection %s completed in %v: %d migrated, %d failed, %d skipped (total %d)",
			collection.Name(), time.Since(startTime), migrated, failed, skipped, total))
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
		case int32:
			return int64(v), true
		case int64:
			// уже нужный тип
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
		case int32:
			return float64(v), true
		case int64:
			return float64(v), true
		case float32:
			return float64(v), true
		case float64:
			// уже нужный тип
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

// getSortedSchemaMigrations возвращает отсортированный список миграций.
// НЕ берёт sm.mu — вызывающий должен обеспечить консистентность
// (например, вызвать под sm.mu.RLock или использовать снимок).
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
	return compareVersions(v1, v2) < 0
}

// isVersionGreater сравнивает версии
func (sm *SchemaMigrator) isVersionGreater(v1, v2 string) bool {
	return compareVersions(v1, v2) > 0
}

// versionToInt преобразует версию в число (для совместимости).
// ВНИМАНИЕ: могут быть коллизии (1.2.1000 == 1.3.0). Для сравнения
// используется compareVersions.
func (sm *SchemaMigrator) versionToInt(version string) int64 {
	major, minor, patch := parseVersion(version)
	return int64(major)*1_000_000_000 + int64(minor)*1_000_000 + int64(patch)
}

// parseVersion разбирает строку версии вида "X.Y.Z" (или "X.Y", или "X").
// Отсутствующие компоненты считаются нулями. Невалидные компоненты — 0.
func parseVersion(version string) (int, int, int) {
	parts := strings.Split(version, ".")
	var major, minor, patch int
	if len(parts) > 0 {
		fmt.Sscanf(parts[0], "%d", &major)
	}
	if len(parts) > 1 {
		fmt.Sscanf(parts[1], "%d", &minor)
	}
	if len(parts) > 2 {
		fmt.Sscanf(parts[2], "%d", &patch)
	}
	return major, minor, patch
}

// compareVersions сравнивает две версии покомпонентно.
// Возвращает -1, 0, 1. Отсутствующие компоненты считаются нулями,
// что устраняет проблему "1.2" vs "1.2.0" (они равны).
func compareVersions(v1, v2 string) int {
	major1, minor1, patch1 := parseVersion(v1)
	major2, minor2, patch2 := parseVersion(v2)

	if major1 != major2 {
		if major1 < major2 {
			return -1
		}
		return 1
	}
	if minor1 != minor2 {
		if minor1 < minor2 {
			return -1
		}
		return 1
	}
	if patch1 != patch2 {
		if patch1 < patch2 {
			return -1
		}
		return 1
	}
	return 0
}

// ========== Конкретные реализации миграций схемы ==========

// migrateSchemaAddTimestamps добавляет поля created_at и updated_at
func (sm *SchemaMigrator) migrateSchemaAddTimestamps(tx *storage.Transaction, schema *SchemaDefinition) error {
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		if collSchema.Fields == nil {
			collSchema.Fields = make(map[string]*FieldDefinition)
		}
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
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		delete(collSchema.Fields, "created_at")
		delete(collSchema.Fields, "updated_at")
		collSchema.UpdatedAt = time.Now().UnixMilli()
	}
	return nil
}

// migrateSchemaAddSoftDelete добавляет поддержку мягкого удаления
func (sm *SchemaMigrator) migrateSchemaAddSoftDelete(tx *storage.Transaction, schema *SchemaDefinition) error {
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		if collSchema.Fields == nil {
			collSchema.Fields = make(map[string]*FieldDefinition)
		}
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
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		delete(collSchema.Fields, "deleted_at")
		delete(collSchema.Fields, "deleted")
		collSchema.UpdatedAt = time.Now().UnixMilli()
	}
	return nil
}

// migrateSchemaAddVersioning добавляет версионирование
func (sm *SchemaMigrator) migrateSchemaAddVersioning(tx *storage.Transaction, schema *SchemaDefinition) error {
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		if collSchema.Fields == nil {
			collSchema.Fields = make(map[string]*FieldDefinition)
		}
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
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		delete(collSchema.Fields, "_version")
		collSchema.UpdatedAt = time.Now().UnixMilli()
	}
	return nil
}

// migrateSchemaAddIndexes добавляет индексы
func (sm *SchemaMigrator) migrateSchemaAddIndexes(tx *storage.Transaction, schema *SchemaDefinition) error {
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for collectionName, collSchema := range schema.Collections {
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
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
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
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		if fieldDef, ok := collSchema.Fields["_id"]; ok && fieldDef != nil {
			fieldDef.Required = true
		}
		collSchema.UpdatedAt = time.Now().UnixMilli()
	}
	return nil
}

// migrateSchemaRemoveValidation удаляет валидацию
func (sm *SchemaMigrator) migrateSchemaRemoveValidation(tx *storage.Transaction, schema *SchemaDefinition) error {
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	for _, collSchema := range schema.Collections {
		for _, fieldDef := range collSchema.Fields {
			if fieldDef == nil {
				continue
			}
			fieldDef.Required = false
			fieldDef.Validate = ""
			fieldDef.ValidateValue = nil
		}
		collSchema.UpdatedAt = time.Now().UnixMilli()
	}
	return nil
}

// ========== Публичные методы для управления схемой ==========

// AddCollectionSchema добавляет схему для новой коллекции.
//
// Изменение сохраняется в отдельной копии, чтобы избежать deadlock
// с saveSchemaLocked (вызывается под Lock).
func (sm *SchemaMigrator) AddCollectionSchema(collectionName string, schema *CollectionSchema) error {
	if collectionName == "" {
		return fmt.Errorf("empty collection name")
	}
	if schema == nil {
		return fmt.Errorf("nil schema")
	}
	if schema.Fields == nil {
		schema.Fields = make(map[string]*FieldDefinition)
	}
	if schema.Indexes == nil {
		schema.Indexes = make([]IndexDefinition, 0)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.schema.Collections == nil {
		sm.schema.Collections = make(map[string]*CollectionSchema)
	}
	sm.schema.Collections[collectionName] = schema
	sm.schema.UpdatedAt = time.Now().UnixMilli()

	return sm.saveSchemaLocked()
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
	if collectionName == "" || fieldName == "" || fieldDef == nil {
		return fmt.Errorf("invalid arguments")
	}

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
	if collSchema.Fields == nil {
		collSchema.Fields = make(map[string]*FieldDefinition)
	}

	collSchema.Fields[fieldName] = fieldDef
	collSchema.UpdatedAt = time.Now().UnixMilli()
	sm.schema.UpdatedAt = time.Now().UnixMilli()

	return sm.saveSchemaLocked()
}

// GetSchema возвращает текущую схему (копию)
func (sm *SchemaMigrator) GetSchema() *SchemaDefinition {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.schema == nil {
		return &SchemaDefinition{
			Version:     "1.0.0",
			Collections: make(map[string]*CollectionSchema),
		}
	}

	copySchema := sm.deepCopySchemaLocked()
	if copySchema == nil {
		return &SchemaDefinition{
			Version:     sm.schema.Version,
			Collections: make(map[string]*CollectionSchema),
		}
	}
	return copySchema
}

// GetSchemaVersion возвращает текущую версию схемы
func (sm *SchemaMigrator) GetSchemaVersion() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.schema == nil {
		return "1.0.0"
	}
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
	if doc == nil {
		return fmt.Errorf("nil document")
	}

	sm.mu.RLock()
	collSchema, exists := sm.schema.Collections[collectionName]
	if !exists {
		collSchema = sm.schema.Collections["_default"]
	}
	schemaCopy := copyCollectionSchema(collSchema)
	sm.mu.RUnlock()

	if schemaCopy == nil {
		return nil
	}

	for _, requiredField := range schemaCopy.Required {
		if _, err := doc.GetField(requiredField); err != nil {
			return fmt.Errorf("required field '%s' is missing", requiredField)
		}
	}

	for fieldName, fieldDef := range schemaCopy.Fields {
		if fieldDef == nil {
			continue
		}
		if value, err := doc.GetField(fieldName); err == nil {
			if err := sm.validateFieldType(value, fieldDef.Type); err != nil {
				return fmt.Errorf("field '%s' validation failed: %v", fieldName, err)
			}
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
		case int, int32, int64, float64:
			// допустимые типы
		default:
			return fmt.Errorf("expected int64, got %T", value)
		}
	case "float64":
		switch value.(type) {
		case float64, float32, int, int32, int64:
			// допустимые типы
		default:
			return fmt.Errorf("expected float64, got %T", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case "array", "object":
		// без строгой проверки
	}
	return nil
}

// validateFieldValue проверяет значение поля по правилам валидации
func (sm *SchemaMigrator) validateFieldValue(value interface{}, fieldDef *FieldDefinition) error {
	switch fieldDef.Validate {
	case "min":
		if min, ok := toFloat64(fieldDef.ValidateValue); ok {
			if v, ok := toFloat64(value); ok && v < min {
				return fmt.Errorf("value %.2f is less than minimum %.2f", v, min)
			}
		}
	case "max":
		if max, ok := toFloat64(fieldDef.ValidateValue); ok {
			if v, ok := toFloat64(value); ok && v > max {
				return fmt.Errorf("value %.2f exceeds maximum %.2f", v, max)
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
				if !strings.Contains(str, pattern) {
					return fmt.Errorf("value '%s' does not match pattern '%s'", str, pattern)
				}
			}
		}
	}
	return nil
}

// toFloat64 пытается привести значение к float64.
func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
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
		Collections:       make([]CollectionSchemaInfo, 0, len(sm.schema.Collections)),
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
	CurrentVersion    string                 `json:"current_version"`
	TotalCollections  int                    `json:"total_collections"`
	TotalMigrations   int                    `json:"total_migrations"`
	AppliedMigrations int                    `json:"applied_migrations"`
	PendingMigrations int                    `json:"pending_migrations"`
	UpdatedAt         int64                  `json:"updated_at"`
	Collections       []CollectionSchemaInfo `json:"collections"`
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
	CurrentVersion    string                `json:"current_version"`
	TotalMigrations   int                   `json:"total_migrations"`
	AppliedMigrations int                   `json:"applied_migrations"`
	PendingMigrations int                   `json:"pending_migrations"`
	Migrations        []MigrationStatusItem `json:"migrations"`
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

// RegisterMigration регистрирует миграцию (старый интерфейс).
// Безопасно обрабатывает nil Up/Down.
func (sm *SchemaMigrator) RegisterMigration(m *Migration) {
	if m == nil {
		return
	}
	schemaMig := &SchemaMigration{
		ID:          m.ID,
		Version:     m.Version,
		Description: m.Description,
		CreatedAt:   m.CreatedAt,
	}
	if m.Up != nil {
		up := m.Up
		schemaMig.Up = func(tx *storage.Transaction, schema *SchemaDefinition) error {
			return up(tx)
		}
	}
	if m.Down != nil {
		down := m.Down
		schemaMig.Down = func(tx *storage.Transaction, schema *SchemaDefinition) error {
			return down(tx)
		}
	}
	sm.RegisterSchemaMigration(schemaMig)
}

// Migrate выполняет миграцию (старый интерфейс)
func (sm *SchemaMigrator) Migrate(targetVersion string) error {
	return sm.MigrateSchema(targetVersion)
}
