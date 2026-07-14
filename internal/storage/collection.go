/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/collection.go
// Назначение: Реализация коллекции с индексами (первичными и вторичными).
// Индексы хранятся отдельно от документов, обеспечивают wait-free доступ.
// Исправлено: корректная работа уникальных индексов, удаление из индексов при обновлении.
// Lock-free: ACL и Constraints переведены на атомарные операции.
// MVCC: добавлена поддержка многоверсионности.

package storage

import (
    "fmt"
    "sync"
    "sync/atomic"
    "time"
    "strings"

    "futriis/internal/serializer"
)

// Collection представляет коллекцию документов (аналог таблицы)
type Collection struct {
    dbName      string                // имя базы данных
    name        string                // имя коллекции
    docs        sync.Map              // map[string]*Document - wait-free хранилище документов
    indexes     sync.Map              // map[string]*Index - индексы для быстрого поиска (lock-free)
    metadata    *CollectionMetadata   // Метаданные коллекции
    docCount    atomic.Int64          // Атомарный счётчик документов
    sizeBytes   atomic.Int64          // Атомарный размер коллекции в байтах
    mu          sync.RWMutex          // Для операций, изменяющих структуру коллекции
    constraints *Constraints          // Ограничения коллекции (lock-free)
    acl         *CollectionACL        // ACL для коллекции (lock-free)

    // MVCC - многоверсионность
    mvccManager *MVCCManager          // MVCC менеджер для управления версиями
    mvccEnabled bool                  // Включен ли MVCC
}

// CollectionMetadata содержит метаданные коллекции
type CollectionMetadata struct {
    Name          string              `msgpack:"name"`
    CreatedAt     int64               `msgpack:"created_at"`
    UpdatedAt     int64               `msgpack:"updated_at"`
    DeletedAt     int64               `msgpack:"deleted_at"`
    DocumentCount int64               `msgpack:"document_count"`
    SizeBytes     int64               `msgpack:"size_bytes"`
    IndexCount    int                 `msgpack:"index_count"`
    Settings      *CollectionSettings `msgpack:"settings"`
}

// CollectionSettings содержит настройки коллекции
type CollectionSettings struct {
    MaxDocuments    int  `msgpack:"max_documents"`     // Максимальное количество документов (0 = безлимит)
    ValidateSchema  bool `msgpack:"validate_schema"`   // Валидировать схему документов
    AutoIndexID     bool `msgpack:"auto_index_id"`     // Автоматически индексировать поле _id
    TTLSeconds      int  `msgpack:"ttl_seconds"`       // Время жизни документов (0 = бессрочно)
    SoftDelete      bool `msgpack:"soft_delete"`       // Мягкое удаление документов
    MVCCEnabled     bool `msgpack:"mvcc_enabled"`      // Включить MVCC
    MaxVersions     int  `msgpack:"max_versions"`      // Максимальное количество версий на документ
}

// Index представляет индекс для ускорения поиска (хранится отдельно от документов)
type Index struct {
    Name      string                 `msgpack:"name"`
    Fields    []string               `msgpack:"fields"`     // Поля для индексации
    Unique    bool                   `msgpack:"unique"`     // Уникальный индекс
    CreatedAt int64                  `msgpack:"created_at"` // Время создания индекса
    UpdatedAt int64                  `msgpack:"updated_at"` // Время последнего обновления индекса
    data      sync.Map               // map[interface{}]string - значение индекса -> ID документа (lock-free)
}

// Constraints представляет ограничения на коллекцию (lock-free версия)
type Constraints struct {
    // Атомарные указатели для lock-free доступа к данным ограничений
    requiredFieldsPtr atomic.Value   // map[string]bool
    uniqueFieldsPtr   atomic.Value   // map[string]bool
    minValuesPtr      atomic.Value   // map[string]float64
    maxValuesPtr      atomic.Value   // map[string]float64
    patternFieldsPtr  atomic.Value   // map[string]string
    enumFieldsPtr     atomic.Value   // map[string][]interface{}

    createdAt        int64
    updatedAt        atomic.Int64
    constraintHistory atomic.Value   // []ConstraintChange
}

// ConstraintChange представляет изменение ограничения
type ConstraintChange struct {
    Timestamp      int64                  `msgpack:"timestamp"`
    TimestampStr   string                 `msgpack:"timestamp_str"`
    Action         string                 `msgpack:"action"`   // ADD, REMOVE, MODIFY
    ConstraintType string                 `msgpack:"constraint_type"` // required, unique, min, max, enum, regex
    Field          string                 `msgpack:"field"`
    OldValue       interface{}            `msgpack:"old_value,omitempty"`
    NewValue       interface{}            `msgpack:"new_value,omitempty"`
}

// CollectionACL представляет список контроля доступа для коллекции (lock-free версия)
type CollectionACL struct {
    // Атомарные указатели для lock-free доступа к ACL
    readRolesPtr   atomic.Value   // map[string]bool
    writeRolesPtr  atomic.Value   // map[string]bool
    deleteRolesPtr atomic.Value   // map[string]bool
    adminRolesPtr  atomic.Value   // map[string]bool

    createdAt int64
    updatedAt atomic.Int64
    aclHistory atomic.Value   // []ACLChange
}

// ACLChange представляет изменение ACL
type ACLChange struct {
    Timestamp    int64                  `msgpack:"timestamp"`
    TimestampStr string                 `msgpack:"timestamp_str"`
    Action       string                 `msgpack:"action"`   // GRANT, REVOKE, SET
    Role         string                 `msgpack:"role"`
    Permission   string                 `msgpack:"permission"` // read, write, delete, admin
    Granted      bool                   `msgpack:"granted"`
}

// NewConstraints создаёт новый экземпляр Constraints с lock-free реализацией
func NewConstraints() *Constraints {
    c := &Constraints{
        createdAt: time.Now().UnixMilli(),
    }

    // Инициализация атомарных указателей с пустыми map
    c.requiredFieldsPtr.Store(make(map[string]bool))
    c.uniqueFieldsPtr.Store(make(map[string]bool))
    c.minValuesPtr.Store(make(map[string]float64))
    c.maxValuesPtr.Store(make(map[string]float64))
    c.patternFieldsPtr.Store(make(map[string]string))
    c.enumFieldsPtr.Store(make(map[string][]interface{}))
    c.constraintHistory.Store(make([]ConstraintChange, 0))
    c.updatedAt.Store(c.createdAt)

    return c
}

// loadRequiredFields загружает карту обязательных полей (lock-free)
func (cons *Constraints) loadRequiredFields() map[string]bool {
    val := cons.requiredFieldsPtr.Load()
    if val == nil {
        return make(map[string]bool)
    }
    return val.(map[string]bool)
}

// storeRequiredFields сохраняет карту обязательных полей (lock-free)
func (cons *Constraints) storeRequiredFields(newMap map[string]bool) {
    cons.requiredFieldsPtr.Store(newMap)
}

// loadUniqueFields загружает карту уникальных полей (lock-free)
func (cons *Constraints) loadUniqueFields() map[string]bool {
    val := cons.uniqueFieldsPtr.Load()
    if val == nil {
        return make(map[string]bool)
    }
    return val.(map[string]bool)
}

// storeUniqueFields сохраняет карту уникальных полей (lock-free)
func (cons *Constraints) storeUniqueFields(newMap map[string]bool) {
    cons.uniqueFieldsPtr.Store(newMap)
}

// loadMinValues загружает карту минимальных значений (lock-free)
func (cons *Constraints) loadMinValues() map[string]float64 {
    val := cons.minValuesPtr.Load()
    if val == nil {
        return make(map[string]float64)
    }
    return val.(map[string]float64)
}

// storeMinValues сохраняет карту минимальных значений (lock-free)
func (cons *Constraints) storeMinValues(newMap map[string]float64) {
    cons.minValuesPtr.Store(newMap)
}

// loadMaxValues загружает карту максимальных значений (lock-free)
func (cons *Constraints) loadMaxValues() map[string]float64 {
    val := cons.maxValuesPtr.Load()
    if val == nil {
        return make(map[string]float64)
    }
    return val.(map[string]float64)
}

// storeMaxValues сохраняет карту максимальных значений (lock-free)
func (cons *Constraints) storeMaxValues(newMap map[string]float64) {
    cons.maxValuesPtr.Store(newMap)
}

// loadPatternFields загружает карту regex паттернов (lock-free)
func (cons *Constraints) loadPatternFields() map[string]string {
    val := cons.patternFieldsPtr.Load()
    if val == nil {
        return make(map[string]string)
    }
    return val.(map[string]string)
}

// storePatternFields сохраняет карту regex паттернов (lock-free)
func (cons *Constraints) storePatternFields(newMap map[string]string) {
    cons.patternFieldsPtr.Store(newMap)
}

// loadEnumFields загружает карту enum ограничений (lock-free)
func (cons *Constraints) loadEnumFields() map[string][]interface{} {
    val := cons.enumFieldsPtr.Load()
    if val == nil {
        return make(map[string][]interface{})
    }
    // Возвращаем копию, чтобы избежать модификации
    original := val.(map[string][]interface{})
    result := make(map[string][]interface{})
    for k, v := range original {
        copied := make([]interface{}, len(v))
        copy(copied, v)
        result[k] = copied
    }
    return result
}

// storeEnumFields сохраняет карту enum ограничений (lock-free)
func (cons *Constraints) storeEnumFields(newMap map[string][]interface{}) {
    cons.enumFieldsPtr.Store(newMap)
}

// loadConstraintHistory загружает историю изменений (lock-free)
func (cons *Constraints) loadConstraintHistory() []ConstraintChange {
    val := cons.constraintHistory.Load()
    if val == nil {
        return make([]ConstraintChange, 0)
    }
    return val.([]ConstraintChange)
}

// storeConstraintHistory сохраняет историю изменений (lock-free)
func (cons *Constraints) storeConstraintHistory(history []ConstraintChange) {
    cons.constraintHistory.Store(history)
}

// NewCollectionACL создаёт новый экземпляр CollectionACL с lock-free реализацией
func NewCollectionACL() *CollectionACL {
    acl := &CollectionACL{
        createdAt: time.Now().UnixMilli(),
    }

    acl.readRolesPtr.Store(make(map[string]bool))
    acl.writeRolesPtr.Store(make(map[string]bool))
    acl.deleteRolesPtr.Store(make(map[string]bool))
    acl.adminRolesPtr.Store(make(map[string]bool))
    acl.aclHistory.Store(make([]ACLChange, 0))
    acl.updatedAt.Store(acl.createdAt)

    return acl
}

// loadReadRoles загружает роли чтения (lock-free)
func (acl *CollectionACL) loadReadRoles() map[string]bool {
    val := acl.readRolesPtr.Load()
    if val == nil {
        return make(map[string]bool)
    }
    return val.(map[string]bool)
}

// storeReadRoles сохраняет роли чтения (lock-free)
func (acl *CollectionACL) storeReadRoles(roles map[string]bool) {
    acl.readRolesPtr.Store(roles)
}

// loadWriteRoles загружает роли записи (lock-free)
func (acl *CollectionACL) loadWriteRoles() map[string]bool {
    val := acl.writeRolesPtr.Load()
    if val == nil {
        return make(map[string]bool)
    }
    return val.(map[string]bool)
}

// storeWriteRoles сохраняет роли записи (lock-free)
func (acl *CollectionACL) storeWriteRoles(roles map[string]bool) {
    acl.writeRolesPtr.Store(roles)
}

// loadDeleteRoles загружает роли удаления (lock-free)
func (acl *CollectionACL) loadDeleteRoles() map[string]bool {
    val := acl.deleteRolesPtr.Load()
    if val == nil {
        return make(map[string]bool)
    }
    return val.(map[string]bool)
}

// storeDeleteRoles сохраняет роли удаления (lock-free)
func (acl *CollectionACL) storeDeleteRoles(roles map[string]bool) {
    acl.deleteRolesPtr.Store(roles)
}

// loadAdminRoles загружает административные роли (lock-free)
func (acl *CollectionACL) loadAdminRoles() map[string]bool {
    val := acl.adminRolesPtr.Load()
    if val == nil {
        return make(map[string]bool)
    }
    return val.(map[string]bool)
}

// storeAdminRoles сохраняет административные роли (lock-free)
func (acl *CollectionACL) storeAdminRoles(roles map[string]bool) {
    acl.adminRolesPtr.Store(roles)
}

// loadACLHistory загружает историю ACL (lock-free)
func (acl *CollectionACL) loadACLHistory() []ACLChange {
    val := acl.aclHistory.Load()
    if val == nil {
        return make([]ACLChange, 0)
    }
    return val.([]ACLChange)
}

// storeACLHistory сохраняет историю ACL (lock-free)
func (acl *CollectionACL) storeACLHistory(history []ACLChange) {
    acl.aclHistory.Store(history)
}

// CheckPermission проверяет наличие разрешения у роли (lock-free)
func (acl *CollectionACL) CheckPermission(role, operation string) bool {
    // Администратор имеет все права (lock-free чтение)
    adminRoles := acl.loadAdminRoles()
    if adminRoles[role] {
        return true
    }

    switch operation {
    case "read":
        readRoles := acl.loadReadRoles()
        return readRoles[role]
    case "write":
        writeRoles := acl.loadWriteRoles()
        return writeRoles[role]
    case "delete":
        deleteRoles := acl.loadDeleteRoles()
        return deleteRoles[role]
    default:
        return false
    }
}

// NewCollection создаёт новую коллекцию
func NewCollection(dbName, name string, settings *CollectionSettings) *Collection {
    if settings == nil {
        settings = &CollectionSettings{
            MaxDocuments:   0,
            ValidateSchema: false,
            AutoIndexID:    true,
            TTLSeconds:     0,
            SoftDelete:     false,
            MVCCEnabled:    false,
            MaxVersions:    MaxVersionsPerDoc,
        }
    }

    now := time.Now().UnixMilli()
    coll := &Collection{
        dbName: dbName,
        name:   name,
        metadata: &CollectionMetadata{
            Name:          name,
            CreatedAt:     now,
            UpdatedAt:     now,
            DeletedAt:     0,
            DocumentCount: 0,
            SizeBytes:     0,
            IndexCount:    0,
            Settings:      settings,
        },
        constraints: NewConstraints(),
        acl:         NewCollectionACL(),
        mvccEnabled: settings.MVCCEnabled,
    }

    // Создаём MVCC менеджер, если включен
    if settings.MVCCEnabled {
        maxVersions := settings.MaxVersions
        if maxVersions <= 0 {
            maxVersions = MaxVersionsPerDoc
        }
        coll.mvccManager = NewMVCCManager(maxVersions, VersionRetentionDays)
    }

    // Автоматически создаём первичный индекс по _id
    if settings.AutoIndexID {
        coll.CreateIndex("_id_", []string{"_id"}, true)
    }

    // Запускаем фоновую задачу для удаления просроченных документов
    if settings.TTLSeconds > 0 {
        go coll.ttlCleanupLoop()
    }

    // Аудит создания коллекции
    AuditCollectionOperation("CREATE", dbName, name, settings)

    return coll
}

// GetMVCCManager возвращает MVCC менеджер
func (c *Collection) GetMVCCManager() *MVCCManager {
    return c.mvccManager
}

// IsMVCCEnabled проверяет, включен ли MVCC
func (c *Collection) IsMVCCEnabled() bool {
    return c.mvccEnabled
}

// GetVersionAt возвращает версию документа на указанный момент времени
func (c *Collection) GetVersionAt(id string, timestamp int64) (*Document, error) {
    if !c.mvccEnabled || c.mvccManager == nil {
        return nil, fmt.Errorf("MVCC is not enabled for this collection")
    }

    doc := c.mvccManager.GetVersionAt(id, timestamp)
    if doc == nil {
        return nil, fmt.Errorf("version not found for document %s at timestamp %d", id, timestamp)
    }
    return doc, nil
}

// GetAllVersions возвращает все версии документа
func (c *Collection) GetAllVersions(id string) ([]*DocumentVersion, error) {
    if !c.mvccEnabled || c.mvccManager == nil {
        return nil, fmt.Errorf("MVCC is not enabled for this collection")
    }

    versions := c.mvccManager.GetAllVersions(id)
    if versions == nil {
        return nil, fmt.Errorf("document %s not found", id)
    }
    return versions, nil
}

// GetLatestVersion возвращает последнюю версию документа
func (c *Collection) GetLatestVersion(id string) (*Document, error) {
    if !c.mvccEnabled || c.mvccManager == nil {
        return nil, fmt.Errorf("MVCC is not enabled for this collection")
    }

    doc := c.mvccManager.GetLatestVersion(id)
    if doc == nil {
        return nil, fmt.Errorf("document %s not found", id)
    }
    return doc, nil
}

// GetMVCCStats возвращает статистику MVCC
func (c *Collection) GetMVCCStats() map[string]interface{} {
    if !c.mvccEnabled || c.mvccManager == nil {
        return map[string]interface{}{
            "enabled": false,
        }
    }
    stats := c.mvccManager.GetMVCCStats()
    stats["enabled"] = true
    return stats
}

// Name возвращает имя коллекции
func (c *Collection) Name() string {
    return c.name
}

// DBName возвращает имя базы данных
func (c *Collection) DBName() string {
    return c.dbName
}

// Insert вставляет документ в коллекцию (wait-free)
func (c *Collection) Insert(doc *Document) error {
    // Проверка ограничений
    if err := c.constraints.ValidateDocument(doc); err != nil {
        return fmt.Errorf("constraint violation: %v", err)
    }

    // Проверка на максимальное количество документов
    if c.metadata.Settings.MaxDocuments > 0 {
        if c.docCount.Load() >= int64(c.metadata.Settings.MaxDocuments) {
            return fmt.Errorf("collection is full: max documents %d reached", c.metadata.Settings.MaxDocuments)
        }
    }

    // Валидация схемы (если включена)
    if c.metadata.Settings.ValidateSchema {
        if err := c.validateDocument(doc); err != nil {
            return fmt.Errorf("document validation failed: %v", err)
        }
    }

    // Проверка уникальных индексов
    if err := c.checkUniqueConstraints(doc); err != nil {
        return err
    }

    // Обновляем временные метки документа
    doc.CreatedAt = time.Now().UnixMilli()
    doc.UpdatedAt = doc.CreatedAt
    doc.DeletedAt = 0

    // Сериализация для проверки (опционально)
    data, err := serializer.Marshal(doc)
    if err != nil {
        return fmt.Errorf("failed to serialize document: %v", err)
    }

    // Атомарное сохранение документа
    if _, loaded := c.docs.LoadOrStore(doc.ID, doc); loaded {
        return fmt.Errorf("document with id %s already exists", doc.ID)
    }

    // Если MVCC включен, создаём версию
    if c.mvccEnabled && c.mvccManager != nil {
        c.mvccManager.CreateVersion(doc, 0) // 0 = системная транзакция
    }

    // Обновление индексов (wait-free)
    c.updateIndexes(doc, true)

    // Обновление метаданных
    c.docCount.Add(1)
    c.sizeBytes.Add(int64(len(data)))

    c.mu.Lock()
    c.metadata.DocumentCount = c.docCount.Load()
    c.metadata.SizeBytes = c.sizeBytes.Load()
    c.metadata.UpdatedAt = time.Now().UnixMilli()
    c.mu.Unlock()

    // Аудит вставки документа
    AuditDocumentOperation("INSERT", c.dbName, c.name, doc.ID, doc.GetFields())

    return nil
}

// InsertFromMap создаёт и вставляет документ из map
func (c *Collection) InsertFromMap(fields map[string]interface{}) error {
    doc := NewDocument()
    for k, v := range fields {
        doc.SetField(k, v)
    }
    return c.Insert(doc)
}

// Find находит документ по ID (с использованием первичного индекса)
func (c *Collection) Find(id string) (*Document, error) {
    if val, ok := c.docs.Load(id); ok {
        doc := val.(*Document)

        // Проверяем мягкое удаление
        if doc.IsDeleted() && c.metadata.Settings.SoftDelete {
            return nil, fmt.Errorf("document deleted at %s", doc.GetDeletedAtStr())
        }

        // Проверяем, не истёк ли TTL
        if c.metadata.Settings.TTLSeconds > 0 {
            if time.Now().UnixMilli()-doc.CreatedAt > int64(c.metadata.Settings.TTLSeconds*1000) {
                if c.metadata.Settings.SoftDelete {
                    doc.SoftDelete()
                    c.docs.Store(id, doc)
                } else {
                    c.Delete(id)
                }
                return nil, fmt.Errorf("key not found")
            }
        }
        return doc, nil
    }
    return nil, fmt.Errorf("key not found")
}

// FindIncludingDeleted находит документ даже если он мягко удалён
func (c *Collection) FindIncludingDeleted(id string) (*Document, error) {
    if val, ok := c.docs.Load(id); ok {
        return val.(*Document), nil
    }
    return nil, fmt.Errorf("key not found")
}

// compareValues сравнивает два значения с учётом типа
func compareValues(a, b interface{}) bool {
    if a == nil && b == nil {
        return true
    }
    if a == nil || b == nil {
        return false
    }

    // Пробуем прямое сравнение
    if a == b {
        return true
    }

    // Пробуем сравнение через строковое представление для разных типов
    aStr := fmt.Sprintf("%v", a)
    bStr := fmt.Sprintf("%v", b)
    return aStr == bStr
}

// FindByIndex находит документы по значению индексированного поля
func (c *Collection) FindByIndex(indexName string, value interface{}) ([]*Document, error) {
    idxVal, ok := c.indexes.Load(indexName)
    if !ok {
        return nil, fmt.Errorf("index not found: %s", indexName)
    }

    index := idxVal.(*Index)
    docs := make([]*Document, 0)

    if index.Unique {
        // Уникальный индекс возвращает один документ
        if docID, ok := index.data.Load(value); ok {
            if doc, err := c.Find(docID.(string)); err == nil {
                docs = append(docs, doc)
            }
        }
    } else {
        // Для неуникального индекса используем Range с правильным сравнением
        index.data.Range(func(key, val interface{}) bool {
            if compareValues(key, value) {
                if doc, err := c.Find(val.(string)); err == nil {
                    docs = append(docs, doc)
                }
            }
            return true
        })
    }

    return docs, nil
}

// FindByIndexPrefix находит документы по префиксу индекса (для строковых полей)
func (c *Collection) FindByIndexPrefix(indexName string, prefix string) ([]*Document, error) {
    idxVal, ok := c.indexes.Load(indexName)
    if !ok {
        return nil, fmt.Errorf("index not found: %s", indexName)
    }

    index := idxVal.(*Index)
    docs := make([]*Document, 0)

    index.data.Range(func(key, val interface{}) bool {
        if keyStr, ok := key.(string); ok {
            if strings.HasPrefix(keyStr, prefix) {
                if doc, err := c.Find(val.(string)); err == nil {
                    docs = append(docs, doc)
                }
            }
        }
        return true
    })

    return docs, nil
}

// Update обновляет документ по ID
func (c *Collection) Update(id string, updates map[string]interface{}) error {
    val, ok := c.docs.Load(id)
    if !ok {
        return fmt.Errorf("key not found")
    }

    oldDoc := val.(*Document)

    // Проверяем, не удалён ли документ мягко
    if oldDoc.IsDeleted() && c.metadata.Settings.SoftDelete {
        return fmt.Errorf("cannot update deleted document")
    }

    // Создаём копию для проверки уникальности
    newDoc := oldDoc.Clone()
    if err := newDoc.Update(updates); err != nil {
        return err
    }

    // Обновляем временную метку
    newDoc.UpdatedAt = time.Now().UnixMilli()

    // Проверяем ограничения
    if err := c.constraints.ValidateDocument(newDoc); err != nil {
        return fmt.Errorf("constraint violation: %v", err)
    }

    // Проверяем уникальные индексы
    if err := c.checkUniqueConstraintsUpdate(oldDoc, newDoc); err != nil {
        return err
    }

    // Сначала удаляем старые индексы, потом добавляем новые
    c.removeFromIndexes(oldDoc)
    c.addToIndexes(newDoc)

    // Сохраняем обновлённый документ
    c.docs.Store(id, newDoc)

    // Если MVCC включен, создаём версию
    if c.mvccEnabled && c.mvccManager != nil {
        c.mvccManager.CreateVersion(newDoc, 0)
    }

    c.mu.Lock()
    c.metadata.UpdatedAt = time.Now().UnixMilli()
    c.mu.Unlock()

    // Аудит обновления документа
    AuditDocumentOperation("UPDATE", c.dbName, c.name, id, updates)

    return nil
}

// Delete удаляет документ по ID
func (c *Collection) Delete(id string) error {
    val, ok := c.docs.Load(id)
    if !ok {
        return fmt.Errorf("key not found")
    }

    doc := val.(*Document)

    if c.metadata.Settings.SoftDelete {
        // Мягкое удаление
        doc.SoftDelete()
        c.docs.Store(id, doc)

        // Удаляем из индексов при мягком удалении
        c.removeFromIndexes(doc)

        // Аудит мягкого удаления
        AuditDocumentOperation("SOFT_DELETE", c.dbName, c.name, id, map[string]interface{}{
            "deleted_at": doc.DeletedAt,
        })
    } else {
        // Физическое удаление
        // Удаляем из индексов
        c.removeFromIndexes(doc)

        // Удаляем документ
        c.docs.Delete(id)

        // Аудит физического удаления
        AuditDocumentOperation("DELETE", c.dbName, c.name, id, nil)
    }

    // Обновляем метаданные
    c.docCount.Add(-1)
    c.mu.Lock()
    c.metadata.DocumentCount = c.docCount.Load()
    c.metadata.UpdatedAt = time.Now().UnixMilli()
    c.mu.Unlock()

    return nil
}

// PermanentDelete выполняет физическое удаление мягко удалённого документа
func (c *Collection) PermanentDelete(id string) error {
    val, ok := c.docs.Load(id)
    if !ok {
        return fmt.Errorf("key not found")
    }

    doc := val.(*Document)

    if !doc.IsDeleted() {
        return fmt.Errorf("document is not soft deleted, use Delete() instead")
    }

    // Удаляем из индексов
    c.removeFromIndexes(doc)

    // Удаляем документ
    c.docs.Delete(id)

    // Обновляем метаданные
    c.docCount.Add(-1)
    c.mu.Lock()
    c.metadata.DocumentCount = c.docCount.Load()
    c.metadata.UpdatedAt = time.Now().UnixMilli()
    c.mu.Unlock()

    // Аудит физического удаления
    AuditDocumentOperation("PERMANENT_DELETE", c.dbName, c.name, id, nil)

    return nil
}

// RestoreDeleted восстанавливает мягко удалённый документ
func (c *Collection) RestoreDeleted(id string) error {
    val, ok := c.docs.Load(id)
    if !ok {
        return fmt.Errorf("key not found")
    }

    doc := val.(*Document)

    if !doc.IsDeleted() {
        return fmt.Errorf("document is not deleted")
    }

    doc.Restore()

    // Восстанавливаем индексы
    c.addToIndexes(doc)

    c.docs.Store(id, doc)
    c.mu.Lock()
    c.metadata.UpdatedAt = time.Now().UnixMilli()
    c.mu.Unlock()

    // Аудит восстановления
    AuditDocumentOperation("RESTORE", c.dbName, c.name, id, nil)

    return nil
}

// removeFromIndexes удаляет документ из всех индексов (wait-free)
func (c *Collection) removeFromIndexes(doc *Document) {
    c.indexes.Range(func(key, value interface{}) bool {
        index := value.(*Index)
        indexValue := c.extractIndexValue(doc, index.Fields)
        index.data.Delete(indexValue)
        return true
    })
}

// addToIndexes добавляет документ во все индексы (wait-free)
func (c *Collection) addToIndexes(doc *Document) {
    c.indexes.Range(func(key, value interface{}) bool {
        index := value.(*Index)
        indexValue := c.extractIndexValue(doc, index.Fields)
        if index.Unique {
            index.data.LoadOrStore(indexValue, doc.ID)
        } else {
            index.data.Store(indexValue, doc.ID)
        }
        return true
    })
}

// CreateIndex создаёт новый индекс на коллекции
func (c *Collection) CreateIndex(name string, fields []string, unique bool) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if _, exists := c.indexes.Load(name); exists {
        return fmt.Errorf("index %s already exists", name)
    }

    now := time.Now().UnixMilli()
    index := &Index{
        Name:      name,
        Fields:    fields,
        Unique:    unique,
        CreatedAt: now,
        UpdatedAt: now,
    }

    // Строим индекс на существующих документах (wait-free)
    c.docs.Range(func(key, value interface{}) bool {
        doc := value.(*Document)
        // Пропускаем мягко удалённые документы
        if doc.IsDeleted() && c.metadata.Settings.SoftDelete {
            return true
        }
        indexValue := c.extractIndexValue(doc, fields)
        if unique {
            if _, loaded := index.data.LoadOrStore(indexValue, doc.ID); loaded {
                // Найден дубликат - откатываем создание индекса
                c.mu.Unlock()
                return false
            }
        } else {
            index.data.Store(indexValue, doc.ID)
        }
        return true
    })

    c.indexes.Store(name, index)
    c.metadata.IndexCount++

    // Аудит создания индекса
    AuditIndexOperation("CREATE_INDEX", c.dbName, c.name, name, fields, unique)

    return nil
}

// DropIndex удаляет индекс
func (c *Collection) DropIndex(name string) error {
    if _, exists := c.indexes.LoadAndDelete(name); !exists {
        return fmt.Errorf("index not found: %s", name)
    }
    c.metadata.IndexCount--

    // Аудит удаления индекса
    AuditIndexOperation("DROP_INDEX", c.dbName, c.name, name, nil, false)

    return nil
}

// GetIndexes возвращает список всех индексов
func (c *Collection) GetIndexes() []string {
    names := make([]string, 0)
    c.indexes.Range(func(key, value interface{}) bool {
        names = append(names, key.(string))
        return true
    })
    return names
}

// GetIndexesInfo возвращает подробную информацию об индексах (для API)
func (c *Collection) GetIndexesInfo() []map[string]interface{} {
    indexes := make([]map[string]interface{}, 0)
    c.indexes.Range(func(key, value interface{}) bool {
        idx := value.(*Index)
        indexes = append(indexes, map[string]interface{}{
            "name":       idx.Name,
            "fields":     idx.Fields,
            "unique":     idx.Unique,
            "created_at": idx.CreatedAt,
            "updated_at": idx.UpdatedAt,
        })
        return true
    })
    return indexes
}

// extractIndexValue извлекает значение из документа для индексации
func (c *Collection) extractIndexValue(doc *Document, fields []string) interface{} {
    if len(fields) == 1 {
        val, _ := doc.GetField(fields[0])
        return val
    }

    // Составной индекс - возвращаем строковое представление
    parts := make([]string, 0, len(fields))
    for _, field := range fields {
        if val, err := doc.GetField(field); err == nil {
            parts = append(parts, fmt.Sprintf("%v", val))
        } else {
            parts = append(parts, "NULL")
        }
    }
    return strings.Join(parts, "|")
}

// updateIndexes обновляет индексы для документа
func (c *Collection) updateIndexes(doc *Document, add bool) {
    if add {
        c.addToIndexes(doc)
    } else {
        c.removeFromIndexes(doc)
    }
}

// checkUniqueConstraints проверяет уникальные индексы перед вставкой
func (c *Collection) checkUniqueConstraints(doc *Document) error {
    var errs []string

    c.indexes.Range(func(key, value interface{}) bool {
        index := value.(*Index)
        if index.Unique {
            indexValue := c.extractIndexValue(doc, index.Fields)
            if _, exists := index.data.Load(indexValue); exists {
                errs = append(errs, fmt.Sprintf("duplicate key for index %s: %v", index.Name, indexValue))
            }
        }
        return true
    })

    if len(errs) > 0 {
        return fmt.Errorf("%s", strings.Join(errs, "; "))
    }
    return nil
}

// checkUniqueConstraintsUpdate проверяет уникальность при обновлении
func (c *Collection) checkUniqueConstraintsUpdate(oldDoc, newDoc *Document) error {
    var errs []string

    c.indexes.Range(func(key, value interface{}) bool {
        index := value.(*Index)
        if index.Unique {
            oldValue := c.extractIndexValue(oldDoc, index.Fields)
            newValue := c.extractIndexValue(newDoc, index.Fields)

            if fmt.Sprintf("%v", oldValue) != fmt.Sprintf("%v", newValue) {
                if _, exists := index.data.Load(newValue); exists {
                    errs = append(errs, fmt.Sprintf("duplicate key for index %s: %v", index.Name, newValue))
                }
            }
        }
        return true
    })

    if len(errs) > 0 {
        return fmt.Errorf("%s", strings.Join(errs, "; "))
    }
    return nil
}

// validateDocument валидирует документ согласно схеме коллекции
func (c *Collection) validateDocument(doc *Document) error {
    if doc.ID == "" {
        return fmt.Errorf("document must have _id field")
    }
    return nil
}

// ttlCleanupLoop периодически удаляет просроченные документы
func (c *Collection) ttlCleanupLoop() {
    ticker := time.NewTicker(time.Duration(c.metadata.Settings.TTLSeconds/2) * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        now := time.Now().UnixMilli()
        toDelete := make([]string, 0)

        c.docs.Range(func(key, value interface{}) bool {
            doc := value.(*Document)
            if !doc.IsDeleted() && now-doc.CreatedAt > int64(c.metadata.Settings.TTLSeconds*1000) {
                toDelete = append(toDelete, doc.ID)
            }
            return true
        })

        for _, id := range toDelete {
            c.Delete(id)
        }
    }
}

// Count возвращает количество активных документов в коллекции
func (c *Collection) Count() int64 {
    if c.metadata.Settings.SoftDelete {
        // Подсчитываем только не удалённые документы
        count := int64(0)
        c.docs.Range(func(key, value interface{}) bool {
            doc := value.(*Document)
            if !doc.IsDeleted() {
                count++
            }
            return true
        })
        return count
    }
    return c.docCount.Load()
}

// CountAll возвращает общее количество документов (включая мягко удалённые)
func (c *Collection) CountAll() int64 {
    return c.docCount.Load()
}

// CountDeleted возвращает количество мягко удалённых документов
func (c *Collection) CountDeleted() int64 {
    if !c.metadata.Settings.SoftDelete {
        return 0
    }

    count := int64(0)
    c.docs.Range(func(key, value interface{}) bool {
        doc := value.(*Document)
        if doc.IsDeleted() {
            count++
        }
        return true
    })
    return count
}

// Size возвращает размер коллекции в байтах
func (c *Collection) Size() int64 {
    return c.sizeBytes.Load()
}

// GetAllDocuments возвращает все активные документы коллекции
func (c *Collection) GetAllDocuments() []*Document {
    docs := make([]*Document, 0, c.docCount.Load())
    c.docs.Range(func(key, value interface{}) bool {
        doc := value.(*Document)
        if !c.metadata.Settings.SoftDelete || !doc.IsDeleted() {
            docs = append(docs, doc)
        }
        return true
    })
    return docs
}

// GetAllDocumentsIncludingDeleted возвращает все документы (включая мягко удалённые)
func (c *Collection) GetAllDocumentsIncludingDeleted() []*Document {
    docs := make([]*Document, 0, c.docCount.Load())
    c.docs.Range(func(key, value interface{}) bool {
        docs = append(docs, value.(*Document))
        return true
    })
    return docs
}

// FindByFilter находит документы по произвольному фильтру
func (c *Collection) FindByFilter(filter func(*Document) bool) []*Document {
    results := make([]*Document, 0)
    c.docs.Range(func(key, value interface{}) bool {
        doc := value.(*Document)
        if (!c.metadata.Settings.SoftDelete || !doc.IsDeleted()) && filter(doc) {
            results = append(results, doc)
        }
        return true
    })
    return results
}

// GetMetadata возвращает метаданные коллекции
func (c *Collection) GetMetadata() *CollectionMetadata {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.metadata
}

// GetTimestamps возвращает временные метки коллекции
func (c *Collection) GetTimestamps() map[string]int64 {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return map[string]int64{
        "created_at": c.metadata.CreatedAt,
        "updated_at": c.metadata.UpdatedAt,
        "deleted_at": c.metadata.DeletedAt,
    }
}

// Drop удаляет все документы из коллекции
func (c *Collection) Drop() error {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.docs = sync.Map{}
    c.indexes = sync.Map{}

    if c.metadata.Settings.AutoIndexID {
        c.CreateIndex("_id_", []string{"_id"}, true)
    }

    c.docCount.Store(0)
    c.sizeBytes.Store(0)
    c.metadata.DocumentCount = 0
    c.metadata.SizeBytes = 0
    c.metadata.UpdatedAt = time.Now().UnixMilli()

    // Аудит удаления коллекции
    AuditCollectionOperation("DROP", c.dbName, c.name, nil)

    return nil
}

// logConstraintChange логирует изменение ограничения (lock-free)
func (c *Collection) logConstraintChange(action, constraintType, field string, oldVal, newVal interface{}) {
    nowMs, nowStr := GetCurrentTimestamp()
    change := ConstraintChange{
        Timestamp:      nowMs,
        TimestampStr:   nowStr,
        Action:         action,
        ConstraintType: constraintType,
        Field:          field,
        OldValue:       oldVal,
        NewValue:       newVal,
    }

    // Lock-free обновление истории
    history := c.constraints.loadConstraintHistory()
    newHistory := make([]ConstraintChange, len(history)+1)
    copy(newHistory, history)
    newHistory[len(history)] = change
    c.constraints.storeConstraintHistory(newHistory)
    c.constraints.updatedAt.Store(nowMs)

    // Аудит изменения ограничения
    LogAudit("CONSTRAINT_"+action, "CONSTRAINT", fmt.Sprintf("%s.%s", c.dbName, c.name), map[string]interface{}{
        "constraint_type": constraintType,
        "field":           field,
        "old_value":       oldVal,
        "new_value":       newVal,
    })
}

// logACLChange логирует изменение ACL (lock-free)
func (c *Collection) logACLChange(action, role, permission string, granted bool) {
    nowMs, nowStr := GetCurrentTimestamp()
    change := ACLChange{
        Timestamp:    nowMs,
        TimestampStr: nowStr,
        Action:       action,
        Role:         role,
        Permission:   permission,
        Granted:      granted,
    }

    // Lock-free обновление истории
    history := c.acl.loadACLHistory()
    newHistory := make([]ACLChange, len(history)+1)
    copy(newHistory, history)
    newHistory[len(history)] = change
    c.acl.storeACLHistory(newHistory)
    c.acl.updatedAt.Store(nowMs)

    // Аудит изменения ACL
    LogAudit("ACL_"+action, "ACL", fmt.Sprintf("%s.%s", c.dbName, c.name), map[string]interface{}{
        "role":       role,
        "permission": permission,
        "granted":    granted,
    })
}

// ========== Constraints Methods (lock-free) ==========

// AddRequiredField добавляет обязательное поле (lock-free)
func (c *Collection) AddRequiredField(field string) {
    oldMap := c.constraints.loadRequiredFields()
    oldVal := oldMap[field]

    newMap := make(map[string]bool)
    for k, v := range oldMap {
        newMap[k] = v
    }
    newMap[field] = true

    c.constraints.storeRequiredFields(newMap)
    c.logConstraintChange("ADD", "required", field, oldVal, true)
}

// AddUniqueConstraint добавляет ограничение уникальности (lock-free)
func (c *Collection) AddUniqueConstraint(field string) {
    oldMap := c.constraints.loadUniqueFields()
    oldVal := oldMap[field]

    newMap := make(map[string]bool)
    for k, v := range oldMap {
        newMap[k] = v
    }
    newMap[field] = true

    c.constraints.storeUniqueFields(newMap)
    c.logConstraintChange("ADD", "unique", field, oldVal, true)
    // Также создаём уникальный индекс
    c.CreateIndex("unique_"+field, []string{field}, true)
}

// AddMinConstraint добавляет минимальное значение (lock-free)
func (c *Collection) AddMinConstraint(field string, min float64) {
    oldMap := c.constraints.loadMinValues()
    oldVal := oldMap[field]

    newMap := make(map[string]float64)
    for k, v := range oldMap {
        newMap[k] = v
    }
    newMap[field] = min

    c.constraints.storeMinValues(newMap)
    c.logConstraintChange("ADD", "min", field, oldVal, min)
}

// AddMaxConstraint добавляет максимальное значение (lock-free)
func (c *Collection) AddMaxConstraint(field string, max float64) {
    oldMap := c.constraints.loadMaxValues()
    oldVal := oldMap[field]

    newMap := make(map[string]float64)
    for k, v := range oldMap {
        newMap[k] = v
    }
    newMap[field] = max

    c.constraints.storeMaxValues(newMap)
    c.logConstraintChange("ADD", "max", field, oldVal, max)
}

// AddRegexConstraint добавляет regexp паттерн (lock-free)
func (c *Collection) AddRegexConstraint(field string, pattern string) {
    oldMap := c.constraints.loadPatternFields()
    oldVal := oldMap[field]

    newMap := make(map[string]string)
    for k, v := range oldMap {
        newMap[k] = v
    }
    newMap[field] = pattern

    c.constraints.storePatternFields(newMap)
    c.logConstraintChange("ADD", "regex", field, oldVal, pattern)
}

// AddEnumConstraint добавляет допустимые значения (lock-free)
func (c *Collection) AddEnumConstraint(field string, values []interface{}) {
    oldMap := c.constraints.loadEnumFields()
    oldVal := oldMap[field]

    newMap := make(map[string][]interface{})
    for k, v := range oldMap {
        // Копируем слайс
        copiedVals := make([]interface{}, len(v))
        copy(copiedVals, v)
        newMap[k] = copiedVals
    }
    // Копируем новые значения
    copiedValues := make([]interface{}, len(values))
    copy(copiedValues, values)
    newMap[field] = copiedValues

    c.constraints.storeEnumFields(newMap)
    c.logConstraintChange("ADD", "enum", field, oldVal, values)
}

// RemoveRequiredField удаляет обязательное поле (lock-free)
func (c *Collection) RemoveRequiredField(field string) {
    oldMap := c.constraints.loadRequiredFields()
    oldVal := oldMap[field]

    newMap := make(map[string]bool)
    for k, v := range oldMap {
        if k != field {
            newMap[k] = v
        }
    }

    c.constraints.storeRequiredFields(newMap)
    c.logConstraintChange("REMOVE", "required", field, oldVal, nil)
}

// RemoveUniqueConstraint удаляет ограничение уникальности (lock-free)
func (c *Collection) RemoveUniqueConstraint(field string) {
    oldMap := c.constraints.loadUniqueFields()
    oldVal := oldMap[field]

    newMap := make(map[string]bool)
    for k, v := range oldMap {
        if k != field {
            newMap[k] = v
        }
    }

    c.constraints.storeUniqueFields(newMap)
    c.logConstraintChange("REMOVE", "unique", field, oldVal, nil)
    // Также удаляем индекс
    c.DropIndex("unique_" + field)
}

// RemoveMinConstraint удаляет минимальное значение (lock-free)
func (c *Collection) RemoveMinConstraint(field string) {
    oldMap := c.constraints.loadMinValues()
    oldVal := oldMap[field]

    newMap := make(map[string]float64)
    for k, v := range oldMap {
        if k != field {
            newMap[k] = v
        }
    }

    c.constraints.storeMinValues(newMap)
    c.logConstraintChange("REMOVE", "min", field, oldVal, nil)
}

// RemoveMaxConstraint удаляет максимальное значение (lock-free)
func (c *Collection) RemoveMaxConstraint(field string) {
    oldMap := c.constraints.loadMaxValues()
    oldVal := oldMap[field]

    newMap := make(map[string]float64)
    for k, v := range oldMap {
        if k != field {
            newMap[k] = v
        }
    }

    c.constraints.storeMaxValues(newMap)
    c.logConstraintChange("REMOVE", "max", field, oldVal, nil)
}

// RemoveRegexConstraint удаляет regexp паттерн (lock-free)
func (c *Collection) RemoveRegexConstraint(field string) {
    oldMap := c.constraints.loadPatternFields()
    oldVal := oldMap[field]

    newMap := make(map[string]string)
    for k, v := range oldMap {
        if k != field {
            newMap[k] = v
        }
    }

    c.constraints.storePatternFields(newMap)
    c.logConstraintChange("REMOVE", "regex", field, oldVal, nil)
}

// RemoveEnumConstraint удаляет допустимые значения (lock-free)
func (c *Collection) RemoveEnumConstraint(field string) {
    oldMap := c.constraints.loadEnumFields()
    oldVal := oldMap[field]

    newMap := make(map[string][]interface{})
    for k, v := range oldMap {
        if k != field {
            copiedVals := make([]interface{}, len(v))
            copy(copiedVals, v)
            newMap[k] = copiedVals
        }
    }

    c.constraints.storeEnumFields(newMap)
    c.logConstraintChange("REMOVE", "enum", field, oldVal, nil)
}

// GetRequiredFields возвращает список обязательных полей (lock-free)
func (c *Collection) GetRequiredFields() []string {
    fieldsMap := c.constraints.loadRequiredFields()
    fields := make([]string, 0, len(fieldsMap))
    for field := range fieldsMap {
        fields = append(fields, field)
    }
    return fields
}

// GetUniqueConstraints возвращает список уникальных полей (lock-free)
func (c *Collection) GetUniqueConstraints() []string {
    fieldsMap := c.constraints.loadUniqueFields()
    fields := make([]string, 0, len(fieldsMap))
    for field := range fieldsMap {
        fields = append(fields, field)
    }
    return fields
}

// GetMinConstraints возвращает карту минимальных значений (lock-free)
func (c *Collection) GetMinConstraints() map[string]float64 {
    result := c.constraints.loadMinValues()
    // Возвращаем копию
    resultCopy := make(map[string]float64)
    for k, v := range result {
        resultCopy[k] = v
    }
    return resultCopy
}

// GetMaxConstraints возвращает карту максимальных значений (lock-free)
func (c *Collection) GetMaxConstraints() map[string]float64 {
    result := c.constraints.loadMaxValues()
    resultCopy := make(map[string]float64)
    for k, v := range result {
        resultCopy[k] = v
    }
    return resultCopy
}

// GetEnumConstraints возвращает карту enum ограничений (lock-free)
func (c *Collection) GetEnumConstraints() map[string][]interface{} {
    result := c.constraints.loadEnumFields()
    resultCopy := make(map[string][]interface{})
    for k, v := range result {
        copied := make([]interface{}, len(v))
        copy(copied, v)
        resultCopy[k] = copied
    }
    return resultCopy
}

// GetRegexConstraints возвращает карту regex паттернов (lock-free)
func (c *Collection) GetRegexConstraints() map[string]string {
    result := c.constraints.loadPatternFields()
    resultCopy := make(map[string]string)
    for k, v := range result {
        resultCopy[k] = v
    }
    return resultCopy
}

// GetConstraints возвращает все ограничения коллекции (для API)
func (c *Collection) GetConstraints() map[string]interface{} {
    return map[string]interface{}{
        "required_fields": c.GetRequiredFields(),
        "unique_fields":   c.GetUniqueConstraints(),
        "min_values":      c.GetMinConstraints(),
        "max_values":      c.GetMaxConstraints(),
        "enum_values":     c.GetEnumConstraints(),
        "regex_patterns":  c.GetRegexConstraints(),
    }
}

// GetConstraintTimestamps возвращает временные метки ограничений (lock-free)
func (c *Collection) GetConstraintTimestamps() map[string]interface{} {
    return map[string]interface{}{
        "created_at":    c.constraints.createdAt,
        "updated_at":    c.constraints.updatedAt.Load(),
        "history_count": len(c.constraints.loadConstraintHistory()),
    }
}

// GetConstraintHistory возвращает историю изменений ограничений (lock-free)
func (c *Collection) GetConstraintHistory() []ConstraintChange {
    history := c.constraints.loadConstraintHistory()
    result := make([]ConstraintChange, len(history))
    copy(result, history)
    return result
}

// ValidateDocument проверяет документ на соответствие ограничениям (lock-free)
func (cons *Constraints) ValidateDocument(doc *Document) error {
    // Проверка обязательных полей (lock-free чтение)
    requiredFields := cons.loadRequiredFields()
    for field := range requiredFields {
        if !doc.HasField(field) {
            return fmt.Errorf("required field '%s' is missing", field)
        }
    }

    // Проверка числовых ограничений (lock-free чтение)
    minValues := cons.loadMinValues()
    for field, minVal := range minValues {
        if val, err := doc.GetField(field); err == nil {
            if numVal, ok := toFloat64(val); ok {
                if numVal < minVal {
                    return fmt.Errorf("field '%s' value %v is less than minimum %v", field, numVal, minVal)
                }
            }
        }
    }

    maxValues := cons.loadMaxValues()
    for field, maxVal := range maxValues {
        if val, err := doc.GetField(field); err == nil {
            if numVal, ok := toFloat64(val); ok {
                if numVal > maxVal {
                    return fmt.Errorf("field '%s' value %v exceeds maximum %v", field, numVal, maxVal)
                }
            }
        }
    }

    // Проверка regex паттернов (lock-free чтение)
    patternFields := cons.loadPatternFields()
    for field, pattern := range patternFields {
        if val, err := doc.GetField(field); err == nil {
            if strVal, ok := val.(string); ok {
                if pattern != "" && !strings.Contains(strVal, pattern) {
                    return fmt.Errorf("field '%s' value '%s' does not match pattern '%s'", field, strVal, pattern)
                }
            }
        }
    }

    // Проверка enum (lock-free чтение)
    enumFields := cons.loadEnumFields()
    for field, allowedValues := range enumFields {
        if val, err := doc.GetField(field); err == nil {
            found := false
            for _, allowed := range allowedValues {
                if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", allowed) {
                    found = true
                    break
                }
            }
            if !found {
                return fmt.Errorf("field '%s' value '%v' not in allowed list", field, val)
            }
        }
    }

    return nil
}

// ========== ACL Methods (lock-free) ==========

// SetACL устанавливает ACL для коллекции (lock-free)
func (c *Collection) SetACL(role string, canRead, canWrite, canDelete, isAdmin bool) {
    c.acl.updatedAt.Store(time.Now().UnixMilli())

    if canRead {
        oldRoles := c.acl.loadReadRoles()
        if !oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                newRoles[k] = v
            }
            newRoles[role] = true
            c.acl.storeReadRoles(newRoles)
            c.logACLChange("GRANT", role, "read", true)
        }
    }
    if canWrite {
        oldRoles := c.acl.loadWriteRoles()
        if !oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                newRoles[k] = v
            }
            newRoles[role] = true
            c.acl.storeWriteRoles(newRoles)
            c.logACLChange("GRANT", role, "write", true)
        }
    }
    if canDelete {
        oldRoles := c.acl.loadDeleteRoles()
        if !oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                newRoles[k] = v
            }
            newRoles[role] = true
            c.acl.storeDeleteRoles(newRoles)
            c.logACLChange("GRANT", role, "delete", true)
        }
    }
    if isAdmin {
        oldRoles := c.acl.loadAdminRoles()
        if !oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                newRoles[k] = v
            }
            newRoles[role] = true
            c.acl.storeAdminRoles(newRoles)
            c.logACLChange("GRANT", role, "admin", true)
        }
    }
}

// RevokeACL отзывает разрешения у роли (lock-free)
func (c *Collection) RevokeACL(role string, permission string) {
    c.acl.updatedAt.Store(time.Now().UnixMilli())

    switch permission {
    case "read":
        oldRoles := c.acl.loadReadRoles()
        if oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                if k != role {
                    newRoles[k] = v
                }
            }
            c.acl.storeReadRoles(newRoles)
            c.logACLChange("REVOKE", role, "read", false)
        }
    case "write":
        oldRoles := c.acl.loadWriteRoles()
        if oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                if k != role {
                    newRoles[k] = v
                }
            }
            c.acl.storeWriteRoles(newRoles)
            c.logACLChange("REVOKE", role, "write", false)
        }
    case "delete":
        oldRoles := c.acl.loadDeleteRoles()
        if oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                if k != role {
                    newRoles[k] = v
                }
            }
            c.acl.storeDeleteRoles(newRoles)
            c.logACLChange("REVOKE", role, "delete", false)
        }
    case "admin":
        oldRoles := c.acl.loadAdminRoles()
        if oldRoles[role] {
            newRoles := make(map[string]bool)
            for k, v := range oldRoles {
                if k != role {
                    newRoles[k] = v
                }
            }
            c.acl.storeAdminRoles(newRoles)
            c.logACLChange("REVOKE", role, "admin", false)
        }
    }
}

// CheckPermission проверяет наличие разрешения у роли (lock-free)
func (c *Collection) CheckPermission(role, operation string) bool {
    return c.acl.CheckPermission(role, operation)
}

// GetACLTimestamps возвращает временные метки ACL (lock-free)
func (c *Collection) GetACLTimestamps() map[string]interface{} {
    return map[string]interface{}{
        "created_at": c.acl.createdAt,
        "updated_at": c.acl.updatedAt.Load(),
    }
}

// GetACLHistory возвращает историю изменений ACL (lock-free)
func (c *Collection) GetACLHistory() []ACLChange {
    history := c.acl.loadACLHistory()
    result := make([]ACLChange, len(history))
    copy(result, history)
    return result
}

// GetACLUpdatedAt возвращает время последнего обновления ACL (lock-free)
func (c *Collection) GetACLUpdatedAt() int64 {
    return c.acl.updatedAt.Load()
}

// GetACLCreatedAt возвращает время создания ACL (lock-free)
func (c *Collection) GetACLCreatedAt() int64 {
    return c.acl.createdAt
}

// toFloat64 конвертирует interface{} в float64
func toFloat64(val interface{}) (float64, bool) {
    switch v := val.(type) {
    case int:
        return float64(v), true
    case int64:
        return float64(v), true
    case float64:
        return v, true
    case float32:
        return float64(v), true
    default:
        return 0, false
    }
}
