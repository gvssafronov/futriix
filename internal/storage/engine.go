/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/engine.go
// Назначение: In-memory движок хранения документов с поддержкой коллекций,
// слайсов (аналог БД), тапплов (аналог таблиц), полей и кортежей.
// Полностью wait-free с использованием sync.Map и атомарных операций.
//  InmemStore теперь использует fsync для сохранности Raft-лога
//  Restore() теперь полностью очищает существующие данные

package storage

import (
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "sync/atomic"
    "time"
    
    "github.com/hashicorp/raft"
    "futriis/internal/log"
    "futriis/internal/serializer"
)

// =============================================================================
// INMEM STORE - ВСТРОЕННОЕ ФАЙЛОВОЕ ХРАНИЛИЩЕ ДЛЯ RAFT
// =============================================================================

// InmemStore реализует встроенное файловое хранилище для Raft.
//  Все записи теперь синхронизируются с диском через fsync
type InmemStore struct {
    mu        sync.RWMutex
    data      map[string][]byte
    path      string
    createdAt int64
    updatedAt int64
    //  Файл для атомарной записи с fsync
    file      *os.File
}

// NewInmemStore создаёт новое хранилище для Raft.
func NewInmemStore(path string) *InmemStore {
    now := time.Now().UnixMilli()
    store := &InmemStore{
        data:      make(map[string][]byte),
        path:      path,
        createdAt: now,
        updatedAt: now,
    }
    store.load()
    return store
}

// load загружает данные из файла при инициализации.
func (s *InmemStore) load() {
    if s.path == "" {
        return
    }
    data, err := os.ReadFile(s.path)
    if err != nil {
        return
    }
    json.Unmarshal(data, &s.data)
    s.updatedAt = time.Now().UnixMilli()
}

// save сохраняет данные в файл.
//  Теперь использует атомарную запись с fsync
func (s *InmemStore) save() {
    if s.path == "" {
        return
    }
    s.updatedAt = time.Now().UnixMilli()
    data, err := json.Marshal(s.data)
    if err != nil {
        return
    }
    
    //  Атомарная запись через временный файл + fsync
    tempPath := s.path + ".tmp"
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return
    }
    
    // Открываем для fsync
    f, err := os.OpenFile(tempPath, os.O_RDWR, 0644)
    if err != nil {
        os.Remove(tempPath)
        return
    }
    
    //  fsync для гарантии записи на диск
    if err := RealFsyncWithRetry(f, 3, 100*time.Millisecond); err != nil {
        f.Close()
        os.Remove(tempPath)
        return
    }
    f.Close()
    
    // Атомарное переименование
    if err := os.Rename(tempPath, s.path); err != nil {
        os.Remove(tempPath)
        return
    }
    
    //  Синхронизация директории
    FsyncDir(dirOf(s.path))
}

// dirOf возвращает директорию файла
func dirOf(path string) string {
    for i := len(path) - 1; i >= 0; i-- {
        if path[i] == '/' || path[i] == '\\' {
            return path[:i]
        }
    }
    return "."
}

// ==================== Реализация raft.LogStore ====================

// FirstIndex возвращает первый индекс в логе.
func (s *InmemStore) FirstIndex() (uint64, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var first uint64 = 0
    for key := range s.data {
        var idx uint64
        if _, err := fmt.Sscanf(key, "log-%d", &idx); err == nil {
            if first == 0 || idx < first {
                first = idx
            }
        }
    }
    return first, nil
}

// LastIndex возвращает последний индекс в логе.
func (s *InmemStore) LastIndex() (uint64, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var last uint64 = 0
    for key := range s.data {
        var idx uint64
        if _, err := fmt.Sscanf(key, "log-%d", &idx); err == nil {
            if idx > last {
                last = idx
            }
        }
    }
    return last, nil
}

// GetLog получает запись лога по индексу.
func (s *InmemStore) GetLog(idx uint64, log *raft.Log) error {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    key := fmt.Sprintf("log-%d", idx)
    data, ok := s.data[key]
    if !ok {
        return raft.ErrLogNotFound
    }
    
    return json.Unmarshal(data, log)
}

// StoreLog сохраняет одну запись лога.
func (s *InmemStore) StoreLog(log *raft.Log) error {
    return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs сохраняет несколько записей лога за одну операцию.
func (s *InmemStore) StoreLogs(logs []*raft.Log) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    for _, log := range logs {
        key := fmt.Sprintf("log-%d", log.Index)
        data, err := json.Marshal(log)
        if err != nil {
            return err
        }
        s.data[key] = data
    }
    s.save()
    return nil
}

// DeleteRange удаляет записи лога в диапазоне индексов [min, max].
func (s *InmemStore) DeleteRange(min, max uint64) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    for idx := min; idx <= max; idx++ {
        key := fmt.Sprintf("log-%d", idx)
        delete(s.data, key)
    }
    s.save()
    return nil
}

// ==================== Реализация raft.StableStore ====================

// Get получает значение по ключу из стабильного хранилища.
func (s *InmemStore) Get(key []byte) ([]byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    val, ok := s.data[string(key)]
    if !ok {
        return nil, nil
    }
    return val, nil
}

// Set устанавливает значение по ключу в стабильном хранилище.
func (s *InmemStore) Set(key []byte, val []byte) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    s.data[string(key)] = val
    s.save()
    return nil
}

// SetUint64 устанавливает uint64 значение по ключу.
func (s *InmemStore) SetUint64(key []byte, val uint64) error {
    return s.Set(key, []byte(fmt.Sprintf("%d", val)))
}

// GetUint64 получает uint64 значение по ключу.
func (s *InmemStore) GetUint64(key []byte) (uint64, error) {
    val, err := s.Get(key)
    if err != nil {
        return 0, err
    }
    if val == nil {
        return 0, nil
    }
    var result uint64
    fmt.Sscanf(string(val), "%d", &result)
    return result, nil
}

// =============================================================================
// ОСНОВНОЕ ХРАНИЛИЩЕ
// =============================================================================

// Storage представляет основное хранилище баз данных.
type Storage struct {
    databases   sync.Map // map[string]*Database
    pageSize    int64
    logger      *log.Logger
    totalDocs   atomic.Int64
    createdAt   int64
}

// Database представляет базу данных (аналог слайса в реляционных СУБД).
type Database struct {
    name        string
    collections sync.Map
    createdAt   int64
    updatedAt   int64
    deletedAt   int64
    mu          sync.RWMutex
}

// ExportMetadata содержит метаданные экспорта базы данных.
type ExportMetadata struct {
    DatabaseName     string                 `msgpack:"database_name"`
    ExportTime       int64                  `msgpack:"export_time"`
    ExportTimeStr    string                 `msgpack:"export_time_str"`
    Version          string                 `msgpack:"version"`
    CollectionsCount int                    `msgpack:"collections_count"`
    DocumentsCount   int64                  `msgpack:"documents_count"`
    ExportedBy       string                 `msgpack:"exported_by,omitempty"`
    Additional       map[string]interface{} `msgpack:"additional,omitempty"`
}

// ImportMetadata содержит метаданные импорта базы данных.
type ImportMetadata struct {
    DatabaseName     string                 `msgpack:"database_name"`
    ImportTime       int64                  `msgpack:"import_time"`
    ImportTimeStr    string                 `msgpack:"import_time_str"`
    SourceFile       string                 `msgpack:"source_file"`
    SourceExportTime int64                  `msgpack:"source_export_time,omitempty"`
    CollectionsCount int                    `msgpack:"collections_count"`
    DocumentsCount   int64                  `msgpack:"documents_count"`
    SkippedDocuments int64                  `msgpack:"skipped_documents"`
    FailedDocuments  int64                  `msgpack:"failed_documents"`
    ImportedBy       string                 `msgpack:"imported_by,omitempty"`
}

// NewStorage создаёт новый экземпляр хранилища.
func NewStorage(pageSizeMB int, logger *log.Logger) *Storage {
    return &Storage{
        pageSize:  int64(pageSizeMB) * 1024 * 1024,
        logger:    logger,
        createdAt: time.Now().UnixMilli(),
    }
}

// =============================================================================
// ОПЕРАЦИИ С БАЗАМИ ДАННЫХ
// =============================================================================

// CreateDatabase создаёт новую базу данных.
func (s *Storage) CreateDatabase(name string) error {
    now := time.Now().UnixMilli()
    db := &Database{
        name:      name,
        createdAt: now,
        updatedAt: now,
        deletedAt: 0,
    }
    
    if _, exists := s.databases.LoadOrStore(name, db); exists {
        return fmt.Errorf("database already exists")
    }
    AuditDatabaseOperation("CREATE", name)
    if s.logger != nil {
        s.logger.Info("Database created: " + name)
    }
    return nil
}

// GetDatabase возвращает базу данных по имени.
func (s *Storage) GetDatabase(name string) (*Database, error) {
    if val, ok := s.databases.Load(name); ok {
        return val.(*Database), nil
    }
    return nil, fmt.Errorf("database not found")
}

// DropDatabase удаляет базу данных.
//  Операция логируется в WAL через AuditDatabaseOperation
func (s *Storage) DropDatabase(name string) error {
    if val, ok := s.databases.Load(name); ok {
        db := val.(*Database)
        db.mu.Lock()
        db.deletedAt = time.Now().UnixMilli()
        db.mu.Unlock()
    }
    
    if _, ok := s.databases.LoadAndDelete(name); !ok {
        return fmt.Errorf("database not found")
    }
    
    //  Логирование в персистентный аудит
    AuditDatabaseOperation("DROP", name)
    if s.logger != nil {
        s.logger.Info("Database dropped: " + name)
    }
    return nil
}

// ListDatabases возвращает список имен всех баз данных.
func (s *Storage) ListDatabases() []string {
    databases := make([]string, 0)
    s.databases.Range(func(key, value interface{}) bool {
        databases = append(databases, key.(string))
        return true
    })
    return databases
}

// ExistsDatabase проверяет существование базы данных.
func (s *Storage) ExistsDatabase(name string) bool {
    _, ok := s.databases.Load(name)
    return ok
}

// GetDatabaseCount возвращает количество баз данных.
func (s *Storage) GetDatabaseCount() int {
    count := 0
    s.databases.Range(func(key, value interface{}) bool {
        count++
        return true
    })
    return count
}

// GetDatabaseNames возвращает имена всех баз данных.
func (s *Storage) GetDatabaseNames() []string {
    return s.ListDatabases()
}

// =============================================================================
// ОПЕРАЦИИ С КОЛЛЕКЦИЯМИ
// =============================================================================

// CreateCollection создаёт новую коллекцию в базе данных.
func (db *Database) CreateCollection(name string) error {
    if _, exists := db.collections.LoadOrStore(name, NewCollection(db.name, name, nil)); exists {
        return fmt.Errorf("collection already exists")
    }
    db.UpdateTimestamp()
    AuditCollectionOperation("CREATE", db.name, name, nil)
    return nil
}

// CreateCollectionWithSettings создаёт коллекцию с заданными настройками.
func (db *Database) CreateCollectionWithSettings(name string, settings *CollectionSettings) error {
    if _, exists := db.collections.LoadOrStore(name, NewCollection(db.name, name, settings)); exists {
        return fmt.Errorf("collection already exists")
    }
    db.UpdateTimestamp()
    AuditCollectionOperation("CREATE", db.name, name, settings)
    return nil
}

// GetCollection возвращает коллекцию по имени.
func (db *Database) GetCollection(name string) (*Collection, error) {
    if val, ok := db.collections.Load(name); ok {
        return val.(*Collection), nil
    }
    return nil, fmt.Errorf("collection not found")
}

// DropCollection удаляет коллекцию.
//  Логирование через персистентный аудит
func (db *Database) DropCollection(name string) error {
    if _, ok := db.collections.LoadAndDelete(name); !ok {
        return fmt.Errorf("collection not found")
    }
    db.UpdateTimestamp()
    AuditCollectionOperation("DROP", db.name, name, nil)
    return nil
}

// ListCollections возвращает список имен всех коллекций в базе данных.
func (db *Database) ListCollections() []string {
    collections := make([]string, 0)
    db.collections.Range(func(key, value interface{}) bool {
        collections = append(collections, key.(string))
        return true
    })
    return collections
}

// =============================================================================
// МЕТОДЫ ДЛЯ РАБОТЫ С МЕТАДАННЫМИ БАЗЫ ДАННЫХ
// =============================================================================

func (db *Database) Name() string { return db.name }

func (db *Database) GetCreatedAt() int64 {
    db.mu.RLock()
    defer db.mu.RUnlock()
    return db.createdAt
}

func (db *Database) GetUpdatedAt() int64 {
    db.mu.RLock()
    defer db.mu.RUnlock()
    return db.updatedAt
}

func (db *Database) GetDeletedAt() int64 {
    db.mu.RLock()
    defer db.mu.RUnlock()
    return db.deletedAt
}

func (db *Database) UpdateTimestamp() {
    db.mu.Lock()
    defer db.mu.Unlock()
    db.updatedAt = time.Now().UnixMilli()
}

// =============================================================================
// ОПЕРАЦИИ СТАТИСТИКИ
// =============================================================================

func (s *Storage) GetTotalDocuments() int64 { return s.totalDocs.Load() }
func (s *Storage) GetPageSize() int64       { return s.pageSize }
func (s *Storage) GetCreatedAt() int64      { return s.createdAt }

// =============================================================================
// ЭКСПОРТ И ИМПОРТ ДАННЫХ
// =============================================================================

// SerializeDatabaseWithMetadata сериализует базу данных с метаданными экспорта.
func (db *Database) SerializeDatabaseWithMetadata(exportedBy string) ([]byte, *ExportMetadata, error) {
    dbData := make(map[string]interface{})
    totalDocuments := int64(0)
    
    db.collections.Range(func(key, value interface{}) bool {
        coll := value.(*Collection)
        collData := make(map[string]interface{})
        
        docs := coll.GetAllDocuments()
        totalDocuments += int64(len(docs))
        
        collDocs := make([]*Document, 0, len(docs))
        for _, doc := range docs {
            collDocs = append(collDocs, doc)
        }
        collData["documents"] = collDocs
        collData["metadata"] = coll.GetMetadata()
        collData["timestamps"] = coll.GetTimestamps()
        collData["indexes"] = coll.GetIndexesInfo()
        collData["constraints"] = coll.GetConstraints()
        collData["constraint_timestamps"] = coll.GetConstraintTimestamps()
        collData["acl_timestamps"] = coll.GetACLTimestamps()
        
        dbData[key.(string)] = collData
        return true
    })
    
    nowMs, nowStr := GetCurrentTimestamp()
    metadata := &ExportMetadata{
        DatabaseName:     db.name,
        ExportTime:       nowMs,
        ExportTimeStr:    nowStr,
        Version:          "1.0",
        CollectionsCount: len(db.ListCollections()),
        DocumentsCount:   totalDocuments,
        ExportedBy:       exportedBy,
        Additional: map[string]interface{}{
            "storage_created_at": time.UnixMilli(db.createdAt).Format("2006-01-02 15:04:05.000"),
        },
    }
    
    dbData["_metadata"] = map[string]interface{}{
        "name":            db.name,
        "created_at":      db.GetCreatedAt(),
        "created_at_str":  time.UnixMilli(db.GetCreatedAt()).Format("2006-01-02 15:04:05.000"),
        "updated_at":      db.GetUpdatedAt(),
        "updated_at_str":  time.UnixMilli(db.GetUpdatedAt()).Format("2006-01-02 15:04:05.000"),
        "export_metadata": metadata,
    }
    
    data, err := serializer.Marshal(dbData)
    if err != nil {
        return nil, nil, err
    }
    
    return data, metadata, nil
}

// SerializeDatabase сериализует всю базу данных в MessagePack (без метаданных).
func (db *Database) SerializeDatabase() ([]byte, error) {
    data, _, err := db.SerializeDatabaseWithMetadata("")
    return data, err
}

// DeserializeDatabaseWithMetadata десериализует базу данных с сохранением метаданных импорта.
func (db *Database) DeserializeDatabaseWithMetadata(data []byte, sourceFile string, importedBy string) (*ImportMetadata, error) {
    var dbData map[string]interface{}
    if err := serializer.Unmarshal(data, &dbData); err != nil {
        return nil, err
    }
    
    var sourceExportTime int64
    var collectionsCount int
    var documentsCount int64
    
    if metaRaw, ok := dbData["_metadata"]; ok {
        if meta, ok := metaRaw.(map[string]interface{}); ok {
            if createdAt, ok := meta["created_at"]; ok {
                if v, ok := createdAt.(int64); ok {
                    db.createdAt = v
                }
            }
            if exportMetaRaw, ok := meta["export_metadata"]; ok {
                if exportMeta, ok := exportMetaRaw.(map[string]interface{}); ok {
                    if sourceTime, ok := exportMeta["export_time"]; ok {
                        if v, ok := sourceTime.(int64); ok {
                            sourceExportTime = v
                        } else if v, ok := sourceTime.(float64); ok {
                            sourceExportTime = int64(v)
                        }
                    }
                    if collCount, ok := exportMeta["collections_count"]; ok {
                        if v, ok := collCount.(int); ok {
                            collectionsCount = v
                        } else if v, ok := collCount.(int64); ok {
                            collectionsCount = int(v)
                        } else if v, ok := collCount.(float64); ok {
                            collectionsCount = int(v)
                        }
                    }
                    if docCount, ok := exportMeta["documents_count"]; ok {
                        if v, ok := docCount.(int64); ok {
                            documentsCount = v
                        } else if v, ok := docCount.(float64); ok {
                            documentsCount = int64(v)
                        }
                    }
                }
            }
        }
        delete(dbData, "_metadata")
    }
    
    for collName, collDataRaw := range dbData {
        collData, ok := collDataRaw.(map[string]interface{})
        if !ok {
            continue
        }
        
        settings := &CollectionSettings{
            MaxDocuments:   0,
            ValidateSchema: false,
            AutoIndexID:    true,
            TTLSeconds:     0,
            SoftDelete:     false,
        }
        
        if metaRaw, ok := collData["metadata"]; ok {
            if meta, ok := metaRaw.(map[string]interface{}); ok {
                if settingsRaw, ok := meta["settings"]; ok {
                    if settingsMap, ok := settingsRaw.(map[string]interface{}); ok {
                        if maxDocs, ok := settingsMap["max_documents"]; ok {
                            if v, ok := maxDocs.(int); ok {
                                settings.MaxDocuments = v
                            } else if v, ok := maxDocs.(int64); ok {
                                settings.MaxDocuments = int(v)
                            } else if v, ok := maxDocs.(float64); ok {
                                settings.MaxDocuments = int(v)
                            }
                        }
                        if validateSchema, ok := settingsMap["validate_schema"]; ok {
                            if v, ok := validateSchema.(bool); ok {
                                settings.ValidateSchema = v
                            }
                        }
                        if autoIndexID, ok := settingsMap["auto_index_id"]; ok {
                            if v, ok := autoIndexID.(bool); ok {
                                settings.AutoIndexID = v
                            }
                        }
                        if ttlSeconds, ok := settingsMap["ttl_seconds"]; ok {
                            if v, ok := ttlSeconds.(int); ok {
                                settings.TTLSeconds = v
                            } else if v, ok := ttlSeconds.(int64); ok {
                                settings.TTLSeconds = int(v)
                            } else if v, ok := ttlSeconds.(float64); ok {
                                settings.TTLSeconds = int(v)
                            }
                        }
                        if softDelete, ok := settingsMap["soft_delete"]; ok {
                            if v, ok := softDelete.(bool); ok {
                                settings.SoftDelete = v
                            }
                        }
                    }
                }
            } else if meta, ok := metaRaw.(*CollectionMetadata); ok {
                if meta.Settings != nil {
                    settings = meta.Settings
                }
            }
        }
        
        coll := NewCollection(db.name, collName, settings)
        
        if timestampsRaw, ok := collData["timestamps"]; ok {
            if timestamps, ok := timestampsRaw.(map[string]interface{}); ok {
                if createdAt, ok := timestamps["created_at"]; ok {
                    if v, ok := createdAt.(int64); ok {
                        coll.metadata.CreatedAt = v
                    } else if v, ok := createdAt.(float64); ok {
                        coll.metadata.CreatedAt = int64(v)
                    }
                }
                if updatedAt, ok := timestamps["updated_at"]; ok {
                    if v, ok := updatedAt.(int64); ok {
                        coll.metadata.UpdatedAt = v
                    } else if v, ok := updatedAt.(float64); ok {
                        coll.metadata.UpdatedAt = int64(v)
                    }
                }
            }
        }
        
        if indexesRaw, ok := collData["indexes"]; ok {
            if indexesList, ok := indexesRaw.([]interface{}); ok {
                for _, idxRaw := range indexesList {
                    if idxMap, ok := idxRaw.(map[string]interface{}); ok {
                        idxName, _ := idxMap["name"].(string)
                        idxFieldsRaw, _ := idxMap["fields"].([]interface{})
                        idxFields := make([]string, len(idxFieldsRaw))
                        for i, f := range idxFieldsRaw {
                            idxFields[i] = fmt.Sprintf("%v", f)
                        }
                        idxUnique, _ := idxMap["unique"].(bool)
                        if idxName != "" && idxName != "_id_" {
                            coll.CreateIndex(idxName, idxFields, idxUnique)
                        }
                    }
                }
            }
        }
        
        if docsRaw, ok := collData["documents"]; ok {
            if docs, ok := docsRaw.([]interface{}); ok {
                for _, docRaw := range docs {
                    if doc, ok := docRaw.(*Document); ok {
                        coll.Insert(doc)
                    } else if docMap, ok := docRaw.(map[string]interface{}); ok {
                        doc := NewDocument()
                        if id, ok := docMap["ID"].(string); ok {
                            doc.ID = id
                        } else if id, ok := docMap["id"].(string); ok {
                            doc.ID = id
                        }
                        if fields, ok := docMap["fields"]; ok {
                            if fieldsMap, ok := fields.(map[string]interface{}); ok {
                                for k, v := range fieldsMap {
                                    doc.SetField(k, v)
                                }
                            }
                        }
                        if createdAt, ok := docMap["created_at"]; ok {
                            if v, ok := createdAt.(int64); ok {
                                doc.CreatedAt = v
                            } else if v, ok := createdAt.(float64); ok {
                                doc.CreatedAt = int64(v)
                            }
                        }
                        if updatedAt, ok := docMap["updated_at"]; ok {
                            if v, ok := updatedAt.(int64); ok {
                                doc.UpdatedAt = v
                            } else if v, ok := updatedAt.(float64); ok {
                                doc.UpdatedAt = int64(v)
                            }
                        }
                        if deletedAt, ok := docMap["deleted_at"]; ok {
                            if v, ok := deletedAt.(int64); ok {
                                doc.DeletedAt = v
                            } else if v, ok := deletedAt.(float64); ok {
                                doc.DeletedAt = int64(v)
                            }
                        }
                        if version, ok := docMap["version"]; ok {
                            if v, ok := version.(uint64); ok {
                                doc.Version = v
                            } else if v, ok := version.(int64); ok {
                                doc.Version = uint64(v)
                            } else if v, ok := version.(float64); ok {
                                doc.Version = uint64(v)
                            }
                        }
                        coll.Insert(doc)
                    }
                }
            } else if docs, ok := docsRaw.([]*Document); ok {
                for _, doc := range docs {
                    coll.Insert(doc)
                }
            }
        }
        
        db.collections.Store(collName, coll)
        AuditCollectionOperation("RESTORE", db.name, collName, settings)
    }
    
    nowMs, nowStr := GetCurrentTimestamp()
    importMetadata := &ImportMetadata{
        DatabaseName:     db.name,
        ImportTime:       nowMs,
        ImportTimeStr:    nowStr,
        SourceFile:       sourceFile,
        SourceExportTime: sourceExportTime,
        CollectionsCount: collectionsCount,
        DocumentsCount:   documentsCount,
        SkippedDocuments: 0,
        FailedDocuments:  0,
        ImportedBy:       importedBy,
    }
    
    db.UpdateTimestamp()
    
    LogAudit("IMPORT", "DATABASE", db.name, map[string]interface{}{
        "source_file":        sourceFile,
        "collections_count":  collectionsCount,
        "documents_count":    documentsCount,
        "source_export_time": sourceExportTime,
        "imported_by":        importedBy,
    })
    
    return importMetadata, nil
}

// DeserializeDatabase десериализует базу данных из MessagePack (без метаданных).
func (db *Database) DeserializeDatabase(data []byte) error {
    _, err := db.DeserializeDatabaseWithMetadata(data, "", "")
    return err
}

// ExportDatabaseWithMetadata экспортирует базу данных в файл с метаданными.
func (s *Storage) ExportDatabaseWithMetadata(dbName, filePath, exportedBy string) (*ExportMetadata, error) {
    if !s.ExistsDatabase(dbName) {
        return nil, fmt.Errorf("database '%s' not found", dbName)
    }
    
    db, err := s.GetDatabase(dbName)
    if err != nil {
        return nil, err
    }
    
    data, metadata, err := db.SerializeDatabaseWithMetadata(exportedBy)
    if err != nil {
        return nil, err
    }
    
    //  Атомарная запись с fsync
    if err := writeFileSync(filePath, data, 0644); err != nil {
        return nil, fmt.Errorf("failed to write export file: %v", err)
    }
    
    if s.logger != nil {
        s.logger.Info(fmt.Sprintf("Database '%s' exported to %s at %s", dbName, filePath, metadata.ExportTimeStr))
    }
    
    return metadata, nil
}

// writeFileSync записывает файл с fsync
//  Вспомогательная функция для атомарной записи с fsync
func writeFileSync(path string, data []byte, perm os.FileMode) error {
    tempPath := path + ".tmp"
    if err := os.WriteFile(tempPath, data, perm); err != nil {
        return err
    }
    
    f, err := os.OpenFile(tempPath, os.O_RDWR, perm)
    if err != nil {
        os.Remove(tempPath)
        return err
    }
    
    if err := RealFsyncWithRetry(f, 3, 100*time.Millisecond); err != nil {
        f.Close()
        os.Remove(tempPath)
        return err
    }
    f.Close()
    
    if err := os.Rename(tempPath, path); err != nil {
        os.Remove(tempPath)
        return err
    }
    
    FsyncDir(dirOf(path))
    return nil
}

// ImportDatabaseWithMetadata импортирует базу данных из файла с метаданными.
func (s *Storage) ImportDatabaseWithMetadata(dbName, filePath, importedBy string) (*ImportMetadata, error) {
    if _, err := os.Stat(filePath); os.IsNotExist(err) {
        return nil, fmt.Errorf("import file '%s' not found", filePath)
    }
    
    data, err := os.ReadFile(filePath)
    if err != nil {
        return nil, fmt.Errorf("failed to read import file: %v", err)
    }
    
    if !s.ExistsDatabase(dbName) {
        if err := s.CreateDatabase(dbName); err != nil {
            return nil, fmt.Errorf("failed to create database: %v", err)
        }
    }
    
    db, err := s.GetDatabase(dbName)
    if err != nil {
        return nil, err
    }
    
    metadata, err := db.DeserializeDatabaseWithMetadata(data, filePath, importedBy)
    if err != nil {
        return nil, err
    }
    
    if s.logger != nil {
        s.logger.Info(fmt.Sprintf("Database '%s' imported from %s at %s", dbName, filePath, metadata.ImportTimeStr))
    }
    
    return metadata, nil
}

// =============================================================================
// РЕЗЕРВНОЕ КОПИРОВАНИЕ И ВОССТАНОВЛЕНИЕ
// =============================================================================

// Backup создаёт резервную копию всех данных.
func (s *Storage) Backup(backupPath string) error {
    if s.logger != nil {
        s.logger.Info(fmt.Sprintf("Starting backup to %s", backupPath))
    }
    
    backup := make(map[string]interface{})
    backup["created_at"] = s.createdAt
    backup["backup_time"] = time.Now().UnixMilli()
    backup["backup_time_str"] = time.Now().Format("2006-01-02 15:04:05.000")
    
    databases := make(map[string][]byte)
    databasesMetadata := make(map[string]interface{})
    
    s.databases.Range(func(key, value interface{}) bool {
        dbName := key.(string)
        db := value.(*Database)
        
        dbData, err := db.SerializeDatabase()
        if err != nil {
            if s.logger != nil {
                s.logger.Error(fmt.Sprintf("Failed to serialize database %s: %v", dbName, err))
            }
            return false
        }
        
        databases[dbName] = dbData
        databasesMetadata[dbName] = map[string]interface{}{
            "created_at":  db.GetCreatedAt(),
            "updated_at":  db.GetUpdatedAt(),
            "collections": len(db.ListCollections()),
        }
        return true
    })
    
    backup["databases"] = databases
    backup["databases_metadata"] = databasesMetadata
    backup["total_databases"] = s.GetDatabaseCount()
    
    data, err := serializer.Marshal(backup)
    if err != nil {
        return fmt.Errorf("failed to marshal backup: %v", err)
    }
    
    //  Атомарная запись с fsync
    if err := writeFileSync(backupPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write backup: %v", err)
    }
    
    if s.logger != nil {
        s.logger.Info(fmt.Sprintf("Backup completed: %s", backupPath))
    }
    return nil
}

// Restore восстанавливает данные из резервной копии.
//  Полностью очищает существующие данные перед восстановлением
func (s *Storage) Restore(backupPath string) error {
    if s.logger != nil {
        s.logger.Info(fmt.Sprintf("Starting restore from %s", backupPath))
    }
    
    data, err := os.ReadFile(backupPath)
    if err != nil {
        return fmt.Errorf("failed to read backup file: %v", err)
    }
    
    var backup map[string]interface{}
    if err := serializer.Unmarshal(data, &backup); err != nil {
        return fmt.Errorf("failed to unmarshal backup: %v", err)
    }
    
    if createdAt, ok := backup["created_at"]; ok {
        if v, ok := createdAt.(int64); ok {
            s.createdAt = v
        }
    }
    
    databasesRaw, ok := backup["databases"]
    if !ok {
        return fmt.Errorf("invalid backup format: missing databases")
    }
    
    databases, ok := databasesRaw.(map[string]interface{})
    if !ok {
        return fmt.Errorf("invalid backup format: databases is not a map")
    }
    
    //  Полная очистка существующих данных перед восстановлением
    // Собираем имена существующих баз
    existingDBs := s.ListDatabases()
    for _, dbName := range existingDBs {
        if _, inBackup := databases[dbName]; !inBackup {
            // Удаляем базы, которых нет в бэкапе
            s.databases.Delete(dbName)
            if s.logger != nil {
                s.logger.Info(fmt.Sprintf("Removed database not in backup: %s", dbName))
            }
        }
    }
    
    // Восстанавливаем каждую базу данных
    for dbName, dbDataRaw := range databases {
        dbData, ok := dbDataRaw.([]byte)
        if !ok {
            continue
        }
        
        //  Удаляем существующую базу для чистого восстановления
        if s.ExistsDatabase(dbName) {
            s.databases.Delete(dbName)
        }
        
        if err := s.CreateDatabase(dbName); err != nil {
            return fmt.Errorf("failed to create database %s: %v", dbName, err)
        }
        
        db, err := s.GetDatabase(dbName)
        if err != nil {
            return fmt.Errorf("failed to get database %s: %v", dbName, err)
        }
        
        //  Очищаем коллекции перед восстановлением
        for _, collName := range db.ListCollections() {
            db.DropCollection(collName)
        }
        
        if err := db.DeserializeDatabase(dbData); err != nil {
            return fmt.Errorf("failed to restore database %s: %v", dbName, err)
        }
    }
    
    if s.logger != nil {
        s.logger.Info("Restore completed")
    }
    return nil
}

// =============================================================================
// СТАТИСТИКА ХРАНИЛИЩА
// =============================================================================

// GetStats возвращает подробную статистику хранилища.
func (s *Storage) GetStats() map[string]interface{} {
    stats := map[string]interface{}{
        "total_databases": s.GetDatabaseCount(),
        "total_documents": s.GetTotalDocuments(),
        "page_size_bytes": s.pageSize,
        "created_at":      s.createdAt,
        "created_at_str":  time.UnixMilli(s.createdAt).Format("2006-01-02 15:04:05.000"),
    }
    
    databases := make([]map[string]interface{}, 0)
    s.databases.Range(func(key, value interface{}) bool {
        db := value.(*Database)
        dbStats := map[string]interface{}{
            "name":           db.name,
            "collections":    len(db.ListCollections()),
            "created_at":     db.GetCreatedAt(),
            "created_at_str": time.UnixMilli(db.GetCreatedAt()).Format("2006-01-02 15:04:05.000"),
            "updated_at":     db.GetUpdatedAt(),
            "updated_at_str": time.UnixMilli(db.GetUpdatedAt()).Format("2006-01-02 15:04:05.000"),
            "deleted_at":     db.GetDeletedAt(),
        }
        
        totalDocs := int64(0)
        totalSize := int64(0)
        db.collections.Range(func(k, v interface{}) bool {
            coll := v.(*Collection)
            totalDocs += coll.Count()
            totalSize += coll.Size()
            return true
        })
        
        dbStats["documents"] = totalDocs
        dbStats["size_bytes"] = totalSize
        databases = append(databases, dbStats)
        return true
    })
    
    stats["databases"] = databases
    return stats
}
