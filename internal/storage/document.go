/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/document.go
// Назначение: Определение структуры документа, его методов для работы
// с полями, кортежами (вложенными документами) и сериализации в MessagePack.
// Документ является основной единицей хранения в СУБД futriis.
// Lock-free: Document.fields переведён на atomic.Value для wait-free доступа.

package storage

import (
    "fmt"
    "strings"
    "sync"
    "sync/atomic"
    "time"
    
    "futriis/internal/compression"
    "futriis/internal/serializer"
    "github.com/google/uuid"
)

// Document представляет документ в коллекции (аналог строки в реляционной СУБД)
type Document struct {
    ID           string         `msgpack:"_id"`           // Уникальный идентификатор документа
    fieldsPtr    atomic.Value   // map[string]interface{} - lock-free хранилище полей
    CreatedAt    int64          `msgpack:"created_at"`    // Время создания (Unix миллисекунды)
    UpdatedAt    int64          `msgpack:"updated_at"`    // Время последнего обновления
    DeletedAt    int64          `msgpack:"deleted_at"`    // Время удаления (Unix миллисекунды, 0 = не удалён)
    Version      uint64         `msgpack:"version"`       // Версия документа (для оптимистичных блокировок)
    Compressed   bool           `msgpack:"compressed"`    // Флаг, сжат ли документ
    OriginalSize int64          `msgpack:"original_size"` // Оригинальный размер до сжатия
}

// Tuple представляет вложенный документ (аналог кортежа в реляционной СУБД)
type Tuple struct {
    Fields    map[string]interface{} `msgpack:"fields"`
    CreatedAt int64                  `msgpack:"created_at"` // Время создания кортежа
    UpdatedAt int64                  `msgpack:"updated_at"` // Время последнего обновления кортежа
    mu        sync.RWMutex
}

// Field представляет отдельное поле документа (аналог колонки)
type Field struct {
    Name      string      `msgpack:"name"`
    Type      FieldType   `msgpack:"type"`
    Value     interface{} `msgpack:"value"`
    UpdatedAt int64       `msgpack:"updated_at"` // Время последнего обновления поля
}

// FieldType определяет тип поля документа
type FieldType int

const (
    TypeString FieldType = iota
    TypeNumber
    TypeBoolean
    TypeTuple // Вложенный документ
    TypeArray
    TypeNull
)

// NewDocument создаёт новый документ с автоматической генерацией ID
func NewDocument() *Document {
    now := time.Now().UnixMilli()
    d := &Document{
        ID:           uuid.New().String(),
        CreatedAt:    now,
        UpdatedAt:    now,
        DeletedAt:    0,
        Version:      1,
        Compressed:   false,
        OriginalSize: 0,
    }
    d.fieldsPtr.Store(make(map[string]interface{}))
    return d
}

// NewDocumentWithID создаёт документ с указанным ID
func NewDocumentWithID(id string) *Document {
    now := time.Now().UnixMilli()
    d := &Document{
        ID:           id,
        CreatedAt:    now,
        UpdatedAt:    now,
        DeletedAt:    0,
        Version:      1,
        Compressed:   false,
        OriginalSize: 0,
    }
    d.fieldsPtr.Store(make(map[string]interface{}))
    return d
}

// loadFields загружает карту полей (lock-free)
func (d *Document) loadFields() map[string]interface{} {
    val := d.fieldsPtr.Load()
    if val == nil {
        return make(map[string]interface{})
    }
    return val.(map[string]interface{})
}

// storeFields сохраняет карту полей (lock-free)
func (d *Document) storeFields(newMap map[string]interface{}) {
    d.fieldsPtr.Store(newMap)
}

// SetField устанавливает значение поля документа (lock-free)
func (d *Document) SetField(name string, value interface{}) {
    for {
        oldFields := d.loadFields()
        newFields := make(map[string]interface{})
        for k, v := range oldFields {
            newFields[k] = v
        }
        newFields[name] = value
        
        if d.compareAndSwapFields(oldFields, newFields) {
            d.UpdatedAt = time.Now().UnixMilli()
            d.Version++
            d.Compressed = false
            
            // Аудит изменения поля
            AuditFieldOperation("UPDATE", "", "", d.ID, name, value)
            return
        }
    }
}

// compareAndSwapFields выполняет CAS операцию для полей (lock-free)
func (d *Document) compareAndSwapFields(old, new map[string]interface{}) bool {
    return d.fieldsPtr.CompareAndSwap(old, new)
}

// GetField возвращает значение поля документа (lock-free)
func (d *Document) GetField(name string) (interface{}, error) {
    fields := d.loadFields()
    if val, ok := fields[name]; ok {
        return val, nil
    }
    return nil, fmt.Errorf("field not found: %s", name)
}

// DeleteField удаляет поле из документа (lock-free)
func (d *Document) DeleteField(name string) {
    for {
        oldFields := d.loadFields()
        if _, exists := oldFields[name]; !exists {
            return
        }
        
        newFields := make(map[string]interface{})
        for k, v := range oldFields {
            if k != name {
                newFields[k] = v
            }
        }
        
        if d.compareAndSwapFields(oldFields, newFields) {
            d.UpdatedAt = time.Now().UnixMilli()
            d.Version++
            d.Compressed = false
            
            // Аудит удаления поля
            AuditFieldOperation("DELETE", "", "", d.ID, name, nil)
            return
        }
    }
}

// HasField проверяет наличие поля в документе (lock-free)
func (d *Document) HasField(name string) bool {
    fields := d.loadFields()
    _, ok := fields[name]
    return ok
}

// GetFields возвращает копию всех полей документа (lock-free)
func (d *Document) GetFields() map[string]interface{} {
    fields := d.loadFields()
    copy := make(map[string]interface{})
    for k, v := range fields {
        copy[k] = v
    }
    return copy
}

// ToMap возвращает полное представление документа в виде map
// Включает метаданные (_id, _created_at, _updated_at, _deleted_at, _version) и все поля
func (d *Document) ToMap() map[string]interface{} {
    fields := d.GetFields()
    result := make(map[string]interface{})
    
    // Добавляем все пользовательские поля
    for k, v := range fields {
        result[k] = v
    }
    
    // Добавляем метаданные
    result["_id"] = d.ID
    result["_created_at"] = d.CreatedAt
    result["_updated_at"] = d.UpdatedAt
    result["_deleted_at"] = d.DeletedAt
    result["_version"] = d.Version
    
    return result
}

// SetTuple устанавливает вложенный документ (кортеж) в поле
func (d *Document) SetTuple(fieldName string, tuple *Tuple) {
    d.SetField(fieldName, tuple)
}

// GetTuple возвращает вложенный документ из поля
func (d *Document) GetTuple(fieldName string) (*Tuple, error) {
    val, err := d.GetField(fieldName)
    if err != nil {
        return nil, err
    }

    if tuple, ok := val.(*Tuple); ok {
        return tuple, nil
    }
    return nil, fmt.Errorf("field %s is not a tuple", fieldName)
}

// Serialize сериализует документ в MessagePack с поддержкой сжатия
func (d *Document) Serialize() ([]byte, error) {
    // Создаём копию для сериализации
    fields := d.GetFields()
    docCopy := &Document{
        ID:           d.ID,
        CreatedAt:    d.CreatedAt,
        UpdatedAt:    d.UpdatedAt,
        DeletedAt:    d.DeletedAt,
        Version:      d.Version,
        Compressed:   d.Compressed,
        OriginalSize: d.OriginalSize,
    }
    docCopy.fieldsPtr.Store(fields)
    
    data, err := serializer.Marshal(docCopy)
    if err != nil {
        return nil, err
    }

    return data, nil
}

// SerializeCompressed сериализует и сжимает документ
func (d *Document) SerializeCompressed(compressionConfig *compression.Config) ([]byte, error) {
    data, err := d.Serialize()
    if err != nil {
        return nil, err
    }

    // Проверяем, нужно ли сжимать
    if compressionConfig != nil && compressionConfig.Enabled && len(data) >= compressionConfig.MinSize {
        compressed, err := compression.Compress(data, compressionConfig)
        if err != nil {
            return data, nil
        }
        return compressed, nil
    }

    return data, nil
}

// Deserialize десериализует документ из MessagePack (автоматически определяет сжатие)
func (d *Document) Deserialize(data []byte) error {
    // Пытаемся определить, сжаты ли данные
    decompressed, err := compression.DecompressAuto(data)
    if err == nil && len(decompressed) < len(data) {
        var doc Document
        if err := serializer.Unmarshal(decompressed, &doc); err != nil {
            return err
        }
        d.ID = doc.ID
        d.fieldsPtr.Store(doc.loadFields())
        d.CreatedAt = doc.CreatedAt
        d.UpdatedAt = doc.UpdatedAt
        d.DeletedAt = doc.DeletedAt
        d.Version = doc.Version
        d.Compressed = true
        d.OriginalSize = int64(len(decompressed))
    } else {
        var doc Document
        if err := serializer.Unmarshal(data, &doc); err != nil {
            return err
        }
        d.ID = doc.ID
        d.fieldsPtr.Store(doc.loadFields())
        d.CreatedAt = doc.CreatedAt
        d.UpdatedAt = doc.UpdatedAt
        d.DeletedAt = doc.DeletedAt
        d.Version = doc.Version
        d.Compressed = false
        d.OriginalSize = 0
    }

    d.UpdatedAt = time.Now().UnixMilli()
    return nil
}

// Clone создаёт глубокую копию документа (lock-free)
func (d *Document) Clone() *Document {
    fields := d.GetFields()
    clone := &Document{
        ID:           d.ID,
        CreatedAt:    d.CreatedAt,
        UpdatedAt:    d.UpdatedAt,
        DeletedAt:    d.DeletedAt,
        Version:      d.Version,
        Compressed:   d.Compressed,
        OriginalSize: d.OriginalSize,
    }
    
    // Глубокое копирование полей
    copiedFields := make(map[string]interface{})
    for k, v := range fields {
        copiedFields[k] = deepCopyValue(v)
    }
    clone.fieldsPtr.Store(copiedFields)
    
    return clone
}

// Update применяет обновление к документу (атомарно, lock-free)
func (d *Document) Update(updates map[string]interface{}) error {
    for {
        oldFields := d.loadFields()
        newFields := make(map[string]interface{})
        for k, v := range oldFields {
            newFields[k] = v
        }
        for k, v := range updates {
            newFields[k] = v
        }
        
        if d.compareAndSwapFields(oldFields, newFields) {
            d.UpdatedAt = time.Now().UnixMilli()
            d.Version++
            d.Compressed = false
            return nil
        }
    }
}

// SoftDelete мягко удаляет документ (устанавливает метку времени удаления)
func (d *Document) SoftDelete() {
    d.DeletedAt = time.Now().UnixMilli()
    d.UpdatedAt = d.DeletedAt
    d.Version++
}

// IsDeleted проверяет, удалён ли документ (мягкое удаление)
func (d *Document) IsDeleted() bool {
    return d.DeletedAt > 0
}

// Restore восстанавливает мягко удалённый документ
func (d *Document) Restore() {
    d.DeletedAt = 0
    d.UpdatedAt = time.Now().UnixMilli()
    d.Version++
}

// GetDeletedAtStr возвращает человекочитаемую строку времени удаления
func (d *Document) GetDeletedAtStr() string {
    if d.DeletedAt == 0 {
        return ""
    }
    return time.UnixMilli(d.DeletedAt).Format("2006-01-02 15:04:05.000")
}

// GetCreatedAtStr возвращает человекочитаемую строку времени создания
func (d *Document) GetCreatedAtStr() string {
    return time.UnixMilli(d.CreatedAt).Format("2006-01-02 15:04:05.000")
}

// GetUpdatedAtStr возвращает человекочитаемую строку времени обновления
func (d *Document) GetUpdatedAtStr() string {
    return time.UnixMilli(d.UpdatedAt).Format("2006-01-02 15:04:05.000")
}

// Compress сжимает документ в памяти
func (d *Document) Compress(config *compression.Config) error {
    if d.Compressed {
        return nil
    }

    fields := d.loadFields()
    originalSize := len(fields)
    if originalSize < config.MinSize {
        return nil
    }

    d.Compressed = true
    d.OriginalSize = int64(originalSize)

    return nil
}

// Decompress распаковывает документ в памяти
func (d *Document) Decompress() error {
    if !d.Compressed {
        return nil
    }

    d.Compressed = false
    d.OriginalSize = 0

    return nil
}

// GetCompressionRatio возвращает коэффициент сжатия
func (d *Document) GetCompressionRatio() float64 {
    if !d.Compressed || d.OriginalSize == 0 {
        return 1.0
    }

    fields := d.loadFields()
    currentSize := len(fields)
    return float64(currentSize) / float64(d.OriginalSize)
}

// GetMetadata возвращает метаданные документа (временные метки)
func (d *Document) GetMetadata() map[string]int64 {
    return map[string]int64{
        "created_at": d.CreatedAt,
        "updated_at": d.UpdatedAt,
        "deleted_at": d.DeletedAt,
        "version":    int64(d.Version),
    }
}

// deepCopyValue выполняет глубокое копирование значения
func deepCopyValue(val interface{}) interface{} {
    switch v := val.(type) {
    case *Tuple:
        return v.Clone()
    case map[string]interface{}:
        copy := make(map[string]interface{})
        for k, val := range v {
            copy[k] = deepCopyValue(val)
        }
        return copy
    case []interface{}:
        copy := make([]interface{}, len(v))
        for i, val := range v {
            copy[i] = deepCopyValue(val)
        }
        return copy
    default:
        return v
    }
}

// NewTuple создаёт новый вложенный документ (кортеж)
func NewTuple() *Tuple {
    now := time.Now().UnixMilli()
    return &Tuple{
        Fields:    make(map[string]interface{}),
        CreatedAt: now,
        UpdatedAt: now,
    }
}

// Set устанавливает поле во вложенном документе
func (t *Tuple) Set(name string, value interface{}) {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.Fields[name] = value
    t.UpdatedAt = time.Now().UnixMilli()
}

// Get возвращает поле из вложенного документа
func (t *Tuple) Get(name string) (interface{}, error) {
    t.mu.RLock()
    defer t.mu.RUnlock()

    if val, ok := t.Fields[name]; ok {
        return val, nil
    }
    return nil, fmt.Errorf("tuple field not found: %s", name)
}

// Clone создаёт копию кортежа
func (t *Tuple) Clone() *Tuple {
    t.mu.RLock()
    defer t.mu.RUnlock()

    clone := NewTuple()
    for k, v := range t.Fields {
        clone.Fields[k] = deepCopyValue(v)
    }
    clone.CreatedAt = t.CreatedAt
    clone.UpdatedAt = t.UpdatedAt
    return clone
}

// ToMap конвертирует кортеж в map
func (t *Tuple) ToMap() map[string]interface{} {
    t.mu.RLock()
    defer t.mu.RUnlock()

    copy := make(map[string]interface{})
    for k, v := range t.Fields {
        copy[k] = v
    }
    return copy
}

// GetTupleMetadata возвращает метаданные кортежа
func (t *Tuple) GetTupleMetadata() map[string]int64 {
    t.mu.RLock()
    defer t.mu.RUnlock()
    
    return map[string]int64{
        "created_at": t.CreatedAt,
        "updated_at": t.UpdatedAt,
    }
}

// GetNestedField получает значение по точечному пути (например, "user.address.city")
func (d *Document) GetNestedField(path string) (interface{}, error) {
    parts := strings.Split(path, ".")
    if len(parts) == 0 {
        return nil, fmt.Errorf("empty path")
    }

    current := interface{}(d)
    for _, part := range parts {
        switch v := current.(type) {
        case *Document:
            val, err := v.GetField(part)
            if err != nil {
                return nil, err
            }
            current = val
        case *Tuple:
            val, err := v.Get(part)
            if err != nil {
                return nil, err
            }
            current = val
        case map[string]interface{}:
            if val, ok := v[part]; ok {
                current = val
            } else {
                return nil, fmt.Errorf("field not found: %s", part)
            }
        default:
            return nil, fmt.Errorf("cannot navigate into non-document value at %s", part)
        }
    }

    return current, nil
}

// SetNestedField устанавливает значение по точечному пути
func (d *Document) SetNestedField(path string, value interface{}) error {
    parts := strings.Split(path, ".")
    if len(parts) == 0 {
        return fmt.Errorf("empty path")
    }

    if len(parts) == 1 {
        d.SetField(parts[0], value)
        return nil
    }

    // Для простоты реализации используем подход с чтением-модификацией-записью
    // В production коде потребуется более сложная lock-free структура
    
    // Сначала проверяем путь
    var current interface{} = d
    for i := 0; i < len(parts)-1; i++ {
        part := parts[i]
        
        switch v := current.(type) {
        case *Document:
            if !v.HasField(part) {
                newTuple := NewTuple()
                v.SetField(part, newTuple)
                current = newTuple
            } else {
                field, _ := v.GetField(part)
                if tuple, ok := field.(*Tuple); ok {
                    current = tuple
                } else {
                    return fmt.Errorf("field %s is not a tuple", part)
                }
            }
        case *Tuple:
            if val, err := v.Get(part); err == nil {
                if tuple, ok := val.(*Tuple); ok {
                    current = tuple
                } else {
                    return fmt.Errorf("field %s is not a tuple", part)
                }
            } else {
                newTuple := NewTuple()
                v.Set(part, newTuple)
                current = newTuple
            }
        default:
            return fmt.Errorf("cannot set nested field on non-document value")
        }
    }

    lastPart := parts[len(parts)-1]
    switch v := current.(type) {
    case *Document:
        v.SetField(lastPart, value)
    case *Tuple:
        v.Set(lastPart, value)
    default:
        return fmt.Errorf("cannot set field on non-document value")
    }

    d.UpdatedAt = time.Now().UnixMilli()
    d.Compressed = false
    return nil
}
