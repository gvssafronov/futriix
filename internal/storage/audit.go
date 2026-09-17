/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/audit.go
// Назначение: Аудит всех операций создания, изменения, удаления данных
// с записью временной метки с точностью до миллисекунды
// Аудит теперь персистентный - пишется в файл с fsync

package storage

import (
    "bufio"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

// AuditEntry представляет запись аудита
type AuditEntry struct {
    ID           string                 `msgpack:"id"`
    Timestamp    int64                  `msgpack:"timestamp"`     // Unix миллисекунды
    TimestampStr string                 `msgpack:"timestamp_str"` // Человекочитаемая строка
    Operation    string                 `msgpack:"operation"`     // CREATE, UPDATE, DELETE, START, COMMIT, ABORT, CLUSTER, SOFT_DELETE, RESTORE, PERMANENT_DELETE
    DataType     string                 `msgpack:"data_type"`     // DATABASE, COLLECTION, DOCUMENT, FIELD, TUPLE, SESSION, TRANSACTION, CLUSTER, INDEX
    Name         string                 `msgpack:"name"`          // Имя объекта
    Details      map[string]interface{} `msgpack:"details"`       // Детали операции
}

// AuditLogger управляет аудитом
// Добавлена персистентность (файл + fsync)
type AuditLogger struct {
    entries   []AuditEntry
    mu        sync.RWMutex
    filePath  string
    fileMu    sync.Mutex
    file      *os.File
    writer    *bufio.Writer
    enabled   bool
    maxMemory int // Максимум записей в памяти
}

var globalAuditLogger = &AuditLogger{
    entries:   make([]AuditEntry, 0),
    maxMemory: 100000,
    enabled:   true,
}

// InitAuditLogger инициализирует персистентный аудит логгер
// Добавлена инициализация файла для аудита
func InitAuditLogger(filePath string) error {
    globalAuditLogger.mu.Lock()
    defer globalAuditLogger.mu.Unlock()

    if filePath == "" {
        return nil // Аудит только в памяти
    }

    if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
        return fmt.Errorf("failed to create audit log directory: %v", err)
    }

    f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return fmt.Errorf("failed to open audit log: %v", err)
    }

    globalAuditLogger.filePath = filePath
    globalAuditLogger.file = f
    globalAuditLogger.writer = bufio.NewWriterSize(f, 64*1024)

    // Загружаем последние записи из файла
    globalAuditLogger.loadFromFileLocked()

    return nil
}

// loadFromFileLocked загружает последние записи из файла (вызывается под mu)
func (al *AuditLogger) loadFromFileLocked() {
    if al.filePath == "" {
        return
    }

    data, err := os.ReadFile(al.filePath)
    if err != nil {
        return
    }

    // Читаем построчно JSON-lines формат
    lines := splitLines(data)
    start := 0
    if len(lines) > al.maxMemory {
        start = len(lines) - al.maxMemory
    }

    al.entries = make([]AuditEntry, 0, len(lines)-start)
    for i := start; i < len(lines); i++ {
        if len(lines[i]) == 0 {
            continue
        }
        var entry AuditEntry
        if err := json.Unmarshal(lines[i], &entry); err != nil {
            continue
        }
        al.entries = append(al.entries, entry)
    }
}

// splitLines разбивает данные на строки
func splitLines(data []byte) [][]byte {
    lines := make([][]byte, 0)
    start := 0
    for i := 0; i < len(data); i++ {
        if data[i] == '\n' {
            lines = append(lines, data[start:i])
            start = i + 1
        }
    }
    if start < len(data) {
        lines = append(lines, data[start:])
    }
    return lines
}

// CloseAuditLogger закрывает аудит логгер с fsync
func CloseAuditLogger() error {
    globalAuditLogger.mu.Lock()
    defer globalAuditLogger.mu.Unlock()

    if globalAuditLogger.writer != nil {
        if err := globalAuditLogger.writer.Flush(); err != nil {
            return err
        }
    }
    if globalAuditLogger.file != nil {
        if err := RealFsync(globalAuditLogger.file); err != nil {
            return err
        }
        return globalAuditLogger.file.Close()
    }
    return nil
}

// GetCurrentTimestamp возвращает текущую временную метку с миллисекундами
func GetCurrentTimestamp() (int64, string) {
    now := time.Now()
    timestampMs := now.UnixMilli()
    timestampStr := now.Format("2006-01-02 15:04:05.000")
    return timestampMs, timestampStr
}

// LogAudit записывает событие в аудит
// Добавлена запись в файл с fsync
func LogAudit(operation, dataType, name string, details map[string]interface{}) {
    if !globalAuditLogger.enabled {
        return
    }

    timestampMs, timestampStr := GetCurrentTimestamp()

    // Если details не содержат timestamp, добавляем его
    if details == nil {
        details = make(map[string]interface{})
    }
    if _, ok := details["audit_timestamp"]; !ok {
        details["audit_timestamp"] = timestampMs
        details["audit_timestamp_str"] = timestampStr
    }

    entry := AuditEntry{
        ID:           fmt.Sprintf("%d", timestampMs),
        Timestamp:    timestampMs,
        TimestampStr: timestampStr,
        Operation:    operation,
        DataType:     dataType,
        Name:         name,
        Details:      details,
    }

    globalAuditLogger.mu.Lock()
    globalAuditLogger.entries = append(globalAuditLogger.entries, entry)
    // Ограничиваем память
    if len(globalAuditLogger.entries) > globalAuditLogger.maxMemory {
        globalAuditLogger.entries = globalAuditLogger.entries[len(globalAuditLogger.entries)-globalAuditLogger.maxMemory:]
    }
    globalAuditLogger.mu.Unlock()

    // Персистентная запись в файл
    globalAuditLogger.persistEntry(entry)
}

// persistEntry записывает запись в файл с fsync
func (al *AuditLogger) persistEntry(entry AuditEntry) {
    if al.file == nil {
        return
    }

    al.fileMu.Lock()
    defer al.fileMu.Unlock()

    data, err := json.Marshal(entry)
    if err != nil {
        return
    }

    data = append(data, '\n')
    if _, err := al.writer.Write(data); err != nil {
        return
    }

    // Flush + fsync для гарантии сохранности
    if err := al.writer.Flush(); err != nil {
        return
    }
    RealFsync(al.file)
}

// GetAuditLog возвращает копию лога аудита
func GetAuditLog() []AuditEntry {
    globalAuditLogger.mu.RLock()
    defer globalAuditLogger.mu.RUnlock()

    result := make([]AuditEntry, len(globalAuditLogger.entries))
    copy(result, globalAuditLogger.entries)
    return result
}

// GetAuditLogFiltered возвращает отфильтрованный лог аудита
func GetAuditLogFiltered(dataType, operation string, fromTime, toTime int64) []AuditEntry {
    globalAuditLogger.mu.RLock()
    defer globalAuditLogger.mu.RUnlock()

    result := make([]AuditEntry, 0)
    for _, entry := range globalAuditLogger.entries {
        if dataType != "" && entry.DataType != dataType {
            continue
        }
        if operation != "" && entry.Operation != operation {
            continue
        }
        if fromTime > 0 && entry.Timestamp < fromTime {
            continue
        }
        if toTime > 0 && entry.Timestamp > toTime {
            continue
        }
        result = append(result, entry)
    }
    return result
}

// ClearAuditLog очищает лог аудита (только для отладки)
func ClearAuditLog() {
    globalAuditLogger.mu.Lock()
    defer globalAuditLogger.mu.Unlock()
    globalAuditLogger.entries = make([]AuditEntry, 0)
}

// GetAuditLogSize возвращает количество записей в логе аудита
func GetAuditLogSize() int {
    globalAuditLogger.mu.RLock()
    defer globalAuditLogger.mu.RUnlock()
    return len(globalAuditLogger.entries)
}

// AuditDatabaseOperation логирует операцию с базой данных
func AuditDatabaseOperation(operation, dbName string) {
    LogAudit(operation, "DATABASE", dbName, map[string]interface{}{
        "database": dbName,
    })
}

// AuditCollectionOperation логирует операцию с коллекцией
func AuditCollectionOperation(operation, dbName, collName string, settings interface{}) {
    LogAudit(operation, "COLLECTION", fmt.Sprintf("%s.%s", dbName, collName), map[string]interface{}{
        "database":   dbName,
        "collection": collName,
        "settings":   settings,
    })
}

// AuditDocumentOperation логирует операцию с документом
func AuditDocumentOperation(operation, dbName, collName, docID string, fields map[string]interface{}) {
    LogAudit(operation, "DOCUMENT", fmt.Sprintf("%s.%s.%s", dbName, collName, docID), map[string]interface{}{
        "database":    dbName,
        "collection":  collName,
        "document_id": docID,
        "fields":      fields,
    })
}

// AuditFieldOperation логирует операцию с полем
func AuditFieldOperation(operation, dbName, collName, docID, fieldName string, value interface{}) {
    LogAudit(operation, "FIELD", fmt.Sprintf("%s.%s.%s.%s", dbName, collName, docID, fieldName), map[string]interface{}{
        "database":    dbName,
        "collection":  collName,
        "document_id": docID,
        "field":       fieldName,
        "value":       value,
    })
}

// AuditTupleOperation логирует операцию с кортежем
func AuditTupleOperation(operation, dbName, collName, docID, tuplePath string) {
    LogAudit(operation, "TUPLE", fmt.Sprintf("%s.%s.%s.%s", dbName, collName, docID, tuplePath), map[string]interface{}{
        "database":    dbName,
        "collection":  collName,
        "document_id": docID,
        "tuple_path":  tuplePath,
    })
}

// AuditIndexOperation логирует операцию с индексом
func AuditIndexOperation(operation, dbName, collName, indexName string, fields []string, unique bool) {
    LogAudit(operation, "INDEX", fmt.Sprintf("%s.%s.%s", dbName, collName, indexName), map[string]interface{}{
        "database":   dbName,
        "collection": collName,
        "index_name": indexName,
        "fields":     fields,
        "unique":     unique,
    })
}
