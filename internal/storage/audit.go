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

package storage

import (
	"fmt"
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
type AuditLogger struct {
	entries []AuditEntry
	mu      sync.RWMutex
}

var globalAuditLogger = &AuditLogger{
	entries: make([]AuditEntry, 0),
}

// GetCurrentTimestamp возвращает текущую временную метку с миллисекундами
func GetCurrentTimestamp() (int64, string) {
	now := time.Now()
	timestampMs := now.UnixMilli()
	timestampStr := now.Format("2006-01-02 15:04:05.000")
	return timestampMs, timestampStr
}

// LogAudit записывает событие в аудит
func LogAudit(operation, dataType, name string, details map[string]interface{}) {
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
	globalAuditLogger.mu.Unlock()
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
