/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/trigger.go
// Назначение: Реализация триггеров, похожих на MongoDB trigger syntax.
// Поддерживает события: INSERT, UPDATE, DELETE, REPLACE.
// Триггеры могут выполняться до или после события.

package storage

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"futriis/internal/log"
)

// =============================================================================
// КОНСТАНТЫ
// =============================================================================

const (
	MaxVersionsPerDoc = 100
	VersionRetentionDays = 7
	VisibilityMapSize = 1024 * 1024
)

// =============================================================================
// ТИПЫ ТРИГГЕРОВ
// =============================================================================

// TriggerEvent определяет тип события для триггера
type TriggerEvent string

const (
	TriggerBeforeInsert TriggerEvent = "BEFORE_INSERT"
	TriggerAfterInsert  TriggerEvent = "AFTER_INSERT"
	TriggerBeforeUpdate TriggerEvent = "BEFORE_UPDATE"
	TriggerAfterUpdate  TriggerEvent = "AFTER_UPDATE"
	TriggerBeforeDelete TriggerEvent = "BEFORE_DELETE"
	TriggerAfterDelete  TriggerEvent = "AFTER_DELETE"
	TriggerBeforeReplace TriggerEvent = "BEFORE_REPLACE"
	TriggerAfterReplace  TriggerEvent = "AFTER_REPLACE"
)

// TriggerAction определяет действие триггера
type TriggerAction string

const (
	ActionAbort  TriggerAction = "abort"   // Прервать операцию
	ActionSkip   TriggerAction = "skip"    // Пропустить операцию
	ActionModify TriggerAction = "modify"  // Модифицировать документ
	ActionLog    TriggerAction = "log"     // Записать в лог
	ActionNotify TriggerAction = "notify"  // Отправить уведомление
	ActionCustom TriggerAction = "custom"  // Пользовательское действие
)

// Trigger представляет триггер на коллекции
type Trigger struct {
	Name         string                 `msgpack:"name" json:"name"`
	Collection   string                 `msgpack:"collection" json:"collection"`
	Event        string                 `msgpack:"event" json:"event"`
	Action       string                 `msgpack:"action" json:"action"`
	Condition    *TriggerCondition      `msgpack:"condition" json:"condition"`
	Operations   []TriggerOperation     `msgpack:"operations" json:"operations"`
	CreatedAt    int64                  `msgpack:"created_at" json:"created_at"`
	UpdatedAt    int64                  `msgpack:"updated_at" json:"updated_at"`
	Enabled      bool                   `msgpack:"enabled" json:"enabled"`
	Description  string                 `msgpack:"description" json:"description"`
	EventType    TriggerEvent           `msgpack:"-" json:"-"`
	ActionType   TriggerAction          `msgpack:"-" json:"-"`
	mu           sync.RWMutex           `msgpack:"-" json:"-"`
}

// GetEventType возвращает типизированное событие
func (t *Trigger) GetEventType() TriggerEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.EventType != "" {
		return t.EventType
	}
	return TriggerEvent(t.Event)
}

// GetActionType возвращает типизированное действие
func (t *Trigger) GetActionType() TriggerAction {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.ActionType != "" {
		return t.ActionType
	}
	return TriggerAction(t.Action)
}

// SetEventType устанавливает типизированное событие
func (t *Trigger) SetEventType(event TriggerEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.EventType = event
	t.Event = string(event)
}

// SetActionType устанавливает типизированное действие
func (t *Trigger) SetActionType(action TriggerAction) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ActionType = action
	t.Action = string(action)
}

// IsEnabled проверяет, включен ли триггер
func (t *Trigger) IsEnabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Enabled
}

// GetEvent возвращает строковое представление события
func (t *Trigger) GetEvent() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Event
}

// GetAction возвращает строковое представление действия
func (t *Trigger) GetAction() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Action
}

// TriggerCondition определяет условие выполнения триггера
type TriggerCondition struct {
	Field    string      `msgpack:"field" json:"field"`
	Operator string      `msgpack:"operator" json:"operator"`
	Value    interface{} `msgpack:"value" json:"value"`
	Match    string      `msgpack:"match" json:"match"`
}

// TriggerOperation определяет операцию, выполняемую триггером
type TriggerOperation struct {
	Type   string                 `msgpack:"type" json:"type"`
	Field  string                 `msgpack:"field" json:"field"`
	Value  interface{}            `msgpack:"value" json:"value"`
	Params map[string]interface{} `msgpack:"params" json:"params"`
}

// TriggerExecution содержит контекст выполнения триггера
type TriggerExecution struct {
	TriggerName  string
	Event        TriggerEvent
	Collection   string
	Database     string
	DocumentID   string
	OldDocument  *Document
	NewDocument  *Document
	Operation    string
	Timestamp    time.Time
	TimestampMs  int64                  `msgpack:"timestamp_ms"`
	TimestampStr string                 `msgpack:"timestamp_str"`
	User         string
	Role         string
	CustomData   map[string]interface{}
	ActionResult string                 `msgpack:"action_result"`
	DurationMs   int64                  `msgpack:"duration_ms"`
}

// =============================================================================
// ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ ЛОГА ВЫПОЛНЕНИЯ ТРИГГЕРОВ
// =============================================================================

// triggerExecutionLog - глобальный лог выполнения триггеров
var triggerExecutionLog []TriggerExecutionLogEntry
var triggerExecutionLogMu sync.RWMutex

// TriggerExecutionLogEntry представляет запись выполнения триггера
type TriggerExecutionLogEntry struct {
	Timestamp    time.Time `json:"timestamp"`
	TriggerName  string    `json:"trigger_name"`
	Event        string    `json:"event"`
	Collection   string    `json:"collection"`
	DocumentID   string    `json:"document_id"`
	Action       string    `json:"action"`
	Success      bool      `json:"success"`
	Error        string    `json:"error,omitempty"`
}

// GetTriggerExecutionLog возвращает лог выполнения триггеров
func GetTriggerExecutionLog() []TriggerExecutionLogEntry {
	triggerExecutionLogMu.RLock()
	defer triggerExecutionLogMu.RUnlock()
	result := make([]TriggerExecutionLogEntry, len(triggerExecutionLog))
	copy(result, triggerExecutionLog)
	return result
}

// LogTriggerExecution записывает выполнение триггера
func LogTriggerExecution(triggerName, event, collection, docID, action string, success bool, err error) {
	triggerExecutionLogMu.Lock()
	defer triggerExecutionLogMu.Unlock()
	
	entry := TriggerExecutionLogEntry{
		Timestamp:   time.Now(),
		TriggerName: triggerName,
		Event:       event,
		Collection:  collection,
		DocumentID:  docID,
		Action:      action,
		Success:     success,
	}
	if err != nil {
		entry.Error = err.Error()
	}
	
	triggerExecutionLog = append(triggerExecutionLog, entry)
	if len(triggerExecutionLog) > 1000 {
		triggerExecutionLog = triggerExecutionLog[len(triggerExecutionLog)-1000:]
	}
}

// =============================================================================
// TRIGGER MANAGER
// =============================================================================

// TriggerManager управляет триггерами в СУБД
type TriggerManager struct {
	triggers   sync.Map              // map[string]*Trigger (ключ: collection|event|name)
	logger     *log.Logger
	mu         sync.RWMutex
	auditLog   []*TriggerExecution
	maxLogSize int
}

var (
	globalTriggerManager *TriggerManager
	triggerManagerOnce   sync.Once
)

// GetTriggerManager возвращает глобальный менеджер триггеров
func GetTriggerManager() *TriggerManager {
	triggerManagerOnce.Do(func() {
		globalTriggerManager = &TriggerManager{
			maxLogSize: 10000,
			auditLog:   make([]*TriggerExecution, 0),
		}
	})
	return globalTriggerManager
}

// InitTriggerManager инициализирует менеджер триггеров с логгером
func InitTriggerManager(logger *log.Logger) {
	tm := GetTriggerManager()
	tm.logger = logger
	if logger != nil {
		logger.Info("Trigger manager initialized")
	}
}

// CreateTrigger создаёт новый триггер (синтаксис MongoDB-like)
func (tm *TriggerManager) CreateTrigger(database, collection, name string, event TriggerEvent, config map[string]interface{}) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	key := tm.getTriggerKey(collection, event, name)
	if _, exists := tm.triggers.Load(key); exists {
		return fmt.Errorf("trigger '%s' already exists on %s for event %s", name, collection, event)
	}

	now := time.Now().UnixMilli()
	
	trigger := &Trigger{
		Name:        name,
		Collection:  collection,
		Event:       string(event),
		EventType:   event,
		Enabled:     true,
		CreatedAt:   now,
		UpdatedAt:   now,
		Operations:  make([]TriggerOperation, 0),
	}

	// Парсим конфигурацию триггера
	if action, ok := config["action"].(string); ok {
		switch strings.ToLower(action) {
		case "abort":
			trigger.Action = string(ActionAbort)
			trigger.ActionType = ActionAbort
		case "skip":
			trigger.Action = string(ActionSkip)
			trigger.ActionType = ActionSkip
		case "modify":
			trigger.Action = string(ActionModify)
			trigger.ActionType = ActionModify
		case "log":
			trigger.Action = string(ActionLog)
			trigger.ActionType = ActionLog
		case "notify":
			trigger.Action = string(ActionNotify)
			trigger.ActionType = ActionNotify
		default:
			trigger.Action = string(ActionCustom)
			trigger.ActionType = ActionCustom
		}
	}

	// Парсим условие
	if cond, ok := config["condition"].(map[string]interface{}); ok {
		trigger.Condition = &TriggerCondition{}
		if field, ok := cond["field"].(string); ok {
			trigger.Condition.Field = field
		}
		if operator, ok := cond["operator"].(string); ok {
			trigger.Condition.Operator = operator
		}
		if value, ok := cond["value"]; ok {
			trigger.Condition.Value = value
		}
		if match, ok := cond["match"].(string); ok {
			trigger.Condition.Match = match
		}
	}

	// Парсим операции
	if ops, ok := config["operations"].([]interface{}); ok {
		for _, opRaw := range ops {
			opMap, ok := opRaw.(map[string]interface{})
			if !ok {
				continue
			}
			operation := TriggerOperation{}
			if opType, ok := opMap["type"].(string); ok {
				operation.Type = opType
			}
			if field, ok := opMap["field"].(string); ok {
				operation.Field = field
			}
			if value, ok := opMap["value"]; ok {
				operation.Value = value
			}
			if params, ok := opMap["params"].(map[string]interface{}); ok {
				operation.Params = params
			}
			trigger.Operations = append(trigger.Operations, operation)
		}
	}

	if desc, ok := config["description"].(string); ok {
		trigger.Description = desc
	}

	tm.triggers.Store(key, trigger)

	// Аудит создания триггера
	LogAudit("CREATE", "TRIGGER", fmt.Sprintf("%s.%s.%s", database, collection, name), map[string]interface{}{
		"event":       event,
		"action":      trigger.Action,
		"description": trigger.Description,
	})

	if tm.logger != nil {
		tm.logger.Info(fmt.Sprintf("Trigger '%s' created on %s.%s for event %s", name, database, collection, event))
	}

	return nil
}

// DropTrigger удаляет триггер
func (tm *TriggerManager) DropTrigger(collection, event, name string) error {
	key := tm.getTriggerKey(collection, TriggerEvent(event), name)
	if _, exists := tm.triggers.LoadAndDelete(key); !exists {
		return fmt.Errorf("trigger '%s' not found on %s for event %s", name, collection, event)
	}

	// Аудит удаления триггера
	LogAudit("DROP", "TRIGGER", fmt.Sprintf("%s.%s", collection, name), map[string]interface{}{
		"event": event,
	})

	if tm.logger != nil {
		tm.logger.Info(fmt.Sprintf("Trigger '%s' dropped from %s for event %s", name, collection, event))
	}
	return nil
}

// GetTrigger возвращает триггер по имени
func (tm *TriggerManager) GetTrigger(collection, event, name string) (*Trigger, error) {
	key := tm.getTriggerKey(collection, TriggerEvent(event), name)
	if val, ok := tm.triggers.Load(key); ok {
		return val.(*Trigger), nil
	}
	return nil, fmt.Errorf("trigger not found: %s", name)
}

// ListTriggers возвращает список всех триггеров для коллекции
func (tm *TriggerManager) ListTriggers(collection string) []*Trigger {
	triggers := make([]*Trigger, 0)
	tm.triggers.Range(func(key, value interface{}) bool {
		trigger := value.(*Trigger)
		if collection == "" || trigger.Collection == collection {
			triggers = append(triggers, trigger)
		}
		return true
	})
	return triggers
}

// ListTriggersByEvent возвращает триггеры для конкретного события
func (tm *TriggerManager) ListTriggersByEvent(collection string, event TriggerEvent) []*Trigger {
	triggers := make([]*Trigger, 0)
	tm.triggers.Range(func(key, value interface{}) bool {
		trigger := value.(*Trigger)
		triggerEvent := trigger.GetEventType()
		if trigger.Collection == collection && triggerEvent == event && trigger.IsEnabled() {
			triggers = append(triggers, trigger)
		}
		return true
	})
	return triggers
}

// EnableTrigger включает триггер
func (tm *TriggerManager) EnableTrigger(collection, event, name string) error {
	key := tm.getTriggerKey(collection, TriggerEvent(event), name)
	val, ok := tm.triggers.Load(key)
	if !ok {
		return fmt.Errorf("trigger not found: %s", name)
	}
	trigger := val.(*Trigger)
	trigger.mu.Lock()
	trigger.Enabled = true
	trigger.UpdatedAt = time.Now().UnixMilli()
	trigger.mu.Unlock()
	tm.triggers.Store(key, trigger)
	
	// Аудит включения триггера
	LogAudit("ENABLE", "TRIGGER", fmt.Sprintf("%s.%s", collection, name), map[string]interface{}{
		"event": event,
	})
	
	return nil
}

// DisableTrigger выключает триггер
func (tm *TriggerManager) DisableTrigger(collection, event, name string) error {
	key := tm.getTriggerKey(collection, TriggerEvent(event), name)
	val, ok := tm.triggers.Load(key)
	if !ok {
		return fmt.Errorf("trigger not found: %s", name)
	}
	trigger := val.(*Trigger)
	trigger.mu.Lock()
	trigger.Enabled = false
	trigger.UpdatedAt = time.Now().UnixMilli()
	trigger.mu.Unlock()
	tm.triggers.Store(key, trigger)
	
	// Аудит выключения триггера
	LogAudit("DISABLE", "TRIGGER", fmt.Sprintf("%s.%s", collection, name), map[string]interface{}{
		"event": event,
	})
	
	return nil
}

// ExecuteTriggers выполняет все триггеры для данного события
// Возвращает: modifiedDocument, shouldAbort, error
func (tm *TriggerManager) ExecuteTriggers(execCtx *TriggerExecution) (*Document, bool, error) {
	triggers := tm.ListTriggersByEvent(execCtx.Collection, execCtx.Event)
	
	if len(triggers) == 0 {
		return execCtx.NewDocument, false, nil
	}

	currentDoc := execCtx.NewDocument
	if currentDoc == nil && execCtx.OldDocument != nil {
		currentDoc = execCtx.OldDocument.Clone()
	}

	for _, trigger := range triggers {
		if !trigger.IsEnabled() {
			continue
		}

		// Проверяем условие
		if trigger.Condition != nil {
			if !tm.evaluateCondition(execCtx, trigger.Condition) {
				continue
			}
		}

		startTime := time.Now()
		
		// Получаем типизированное действие
		action := trigger.GetActionType()
		
		// Выполняем действие триггера
		switch action {
		case ActionAbort:
			tm.logExecution(execCtx, trigger, "aborted", startTime)
			return currentDoc, true, fmt.Errorf("operation aborted by trigger: %s", trigger.Name)

		case ActionSkip:
			tm.logExecution(execCtx, trigger, "skipped", startTime)
			return currentDoc, true, nil

		case ActionModify:
			if currentDoc != nil {
				currentDoc = tm.applyOperations(currentDoc, trigger.Operations, execCtx)
			}
			tm.logExecution(execCtx, trigger, "modified", startTime)

		case ActionLog:
			tm.logExecution(execCtx, trigger, "logged", startTime)
			if tm.logger != nil {
				tm.logger.Info(fmt.Sprintf("Trigger %s executed on %s.%s (event: %s, doc: %s)",
					trigger.Name, execCtx.Database, execCtx.Collection, execCtx.Event, execCtx.DocumentID))
			}

		case ActionNotify:
			tm.logExecution(execCtx, trigger, "notified", startTime)
			// Здесь можно отправить уведомление через WebSocket или другой канал
		}
	}

	return currentDoc, false, nil
}

// evaluateCondition проверяет условие триггера
func (tm *TriggerManager) evaluateCondition(execCtx *TriggerExecution, cond *TriggerCondition) bool {
	var docToCheck *Document
	if execCtx.NewDocument != nil {
		docToCheck = execCtx.NewDocument
	} else if execCtx.OldDocument != nil {
		docToCheck = execCtx.OldDocument
	} else {
		return false
	}

	fieldValue, err := docToCheck.GetField(cond.Field)
	if err != nil {
		if cond.Operator == "exists" {
			if existsVal, ok := cond.Value.(bool); ok && !existsVal {
				return true
			}
		}
		return false
	}

	switch cond.Operator {
	case "eq":
		return fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", cond.Value)
	case "ne":
		return fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", cond.Value)
	case "gt":
		return compareNumbers(fieldValue, cond.Value) > 0
	case "lt":
		return compareNumbers(fieldValue, cond.Value) < 0
	case "gte":
		return compareNumbers(fieldValue, cond.Value) >= 0
	case "lte":
		return compareNumbers(fieldValue, cond.Value) <= 0
	case "in":
		if arr, ok := cond.Value.([]interface{}); ok {
			for _, v := range arr {
				if fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", v) {
					return true
				}
			}
		}
		return false
	case "nin":
		if arr, ok := cond.Value.([]interface{}); ok {
			for _, v := range arr {
				if fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", v) {
					return false
				}
			}
		}
		return true
	case "exists":
		if existsVal, ok := cond.Value.(bool); ok {
			return existsVal
		}
		return true
	case "regex":
		if pattern, ok := cond.Value.(string); ok {
			matched, _ := regexp.MatchString(pattern, fmt.Sprintf("%v", fieldValue))
			return matched
		}
		return false
	default:
		return true
	}
}

// applyOperations применяет операции к документу
func (tm *TriggerManager) applyOperations(doc *Document, ops []TriggerOperation, execCtx *TriggerExecution) *Document {
	if doc == nil {
		return nil
	}

	result := doc.Clone()

	for _, op := range ops {
		switch op.Type {
		case "set":
			value := tm.resolveValue(op.Value, execCtx)
			result.SetField(op.Field, value)

		case "unset":
			result.DeleteField(op.Field)

		case "inc":
			if incVal, ok := toFloat64(op.Value); ok {
				if current, err := result.GetField(op.Field); err == nil {
					if currVal, ok := toFloat64(current); ok {
						result.SetField(op.Field, currVal+incVal)
					}
				} else {
					result.SetField(op.Field, incVal)
				}
			}

		case "mul":
			if mulVal, ok := toFloat64(op.Value); ok {
				if current, err := result.GetField(op.Field); err == nil {
					if currVal, ok := toFloat64(current); ok {
						result.SetField(op.Field, currVal*mulVal)
					}
				}
			}

		case "rename":
			if newName, ok := op.Value.(string); ok {
				if val, err := result.GetField(op.Field); err == nil {
					result.SetField(newName, val)
					result.DeleteField(op.Field)
				}
			}

		case "currentDate":
			result.SetField(op.Field, time.Now().UnixMilli())
		}
	}

	return result
}

// resolveValue разрешает специальные значения типа $$NOW, $$USER
func (tm *TriggerManager) resolveValue(value interface{}, execCtx *TriggerExecution) interface{} {
	if strVal, ok := value.(string); ok {
		switch strVal {
		case "$$NOW":
			return time.Now().UnixMilli()
		case "$$USER":
			if execCtx.User != "" {
				return execCtx.User
			}
			return "anonymous"
		case "$$ROLE":
			if execCtx.Role != "" {
				return execCtx.Role
			}
			return "anonymous"
		}
	}
	return value
}

// logExecution логирует выполнение триггера
func (tm *TriggerManager) logExecution(execCtx *TriggerExecution, trigger *Trigger, result string, startTime time.Time) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	duration := time.Since(startTime)
	now := time.Now()
	nowMs := now.UnixMilli()
	nowStr := now.Format("2006-01-02 15:04:05.000")
	
	execCtx.TriggerName = trigger.Name
	execCtx.Timestamp = now
	execCtx.TimestampMs = nowMs
	execCtx.TimestampStr = nowStr
	execCtx.ActionResult = result
	execCtx.DurationMs = duration.Milliseconds()

	if len(tm.auditLog) >= tm.maxLogSize {
		tm.auditLog = tm.auditLog[1:]
	}
	tm.auditLog = append(tm.auditLog, execCtx)
	
	// Аудит выполнения триггера
	LogAudit("TRIGGER_EXECUTE", "TRIGGER", fmt.Sprintf("%s.%s", trigger.Collection, trigger.Name), map[string]interface{}{
		"event":       execCtx.Event,
		"result":      result,
		"duration_ms": duration.Milliseconds(),
		"document_id": execCtx.DocumentID,
	})
}

// GetTriggerExecutionLog возвращает лог выполнения триггеров
func (tm *TriggerManager) GetTriggerExecutionLog() []*TriggerExecution {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	result := make([]*TriggerExecution, len(tm.auditLog))
	copy(result, tm.auditLog)
	return result
}

// GetTriggerExecutionLogFiltered возвращает отфильтрованный лог выполнения триггеров
func (tm *TriggerManager) GetTriggerExecutionLogFiltered(triggerName, collection string, fromTime, toTime int64) []*TriggerExecution {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	result := make([]*TriggerExecution, 0)
	for _, entry := range tm.auditLog {
		if triggerName != "" && entry.TriggerName != triggerName {
			continue
		}
		if collection != "" && entry.Collection != collection {
			continue
		}
		if fromTime > 0 && entry.TimestampMs < fromTime {
			continue
		}
		if toTime > 0 && entry.TimestampMs > toTime {
			continue
		}
		result = append(result, entry)
	}
	return result
}

// ClearTriggerLog очищает лог выполнения триггеров
func (tm *TriggerManager) ClearTriggerLog() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.auditLog = make([]*TriggerExecution, 0)
	LogAudit("CLEAR", "TRIGGER_LOG", "all", nil)
}

// GetTriggerExecutionStats возвращает статистику выполнения триггеров
func (tm *TriggerManager) GetTriggerExecutionStats() map[string]interface{} {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	
	triggerStats := make(map[string]map[string]interface{})
	for _, entry := range tm.auditLog {
		if _, ok := triggerStats[entry.TriggerName]; !ok {
			triggerStats[entry.TriggerName] = map[string]interface{}{
				"total_executions": 0,
				"aborted":          0,
				"skipped":          0,
				"modified":         0,
				"logged":           0,
				"notified":         0,
				"total_duration_ms": int64(0),
				"avg_duration_ms":   float64(0),
			}
		}
		
		stats := triggerStats[entry.TriggerName]
		stats["total_executions"] = stats["total_executions"].(int) + 1
		stats["total_duration_ms"] = stats["total_duration_ms"].(int64) + entry.DurationMs
		
		switch entry.ActionResult {
		case "aborted":
			stats["aborted"] = stats["aborted"].(int) + 1
		case "skipped":
			stats["skipped"] = stats["skipped"].(int) + 1
		case "modified":
			stats["modified"] = stats["modified"].(int) + 1
		case "logged":
			stats["logged"] = stats["logged"].(int) + 1
		case "notified":
			stats["notified"] = stats["notified"].(int) + 1
		}
	}
	
	// Вычисляем среднюю длительность
	for _, stats := range triggerStats {
		if totalExec, ok := stats["total_executions"].(int); ok && totalExec > 0 {
			if totalDuration, ok := stats["total_duration_ms"].(int64); ok {
				stats["avg_duration_ms"] = float64(totalDuration) / float64(totalExec)
			}
		}
	}
	
	return map[string]interface{}{
		"total_trigger_executions": len(tm.auditLog),
		"triggers_stats":           triggerStats,
		"log_size":                 len(tm.auditLog),
		"max_log_size":             tm.maxLogSize,
	}
}

// getTriggerKey возвращает ключ для хранения триггера
func (tm *TriggerManager) getTriggerKey(collection string, event TriggerEvent, name string) string {
	return fmt.Sprintf("%s|%s|%s", collection, event, name)
}

// compareNumbers сравнивает два числа
func compareNumbers(a, b interface{}) int {
	aVal, aOk := toFloat64(a)
	bVal, bOk := toFloat64(b)
	
	if aOk && bOk {
		if aVal < bVal {
			return -1
		}
		if aVal > bVal {
			return 1
		}
		return 0
	}
	return 0
}

// =============================================================================
// TRIGGER CONFIG BUILDER
// =============================================================================

// MongoDBLikeTriggerConfig создаёт конфигурацию триггера в стиле MongoDB
func MongoDBLikeTriggerConfig() *TriggerConfigBuilder {
	return &TriggerConfigBuilder{
		config: make(map[string]interface{}),
		ops:    make([]interface{}, 0),
	}
}

// TriggerConfigBuilder строитель конфигурации триггера
type TriggerConfigBuilder struct {
	config map[string]interface{}
	ops    []interface{}
}

// On устанавливает событие триггера
func (b *TriggerConfigBuilder) On(event string) *TriggerConfigBuilder {
	b.config["event"] = event
	return b
}

// Condition добавляет условие
func (b *TriggerConfigBuilder) Condition(field, operator string, value interface{}) *TriggerConfigBuilder {
	b.config["condition"] = map[string]interface{}{
		"field":    field,
		"operator": operator,
		"value":    value,
	}
	return b
}

// ConditionRegex добавляет regex условие
func (b *TriggerConfigBuilder) ConditionRegex(field, pattern string) *TriggerConfigBuilder {
	b.config["condition"] = map[string]interface{}{
		"field":    field,
		"operator": "regex",
		"value":    pattern,
	}
	return b
}

// Set добавляет операцию установки поля
func (b *TriggerConfigBuilder) Set(field string, value interface{}) *TriggerConfigBuilder {
	b.ops = append(b.ops, map[string]interface{}{
		"type":  "set",
		"field": field,
		"value": value,
	})
	return b
}

// Unset добавляет операцию удаления поля
func (b *TriggerConfigBuilder) Unset(field string) *TriggerConfigBuilder {
	b.ops = append(b.ops, map[string]interface{}{
		"type":  "unset",
		"field": field,
	})
	return b
}

// Inc добавляет операцию инкремента
func (b *TriggerConfigBuilder) Inc(field string, value float64) *TriggerConfigBuilder {
	b.ops = append(b.ops, map[string]interface{}{
		"type":  "inc",
		"field": field,
		"value": value,
	})
	return b
}

// Mul добавляет операцию умножения
func (b *TriggerConfigBuilder) Mul(field string, value float64) *TriggerConfigBuilder {
	b.ops = append(b.ops, map[string]interface{}{
		"type":  "mul",
		"field": field,
		"value": value,
	})
	return b
}

// Rename добавляет операцию переименования поля
func (b *TriggerConfigBuilder) Rename(oldName, newName string) *TriggerConfigBuilder {
	b.ops = append(b.ops, map[string]interface{}{
		"type":  "rename",
		"field": oldName,
		"value": newName,
	})
	return b
}

// CurrentDate добавляет операцию установки текущей даты
func (b *TriggerConfigBuilder) CurrentDate(field string) *TriggerConfigBuilder {
	b.ops = append(b.ops, map[string]interface{}{
		"type":  "currentDate",
		"field": field,
	})
	return b
}

// Action устанавливает действие триггера
func (b *TriggerConfigBuilder) Action(action string) *TriggerConfigBuilder {
	b.config["action"] = action
	return b
}

// Description устанавливает описание триггера
func (b *TriggerConfigBuilder) Description(desc string) *TriggerConfigBuilder {
	b.config["description"] = desc
	return b
}

// Build собирает конфигурацию
func (b *TriggerConfigBuilder) Build() map[string]interface{} {
	b.config["operations"] = b.ops
	return b.config
}
