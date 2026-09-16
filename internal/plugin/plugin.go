/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/plugin/plugin.go
// Назначение: Система плагинов на основе Lua для расширения функциональности СУБД.
//
// ОСНОВНЫЕ ФУНКЦИИ:
// 1. Загрузка Lua-скриптов как плагинов с изолированным окружением (sandbox)
// 2. Выполнение Lua-скриптов с ограничениями по CPU и памяти
// 3. Взаимодействие с данными СУБД через API (базы данных, коллекции, документы)
// 4. Логирование действий плагинов в общий лог-файл
// 5. Поддержка зависимостей между плагинами (Plugin Dependencies)
// 6. Горячая перезагрузка плагинов без остановки системы (Hot Reload)
// 7. Пул Lua-состояний для эффективного использования ресурсов
// 8. Поддержка кастомных движков хранения через Lua-плагины
// 9. Плагин Marketplace для установки плагинов из репозитория
//
// АРХИТЕКТУРНЫЕ РЕШЕНИЯ:
// - Каждый плагин выполняется в отдельном Lua-состоянии с ограничениями
// - Используется пул состояний для предотвращения утечек ресурсов
// - Плагины могут регистрировать собственные движки хранения данных
// - Поддерживается горячая перезагрузка с сохранением состояния
//
// ОГРАНИЧЕНИЯ SANDBOX:
// В gopher-lua (github.com/yuin/gopher-lua) отсутствует публичный API
// lua_sethook/LUA_MASKCOUNT, поэтому лимит инструкций (MaxInstructions)
// реализуется косвенно — через context.WithTimeout, который проверяется
// виртуальной машиной gopher-lua на каждой инструкции при ненулевом ctx.
// Это даёт реальное ограничение по времени выполнения, но не по точному
// числу инструкций. Поле MaxInstructions сохранено для совместимости с
// конфигурацией и возможного будущего использования.

package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"futriis/internal/storage"

	lua "github.com/yuin/gopher-lua"
)

// ========== Константы ==========

const (
	// Лимиты песочницы по умолчанию
	DefaultCPULimit         = 100 * time.Millisecond // Максимальное время CPU на выполнение
	DefaultMemoryLimit      = 50 * 1024 * 1024       // Максимальная память (50 MB)
	DefaultExecutionTimeout = 5 * time.Second        // Таймаут выполнения скрипта

	// Интервалы проверок
	HotReloadCheckInterval = 30 * time.Second // Проверка изменений плагинов
	MaxPluginLoadTime      = 10 * time.Second // Максимальное время загрузки плагина

	// Размеры буферов
	DefaultMaxEventLogSize = 1000      // Максимальное количество записей в логе
	DefaultBatchSize       = 100       // Размер пакета для операций
	DefaultWriteBufferSize = 64 * 1024 // Буфер записи (64 KB)

	// Константы для пула Lua-состояний
	MaxLuaStates     = 100              // Максимальное количество состояний
	LuaStateTTL      = 10 * time.Minute // Время жизни состояния в пуле
	LuaStatePoolSize = 50               // Размер пула состояний
)

// PluginConfig представляет конфигурацию плагинов для main.go
type PluginConfig struct {
	Enabled     bool     `toml:"enabled"`       // Включена ли система плагинов
	ScriptDir   string   `toml:"script_dir"`    // Директория с Lua-скриптами
	AllowList   []string `toml:"allow_list"`    // Список разрешённых плагинов
	MaxCPUMS    int64    `toml:"max_cpu_ms"`    // Максимальное время CPU (мс)
	MaxMemoryMB int64    `toml:"max_memory_mb"` // Максимальная память (MB)
}

// =============================================================================
// СОВМЕСТИМЫЕ АТОМАРНЫЕ ОБЁРТКИ (Go 1.13+)
// =============================================================================
// Используются вместо atomic.Bool/Int32/Int64/Uint64/Float64 (Go 1.19+),
// чтобы обеспечить сборку на старых версиях Go (актуально для OpenIndiana).

type atomicBool struct{ v int32 }

func (b *atomicBool) Load() bool   { return atomic.LoadInt32(&b.v) != 0 }
func (b *atomicBool) Store(v bool) { atomic.StoreInt32(&b.v, boolToInt32(v)) }
func (b *atomicBool) CAS(old, new bool) bool {
	return atomic.CompareAndSwapInt32(&b.v, boolToInt32(old), boolToInt32(new))
}

func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

type atomicInt32 struct{ v int32 }

func (a *atomicInt32) Load() int32       { return atomic.LoadInt32(&a.v) }
func (a *atomicInt32) Store(v int32)     { atomic.StoreInt32(&a.v, v) }
func (a *atomicInt32) Add(d int32) int32 { return atomic.AddInt32(&a.v, d) }
func (a *atomicInt32) CAS(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&a.v, old, new)
}

type atomicInt64 struct{ v int64 }

func (a *atomicInt64) Load() int64       { return atomic.LoadInt64(&a.v) }
func (a *atomicInt64) Store(v int64)     { atomic.StoreInt64(&a.v, v) }
func (a *atomicInt64) Add(d int64) int64 { return atomic.AddInt64(&a.v, d) }

type atomicUint64 struct{ v uint64 }

func (a *atomicUint64) Load() uint64        { return atomic.LoadUint64(&a.v) }
func (a *atomicUint64) Store(v uint64)      { atomic.StoreUint64(&a.v, v) }
func (a *atomicUint64) Add(d uint64) uint64 { return atomic.AddUint64(&a.v, d) }

type atomicFloat64 struct{ v uint64 }

func (f *atomicFloat64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&f.v))
}
func (f *atomicFloat64) Store(v float64) {
	atomic.StoreUint64(&f.v, math.Float64bits(v))
}

// ========== Интерфейсы для кастомных движков ==========

// CustomEngine определяет интерфейс для кастомного движка хранения.
// Движки могут быть реализованы как на Go, так и на Lua через плагины.
type CustomEngine interface {
	// Базовые операции
	Name() string
	Version() string

	// Операции с документами
	Insert(doc *storage.Document) error
	Find(id string) (*storage.Document, error)
	Update(id string, updates map[string]interface{}) error
	Delete(id string) error

	// Пакетные операции
	BatchInsert(docs []*storage.Document) error
	BatchUpdate(updates map[string]map[string]interface{}) error
	BatchDelete(ids []string) error

	// Поисковые операции
	FindByFilter(filter func(*storage.Document) bool) ([]*storage.Document, error)
	FindByIndex(indexName string, value interface{}) ([]*storage.Document, error)

	// Индексы
	CreateIndex(name string, fields []string, unique bool) error
	DropIndex(name string) error
	GetIndexes() []string

	// Статистика и метаданные
	Count() int64
	Size() int64
	GetStats() map[string]interface{}

	// Жизненный цикл
	Initialize(config map[string]interface{}) error
	Close() error

	// События (для интеграции с триггерами)
	OnDocumentInserted(doc *storage.Document)
	OnDocumentUpdated(oldDoc, newDoc *storage.Document)
	OnDocumentDeleted(doc *storage.Document)
}

// EngineRegistry управляет зарегистрированными кастомными движками.
// Поддерживает регистрацию фабрик движков и создание экземпляров.
type EngineRegistry struct {
	engines       map[string]EngineFactory // Зарегистрированные фабрики
	activeEngines map[string]CustomEngine  // Активные экземпляры движков
	mu            sync.RWMutex             // Защита доступа
	logger        storage.LoggerInterface  // Логгер
	pm            *PluginManager           // Менеджер плагинов
}

// EngineFactory создаёт экземпляр кастомного движка.
type EngineFactory func(config map[string]interface{}, logger storage.LoggerInterface) (CustomEngine, error)

// NewEngineRegistry создаёт новый реестр движков.
func NewEngineRegistry(logger storage.LoggerInterface, pm *PluginManager) *EngineRegistry {
	return &EngineRegistry{
		engines:       make(map[string]EngineFactory),
		activeEngines: make(map[string]CustomEngine),
		logger:        logger,
		pm:            pm,
	}
}

// RegisterEngine регистрирует фабрику движка в реестре.
func (er *EngineRegistry) RegisterEngine(name string, factory EngineFactory) {
	er.mu.Lock()
	defer er.mu.Unlock()
	er.engines[name] = factory
	if er.logger != nil {
		er.logger.Info(fmt.Sprintf("Engine registered: %s", name))
	}
}

// CreateEngine создаёт экземпляр движка по имени.
func (er *EngineRegistry) CreateEngine(name string, config map[string]interface{}) (CustomEngine, error) {
	er.mu.RLock()
	factory, ok := er.engines[name]
	er.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("engine not found: %s", name)
	}

	engine, err := factory(config, er.logger)
	if err != nil {
		return nil, err
	}

	if err := engine.Initialize(config); err != nil {
		return nil, err
	}

	er.mu.Lock()
	er.activeEngines[engine.Name()] = engine
	er.mu.Unlock()

	return engine, nil
}

// GetEngine возвращает активный движок по имени.
func (er *EngineRegistry) GetEngine(name string) CustomEngine {
	er.mu.RLock()
	defer er.mu.RUnlock()
	return er.activeEngines[name]
}

// ListEngines возвращает список зарегистрированных движков.
func (er *EngineRegistry) ListEngines() []string {
	er.mu.RLock()
	defer er.mu.RUnlock()
	engines := make([]string, 0, len(er.engines))
	for name := range er.engines {
		engines = append(engines, name)
	}
	return engines
}

// ListActiveEngines возвращает список активных движков.
func (er *EngineRegistry) ListActiveEngines() []string {
	er.mu.RLock()
	defer er.mu.RUnlock()
	engines := make([]string, 0, len(er.activeEngines))
	for name := range er.activeEngines {
		engines = append(engines, name)
	}
	return engines
}

// CloseAll закрывает все активные движки.
func (er *EngineRegistry) CloseAll() error {
	er.mu.Lock()
	defer er.mu.Unlock()

	var lastErr error
	for name, engine := range er.activeEngines {
		if err := engine.Close(); err != nil {
			lastErr = err
			if er.logger != nil {
				er.logger.Error(fmt.Sprintf("Failed to close engine %s: %v", name, err))
			}
		}
	}
	er.activeEngines = make(map[string]CustomEngine)
	return lastErr
}

// ========== Пул Lua-состояний ==========

// LuaStatePool управляет пулом Lua-состояний для ограничения ресурсов.
//
// Close() не закрывает канал states, чтобы избежать паники
// "send on closed channel" при одновременном Release из других горутин.
// Вместо этого используется флаг closed (atomicBool).
type LuaStatePool struct {
	states    chan *InstrumentedLState
	active    atomicInt32
	maxSize   int
	mu        sync.RWMutex
	createdAt map[*InstrumentedLState]time.Time
	closed    atomicBool
}

// Глобальный пул состояний (синглтон)
var globalStatePool *LuaStatePool
var statePoolOnce sync.Once

// GetLuaStatePool возвращает глобальный пул Lua-состояний.
func GetLuaStatePool() *LuaStatePool {
	statePoolOnce.Do(func() {
		globalStatePool = &LuaStatePool{
			states:    make(chan *InstrumentedLState, LuaStatePoolSize),
			maxSize:   MaxLuaStates,
			createdAt: make(map[*InstrumentedLState]time.Time),
		}
	})
	return globalStatePool
}

// Acquire получает состояние из пула или создаёт новое.
func (p *LuaStatePool) Acquire(limits SandboxLimits) (*InstrumentedLState, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("state pool is closed")
	}

	// Пробуем получить готовое состояние
	select {
	case ls := <-p.states:
		if ls == nil {
			return p.Acquire(limits)
		}
		p.mu.RLock()
		createdAt, ok := p.createdAt[ls]
		p.mu.RUnlock()

		if !ok || time.Since(createdAt) > LuaStateTTL {
			// Состояние устарело - закрываем и создаём новое
			ls.Close()
			p.mu.Lock()
			delete(p.createdAt, ls)
			p.mu.Unlock()
			p.active.Add(-1)
			return p.Acquire(limits)
		}
		// Применяем новые лимиты (включая пересоздание контекста)
		ls.SetLimits(limits)
		return ls, nil
	default:
	}

	// Пул пуст — проверяем лимит
	if p.active.Load() >= int32(p.maxSize) {
		return nil, fmt.Errorf("max Lua states limit reached: %d", p.maxSize)
	}

	ls := NewInstrumentedLState(limits)
	p.active.Add(1)
	p.mu.Lock()
	p.createdAt[ls] = time.Now()
	p.mu.Unlock()
	return ls, nil
}

// Release возвращает состояние в пул для переиспользования.
func (p *LuaStatePool) Release(ls *InstrumentedLState) {
	if ls == nil {
		return
	}

	if p.closed.Load() {
		ls.Close()
		p.active.Add(-1)
		p.mu.Lock()
		delete(p.createdAt, ls)
		p.mu.Unlock()
		return
	}

	// Сбрасываем счётчики для нового использования
	ls.Reset()

	select {
	case p.states <- ls:
		// Успешно вернули в пул
	default:
		// Пул заполнен - закрываем состояние
		ls.Close()
		p.active.Add(-1)
		p.mu.Lock()
		delete(p.createdAt, ls)
		p.mu.Unlock()
	}
}

// GetActiveCount возвращает количество активных состояний.
func (p *LuaStatePool) GetActiveCount() int32 { return p.active.Load() }

// GetPoolSize возвращает размер пула (количество свободных состояний).
func (p *LuaStatePool) GetPoolSize() int { return len(p.states) }

// Close закрывает пул и все состояния.
// Безопасен для многократного вызова. НЕ закрывает канал states.
func (p *LuaStatePool) Close() {
	if !p.closed.CAS(false, true) {
		return
	}

	// Дренируем канал без его закрытия
	for {
		select {
		case ls := <-p.states:
			if ls != nil {
				ls.Close()
			}
		default:
			goto drained
		}
	}
drained:

	p.mu.Lock()
	for ls := range p.createdAt {
		if ls != nil {
			ls.Close()
		}
	}
	p.createdAt = make(map[*InstrumentedLState]time.Time)
	p.mu.Unlock()

	p.active.Store(0)
}

// ========== Sandbox с ограничениями ==========

// SandboxLimits определяет ограничения для Lua-скрипта.
//
// ВАЖНО: MaxInstructions в gopher-lua не имеет прямого API для
// подсчёта инструкций (нет аналога lua_sethook/LUA_MASKCOUNT).
// Поле оставлено для совместимости с конфигом и может быть
// реализовано через context.WithTimeout (косвенно, по времени).
type SandboxLimits struct {
	MaxCPUTime       time.Duration // Максимальное время CPU
	MaxMemory        int64         // Максимальный размер памяти (в байтах)
	MaxExecutionTime time.Duration // Максимальное время выполнения скрипта
	MaxStackDepth    int           // Максимальная глубина стека
	MaxInstructions  int64         // Максимальное количество инструкций (зарезервировано)
}

// InstrumentedLState расширяет lua.LState с мониторингом ресурсов.
//
// ОГРАНИЧЕНИЯ SANDBOX:
// gopher-lua не предоставляет публичного API lua_sethook/LUA_MASKCOUNT
// для подсчёта инструкций. Вместо этого используется L.SetContext(ctx)
// с таймаутом — виртуальная машина gopher-lua проверяет ctx.Done() на
// каждой инструкции, что даёт реальное прерывание по времени.
//
// НЕ используем runtime.SetFinalizer: финализатор держит ссылку на
// объект, мешает сборке мусора и приводит к двойному закрытию.
type InstrumentedLState struct {
	*lua.LState
	limits       SandboxLimits
	instructions atomicInt64 // Зарезервировано; в gopher-lua не инкрементируется автоматически
	startTime    time.Time
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.RWMutex
	closed       bool
}

// NewInstrumentedLState создаёт Lua-состояние с ограничениями.
// Открывает только безопасные библиотеки: base, string, table, math.
func NewInstrumentedLState(limits SandboxLimits) *InstrumentedLState {
	ctx, cancel := context.WithCancel(context.Background())

	ls := &InstrumentedLState{
		LState:    lua.NewState(lua.Options{SkipOpenLibs: true}),
		limits:    limits,
		startTime: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Открываем только безопасные библиотеки
	lua.OpenBase(ls.LState)
	lua.OpenString(ls.LState)
	lua.OpenTable(ls.LState)
	lua.OpenMath(ls.LState)

	// Устанавливаем контекст с таймаутом.
	// gopher-lua проверяет ctx.Done() на каждой инструкции — это даёт
	// реальное прерывание выполнения при превышении лимита времени,
	// что компенсирует отсутствие LUA_MASKCOUNT.
	ls.applyContext()

	return ls
}

// applyContext создаёт новый контекст с таймаутом на основе лимитов
// и устанавливает его в LState. Вызывается при создании состояния
// и при обновлении лимитов (SetLimits, Reset).
//
// Логика выбора таймаута:
//   - Если задан MaxExecutionTime > 0 — используем его.
//   - Иначе, если задан MaxCPUTime > 0 — используем его.
//   - Иначе — без таймаута (только context.Background с возможностью
//     внешней отмены через cancel()).
func (ls *InstrumentedLState) applyContext() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Отменяем предыдущий контекст, если был
	if ls.cancel != nil {
		ls.cancel()
	}

	var ctx context.Context
	var cancel context.CancelFunc

	timeout := ls.limits.MaxExecutionTime
	if timeout <= 0 {
		timeout = ls.limits.MaxCPUTime
	}

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

	ls.ctx = ctx
	ls.cancel = cancel

	if ls.LState != nil {
		ls.LState.SetContext(ctx)
	}
}

// SetLimits обновляет лимиты для повторного использования состояния.
// Пересоздаёт контекст с новым таймаутом.
func (ls *InstrumentedLState) SetLimits(limits SandboxLimits) {
	ls.mu.Lock()
	ls.limits = limits
	ls.mu.Unlock()

	ls.applyContext()
}

// Reset сбрасывает счётчики для нового использования.
// Пересоздаёт контекст с таймаутом, чтобы состояние можно было
// безопасно переиспользовать после предыдущего выполнения.
func (ls *InstrumentedLState) Reset() {
	ls.instructions.Store(0)
	ls.mu.Lock()
	ls.startTime = time.Now()
	ls.mu.Unlock()

	ls.applyContext()
}

// SetCPULimit устанавливает лимит CPU времени.
func (ls *InstrumentedLState) SetCPULimit(limit time.Duration) {
	ls.mu.Lock()
	ls.limits.MaxCPUTime = limit
	ls.mu.Unlock()
	ls.applyContext()
}

// SetMemoryLimit устанавливает лимит памяти.
func (ls *InstrumentedLState) SetMemoryLimit(limit int64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.limits.MaxMemory = limit
}

// CheckMemory проверяет текущее использование памяти.
func (ls *InstrumentedLState) CheckMemory() bool {
	ls.mu.RLock()
	maxMem := ls.limits.MaxMemory
	ls.mu.RUnlock()
	if maxMem <= 0 {
		return true
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.Alloc) <= maxMem
}

// GetSandboxStats возвращает статистику песочницы.
func (ls *InstrumentedLState) GetSandboxStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"instructions":   ls.instructions.Load(),
		"execution_ms":   time.Since(ls.startTime).Milliseconds(),
		"memory_alloc":   m.Alloc,
		"memory_sys":     m.Sys,
		"num_gc":         m.NumGC,
		"pause_total_ms": m.PauseTotalNs / 1000000,
	}
}

// IsClosed возвращает статус закрытия состояния.
func (ls *InstrumentedLState) IsClosed() bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.closed
}

// Close закрывает Lua состояние и освобождает ресурсы.
// Безопасно вызывать несколько раз.
func (ls *InstrumentedLState) Close() {
	ls.mu.Lock()
	if ls.closed {
		ls.mu.Unlock()
		return
	}
	ls.closed = true
	ls.mu.Unlock()

	if ls.cancel != nil {
		ls.cancel()
	}
	if ls.LState != nil {
		ls.LState.Close()
	}
}

// ========== Plugin Dependencies ==========

// PluginDependency определяет зависимость плагина.
type PluginDependency struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	MinVersion string `json:"min_version,omitempty"`
	MaxVersion string `json:"max_version,omitempty"`
	Optional   bool   `json:"optional,omitempty"`
}

// PluginManifest описывает метаданные плагина.
type PluginManifest struct {
	Name         string             `json:"name"`
	Version      string             `json:"version"`
	Author       string             `json:"author"`
	Description  string             `json:"description"`
	Dependencies []PluginDependency `json:"dependencies,omitempty"`
	APIVersion   string             `json:"api_version"`
	MinGoVersion string             `json:"min_go_version,omitempty"`
	EntryPoint   string             `json:"entry_point,omitempty"`
	CreatedAt    int64              `json:"created_at"`
	UpdatedAt    int64              `json:"updated_at"`
	EngineType   string             `json:"engine_type,omitempty"`
}

// PluginWithDeps расширяет Plugin с поддержкой зависимостей.
type PluginWithDeps struct {
	*Plugin
	Manifest     *PluginManifest
	Dependencies map[string]*PluginWithDeps
	Dependents   map[string]*PluginWithDeps
	LoadOrder    int
	depMu        sync.RWMutex
}

// DependencyResolver разрешает зависимости между плагинами.
type DependencyResolver struct {
	plugins map[string]*PluginWithDeps
	mu      sync.RWMutex
}

// NewDependencyResolver создаёт новый резолвер зависимостей.
func NewDependencyResolver() *DependencyResolver {
	return &DependencyResolver{
		plugins: make(map[string]*PluginWithDeps),
	}
}

// RegisterPlugin регистрирует плагин с его манифестом в резолвере.
func (dr *DependencyResolver) RegisterPlugin(plugin *PluginWithDeps) error {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	dr.plugins[plugin.Name] = plugin
	return nil
}

// ResolveOrder возвращает порядок загрузки плагинов (топологическая сортировка).
func (dr *DependencyResolver) ResolveOrder() ([]*PluginWithDeps, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	graph := make(map[string][]string)
	inDegree := make(map[string]int)

	for name, plugin := range dr.plugins {
		inDegree[name] = 0
		for _, dep := range plugin.Manifest.Dependencies {
			if !dep.Optional {
				graph[dep.Name] = append(graph[dep.Name], name)
				inDegree[name]++
			}
		}
	}

	queue := make([]string, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}

	result := make([]*PluginWithDeps, 0)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		result = append(result, dr.plugins[current])

		for _, dependent := range graph[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(result) != len(dr.plugins) {
		return nil, fmt.Errorf("circular dependency detected among plugins")
	}

	for i, plugin := range result {
		plugin.LoadOrder = i
	}

	return result, nil
}

// CheckVersionCompatibility проверяет совместимость версий.
func (dr *DependencyResolver) CheckVersionCompatibility(dep PluginDependency, actualVersion string) bool {
	if dep.MinVersion != "" && actualVersion < dep.MinVersion {
		return false
	}
	if dep.MaxVersion != "" && actualVersion > dep.MaxVersion {
		return false
	}
	if dep.Version != "" && actualVersion != dep.Version {
		return false
	}
	return true
}

// ========== File Watcher (без fsnotify) ==========

// FileWatcher самостоятельно отслеживает изменения файлов.
type FileWatcher struct {
	files    map[string]time.Time
	mu       sync.RWMutex
	interval time.Duration
	onChange func(path string) error
	stopChan chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	logger   storage.LoggerInterface
}

// NewFileWatcher создаёт новый файловый вотчер без fsnotify.
func NewFileWatcher(interval time.Duration, onChange func(path string) error, logger storage.LoggerInterface) *FileWatcher {
	if interval <= 0 {
		interval = HotReloadCheckInterval
	}
	return &FileWatcher{
		files:    make(map[string]time.Time),
		interval: interval,
		onChange: onChange,
		stopChan: make(chan struct{}),
		logger:   logger,
	}
}

// Add добавляет файл для отслеживания.
func (fw *FileWatcher) Add(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.files[path] = info.ModTime()
	return nil
}

// Remove прекращает отслеживание файла.
func (fw *FileWatcher) Remove(path string) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	delete(fw.files, path)
}

// Start запускает цикл проверки изменений.
func (fw *FileWatcher) Start() {
	fw.wg.Add(1)
	go fw.watchLoop()
}

// Stop останавливает цикл проверки. Безопасен для многократного вызова.
func (fw *FileWatcher) Stop() {
	fw.stopOnce.Do(func() { close(fw.stopChan) })
	fw.wg.Wait()
}

// watchLoop периодически проверяет изменения файлов.
func (fw *FileWatcher) watchLoop() {
	defer fw.wg.Done()

	ticker := time.NewTicker(fw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fw.checkChanges()
		case <-fw.stopChan:
			return
		}
	}
}

// checkChanges проверяет изменения всех отслеживаемых файлов.
func (fw *FileWatcher) checkChanges() {
	fw.mu.RLock()
	files := make([]string, 0, len(fw.files))
	for path := range fw.files {
		files = append(files, path)
	}
	fw.mu.RUnlock()

	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			if fw.logger != nil {
				fw.logger.Warn(fmt.Sprintf("Failed to stat file %s: %v", path, err))
			}
			continue
		}

		fw.mu.RLock()
		lastMod := fw.files[path]
		fw.mu.RUnlock()

		if info.ModTime().After(lastMod) {
			if fw.logger != nil {
				fw.logger.Info(fmt.Sprintf("File changed: %s (was %v, now %v)", path, lastMod, info.ModTime()))
			}

			if err := fw.onChange(path); err != nil {
				if fw.logger != nil {
					fw.logger.Error(fmt.Sprintf("Failed to handle change for %s: %v", path, err))
				}
			}

			fw.mu.Lock()
			fw.files[path] = info.ModTime()
			fw.mu.Unlock()
		}
	}
}

// ========== HotReloadManager управляет горячей перезагрузкой плагинов ==========

// HotReloadManager управляет горячей перезагрузкой плагинов.
type HotReloadManager struct {
	pluginManager *PluginManager
	watcher       *FileWatcher
	reloadQueue   chan string
	reloading     map[string]bool
	mu            sync.RWMutex
	logger        storage.LoggerInterface
	stopOnce      sync.Once
	stopped       chan struct{}
}

// NewHotReloadManager создаёт менеджер горячей перезагрузки.
func NewHotReloadManager(pm *PluginManager, logger storage.LoggerInterface) *HotReloadManager {
	hrm := &HotReloadManager{
		pluginManager: pm,
		reloadQueue:   make(chan string, 100),
		reloading:     make(map[string]bool),
		logger:        logger,
		stopped:       make(chan struct{}),
	}

	hrm.watcher = NewFileWatcher(HotReloadCheckInterval, hrm.handleFileChange, logger)

	go hrm.processReloadQueue()

	return hrm
}

// Start запускает мониторинг изменений.
func (hrm *HotReloadManager) Start() { hrm.watcher.Start() }

// Stop останавливает мониторинг. Безопасен для многократного вызова.
func (hrm *HotReloadManager) Stop() {
	hrm.stopOnce.Do(func() {
		hrm.watcher.Stop()
		close(hrm.stopped)
	})
}

// WatchPlugin начинает отслеживать плагин.
func (hrm *HotReloadManager) WatchPlugin(pluginPath string) error {
	return hrm.watcher.Add(pluginPath)
}

// handleFileChange обрабатывает изменение файла.
func (hrm *HotReloadManager) handleFileChange(path string) error {
	baseName := filepath.Base(path)
	pluginName := strings.TrimSuffix(baseName, ".lua")

	hrm.mu.Lock()
	if hrm.reloading[pluginName] {
		hrm.mu.Unlock()
		return nil
	}
	hrm.reloading[pluginName] = true
	hrm.mu.Unlock()

	select {
	case hrm.reloadQueue <- pluginName:
	default:
		if hrm.logger != nil {
			hrm.logger.Warn(fmt.Sprintf("Reload queue full, skipping reload for %s", pluginName))
		}
	}

	return nil
}

// processReloadQueue обрабатывает очередь перезагрузки.
// Завершается при закрытии канала stopped.
func (hrm *HotReloadManager) processReloadQueue() {
	for {
		select {
		case pluginName := <-hrm.reloadQueue:
			hrm.reloadPlugin(pluginName)
			hrm.mu.Lock()
			delete(hrm.reloading, pluginName)
			hrm.mu.Unlock()
		case <-hrm.stopped:
			return
		}
	}
}

// reloadPlugin выполняет горячую перезагрузку плагина.
func (hrm *HotReloadManager) reloadPlugin(pluginName string) {
	if hrm.logger != nil {
		hrm.logger.Info(fmt.Sprintf("Hot reloading plugin: %s", pluginName))
	}

	oldPlugin, err := hrm.pluginManager.GetPlugin(pluginName)
	if err != nil {
		if hrm.logger != nil {
			hrm.logger.Error(fmt.Sprintf("Plugin not found for reload: %s", pluginName))
		}
		return
	}

	// Сохраняем состояние старого плагина
	var oldState map[string]interface{}

	if oldPlugin.LState != nil && !oldPlugin.IsClosed() {
		fn := oldPlugin.LState.GetGlobal("on_before_reload")
		if fn != lua.LNil {
			oldPlugin.LState.CallByParam(lua.P{
				Fn:      fn,
				NRet:    1,
				Protect: true,
			})
			ret := oldPlugin.LState.Get(-1)
			if table, ok := ret.(*lua.LTable); ok {
				oldState = make(map[string]interface{})
				table.ForEach(func(key, value lua.LValue) {
					oldState[key.String()] = value.String()
				})
			}
			oldPlugin.LState.Pop(1)
		}
	}

	if err := hrm.pluginManager.StopPlugin(pluginName); err != nil {
		if hrm.logger != nil {
			hrm.logger.Warn(fmt.Sprintf("Failed to stop plugin before reload: %v", err))
		}
	}

	if err := hrm.pluginManager.UnloadPlugin(pluginName); err != nil {
		if hrm.logger != nil {
			hrm.logger.Error(fmt.Sprintf("Failed to unload plugin for reload: %v", err))
		}
		return
	}

	pluginPath := filepath.Join(hrm.pluginManager.GetPluginsDir(), pluginName+".lua")
	if err := hrm.pluginManager.LoadPluginWithSandbox(pluginName, pluginPath, SandboxLimits{
		MaxCPUTime:       DefaultCPULimit,
		MaxMemory:        DefaultMemoryLimit,
		MaxExecutionTime: DefaultExecutionTimeout,
		MaxInstructions:  1000000,
	}); err != nil {
		if hrm.logger != nil {
			hrm.logger.Error(fmt.Sprintf("Failed to load plugin during reload: %v", err))
		}
		return
	}

	newPlugin, _ := hrm.pluginManager.GetPlugin(pluginName)

	if newPlugin != nil && newPlugin.LState != nil && !newPlugin.IsClosed() && oldState != nil {
		fn := newPlugin.LState.GetGlobal("on_after_reload")
		if fn != lua.LNil {
			stateTable := newPlugin.LState.NewTable()
			for k, v := range oldState {
				stateTable.RawSetString(k, lua.LString(fmt.Sprintf("%v", v)))
			}
			newPlugin.LState.CallByParam(lua.P{
				Fn:      fn,
				NRet:    0,
				Protect: true,
			}, stateTable)
		}
	}

	if err := hrm.pluginManager.StartPlugin(pluginName); err != nil {
		if hrm.logger != nil {
			hrm.logger.Error(fmt.Sprintf("Failed to start plugin after reload: %v", err))
		}
	}

	if hrm.logger != nil {
		hrm.logger.Info(fmt.Sprintf("Plugin hot reload completed: %s", pluginName))
	}

	storage.LogAudit("PLUGIN_HOT_RELOAD", "PLUGIN", pluginName, map[string]interface{}{
		"reload_time": time.Now().UnixMilli(),
	})
}

// ========== Plugin Marketplace API ==========

// MarketplacePlugin представляет плагин в маркетплейсе.
type MarketplacePlugin struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Author      string   `json:"author"`
	Description string   `json:"description"`
	Downloads   int64    `json:"downloads"`
	Rating      float64  `json:"rating"`
	Tags        []string `json:"tags"`
	Repository  string   `json:"repository"`
	CreatedAt   int64    `json:"created_at"`
	UpdatedAt   int64    `json:"updated_at"`
}

// PluginMarketplace управляет плагинами из маркетплейса.
type PluginMarketplace struct {
	plugins     map[string]*MarketplacePlugin
	installed   map[string]bool
	apiEndpoint string
	mu          sync.RWMutex
	logger      storage.LoggerInterface
	pm          *PluginManager
}

// NewPluginMarketplace создаёт новый маркетплейс.
func NewPluginMarketplace(apiEndpoint string, pm *PluginManager, logger storage.LoggerInterface) *PluginMarketplace {
	return &PluginMarketplace{
		plugins:     make(map[string]*MarketplacePlugin),
		installed:   make(map[string]bool),
		apiEndpoint: apiEndpoint,
		logger:      logger,
		pm:          pm,
	}
}

// ListAvailablePlugins возвращает список доступных плагинов.
func (pmkt *PluginMarketplace) ListAvailablePlugins() []*MarketplacePlugin {
	pmkt.mu.RLock()
	defer pmkt.mu.RUnlock()

	result := make([]*MarketplacePlugin, 0, len(pmkt.plugins))
	for _, p := range pmkt.plugins {
		result = append(result, p)
	}
	return result
}

// RegisterPlugin регистрирует плагин в маркетплейсе.
func (pmkt *PluginMarketplace) RegisterPlugin(plugin *MarketplacePlugin) {
	pmkt.mu.Lock()
	defer pmkt.mu.Unlock()
	pmkt.plugins[plugin.ID] = plugin
}

// InstallPlugin устанавливает плагин из маркетплейса.
func (pmkt *PluginMarketplace) InstallPlugin(pluginID, version string) error {
	pmkt.mu.RLock()
	plugin, ok := pmkt.plugins[pluginID]
	pmkt.mu.RUnlock()

	if !ok {
		return fmt.Errorf("plugin not found in marketplace: %s", pluginID)
	}

	pluginPath := filepath.Join(pmkt.pm.GetPluginsDir(), plugin.Name+".lua")

	if err := pmkt.pm.LoadPluginWithSandbox(plugin.Name, pluginPath, SandboxLimits{
		MaxCPUTime:       DefaultCPULimit,
		MaxMemory:        DefaultMemoryLimit,
		MaxExecutionTime: DefaultExecutionTimeout,
		MaxInstructions:  1000000,
	}); err != nil {
		return fmt.Errorf("failed to load plugin: %v", err)
	}

	if err := pmkt.pm.StartPlugin(plugin.Name); err != nil {
		return fmt.Errorf("failed to start plugin: %v", err)
	}

	pmkt.mu.Lock()
	pmkt.installed[pluginID] = true
	pmkt.mu.Unlock()

	if pmkt.logger != nil {
		pmkt.logger.Info(fmt.Sprintf("Installed plugin from marketplace: %s v%s", plugin.Name, plugin.Version))
	}

	return nil
}

// UninstallPlugin удаляет плагин из системы.
func (pmkt *PluginMarketplace) UninstallPlugin(pluginID string) error {
	pmkt.mu.RLock()
	plugin, ok := pmkt.plugins[pluginID]
	pmkt.mu.RUnlock()

	if !ok {
		return fmt.Errorf("plugin not found: %s", pluginID)
	}

	if err := pmkt.pm.StopPlugin(plugin.Name); err != nil {
		return err
	}

	if err := pmkt.pm.UnloadPlugin(plugin.Name); err != nil {
		return err
	}

	pmkt.mu.Lock()
	delete(pmkt.installed, pluginID)
	pmkt.mu.Unlock()

	return nil
}

// UpdatePlugin обновляет плагин до новой версии.
func (pmkt *PluginMarketplace) UpdatePlugin(pluginID, newVersion string) error {
	if err := pmkt.UninstallPlugin(pluginID); err != nil {
		return err
	}
	return pmkt.InstallPlugin(pluginID, newVersion)
}

// GetInstalledPlugins возвращает список установленных плагинов.
func (pmkt *PluginMarketplace) GetInstalledPlugins() []string {
	pmkt.mu.RLock()
	defer pmkt.mu.RUnlock()

	result := make([]string, 0, len(pmkt.installed))
	for id := range pmkt.installed {
		result = append(result, id)
	}
	return result
}

// ========== Plugin Struct ==========

// PluginStatus представляет состояние плагина.
type PluginStatus int32

const (
	StatusLoaded PluginStatus = iota // Загружен, но не запущен
	StatusRunning                    // Запущен и работает
	StatusStopped                    // Остановлен
	StatusError                      // В состоянии ошибки
)

// PluginEventLog представляет событие плагина с временными метками.
type PluginEventLog struct {
	PluginName   string      `json:"plugin_name"`
	EventType    string      `json:"event_type"`
	Data         interface{} `json:"data"`
	Timestamp    int64       `json:"timestamp"`
	TimestampStr string      `json:"timestamp_str"`
	DurationMs   int64       `json:"duration_ms,omitempty"`
	Error        string      `json:"error,omitempty"`
}

// Plugin представляет загруженный Lua-плагин.
//
// Поле instrumented хранит ссылку на *InstrumentedLState, чтобы
// избежать утечки при выгрузке плагина (ранее type assertion
// interface{}(L).(*InstrumentedLState) всегда возвращал false,
// поскольку L имеет тип *lua.LState).
type Plugin struct {
	Name         string
	FilePath     string
	Status       atomicInt32
	LState       *lua.LState
	instrumented *InstrumentedLState // Ссылка на обёртку для возврата в пул
	logger       storage.LoggerInterface
	storage      *storage.Storage
	mu           sync.RWMutex
	loadedAt     time.Time
	loadedAtMs   int64
	loadedAtStr  string
	version      string
	author       string
	description  string
	eventLog     []PluginEventLog
	eventLogMu   sync.RWMutex
	maxEventLog  int
	closed       bool
	isEngine     bool
	engineName   string
}

// IsClosed возвращает статус закрытия плагина.
func (p *Plugin) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// closePlugin закрывает плагин и освобождает ресурсы.
func (p *Plugin) closePlugin() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	if p.LState != nil {
		p.LState.Close()
	}
}

// IsEnginePlugin возвращает true, если плагин является движком.
func (p *Plugin) IsEnginePlugin() bool { return p.isEngine }

// GetEngineName возвращает имя движка, если плагин является движком.
func (p *Plugin) GetEngineName() string { return p.engineName }

// ========== PluginManager ==========

// PluginManager управляет всеми загруженными плагинами.
type PluginManager struct {
	plugins        sync.Map
	logger         storage.LoggerInterface
	storage        *storage.Storage
	pluginsDir     string
	eventBus       chan PluginEvent
	eventBusClosed atomicBool
	enabled        bool
	depResolver    *DependencyResolver
	hotReload      *HotReloadManager
	marketplace    *PluginMarketplace
	statePool      *LuaStatePool
	engineRegistry *EngineRegistry
}

// PluginEvent представляет событие от плагина.
type PluginEvent struct {
	PluginName   string
	EventType    string
	Data         interface{}
	Timestamp    int64
	TimestampStr string
}

// NewPluginManager создаёт новый менеджер плагинов.
func NewPluginManager(pluginsDir string, logger storage.LoggerInterface, store *storage.Storage, enabled bool) *PluginManager {
	pm := &PluginManager{
		logger:      logger,
		storage:     store,
		pluginsDir:  pluginsDir,
		eventBus:    make(chan PluginEvent, 1000),
		enabled:     enabled,
		depResolver: NewDependencyResolver(),
		statePool:   GetLuaStatePool(),
	}

	pm.engineRegistry = NewEngineRegistry(logger, pm)

	if !enabled {
		if logger != nil {
			logger.Info("Plugin system is disabled")
		}
		return pm
	}

	if err := os.MkdirAll(pluginsDir, 0755); err != nil {
		if logger != nil {
			logger.Error(fmt.Sprintf("Failed to create plugins directory: %v", err))
		}
	}

	go pm.eventLoop()

	pm.hotReload = NewHotReloadManager(pm, logger)
	pm.hotReload.Start()

	go pm.autoLoadPlugins()

	if logger != nil {
		logger.Info(fmt.Sprintf("Plugin system initialized, plugins directory: %s", pluginsDir))
	}

	return pm
}

// NewPluginManagerFromConfig создаёт новый менеджер плагинов из конфигурации.
func NewPluginManagerFromConfig(cfg *PluginConfig, logger storage.LoggerInterface, store *storage.Storage) *PluginManager {
	if cfg == nil {
		return NewPluginManager("plugins", logger, store, false)
	}

	pm := NewPluginManager(cfg.ScriptDir, logger, store, cfg.Enabled)

	if cfg.Enabled && len(cfg.AllowList) > 0 {
		if pm.logger != nil {
			pm.logger.Info(fmt.Sprintf("Plugin allow list configured: %v", cfg.AllowList))
		}
	}

	return pm
}

// GetEngineRegistry возвращает реестр движков.
func (pm *PluginManager) GetEngineRegistry() *EngineRegistry { return pm.engineRegistry }

// getPluginLState извлекает *lua.LState и *Plugin из значения sync.Map.
func getPluginLState(plugin interface{}) (*lua.LState, *Plugin) {
	switch v := plugin.(type) {
	case *Plugin:
		v.mu.RLock()
		defer v.mu.RUnlock()
		return v.LState, v
	case *PluginWithDeps:
		v.Plugin.mu.RLock()
		defer v.Plugin.mu.RUnlock()
		return v.Plugin.LState, v.Plugin
	default:
		return nil, nil
	}
}

// getPluginInstrumented извлекает *InstrumentedLState из плагина.
func getPluginInstrumented(plugin interface{}) *InstrumentedLState {
	switch v := plugin.(type) {
	case *Plugin:
		v.mu.RLock()
		defer v.mu.RUnlock()
		return v.instrumented
	case *PluginWithDeps:
		v.Plugin.mu.RLock()
		defer v.Plugin.mu.RUnlock()
		return v.Plugin.instrumented
	default:
		return nil
	}
}

// RegisterEngineFromLua регистрирует движок из Lua-плагина.
func (pm *PluginManager) RegisterEngineFromLua(pluginName string, engineName string, factoryFn *lua.LFunction) error {
	if !pm.enabled {
		return fmt.Errorf("plugin system is disabled")
	}

	val, ok := pm.plugins.Load(pluginName)
	if !ok {
		return fmt.Errorf("plugin not found: %s", pluginName)
	}

	L, p := getPluginLState(val)
	if L == nil || p == nil || p.IsClosed() {
		return fmt.Errorf("plugin has no Lua state or is closed")
	}

	factory := func(config map[string]interface{}, logger storage.LoggerInterface) (CustomEngine, error) {
		return pm.createEngineFromLua(pluginName, engineName, factoryFn, config, logger)
	}

	pm.engineRegistry.RegisterEngine(engineName, factory)
	p.mu.Lock()
	p.isEngine = true
	p.engineName = engineName
	p.mu.Unlock()

	if pm.logger != nil {
		pm.logger.Info(fmt.Sprintf("Engine registered from plugin %s: %s", pluginName, engineName))
	}

	return nil
}

// createEngineFromLua создаёт экземпляр Lua-движка.
func (pm *PluginManager) createEngineFromLua(pluginName, engineName string, factoryFn *lua.LFunction, config map[string]interface{}, logger storage.LoggerInterface) (CustomEngine, error) {
	val, ok := pm.plugins.Load(pluginName)
	if !ok {
		return nil, fmt.Errorf("plugin not found: %s", pluginName)
	}

	L, p := getPluginLState(val)
	if L == nil || p == nil || p.IsClosed() {
		return nil, fmt.Errorf("plugin has no Lua state or is closed")
	}

	configTable := pm.goValueToLua(L, config).(*lua.LTable)

	if err := L.CallByParam(lua.P{
		Fn:      factoryFn,
		NRet:    1,
		Protect: true,
	}, configTable); err != nil {
		return nil, fmt.Errorf("failed to create engine instance: %v", err)
	}

	engineObj := L.Get(-1)
	L.Pop(1)

	if engineObj.Type() != lua.LTUserData {
		return nil, fmt.Errorf("factory function must return a userdata object")
	}

	engine := &LuaCustomEngine{
		pluginName: pluginName,
		engineName: engineName,
		L:          L,
		engineObj:  engineObj.(*lua.LUserData),
		logger:     logger,
		pm:         pm,
	}

	return engine, nil
}

// ========== LuaCustomEngine - обёртка для Lua-движка ==========

// LuaCustomEngine реализует CustomEngine для Lua-плагинов.
type LuaCustomEngine struct {
	pluginName  string
	engineName  string
	L           *lua.LState
	engineObj   *lua.LUserData
	logger      storage.LoggerInterface
	pm          *PluginManager
	mu          sync.RWMutex
	initialized bool
	closed      bool
}

// Name возвращает имя движка.
func (e *LuaCustomEngine) Name() string { return e.engineName }

// Version возвращает версию движка.
func (e *LuaCustomEngine) Version() string { return e.callStringMethod("version", nil) }

// Initialize инициализирует движок с переданной конфигурацией.
func (e *LuaCustomEngine) Initialize(config map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	configTable := e.pm.goValueToLua(e.L, config).(*lua.LTable)
	err := e.callMethod("initialize", configTable)
	if err != nil {
		return err
	}
	e.initialized = true
	return nil
}

// Insert вставляет документ в движок.
func (e *LuaCustomEngine) Insert(doc *storage.Document) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	docTable := e.pm.goValueToLua(e.L, doc.ToMap()).(*lua.LTable)
	return e.callMethod("insert", docTable)
}

// Find находит документ по ID.
func (e *LuaCustomEngine) Find(id string) (*storage.Document, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return nil, fmt.Errorf("engine not initialized or closed")
	}

	result := e.callMethodWithReturn("find", lua.LString(id))
	if result == nil || result == lua.LNil {
		return nil, fmt.Errorf("document not found: %s", id)
	}

	if table, ok := result.(*lua.LTable); ok {
		return e.tableToDocument(table), nil
	}

	return nil, fmt.Errorf("invalid return type from find")
}

// Update обновляет документ.
func (e *LuaCustomEngine) Update(id string, updates map[string]interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	updatesTable := e.pm.goValueToLua(e.L, updates).(*lua.LTable)
	return e.callMethod("update", lua.LString(id), updatesTable)
}

// Delete удаляет документ.
func (e *LuaCustomEngine) Delete(id string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	return e.callMethod("delete", lua.LString(id))
}

// BatchInsert вставляет несколько документов.
func (e *LuaCustomEngine) BatchInsert(docs []*storage.Document) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	docsTable := e.L.NewTable()
	for i, doc := range docs {
		docTable := e.pm.goValueToLua(e.L, doc.ToMap()).(*lua.LTable)
		docsTable.RawSetInt(i+1, docTable)
	}
	return e.callMethod("batch_insert", docsTable)
}

// BatchUpdate выполняет пакетное обновление.
func (e *LuaCustomEngine) BatchUpdate(updates map[string]map[string]interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	updatesTable := e.pm.goValueToLua(e.L, updates).(*lua.LTable)
	return e.callMethod("batch_update", updatesTable)
}

// BatchDelete выполняет пакетное удаление.
func (e *LuaCustomEngine) BatchDelete(ids []string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	idsTable := e.L.NewTable()
	for i, id := range ids {
		idsTable.RawSetInt(i+1, lua.LString(id))
	}
	return e.callMethod("batch_delete", idsTable)
}

// FindByFilter находит документы по фильтру.
func (e *LuaCustomEngine) FindByFilter(filter func(*storage.Document) bool) ([]*storage.Document, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return nil, fmt.Errorf("engine not initialized or closed")
	}

	result := e.callMethodWithReturn("find_all", nil)
	if result == nil || result == lua.LNil {
		return []*storage.Document{}, nil
	}

	docs := make([]*storage.Document, 0)
	if table, ok := result.(*lua.LTable); ok {
		table.ForEach(func(key, value lua.LValue) {
			if docTable, ok := value.(*lua.LTable); ok {
				doc := e.tableToDocument(docTable)
				if filter(doc) {
					docs = append(docs, doc)
				}
			}
		})
	}

	return docs, nil
}

// FindByIndex находит документы по индексу.
func (e *LuaCustomEngine) FindByIndex(indexName string, value interface{}) ([]*storage.Document, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return nil, fmt.Errorf("engine not initialized or closed")
	}

	luaValue := e.pm.goValueToLua(e.L, value)
	result := e.callMethodWithReturn("find_by_index", lua.LString(indexName), luaValue)
	if result == nil || result == lua.LNil {
		return []*storage.Document{}, nil
	}

	docs := make([]*storage.Document, 0)
	if table, ok := result.(*lua.LTable); ok {
		table.ForEach(func(key, value lua.LValue) {
			if docTable, ok := value.(*lua.LTable); ok {
				docs = append(docs, e.tableToDocument(docTable))
			}
		})
	}

	return docs, nil
}

// CreateIndex создаёт индекс.
func (e *LuaCustomEngine) CreateIndex(name string, fields []string, unique bool) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	fieldsTable := e.L.NewTable()
	for i, field := range fields {
		fieldsTable.RawSetInt(i+1, lua.LString(field))
	}
	return e.callMethod("create_index", lua.LString(name), fieldsTable, lua.LBool(unique))
}

// DropIndex удаляет индекс.
func (e *LuaCustomEngine) DropIndex(name string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return fmt.Errorf("engine not initialized or closed")
	}

	return e.callMethod("drop_index", lua.LString(name))
}

// GetIndexes возвращает список индексов.
func (e *LuaCustomEngine) GetIndexes() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return []string{}
	}

	result := e.callMethodWithReturn("get_indexes", nil)
	if result == nil || result == lua.LNil {
		return []string{}
	}

	indexes := make([]string, 0)
	if table, ok := result.(*lua.LTable); ok {
		table.ForEach(func(key, value lua.LValue) {
			if str, ok := value.(lua.LString); ok {
				indexes = append(indexes, string(str))
			}
		})
	}

	return indexes
}

// Count возвращает количество документов.
func (e *LuaCustomEngine) Count() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return 0
	}

	result := e.callMethodWithReturn("count", nil)
	if result == nil || result == lua.LNil {
		return 0
	}

	if num, ok := result.(lua.LNumber); ok {
		return int64(num)
	}

	return 0
}

// Size возвращает размер данных в байтах.
func (e *LuaCustomEngine) Size() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return 0
	}

	result := e.callMethodWithReturn("size", nil)
	if result == nil || result == lua.LNil {
		return 0
	}

	if num, ok := result.(lua.LNumber); ok {
		return int64(num)
	}

	return 0
}

// GetStats возвращает статистику движка.
func (e *LuaCustomEngine) GetStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return make(map[string]interface{})
	}

	result := e.callMethodWithReturn("get_stats", nil)
	if result == nil || result == lua.LNil {
		return make(map[string]interface{})
	}

	if table, ok := result.(*lua.LTable); ok {
		return e.pm.luaTableToMap(table)
	}

	return make(map[string]interface{})
}

// Close закрывает движок и освобождает ресурсы.
func (e *LuaCustomEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	if e.initialized {
		_ = e.callMethod("close", nil)
	}

	e.closed = true
	return nil
}

// OnDocumentInserted вызывается при вставке документа.
func (e *LuaCustomEngine) OnDocumentInserted(doc *storage.Document) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return
	}

	docTable := e.pm.goValueToLua(e.L, doc.ToMap()).(*lua.LTable)
	_ = e.callMethod("on_document_inserted", docTable)
}

// OnDocumentUpdated вызывается при обновлении документа.
func (e *LuaCustomEngine) OnDocumentUpdated(oldDoc, newDoc *storage.Document) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return
	}

	oldTable := e.pm.goValueToLua(e.L, oldDoc.ToMap()).(*lua.LTable)
	newTable := e.pm.goValueToLua(e.L, newDoc.ToMap()).(*lua.LTable)
	_ = e.callMethod("on_document_updated", oldTable, newTable)
}

// OnDocumentDeleted вызывается при удалении документа.
func (e *LuaCustomEngine) OnDocumentDeleted(doc *storage.Document) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.initialized || e.closed {
		return
	}

	docTable := e.pm.goValueToLua(e.L, doc.ToMap()).(*lua.LTable)
	_ = e.callMethod("on_document_deleted", docTable)
}

// ========== Вспомогательные методы LuaCustomEngine ==========

// callMethod вызывает метод Lua-объекта без возвращаемого значения.
func (e *LuaCustomEngine) callMethod(methodName string, args ...lua.LValue) error {
	method := e.L.GetField(e.engineObj, methodName)
	if method.Type() != lua.LTFunction {
		return fmt.Errorf("method %s not found in engine", methodName)
	}

	callArgs := make([]lua.LValue, 0, len(args)+1)
	callArgs = append(callArgs, e.engineObj)
	callArgs = append(callArgs, args...)

	if err := e.L.CallByParam(lua.P{
		Fn:      method,
		NRet:    0,
		Protect: true,
	}, callArgs...); err != nil {
		return fmt.Errorf("failed to call method %s: %v", methodName, err)
	}

	return nil
}

// callMethodWithReturn вызывает метод Lua-объекта с возвращаемым значением.
func (e *LuaCustomEngine) callMethodWithReturn(methodName string, args ...lua.LValue) lua.LValue {
	method := e.L.GetField(e.engineObj, methodName)
	if method.Type() != lua.LTFunction {
		return lua.LNil
	}

	callArgs := make([]lua.LValue, 0, len(args)+1)
	callArgs = append(callArgs, e.engineObj)
	callArgs = append(callArgs, args...)

	if err := e.L.CallByParam(lua.P{
		Fn:      method,
		NRet:    1,
		Protect: true,
	}, callArgs...); err != nil {
		if e.logger != nil {
			e.logger.Error(fmt.Sprintf("Failed to call method %s: %v", methodName, err))
		}
		return lua.LNil
	}

	result := e.L.Get(-1)
	e.L.Pop(1)
	return result
}

// callStringMethod вызывает метод и возвращает строковый результат.
func (e *LuaCustomEngine) callStringMethod(methodName string, args ...lua.LValue) string {
	result := e.callMethodWithReturn(methodName, args...)
	if result == nil || result == lua.LNil {
		return ""
	}
	if str, ok := result.(lua.LString); ok {
		return string(str)
	}
	return ""
}

// tableToDocument преобразует Lua-таблицу в документ.
func (e *LuaCustomEngine) tableToDocument(table *lua.LTable) *storage.Document {
	doc := storage.NewDocument()

	table.ForEach(func(key, value lua.LValue) {
		keyStr := key.String()
		if strings.HasPrefix(keyStr, "_") {
			return
		}
		goValue := e.pm.luaValueToGo(value)
		doc.SetField(keyStr, goValue)
	})

	return doc
}

// ========== PluginManager методы ==========

// autoLoadPlugins автоматически загружает все .lua файлы из директории плагинов.
func (pm *PluginManager) autoLoadPlugins() {
	if !pm.enabled {
		return
	}

	pm.loadPluginsFromDir()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		pm.loadPluginsFromDir()
	}
}

// loadPluginsFromDir загружает все плагины из директории.
func (pm *PluginManager) loadPluginsFromDir() {
	entries, err := os.ReadDir(pm.pluginsDir)
	if err != nil {
		if pm.logger != nil {
			pm.logger.Error(fmt.Sprintf("Failed to read plugins directory: %v", err))
		}
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".lua") {
			continue
		}

		pluginName := strings.TrimSuffix(name, ".lua")
		if _, exists := pm.plugins.Load(pluginName); !exists {
			pluginPath := filepath.Join(pm.pluginsDir, name)
			if err := pm.LoadPluginWithSandbox(pluginName, pluginPath, SandboxLimits{
				MaxCPUTime:       DefaultCPULimit,
				MaxMemory:        DefaultMemoryLimit,
				MaxExecutionTime: DefaultExecutionTimeout,
				MaxInstructions:  1000000,
			}); err != nil {
				if pm.logger != nil {
					pm.logger.Error(fmt.Sprintf("Failed to auto-load plugin %s: %v", pluginName, err))
				}
			} else if pm.logger != nil {
				pm.logger.Info(fmt.Sprintf("Auto-loaded plugin: %s", pluginName))
			}
		}
	}
}

// LoadPlugin загружает Lua-плагин из файла с ограничениями по умолчанию.
func (pm *PluginManager) LoadPlugin(name, filePath string) error {
	return pm.LoadPluginWithSandbox(name, filePath, SandboxLimits{
		MaxCPUTime:       DefaultCPULimit,
		MaxMemory:        DefaultMemoryLimit,
		MaxExecutionTime: DefaultExecutionTimeout,
		MaxInstructions:  1000000,
	})
}

// LoadPluginWithSandbox загружает плагин с ограничениями, используя пул состояний.
func (pm *PluginManager) LoadPluginWithSandbox(name, filePath string, limits SandboxLimits) error {
	if !pm.enabled {
		return fmt.Errorf("plugin system is disabled")
	}

	if pm.statePool.GetActiveCount() >= int32(MaxLuaStates) {
		return fmt.Errorf("max Lua states limit reached: %d", MaxLuaStates)
	}

	script, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read plugin file: %v", err)
	}

	manifest := pm.loadManifest(filePath)
	if manifest != nil && len(manifest.Dependencies) > 0 {
		if err := pm.checkDependencies(manifest.Dependencies); err != nil {
			return fmt.Errorf("dependency check failed: %v", err)
		}
	}

	L, err := pm.statePool.Acquire(limits)
	if err != nil {
		return fmt.Errorf("failed to acquire Lua state: %v", err)
	}

	pm.registerDatabaseFunctions(L.LState)
	pm.registerTransactionFunctions(L.LState)
	pm.registerTriggerFunctions(L.LState)
	pm.registerTimestampFunctions(L.LState)
	pm.registerEngineFunctions(L.LState, name)

	// Выполняем скрипт с таймаутом.
	// Ограничение по времени работает через L.SetContext(ctx),
	// который был установлен в NewInstrumentedLState/Reset/SetLimits.
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic during script execution: %v", r)
			}
		}()
		done <- L.DoString(string(script))
	}()

	select {
	case err := <-done:
		if err != nil {
			pm.statePool.Release(L)
			return fmt.Errorf("failed to execute plugin script: %v", err)
		}
	case <-time.After(MaxPluginLoadTime):
		// Отменяем через контекст
		L.mu.RLock()
		cancel := L.cancel
		L.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
		// Ждём завершения горутины, чтобы не было утечки
		select {
		case <-done:
		case <-time.After(1 * time.Second):
		}
		pm.statePool.Release(L)
		return fmt.Errorf("plugin load timeout")
	}

	version := pm.getPluginMetadata(L.LState, "version")
	author := pm.getPluginMetadata(L.LState, "author")
	description := pm.getPluginMetadata(L.LState, "description")
	isEngine := pm.getPluginMetadata(L.LState, "engine_type") != ""
	engineName := pm.getPluginMetadata(L.LState, "engine_type")

	now := time.Now()
	nowMs := now.UnixMilli()
	nowStr := now.Format("2006-01-02 15:04:05.000")

	basePlugin := &Plugin{
		Name:         name,
		FilePath:     filePath,
		LState:       L.LState,
		instrumented: L,
		logger:       pm.logger,
		storage:      pm.storage,
		loadedAt:     now,
		loadedAtMs:   nowMs,
		loadedAtStr:  nowStr,
		version:      version,
		author:       author,
		description:  description,
		eventLog:     make([]PluginEventLog, 0),
		maxEventLog:  DefaultMaxEventLogSize,
		isEngine:     isEngine,
		engineName:   engineName,
	}
	basePlugin.Status.Store(int32(StatusLoaded))

	var plugin interface{}
	if manifest != nil {
		pluginWithDeps := &PluginWithDeps{
			Plugin:       basePlugin,
			Manifest:     manifest,
			Dependencies: make(map[string]*PluginWithDeps),
			Dependents:   make(map[string]*PluginWithDeps),
		}
		plugin = pluginWithDeps
		pm.depResolver.RegisterPlugin(pluginWithDeps)
	} else {
		plugin = basePlugin
	}

	pm.plugins.Store(name, plugin)

	if pm.hotReload != nil {
		pm.hotReload.WatchPlugin(filePath)
	}

	if pm.logger != nil {
		engineInfo := ""
		if isEngine {
			engineInfo = fmt.Sprintf(" [ENGINE: %s]", engineName)
		}
		pm.logger.Info(fmt.Sprintf("Plugin loaded: %s v%s by %s - %s%s at %s", name, version, author, description, engineInfo, nowStr))
	}

	auditTimeMs, auditTimeStr := storage.GetCurrentTimestamp()
	storage.LogAudit("PLUGIN_LOAD", "PLUGIN", name, map[string]interface{}{
		"version":           version,
		"author":            author,
		"description":       description,
		"loaded_at":         nowMs,
		"loaded_at_str":     nowStr,
		"audit_time":        auditTimeMs,
		"audit_time_str":    auditTimeStr,
		"limits_cpu_ms":     limits.MaxCPUTime.Milliseconds(),
		"limits_memory_mb":  limits.MaxMemory / 1024 / 1024,
		"active_lua_states": pm.statePool.GetActiveCount(),
		"is_engine":         isEngine,
		"engine_type":       engineName,
	})

	if err := pm.callPluginFunction(plugin, "on_load"); err != nil {
		if pm.logger != nil {
			pm.logger.Warn(fmt.Sprintf("Plugin %s on_load error: %v", name, err))
		}
		pm.addPluginEventLog(plugin, "on_load_error", nil, err.Error())
	} else {
		pm.addPluginEventLog(plugin, "on_load", nil, "")
	}

	if isEngine && engineName != "" {
		if err := pm.registerEngineFromPlugin(plugin, engineName); err != nil {
			if pm.logger != nil {
				pm.logger.Warn(fmt.Sprintf("Failed to register engine from plugin %s: %v", name, err))
			}
		}
	}

	return nil
}

// registerEngineFromPlugin регистрирует движок из загруженного плагина.
func (pm *PluginManager) registerEngineFromPlugin(plugin interface{}, engineName string) error {
	L, p := getPluginLState(plugin)
	if L == nil || p == nil {
		return fmt.Errorf("plugin has no Lua state")
	}

	factoryFn := L.GetGlobal("create_engine")
	if factoryFn.Type() != lua.LTFunction {
		factoryFn = L.GetGlobal("new_engine")
		if factoryFn.Type() != lua.LTFunction {
			return fmt.Errorf("engine plugin must export create_engine or new_engine function")
		}
	}

	return pm.RegisterEngineFromLua(p.Name, engineName, factoryFn.(*lua.LFunction))
}

// loadManifest загружает манифест плагина из JSON-файла.
func (pm *PluginManager) loadManifest(pluginPath string) *PluginManifest {
	manifestPath := strings.TrimSuffix(pluginPath, ".lua") + ".json"
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil
	}

	var manifest PluginManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil
	}
	return &manifest
}

// checkDependencies проверяет зависимости плагина.
func (pm *PluginManager) checkDependencies(deps []PluginDependency) error {
	for _, dep := range deps {
		if dep.Optional {
			continue
		}

		val, ok := pm.plugins.Load(dep.Name)
		if !ok {
			return fmt.Errorf("missing dependency: %s", dep.Name)
		}

		if withDeps, ok := val.(*PluginWithDeps); ok && withDeps.Manifest != nil {
			if !pm.depResolver.CheckVersionCompatibility(dep, withDeps.Manifest.Version) {
				return fmt.Errorf("version incompatibility: %s requires %s but have %s",
					dep.Name, dep.Version, withDeps.Manifest.Version)
			}
		}
	}
	return nil
}

// registerTimestampFunctions регистрирует функции для работы с временными метками в Lua.
func (pm *PluginManager) registerTimestampFunctions(L *lua.LState) {
	L.SetGlobal("get_current_timestamp_ms", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(time.Now().UnixMilli()))
		return 1
	}))

	L.SetGlobal("get_current_timestamp_str", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(time.Now().Format("2006-01-02 15:04:05.000")))
		return 1
	}))

	L.SetGlobal("get_current_time", L.NewFunction(func(L *lua.LState) int {
		now := time.Now()
		table := L.NewTable()
		table.RawSetString("unix_ms", lua.LNumber(now.UnixMilli()))
		table.RawSetString("unix_sec", lua.LNumber(now.Unix()))
		table.RawSetString("string", lua.LString(now.Format("2006-01-02 15:04:05.000")))
		table.RawSetString("year", lua.LNumber(now.Year()))
		table.RawSetString("month", lua.LNumber(int(now.Month())))
		table.RawSetString("day", lua.LNumber(now.Day()))
		table.RawSetString("hour", lua.LNumber(now.Hour()))
		table.RawSetString("minute", lua.LNumber(now.Minute()))
		table.RawSetString("second", lua.LNumber(now.Second()))
		L.Push(table)
		return 1
	}))
}

// registerDatabaseFunctions регистрирует функции доступа к СУБД в Lua.
func (pm *PluginManager) registerDatabaseFunctions(L *lua.LState) {
	L.SetGlobal("get_database", L.NewFunction(func(L *lua.LState) int {
		if pm.storage == nil {
			L.Push(lua.LNil)
			L.Push(lua.LString("storage not available"))
			return 2
		}
		dbName := L.CheckString(1)
		db, err := pm.storage.GetDatabase(dbName)
		if err != nil {
			L.Push(lua.LNil)
			L.Push(lua.LString(err.Error()))
			return 2
		}

		ud := L.NewUserData()
		ud.Value = db
		L.SetMetatable(ud, L.GetTypeMetatable("database"))
		L.Push(ud)
		return 1
	}))

	L.SetGlobal("get_collection", L.NewFunction(func(L *lua.LState) int {
		if pm.storage == nil {
			L.Push(lua.LNil)
			L.Push(lua.LString("storage not available"))
			return 2
		}
		dbName := L.CheckString(1)
		collName := L.CheckString(2)

		db, err := pm.storage.GetDatabase(dbName)
		if err != nil {
			L.Push(lua.LNil)
			L.Push(lua.LString(err.Error()))
			return 2
		}

		coll, err := db.GetCollection(collName)
		if err != nil {
			L.Push(lua.LNil)
			L.Push(lua.LString(err.Error()))
			return 2
		}

		ud := L.NewUserData()
		ud.Value = coll
		L.SetMetatable(ud, L.GetTypeMetatable("collection"))
		L.Push(ud)
		return 1
	}))

	L.SetGlobal("plugin_log", L.NewFunction(func(L *lua.LState) int {
		level := L.CheckString(1)
		message := L.CheckString(2)

		if pm.logger != nil {
			logMsg := fmt.Sprintf("[PLUGIN] %s: %s", level, message)
			switch level {
			case "debug":
				pm.logger.Debug(logMsg)
			case "info":
				pm.logger.Info(logMsg)
			case "warn":
				pm.logger.Warn(logMsg)
			case "error":
				pm.logger.Error(logMsg)
			default:
				pm.logger.Info(logMsg)
			}
		}

		return 0
	}))

	L.SetGlobal("emit_event", L.NewFunction(func(L *lua.LState) int {
		// Проверяем, не закрыт ли eventBus, чтобы избежать
		// паники "send on closed channel".
		if pm.eventBusClosed.Load() {
			return 0
		}
		eventType := L.CheckString(1)
		eventData := L.CheckAny(2)

		nowMs := time.Now().UnixMilli()
		nowStr := time.Now().Format("2006-01-02 15:04:05.000")
		event := PluginEvent{
			EventType:    eventType,
			Data:         pm.luaValueToGo(eventData),
			Timestamp:    nowMs,
			TimestampStr: nowStr,
		}

		select {
		case pm.eventBus <- event:
		default:
			if pm.logger != nil {
				pm.logger.Warn("Plugin event bus full, event dropped")
			}
		}

		return 0
	}))

	pm.setupDatabaseMetatable(L)
	pm.setupCollectionMetatable(L)
}

// registerEngineFunctions регистрирует функции для создания кастомных движков.
func (pm *PluginManager) registerEngineFunctions(L *lua.LState, pluginName string) {
	L.SetGlobal("register_engine", L.NewFunction(func(L *lua.LState) int {
		engineName := L.CheckString(1)
		factoryFn := L.CheckFunction(2)

		if err := pm.RegisterEngineFromLua(pluginName, engineName, factoryFn); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}

		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("get_engine", L.NewFunction(func(L *lua.LState) int {
		engineName := L.CheckString(1)

		engine := pm.engineRegistry.GetEngine(engineName)
		if engine == nil {
			L.Push(lua.LNil)
			L.Push(lua.LString(fmt.Sprintf("engine not found: %s", engineName)))
			return 2
		}

		ud := L.NewUserData()
		ud.Value = engine
		L.SetMetatable(ud, L.GetTypeMetatable("custom_engine"))
		L.Push(ud)
		return 1
	}))

	L.SetGlobal("list_engines", L.NewFunction(func(L *lua.LState) int {
		engines := pm.engineRegistry.ListEngines()
		table := L.NewTable()
		for i, name := range engines {
			table.RawSetInt(i+1, lua.LString(name))
		}
		L.Push(table)
		return 1
	}))

	L.SetGlobal("list_active_engines", L.NewFunction(func(L *lua.LState) int {
		engines := pm.engineRegistry.ListActiveEngines()
		table := L.NewTable()
		for i, name := range engines {
			table.RawSetInt(i+1, lua.LString(name))
		}
		L.Push(table)
		return 1
	}))

	// Настройка метатаблицы для кастомного движка
	engineMt := L.NewTypeMetatable("custom_engine")
	L.SetField(engineMt, "__index", L.NewFunction(func(L *lua.LState) int {
		engine := L.CheckUserData(1).Value.(CustomEngine)
		method := L.CheckString(2)

		switch method {
		case "name":
			L.Push(lua.LString(engine.Name()))
		case "version":
			L.Push(lua.LString(engine.Version()))
		case "count":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				L.Push(lua.LNumber(engine.Count()))
				return 1
			}))
		case "size":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				L.Push(lua.LNumber(engine.Size()))
				return 1
			}))
		case "insert":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				docData := L.CheckTable(1)
				doc := storage.NewDocument()
				docData.ForEach(func(key, value lua.LValue) {
					if key.Type() == lua.LTString {
						doc.SetField(key.String(), pm.luaValueToGo(value))
					}
				})
				if err := engine.Insert(doc); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "find":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				id := L.CheckString(1)
				doc, err := engine.Find(id)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				resultTable := pm.goValueToLua(L, doc.ToMap()).(*lua.LTable)
				L.Push(resultTable)
				return 1
			}))
		case "update":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				id := L.CheckString(1)
				updates := L.CheckTable(2)
				updatesMap := pm.luaTableToMap(updates)
				if err := engine.Update(id, updatesMap); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "delete":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				id := L.CheckString(1)
				if err := engine.Delete(id); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "create_index":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				name := L.CheckString(1)
				fieldsTable := L.CheckTable(2)
				unique := L.OptBool(3, false)
				fields := make([]string, 0)
				fieldsTable.ForEach(func(key, value lua.LValue) {
					if str, ok := value.(lua.LString); ok {
						fields = append(fields, string(str))
					}
				})
				if err := engine.CreateIndex(name, fields, unique); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "drop_index":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				name := L.CheckString(1)
				if err := engine.DropIndex(name); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "get_stats":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				stats := engine.GetStats()
				statsTable := pm.goValueToLua(L, stats).(*lua.LTable)
				L.Push(statsTable)
				return 1
			}))
		default:
			L.Push(lua.LNil)
		}
		return 1
	}))
}

// registerTransactionFunctions регистрирует функции для работы с транзакциями.
func (pm *PluginManager) registerTransactionFunctions(L *lua.LState) {
	L.SetGlobal("begin_transaction", L.NewFunction(func(L *lua.LState) int {
		tx := storage.BeginTransaction()
		if tx == nil {
			L.Push(lua.LNil)
			L.Push(lua.LString("Failed to begin transaction"))
			return 2
		}

		ud := L.NewUserData()
		ud.Value = tx
		L.SetMetatable(ud, L.GetTypeMetatable("transaction"))
		L.Push(ud)
		return 1
	}))

	L.SetGlobal("commit_transaction", L.NewFunction(func(L *lua.LState) int {
		if err := storage.CommitCurrentTransaction(); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}
		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("abort_transaction", L.NewFunction(func(L *lua.LState) int {
		if err := storage.AbortCurrentTransaction(); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}
		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("has_active_transaction", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LBool(storage.HasActiveTransaction()))
		return 1
	}))

	L.SetGlobal("get_current_transaction_id", L.NewFunction(func(L *lua.LState) int {
		txID := storage.GetCurrentTransactionID()
		if txID == "" {
			L.Push(lua.LNil)
		} else {
			L.Push(lua.LString(txID))
		}
		return 1
	}))

	L.SetGlobal("get_active_transactions", L.NewFunction(func(L *lua.LState) int {
		txs := storage.GetActiveTransactions()
		table := L.NewTable()
		for i, tx := range txs {
			txTable := L.NewTable()
			txTable.RawSetString("id", lua.LString(tx.ID))
			txTable.RawSetString("status", lua.LString(tx.Status))
			txTable.RawSetString("start_time", lua.LNumber(tx.StartTime))
			txTable.RawSetString("operation_count", lua.LNumber(tx.OperationCount))
			table.RawSetInt(i+1, txTable)
		}
		L.Push(table)
		return 1
	}))

	// Настройка метатаблицы для транзакций
	mt := L.NewTypeMetatable("transaction")
	L.SetField(mt, "__index", L.NewFunction(func(L *lua.LState) int {
		ud := L.CheckUserData(1)
		tx, ok := ud.Value.(*storage.Transaction)
		if !ok {
			L.Push(lua.LNil)
			return 1
		}
		method := L.CheckString(2)

		switch method {
		case "get_id":
			L.Push(lua.LString(fmt.Sprintf("%d", tx.ID)))
		case "get_operation_count":
			L.Push(lua.LNumber(0))
		case "get_start_time":
			L.Push(lua.LNumber(tx.StartTime))
		case "get_status":
			L.Push(lua.LString("active"))
		default:
			L.Push(lua.LNil)
		}
		return 1
	}))
}

// registerTriggerFunctions регистрирует функции для работы с триггерами.
func (pm *PluginManager) registerTriggerFunctions(L *lua.LState) {
	tm := storage.GetTriggerManager()

	L.SetGlobal("create_trigger", L.NewFunction(func(L *lua.LState) int {
		if tm == nil {
			L.Push(lua.LString("trigger manager not available"))
			return 1
		}
		database := L.CheckString(1)
		collection := L.CheckString(2)
		name := L.CheckString(3)
		event := L.CheckString(4)
		config := L.CheckTable(5)

		configMap := make(map[string]interface{})
		config.ForEach(func(key, value lua.LValue) {
			if key.Type() == lua.LTString {
				configMap[key.String()] = pm.luaValueToGo(value)
			}
		})

		if err := tm.CreateTrigger(database, collection, name, storage.TriggerEvent(event), configMap); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}
		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("drop_trigger", L.NewFunction(func(L *lua.LState) int {
		if tm == nil {
			L.Push(lua.LString("trigger manager not available"))
			return 1
		}
		collection := L.CheckString(1)
		event := L.CheckString(2)
		name := L.CheckString(3)

		if err := tm.DropTrigger(collection, event, name); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}
		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("enable_trigger", L.NewFunction(func(L *lua.LState) int {
		if tm == nil {
			L.Push(lua.LString("trigger manager not available"))
			return 1
		}
		collection := L.CheckString(1)
		event := L.CheckString(2)
		name := L.CheckString(3)

		if err := tm.EnableTrigger(collection, event, name); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}
		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("disable_trigger", L.NewFunction(func(L *lua.LState) int {
		if tm == nil {
			L.Push(lua.LString("trigger manager not available"))
			return 1
		}
		collection := L.CheckString(1)
		event := L.CheckString(2)
		name := L.CheckString(3)

		if err := tm.DisableTrigger(collection, event, name); err != nil {
			L.Push(lua.LString(err.Error()))
			return 1
		}
		L.Push(lua.LNil)
		return 1
	}))

	L.SetGlobal("list_triggers", L.NewFunction(func(L *lua.LState) int {
		if tm == nil {
			L.Push(L.NewTable())
			return 1
		}
		collection := L.OptString(1, "")
		triggers := tm.ListTriggers(collection)

		table := L.NewTable()
		for i, trigger := range triggers {
			triggerTable := L.NewTable()
			triggerTable.RawSetString("name", lua.LString(trigger.Name))
			triggerTable.RawSetString("collection", lua.LString(trigger.Collection))
			triggerTable.RawSetString("event", lua.LString(string(trigger.Event)))
			triggerTable.RawSetString("action", lua.LString(string(trigger.Action)))
			triggerTable.RawSetString("enabled", lua.LBool(trigger.Enabled))
			triggerTable.RawSetString("description", lua.LString(trigger.Description))
			table.RawSetInt(i+1, triggerTable)
		}
		L.Push(table)
		return 1
	}))
}

// setupDatabaseMetatable настраивает методы для объекта базы данных в Lua.
func (pm *PluginManager) setupDatabaseMetatable(L *lua.LState) {
	mt := L.NewTypeMetatable("database")
	L.SetField(mt, "__index", L.NewFunction(func(L *lua.LState) int {
		db := L.CheckUserData(1).Value.(*storage.Database)
		method := L.CheckString(2)

		switch method {
		case "create_collection":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				name := L.CheckString(1)
				err := db.CreateCollection(name)
				if err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "get_collection":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				name := L.CheckString(1)
				coll, err := db.GetCollection(name)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				ud := L.NewUserData()
				ud.Value = coll
				L.SetMetatable(ud, L.GetTypeMetatable("collection"))
				L.Push(ud)
				return 1
			}))
		case "drop_collection":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				name := L.CheckString(1)
				err := db.DropCollection(name)
				if err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "list_collections":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				collections := db.ListCollections()
				table := L.NewTable()
				for i, name := range collections {
					table.RawSetInt(i+1, lua.LString(name))
				}
				L.Push(table)
				return 1
			}))
		case "name":
			L.Push(lua.LString(db.Name()))
		default:
			L.Push(lua.LNil)
		}
		return 1
	}))
}

// setupCollectionMetatable настраивает методы для объекта коллекции в Lua.
func (pm *PluginManager) setupCollectionMetatable(L *lua.LState) {
	mt := L.NewTypeMetatable("collection")
	L.SetField(mt, "__index", L.NewFunction(func(L *lua.LState) int {
		coll := L.CheckUserData(1).Value.(*storage.Collection)
		method := L.CheckString(2)

		switch method {
		case "insert":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				doc := L.CheckTable(1)
				fields := make(map[string]interface{})
				doc.ForEach(func(key, value lua.LValue) {
					if key.Type() == lua.LTString {
						fields[key.String()] = pm.luaValueToGo(value)
					}
				})

				newDoc := storage.NewDocument()
				for k, v := range fields {
					newDoc.SetField(k, v)
				}

				err := coll.Insert(newDoc)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				L.Push(lua.LString(newDoc.ID))
				L.Push(lua.LNil)
				return 2
			}))
		case "find":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				id := L.CheckString(1)
				doc, err := coll.Find(id)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}
				table := L.NewTable()
				table.RawSetString("_id", lua.LString(doc.ID))
				for k, v := range doc.GetFields() {
					table.RawSetString(k, pm.goValueToLua(L, v))
				}
				L.Push(table)
				return 1
			}))
		case "find_by_index":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				indexName := L.CheckString(1)
				value := pm.luaValueToGo(L.CheckAny(2))

				docs, err := coll.FindByIndex(indexName, value)
				if err != nil {
					L.Push(lua.LNil)
					L.Push(lua.LString(err.Error()))
					return 2
				}

				table := L.NewTable()
				for i, doc := range docs {
					docTable := L.NewTable()
					docTable.RawSetString("_id", lua.LString(doc.ID))
					for k, v := range doc.GetFields() {
						docTable.RawSetString(k, pm.goValueToLua(L, v))
					}
					table.RawSetInt(i+1, docTable)
				}
				L.Push(table)
				return 1
			}))
		case "update":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				id := L.CheckString(1)
				updates := L.CheckTable(2)

				fields := make(map[string]interface{})
				updates.ForEach(func(key, value lua.LValue) {
					if key.Type() == lua.LTString {
						fields[key.String()] = pm.luaValueToGo(value)
					}
				})

				err := coll.Update(id, fields)
				if err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "delete":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				id := L.CheckString(1)
				err := coll.Delete(id)
				if err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LNil)
				return 1
			}))
		case "count":
			L.Push(L.NewFunction(func(L *lua.LState) int {
				count := coll.Count()
				L.Push(lua.LNumber(count))
				return 1
			}))
		case "name":
			L.Push(lua.LString(coll.Name()))
		default:
			L.Push(lua.LNil)
		}
		return 1
	}))
}

// luaValueToGo конвертирует Lua-значение в Go-значение.
func (pm *PluginManager) luaValueToGo(val lua.LValue) interface{} {
	if val == nil || val == lua.LNil {
		return nil
	}

	switch v := val.(type) {
	case lua.LString:
		return string(v)
	case lua.LNumber:
		return float64(v)
	case lua.LBool:
		return bool(v)
	case *lua.LTable:
		return pm.luaTableToMap(v)
	default:
		return v.String()
	}
}

// luaTableToMap конвертирует Lua-таблицу в Go-карту.
func (pm *PluginManager) luaTableToMap(table *lua.LTable) map[string]interface{} {
	result := make(map[string]interface{})
	table.ForEach(func(key, value lua.LValue) {
		keyStr := "unknown"
		if key.Type() == lua.LTString {
			keyStr = key.String()
		} else if key.Type() == lua.LTNumber {
			keyStr = fmt.Sprintf("%d", int64(key.(lua.LNumber)))
		}
		result[keyStr] = pm.luaValueToGo(value)
	})
	return result
}

// goValueToLua конвертирует Go-значение в Lua-значение.
func (pm *PluginManager) goValueToLua(L *lua.LState, val interface{}) lua.LValue {
	if val == nil {
		return lua.LNil
	}

	switch v := val.(type) {
	case string:
		return lua.LString(v)
	case int:
		return lua.LNumber(float64(v))
	case int64:
		return lua.LNumber(float64(v))
	case float32:
		return lua.LNumber(float64(v))
	case float64:
		return lua.LNumber(v)
	case bool:
		return lua.LBool(v)
	case map[string]interface{}:
		table := L.NewTable()
		for k, val := range v {
			table.RawSetString(k, pm.goValueToLua(L, val))
		}
		return table
	case []interface{}:
		table := L.NewTable()
		for i, val := range v {
			table.RawSetInt(i+1, pm.goValueToLua(L, val))
		}
		return table
	default:
		return lua.LString(fmt.Sprintf("%v", v))
	}
}

// getPluginMetadata извлекает метаданные из загруженного Lua-скрипта.
func (pm *PluginManager) getPluginMetadata(L *lua.LState, field string) string {
	val := L.GetGlobal(field)
	if str, ok := val.(lua.LString); ok {
		return string(str)
	}
	return ""
}

// callPluginFunction вызывает функцию плагина по имени с защитой от паники.
func (pm *PluginManager) callPluginFunction(plugin interface{}, funcName string) error {
	L, p := getPluginLState(plugin)
	if L == nil || p == nil || p.IsClosed() {
		return fmt.Errorf("plugin has no Lua state or is closed")
	}

	fn := L.GetGlobal(funcName)
	if fn == lua.LNil {
		return nil // Функция не определена - не ошибка
	}

	var callErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				callErr = fmt.Errorf("panic during %s: %v", funcName, r)
			}
		}()
		callErr = L.CallByParam(lua.P{
			Fn:      fn,
			NRet:    0,
			Protect: true,
		})
	}()

	if callErr != nil {
		return fmt.Errorf("failed to call %s: %v", funcName, callErr)
	}

	return nil
}

// addPluginEventLog добавляет событие в лог плагина.
func (pm *PluginManager) addPluginEventLog(plugin interface{}, eventType string, data interface{}, errMsg string) {
	var p *Plugin

	switch v := plugin.(type) {
	case *Plugin:
		p = v
	case *PluginWithDeps:
		p = v.Plugin
	default:
		return
	}

	p.eventLogMu.Lock()
	defer p.eventLogMu.Unlock()

	nowMs := time.Now().UnixMilli()
	nowStr := time.Now().Format("2006-01-02 15:04:05.000")
	eventLog := PluginEventLog{
		PluginName:   p.Name,
		EventType:    eventType,
		Data:         data,
		Timestamp:    nowMs,
		TimestampStr: nowStr,
		Error:        errMsg,
	}

	p.eventLog = append(p.eventLog, eventLog)
	if len(p.eventLog) > p.maxEventLog {
		p.eventLog = p.eventLog[1:] // Ограничиваем размер лога
	}
}

// eventLoop обрабатывает события от плагинов.
// Завершается при закрытии eventBus (флаг eventBusClosed и close канала).
func (pm *PluginManager) eventLoop() {
	for event := range pm.eventBus {
		if pm.logger != nil {
			pm.logger.Debug(fmt.Sprintf("Plugin event [%s] at %s: %+v", event.EventType, event.TimestampStr, event.Data))
		}

		pm.plugins.Range(func(key, value interface{}) bool {
			go pm.notifyPlugin(value, event)
			return true
		})
	}
}

// notifyPlugin уведомляет конкретный плагин о событии.
func (pm *PluginManager) notifyPlugin(plugin interface{}, event PluginEvent) {
	L, p := getPluginLState(plugin)
	if L == nil || p == nil || p.IsClosed() {
		return
	}

	fn := L.GetGlobal("on_event")
	if fn == lua.LNil {
		return
	}

	event.PluginName = p.Name

	eventTable := L.NewTable()
	eventTable.RawSetString("type", lua.LString(event.EventType))
	eventTable.RawSetString("plugin_name", lua.LString(event.PluginName))
	eventTable.RawSetString("timestamp", lua.LNumber(event.Timestamp))
	eventTable.RawSetString("timestamp_str", lua.LString(event.TimestampStr))
	eventTable.RawSetString("data", pm.goValueToLua(L, event.Data))

	var callErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				callErr = fmt.Errorf("panic during on_event: %v", r)
			}
		}()
		callErr = L.CallByParam(lua.P{
			Fn:      fn,
			NRet:    0,
			Protect: true,
		}, eventTable)
	}()

	if callErr != nil && pm.logger != nil {
		pm.logger.Error(fmt.Sprintf("Plugin %s on_event error: %v", p.Name, callErr))
	}
}

// ExecutePlugin выполняет пользовательскую функцию плагина.
func (pm *PluginManager) ExecutePlugin(pluginName, funcName string, args ...interface{}) (interface{}, error) {
	if !pm.enabled {
		return nil, fmt.Errorf("plugin system is disabled")
	}

	val, ok := pm.plugins.Load(pluginName)
	if !ok {
		return nil, fmt.Errorf("plugin not found: %s", pluginName)
	}

	L, p := getPluginLState(val)
	if L == nil || p == nil {
		return nil, fmt.Errorf("unknown plugin type")
	}

	if PluginStatus(p.Status.Load()) != StatusRunning {
		return nil, fmt.Errorf("plugin %s is not running", pluginName)
	}

	if p.IsClosed() {
		return nil, fmt.Errorf("plugin %s is closed", pluginName)
	}

	fn := L.GetGlobal(funcName)
	if fn == lua.LNil {
		return nil, fmt.Errorf("function %s not found in plugin %s", funcName, pluginName)
	}

	luaArgs := make([]lua.LValue, len(args))
	for i, arg := range args {
		luaArgs[i] = pm.goValueToLua(L, arg)
	}

	startTime := time.Now()
	var ret lua.LValue
	var callErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				callErr = fmt.Errorf("panic during execution: %v", r)
			}
		}()
		callErr = L.CallByParam(lua.P{
			Fn:      fn,
			NRet:    1,
			Protect: true,
		}, luaArgs...)
		if callErr == nil {
			ret = L.Get(-1)
			L.Pop(1)
		}
	}()

	duration := time.Since(startTime)

	if callErr != nil {
		pm.addPluginEventLog(val, funcName, args, callErr.Error())
		return nil, fmt.Errorf("plugin execution failed: %v", callErr)
	}

	pm.addPluginEventLog(val, funcName, args, "")

	if pm.logger != nil {
		pm.logger.Debug(fmt.Sprintf("Plugin %s executed %s in %dms", pluginName, funcName, duration.Milliseconds()))
	}

	return pm.luaValueToGo(ret), nil
}

// UnloadPlugin выгружает плагин и возвращает состояние в пул.
//
// Используем поле plugin.instrumented вместо type assertion
// interface{}(L).(*InstrumentedLState), который всегда возвращал false,
// что приводило к утечке Lua-состояний и некорректному уменьшению active.
func (pm *PluginManager) UnloadPlugin(name string) error {
	if !pm.enabled {
		return fmt.Errorf("plugin system is disabled")
	}

	val, ok := pm.plugins.Load(name)
	if !ok {
		return fmt.Errorf("plugin not found: %s", name)
	}

	if err := pm.callPluginFunction(val, "on_unload"); err != nil {
		if pm.logger != nil {
			pm.logger.Warn(fmt.Sprintf("Plugin %s on_unload error: %v", name, err))
		}
		pm.addPluginEventLog(val, "on_unload_error", nil, err.Error())
	} else {
		pm.addPluginEventLog(val, "on_unload", nil, "")
	}

	// Извлекаем InstrumentedLState из плагина
	inst := getPluginInstrumented(val)

	var p *Plugin
	switch v := val.(type) {
	case *Plugin:
		p = v
	case *PluginWithDeps:
		p = v.Plugin
	}

	if p != nil {
		p.Status.Store(int32(StatusStopped))
		p.closePlugin()
	}

	// Возвращаем состояние в пул (или закрываем, если пул закрыт)
	if inst != nil {
		pm.statePool.Release(inst)
	}

	auditTimeMs, auditTimeStr := storage.GetCurrentTimestamp()
	storage.LogAudit("PLUGIN_UNLOAD", "PLUGIN", name, map[string]interface{}{
		"unloaded_at":       time.Now().UnixMilli(),
		"unloaded_at_str":   time.Now().Format("2006-01-02 15:04:05.000"),
		"audit_time":        auditTimeMs,
		"audit_time_str":    auditTimeStr,
		"active_lua_states": pm.statePool.GetActiveCount(),
	})

	pm.plugins.Delete(name)
	if pm.logger != nil {
		pm.logger.Info(fmt.Sprintf("Plugin unloaded: %s", name))
	}

	return nil
}

// StartPlugin запускает плагин.
func (pm *PluginManager) StartPlugin(name string) error {
	if !pm.enabled {
		return fmt.Errorf("plugin system is disabled")
	}

	val, ok := pm.plugins.Load(name)
	if !ok {
		return fmt.Errorf("plugin not found: %s", name)
	}

	var p *Plugin
	switch v := val.(type) {
	case *Plugin:
		p = v
	case *PluginWithDeps:
		p = v.Plugin
	default:
		return fmt.Errorf("unknown plugin type")
	}

	if p.IsClosed() {
		return fmt.Errorf("plugin is closed")
	}

	p.Status.Store(int32(StatusRunning))

	if err := pm.callPluginFunction(val, "on_start"); err != nil {
		p.Status.Store(int32(StatusError))
		pm.addPluginEventLog(val, "on_start_error", nil, err.Error())
		return fmt.Errorf("failed to start plugin: %v", err)
	}

	pm.addPluginEventLog(val, "on_start", nil, "")

	auditTimeMs, auditTimeStr := storage.GetCurrentTimestamp()
	storage.LogAudit("PLUGIN_START", "PLUGIN", name, map[string]interface{}{
		"started_at":        time.Now().UnixMilli(),
		"started_at_str":    time.Now().Format("2006-01-02 15:04:05.000"),
		"audit_time":        auditTimeMs,
		"audit_time_str":    auditTimeStr,
		"active_lua_states": pm.statePool.GetActiveCount(),
	})

	if pm.logger != nil {
		pm.logger.Info(fmt.Sprintf("Plugin started: %s", name))
	}
	return nil
}

// StopPlugin останавливает плагин.
func (pm *PluginManager) StopPlugin(name string) error {
	if !pm.enabled {
		return fmt.Errorf("plugin system is disabled")
	}

	val, ok := pm.plugins.Load(name)
	if !ok {
		return fmt.Errorf("plugin not found: %s", name)
	}

	if err := pm.callPluginFunction(val, "on_stop"); err != nil {
		if pm.logger != nil {
			pm.logger.Warn(fmt.Sprintf("Plugin %s on_stop error: %v", name, err))
		}
		pm.addPluginEventLog(val, "on_stop_error", nil, err.Error())
	} else {
		pm.addPluginEventLog(val, "on_stop", nil, "")
	}

	var p *Plugin
	switch v := val.(type) {
	case *Plugin:
		p = v
	case *PluginWithDeps:
		p = v.Plugin
	}
	if p != nil {
		p.Status.Store(int32(StatusStopped))
	}

	auditTimeMs, auditTimeStr := storage.GetCurrentTimestamp()
	storage.LogAudit("PLUGIN_STOP", "PLUGIN", name, map[string]interface{}{
		"stopped_at":        time.Now().UnixMilli(),
		"stopped_at_str":    time.Now().Format("2006-01-02 15:04:05.000"),
		"audit_time":        auditTimeMs,
		"audit_time_str":    auditTimeStr,
		"active_lua_states": pm.statePool.GetActiveCount(),
	})

	if pm.logger != nil {
		pm.logger.Info(fmt.Sprintf("Plugin stopped: %s", name))
	}

	return nil
}

// ListPlugins возвращает список всех загруженных плагинов.
func (pm *PluginManager) ListPlugins() []*Plugin {
	plugins := make([]*Plugin, 0)
	pm.plugins.Range(func(key, value interface{}) bool {
		switch v := value.(type) {
		case *Plugin:
			plugins = append(plugins, v)
		case *PluginWithDeps:
			plugins = append(plugins, v.Plugin)
		}
		return true
	})
	return plugins
}

// GetPlugin возвращает плагин по имени.
func (pm *PluginManager) GetPlugin(name string) (*Plugin, error) {
	val, ok := pm.plugins.Load(name)
	if !ok {
		return nil, fmt.Errorf("plugin not found: %s", name)
	}
	switch v := val.(type) {
	case *Plugin:
		return v, nil
	case *PluginWithDeps:
		return v.Plugin, nil
	default:
		return nil, fmt.Errorf("unknown plugin type")
	}
}

// GetPluginWithDeps возвращает плагин с зависимостями.
func (pm *PluginManager) GetPluginWithDeps(name string) (*PluginWithDeps, error) {
	val, ok := pm.plugins.Load(name)
	if !ok {
		return nil, fmt.Errorf("plugin not found: %s", name)
	}
	if withDeps, ok := val.(*PluginWithDeps); ok {
		return withDeps, nil
	}
	return nil, fmt.Errorf("plugin %s does not support dependencies", name)
}

// LoadPluginsInOrder загружает плагины в правильном порядке зависимостей.
func (pm *PluginManager) LoadPluginsInOrder() error {
	order, err := pm.depResolver.ResolveOrder()
	if err != nil {
		return err
	}

	for _, plugin := range order {
		if err := pm.StartPlugin(plugin.Name); err != nil {
			return fmt.Errorf("failed to start plugin %s: %v", plugin.Name, err)
		}
	}

	return nil
}

// GetHotReloadManager возвращает менеджер горячей перезагрузки.
func (pm *PluginManager) GetHotReloadManager() *HotReloadManager { return pm.hotReload }

// GetMarketplace возвращает маркетплейс плагинов.
func (pm *PluginManager) GetMarketplace() *PluginMarketplace { return pm.marketplace }

// EnablePluginMarketplace включает маркетплейс плагинов.
func (pm *PluginManager) EnablePluginMarketplace(apiEndpoint string) {
	pm.marketplace = NewPluginMarketplace(apiEndpoint, pm, pm.logger)
}

// IsEnabled возвращает статус системы плагинов.
func (pm *PluginManager) IsEnabled() bool { return pm.enabled }

// GetPluginsDir возвращает директорию с плагинами.
func (pm *PluginManager) GetPluginsDir() string { return pm.pluginsDir }

// GetPluginEventLog возвращает лог событий плагина.
func (p *Plugin) GetPluginEventLog() []PluginEventLog {
	p.eventLogMu.RLock()
	defer p.eventLogMu.RUnlock()

	result := make([]PluginEventLog, len(p.eventLog))
	copy(result, p.eventLog)
	return result
}

// ClearPluginEventLog очищает лог событий плагина.
func (p *Plugin) ClearPluginEventLog() {
	p.eventLogMu.Lock()
	defer p.eventLogMu.Unlock()
	p.eventLog = make([]PluginEventLog, 0)
}

// Version возвращает версию плагина.
func (p *Plugin) Version() string { return p.version }

// Author возвращает автора плагина.
func (p *Plugin) Author() string { return p.author }

// Description возвращает описание плагина.
func (p *Plugin) Description() string { return p.description }

// LoadedAt возвращает время загрузки плагина.
func (p *Plugin) LoadedAt() time.Time { return p.loadedAt }

// LoadedAtMs возвращает время загрузки плагина в миллисекундах.
func (p *Plugin) LoadedAtMs() int64 { return p.loadedAtMs }

// LoadedAtStr возвращает строковое представление времени загрузки плагина.
func (p *Plugin) LoadedAtStr() string { return p.loadedAtStr }

// GetStatus возвращает статус плагина.
func (p *Plugin) GetStatus() PluginStatus {
	return PluginStatus(p.Status.Load())
}

// GetLuaStatePoolStats возвращает статистику пула Lua-состояний.
func (pm *PluginManager) GetLuaStatePoolStats() map[string]interface{} {
	return map[string]interface{}{
		"active_count": pm.statePool.GetActiveCount(),
		"pool_size":    pm.statePool.GetPoolSize(),
		"max_allowed":  MaxLuaStates,
		"ttl_seconds":  LuaStateTTL.Seconds(),
	}
}

// Close закрывает менеджер плагинов и все ресурсы.
//
// eventBus закрывается через флаг eventBusClosed, чтобы
// избежать паники "send on closed channel" при отправке событий
// из плагинов после закрытия.
func (pm *PluginManager) Close() error {
	if pm.hotReload != nil {
		pm.hotReload.Stop()
	}

	if err := pm.engineRegistry.CloseAll(); err != nil {
		if pm.logger != nil {
			pm.logger.Error(fmt.Sprintf("Error closing engines: %v", err))
		}
	}

	// Выгружаем все плагины
	pm.plugins.Range(func(key, value interface{}) bool {
		name := key.(string)
		_ = pm.UnloadPlugin(name)
		return true
	})

	// Помечаем eventBus как закрытый и закрываем канал.
	// eventLoop завершится по range.
	if pm.eventBusClosed.CAS(false, true) {
		close(pm.eventBus)
	}

	pm.statePool.Close()

	return nil
}
