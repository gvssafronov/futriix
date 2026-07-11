/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/api/webui.go
// Назначение: Веб-интерфейс в стиле dashboard для управления СУБД futriis

package api

import (
	"bufio"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"futriis/internal/acl"
	"futriis/internal/cluster"
	"futriis/internal/log"
	"futriis/internal/plugin"
	"futriis/internal/storage"
)

//go:embed static/*
var staticFiles embed.FS

// LogEntry представляет запись в логе веб-интерфейса
type LogEntry struct {
	Timestamp   int64                  `json:"timestamp"`
	Operation   string                 `json:"operation"`
	Target      string                 `json:"target"`
	User        string                 `json:"user"`
	Status      string                 `json:"status"`
	ErrorMsg    string                 `json:"error_msg,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
}

// LogBuffer буферизирует логи веб-интерфейса
type LogBuffer struct {
	entries   []LogEntry
	mu        sync.RWMutex
	maxSize   int
	filePath  string
	file      *os.File
	writer    *bufio.Writer
	closed    bool
}

// NewLogBuffer создаёт новый буфер логов
func NewLogBuffer(filePath string, maxSize int) (*LogBuffer, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %v", err)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	return &LogBuffer{
		entries:  make([]LogEntry, 0, maxSize),
		maxSize:  maxSize,
		filePath: filePath,
		file:     file,
		writer:   bufio.NewWriter(file),
	}, nil
}

// Add добавляет запись в лог
func (lb *LogBuffer) Add(entry LogEntry) {
	if lb == nil {
		return
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.closed {
		return
	}

	lb.entries = append(lb.entries, entry)
	if len(lb.entries) > lb.maxSize {
		lb.entries = lb.entries[len(lb.entries)-lb.maxSize:]
	}

	data, err := json.Marshal(entry)
	if err == nil && lb.writer != nil {
		lb.writer.Write(data)
		lb.writer.Write([]byte("\n"))
		lb.writer.Flush()
	}
}

// GetEntries возвращает все записи лога
func (lb *LogBuffer) GetEntries() []LogEntry {
	if lb == nil {
		return []LogEntry{}
	}
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	result := make([]LogEntry, len(lb.entries))
	copy(result, lb.entries)
	return result
}

// Close закрывает буфер логов
func (lb *LogBuffer) Close() error {
	if lb == nil {
		return nil
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.closed = true
	if lb.writer != nil {
		lb.writer.Flush()
	}
	if lb.file != nil {
		return lb.file.Close()
	}
	return nil
}

// ========== SystemMetrics ==========

// SystemMetrics представляет системные метрики
type SystemMetrics struct {
	mu                sync.RWMutex
	RequestCount      uint64                     `json:"request_count"`
	ErrorCount        uint64                     `json:"error_count"`
	AverageLatencyMs  float64                    `json:"average_latency_ms"`
	P99LatencyMs      float64                    `json:"p99_latency_ms"`
	ActiveConnections int                        `json:"active_connections"`
	GoroutineCount    int                        `json:"goroutine_count"`
	MemoryAllocBytes  uint64                     `json:"memory_alloc_bytes"`
	MemoryTotalBytes  uint64                     `json:"memory_total_bytes"`
	GCPerSecond       float64                    `json:"gc_per_second"`
	CPUPercent        float64                    `json:"cpu_percent"`
	StartTime         int64                      `json:"start_time"`
	LastMetricTime    int64                      `json:"last_metric_time"`
	EndpointMetrics   map[string]*EndpointMetric `json:"endpoint_metrics"`
	latencyHistory    []float64
	maxHistorySize    int
}

// EndpointMetric представляет метрики для конкретного эндпоинта
type EndpointMetric struct {
	Count          uint64  `json:"count"`
	ErrorCount     uint64  `json:"error_count"`
	TotalLatencyMs float64 `json:"total_latency_ms"`
	AvgLatencyMs   float64 `json:"avg_latency_ms"`
	MaxLatencyMs   float64 `json:"max_latency_ms"`
	MinLatencyMs   float64 `json:"min_latency_ms"`
}

// NewSystemMetrics создаёт новый сборщик метрик
func NewSystemMetrics() *SystemMetrics {
	return &SystemMetrics{
		StartTime:       time.Now().UnixMilli(),
		EndpointMetrics: make(map[string]*EndpointMetric),
		latencyHistory:  make([]float64, 0, 1000),
		maxHistorySize:  10000,
	}
}

// RecordRequest записывает информацию о запросе
func (sm *SystemMetrics) RecordRequest(endpoint string, latencyMs float64, isError bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.RequestCount++
	if isError {
		sm.ErrorCount++
	}

	em, exists := sm.EndpointMetrics[endpoint]
	if !exists {
		em = &EndpointMetric{
			MinLatencyMs: latencyMs,
			MaxLatencyMs: latencyMs,
		}
		sm.EndpointMetrics[endpoint] = em
	}

	em.Count++
	if isError {
		em.ErrorCount++
	}
	em.TotalLatencyMs += latencyMs
	em.AvgLatencyMs = em.TotalLatencyMs / float64(em.Count)
	if latencyMs > em.MaxLatencyMs {
		em.MaxLatencyMs = latencyMs
	}
	if latencyMs < em.MinLatencyMs {
		em.MinLatencyMs = latencyMs
	}

	sm.AverageLatencyMs = (sm.AverageLatencyMs*float64(sm.RequestCount-1) + latencyMs) / float64(sm.RequestCount)

	sm.latencyHistory = append(sm.latencyHistory, latencyMs)
	if len(sm.latencyHistory) > sm.maxHistorySize {
		sm.latencyHistory = sm.latencyHistory[len(sm.latencyHistory)-sm.maxHistorySize:]
	}

	if len(sm.latencyHistory) > 0 {
		sorted := make([]float64, len(sm.latencyHistory))
		copy(sorted, sm.latencyHistory)
		sort.Float64s(sorted)
		p99Index := int(float64(len(sorted)) * 0.99)
		if p99Index < len(sorted) {
			sm.P99LatencyMs = sorted[p99Index]
		}
	}
}

// UpdateSystemInfo обновляет системную информацию
func (sm *SystemMetrics) UpdateSystemInfo() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	sm.MemoryAllocBytes = memStats.Alloc
	sm.MemoryTotalBytes = memStats.TotalAlloc
	sm.GoroutineCount = runtime.NumGoroutine()
	sm.LastMetricTime = time.Now().UnixMilli()

	gcPauseTotal := memStats.PauseTotalNs
	timeDiff := float64(sm.LastMetricTime-sm.StartTime) / 1000.0
	if timeDiff > 0 {
		sm.GCPerSecond = float64(gcPauseTotal) / timeDiff / 1e9
	}
}

// GetMetrics возвращает копию метрик
func (sm *SystemMetrics) GetMetrics() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sm.UpdateSystemInfo()

	return map[string]interface{}{
		"request_count":       sm.RequestCount,
		"error_count":         sm.ErrorCount,
		"average_latency_ms":  sm.AverageLatencyMs,
		"p99_latency_ms":      sm.P99LatencyMs,
		"active_connections":  sm.ActiveConnections,
		"goroutine_count":     sm.GoroutineCount,
		"memory_alloc_bytes":  sm.MemoryAllocBytes,
		"memory_total_bytes":  sm.MemoryTotalBytes,
		"gc_per_second":       sm.GCPerSecond,
		"start_time":          sm.StartTime,
		"last_metric_time":    sm.LastMetricTime,
		"uptime_seconds":      (time.Now().UnixMilli() - sm.StartTime) / 1000,
		"endpoint_metrics":    sm.EndpointMetrics,
	}
}

// RecordConnection записывает изменение количества соединений
func (sm *SystemMetrics) RecordConnection(delta int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.ActiveConnections += delta
}

// ========== Health Check ==========

// HealthStatus представляет статус здоровья системы
type HealthStatus struct {
	Status     string                 `json:"status"`
	Timestamp  int64                  `json:"timestamp"`
	Components map[string]string      `json:"components"`
	Details    map[string]interface{} `json:"details"`
	Message    string                 `json:"message,omitempty"`
}

// HealthChecker проверяет здоровье компонентов
type HealthChecker struct {
	store       *storage.Storage
	coordinator *cluster.RaftCoordinator
	logger      *log.Logger
	mu          sync.RWMutex
	lastStatus  *HealthStatus
}

// NewHealthChecker создаёт новый проверяльщик здоровья
func NewHealthChecker(store *storage.Storage, coord *cluster.RaftCoordinator, logger *log.Logger) *HealthChecker {
	hc := &HealthChecker{
		store:       store,
		coordinator: coord,
		logger:      logger,
	}

	go hc.periodicCheck()

	return hc
}

// periodicCheck периодически проверяет здоровье
func (hc *HealthChecker) periodicCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		hc.Check()
	}
}

// Check выполняет проверку здоровья
func (hc *HealthChecker) Check() *HealthStatus {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	status := &HealthStatus{
		Timestamp:  time.Now().UnixMilli(),
		Components: make(map[string]string),
		Details:    make(map[string]interface{}),
	}

	overallStatus := "healthy"

	if hc.store == nil {
		status.Components["storage"] = "unavailable"
		overallStatus = "unhealthy"
	} else {
		databases := hc.store.ListDatabases()
		status.Details["database_count"] = len(databases)
		if len(databases) >= 0 {
			status.Components["storage"] = "healthy"
		} else {
			status.Components["storage"] = "degraded"
			if overallStatus == "healthy" {
				overallStatus = "degraded"
			}
		}
	}

	if hc.coordinator == nil {
		status.Components["cluster"] = "disabled"
	} else {
		clusterStatus := hc.coordinator.GetClusterStatus()
		status.Details["cluster_health"] = clusterStatus.Health
		status.Details["active_nodes"] = clusterStatus.ActiveNodes
		status.Details["total_nodes"] = clusterStatus.TotalNodes

		if clusterStatus.Health == "critical" {
			status.Components["cluster"] = "critical"
			overallStatus = "unhealthy"
		} else if clusterStatus.Health == "degraded" {
			status.Components["cluster"] = "degraded"
			if overallStatus == "healthy" {
				overallStatus = "degraded"
			}
		} else {
			status.Components["cluster"] = "healthy"
		}
	}

	status.Status = overallStatus
	hc.lastStatus = status

	return status
}

// GetLastStatus возвращает последний статус здоровья
func (hc *HealthChecker) GetLastStatus() *HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.lastStatus
}

// ========== WebUIServer ==========

// WebUIServer представляет сервер веб-интерфейса
type WebUIServer struct {
	store          *storage.Storage
	coordinator    *cluster.RaftCoordinator
	aclManager     *acl.ACLManager
	logger         *log.Logger
	server         *http.Server
	port           int
	enabled        bool
	credentialMgr  *CredentialManager
	logBuffer      *LogBuffer
	pluginManager  *plugin.PluginManager
	metrics        *SystemMetrics
	healthChecker  *HealthChecker
	rateLimiter    *RateLimiter
}

// NewWebUIServer создаёт новый веб-сервер интерфейса
func NewWebUIServer(port int, enabled bool, store *storage.Storage, coord *cluster.RaftCoordinator, aclMgr *acl.ACLManager, logger *log.Logger) *WebUIServer {
	credMgr := NewCredentialManager()

	if err := credMgr.Load(); err != nil {
		logger.Warn(fmt.Sprintf("Failed to load credentials: %v, using defaults", err))
		if err := credMgr.CreateDefault(); err != nil {
			logger.Error(fmt.Sprintf("Failed to create default credentials: %v", err))
		}
	}

	logBuffer, err := NewLogBuffer("futriis/webui.log", 10000)
	if err != nil {
		logger.Warn(fmt.Sprintf("Failed to create log buffer: %v", err))
		logBuffer = &LogBuffer{entries: make([]LogEntry, 0, 1000)}
	}

	pluginManager := plugin.NewPluginManager("futriis/plugins", logger, store, true)

	w := &WebUIServer{
		store:         store,
		coordinator:   coord,
		aclManager:    aclMgr,
		logger:        logger,
		port:          port,
		enabled:       enabled,
		credentialMgr: credMgr,
		logBuffer:     logBuffer,
		pluginManager: pluginManager,
		metrics:       NewSystemMetrics(),
		rateLimiter:   NewRateLimiter(DefaultRateLimiterConfig()),
	}

	w.healthChecker = NewHealthChecker(store, coord, logger)

	return w
}

// generateSecureSessionID генерирует криптостойкий session ID
func generateSecureSessionID(username string) string {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		return fmt.Sprintf("fallback_%d_%s_%d", time.Now().UnixNano(), username, os.Getpid())
	}

	sessionID := base64.URLEncoding.EncodeToString(bytes)
	return fmt.Sprintf("%s_%s", sessionID[:24], username)
}

// middlewareWithMetrics добавляет метрики к обработчикам
func (w *WebUIServer) middlewareWithMetrics(handler http.HandlerFunc, endpoint string) http.HandlerFunc {
	return func(wr http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rw := &responseWriter{ResponseWriter: wr, statusCode: http.StatusOK}

		if !w.rateLimiter.Allow(r.RemoteAddr) {
			w.rateLimiter.rejected.Add(1)
			wr.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", w.rateLimiter.config.RequestsPerSecond))
			wr.Header().Set("X-RateLimit-Remaining", "0")
			wr.Header().Set("Retry-After", "1")
			http.Error(wr, "Rate limit exceeded. Please try again later.", http.StatusTooManyRequests)
			return
		}
		w.rateLimiter.allowed.Add(1)

		handler(rw, r)

		latency := float64(time.Since(start).Microseconds()) / 1000.0
		isError := rw.statusCode >= 400

		w.metrics.RecordRequest(endpoint, latency, isError)

		if w.logger != nil && isError {
			w.logger.Debugf("[METRICS] %s %s: %d in %.2fms", r.Method, endpoint, rw.statusCode, latency)
		}
	}
}

// responseWriter захватывает статус код
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// logOperation логирует операцию веб-интерфейса
func (w *WebUIServer) logOperation(operation, target, status, errMsg string, details map[string]interface{}) {
	entry := LogEntry{
		Timestamp: time.Now().UnixMilli(),
		Operation: operation,
		Target:    target,
		User:      w.credentialMgr.GetCurrentUsername(),
		Status:    status,
		ErrorMsg:  errMsg,
		Details:   details,
	}

	if w.logBuffer != nil {
		w.logBuffer.Add(entry)
	}

	if w.logger != nil {
		if status == "error" {
			w.logger.Error(fmt.Sprintf("[WEBUI] %s: %s - %s", operation, target, errMsg))
		} else {
			w.logger.Info(fmt.Sprintf("[WEBUI] %s: %s", operation, target))
		}
	}
}

// Start запускает веб-сервер интерфейса
func (w *WebUIServer) Start() error {
	if !w.enabled {
		w.logger.Info("Web UI is disabled in configuration")
		return nil
	}

	mux := http.NewServeMux()

	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return fmt.Errorf("failed to load static files: %v", err)
	}
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	mux.HandleFunc("/", w.handleWebIndex)

	// Observability endpoints
	mux.HandleFunc("/api/webui/metrics", w.middlewareWithMetrics(w.handleMetrics, "metrics"))
	mux.HandleFunc("/api/webui/health", w.middlewareWithMetrics(w.handleHealth, "health"))
	mux.HandleFunc("/api/webui/pipeline/status", w.middlewareWithMetrics(w.handlePipelineStatus, "pipeline_status"))
	mux.HandleFunc("/api/webui/resharding/trigger", w.middlewareWithMetrics(w.handleReshardingTrigger, "resharding_trigger"))

	// API для веб-интерфейса
	mux.HandleFunc("/api/webui/databases", w.middlewareWithMetrics(w.handleGetDatabases, "databases"))
	mux.HandleFunc("/api/webui/collections/", w.middlewareWithMetrics(w.handleGetCollections, "collections"))
	mux.HandleFunc("/api/webui/documents/", w.middlewareWithMetrics(w.handleDocuments, "documents"))
	mux.HandleFunc("/api/webui/cluster/status", w.middlewareWithMetrics(w.handleClusterStatus, "cluster_status"))
	mux.HandleFunc("/api/webui/cluster/nodes", w.middlewareWithMetrics(w.handleClusterNodes, "cluster_nodes"))
	mux.HandleFunc("/api/webui/stats", w.middlewareWithMetrics(w.handleStats, "stats"))
	mux.HandleFunc("/api/webui/login", w.middlewareWithMetrics(w.handleWebLogin, "login"))
	mux.HandleFunc("/api/webui/logout", w.middlewareWithMetrics(w.handleWebLogout, "logout"))
	mux.HandleFunc("/api/webui/session", w.middlewareWithMetrics(w.handleSessionCheck, "session"))
	mux.HandleFunc("/api/webui/change-password", w.middlewareWithMetrics(w.handleChangePassword, "change_password"))
	mux.HandleFunc("/api/webui/user/avatar", w.middlewareWithMetrics(w.handleUserAvatar, "user_avatar"))
	mux.HandleFunc("/api/webui/user/info", w.middlewareWithMetrics(w.handleUserInfo, "user_info"))
	mux.HandleFunc("/api/webui/admin/users", w.middlewareWithMetrics(w.handleAdminUsers, "admin_users"))
	mux.HandleFunc("/api/webui/logs", w.middlewareWithMetrics(w.handleWebLogs, "logs"))
	mux.HandleFunc("/api/webui/plugins", w.middlewareWithMetrics(w.handlePlugins, "plugins"))
	mux.HandleFunc("/api/webui/plugin/", w.middlewareWithMetrics(w.handlePluginOperation, "plugin_operation"))
	mux.HandleFunc("/api/webui/acl/users", w.middlewareWithMetrics(w.handleACLUsers, "acl_users"))
	mux.HandleFunc("/api/webui/acl/roles", w.middlewareWithMetrics(w.handleACLRoles, "acl_roles"))
	mux.HandleFunc("/api/webui/acl/permissions", w.middlewareWithMetrics(w.handleACLPermissions, "acl_permissions"))
	mux.HandleFunc("/api/webui/acl/user/", w.middlewareWithMetrics(w.handleACLUser, "acl_user"))
	mux.HandleFunc("/api/webui/acl/role/", w.middlewareWithMetrics(w.handleACLRole, "acl_role"))
	mux.HandleFunc("/api/webui/transactions", w.middlewareWithMetrics(w.handleTransactions, "transactions"))
	mux.HandleFunc("/api/webui/transaction/", w.middlewareWithMetrics(w.handleTransactionAction, "transaction_action"))
	mux.HandleFunc("/api/webui/indexes/", w.middlewareWithMetrics(w.handleIndexesList, "indexes_list"))
	mux.HandleFunc("/api/webui/index/", w.middlewareWithMetrics(w.handleIndexOperation, "index_operation"))
	mux.HandleFunc("/api/webui/export", w.middlewareWithMetrics(w.handleExportData, "export"))
	mux.HandleFunc("/api/webui/import", w.middlewareWithMetrics(w.handleImportData, "import"))
	mux.HandleFunc("/api/webui/triggers/", w.middlewareWithMetrics(w.handleTriggers, "triggers"))
	mux.HandleFunc("/api/webui/trigger/", w.middlewareWithMetrics(w.handleTriggerOperation, "trigger_operation"))
	mux.HandleFunc("/api/webui/trigger/log", w.middlewareWithMetrics(w.handleTriggerLog, "trigger_log"))
	mux.HandleFunc("/api/webui/constraints/", w.middlewareWithMetrics(w.handleConstraintsList, "constraints_list"))
	mux.HandleFunc("/api/webui/constraint/", w.middlewareWithMetrics(w.handleConstraintOperation, "constraint_operation"))

	w.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", w.port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	w.logger.Info(fmt.Sprintf("Web UI started on port %d with observability enabled", w.port))
	return w.server.ListenAndServe()
}

// Stop останавливает веб-сервер
func (w *WebUIServer) Stop() error {
	if w.logBuffer != nil {
		w.logBuffer.Close()
	}
	if w.server != nil {
		return w.server.Close()
	}
	return nil
}

// checkAuth проверяет аутентификацию
func (w *WebUIServer) checkAuth(r *http.Request) bool {
	sessionID := w.getSessionID(r)
	return sessionID != ""
}

// getSessionID возвращает ID сессии из cookie
func (w *WebUIServer) getSessionID(r *http.Request) string {
	if cookie, err := r.Cookie("session_id"); err == nil {
		return cookie.Value
	}
	return ""
}

// sendJSONSuccess отправляет успешный JSON ответ
func (w *WebUIServer) sendJSONSuccess(wr http.ResponseWriter, data interface{}) {
	wr.Header().Set("Content-Type", "application/json")
	wr.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(wr).Encode(map[string]interface{}{
		"success": true,
		"data":    data,
	}); err != nil {
		w.logger.Error(fmt.Sprintf("Failed to encode JSON response: %v", err))
	}
}

// sendJSONError отправляет JSON ответ с ошибкой
func (w *WebUIServer) sendJSONError(wr http.ResponseWriter, errMsg string, statusCode int) {
	wr.Header().Set("Content-Type", "application/json")
	wr.WriteHeader(statusCode)
	if err := json.NewEncoder(wr).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	}); err != nil {
		w.logger.Error(fmt.Sprintf("Failed to encode JSON error response: %v", err))
	}
}

// handleWebIndex возвращает главную HTML страницу
func (w *WebUIServer) handleWebIndex(wr http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(wr, r)
		return
	}

	html := `<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>Futriis Database Management System</title>
    <link rel="stylesheet" href="/static/style.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
</head>
<body>
    <div class="dashboard-container">
        <nav class="sidebar">
            <div class="sidebar-header">
                <div class="logo">
                    <img src="/static/logo.png" alt="Futriis" style="width: 112px; height: 53px; object-fit: contain;">
                </div>
                <button class="menu-toggle" id="menuToggle">
                    <i class="fas fa-bars"></i>
                </button>
            </div>
            
            <ul class="nav-menu">
                <li class="nav-item">
                    <a href="#" class="nav-link" data-section="dashboard">
                        <i class="fas fa-tachometer-alt"></i>
                        <span>Панель управления</span>
                    </a>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="crud">
                        <i class="fas fa-table"></i>
                        <span>Управление СУБД</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-action="create-db"><i class="fas fa-plus-circle"></i>Создать БД</a></li>
                        <li><a href="#" data-action="create-collection"><i class="fas fa-layer-group"></i>Создать коллекцию</a></li>
                        <li><a href="#" data-action="insert-doc"><i class="fas fa-file-import"></i>Вставить документ</a></li>
                        <li><a href="#" data-action="find-doc"><i class="fas fa-search"></i>Найти документ</a></li>
                        <li><a href="#" data-action="update-doc"><i class="fas fa-edit"></i>Обновить документ</a></li>
                        <li><a href="#" data-action="delete-doc"><i class="fas fa-trash-alt"></i>Удалить документ</a></li>
                    </ul>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="constraints">
                        <i class="fas fa-check-double"></i>
                        <span>Ограничения</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-section="constraints-list"><i class="fas fa-list"></i>Список ограничений</a></li>
                        <li><a href="#" data-action="constraint-add-required"><i class="fas fa-exclamation-circle"></i>Обязательное поле</a></li>
                        <li><a href="#" data-action="constraint-add-unique"><i class="fas fa-unique"></i>Уникальность</a></li>
                        <li><a href="#" data-action="constraint-add-min"><i class="fas fa-greater-than"></i>Минимум</a></li>
                        <li><a href="#" data-action="constraint-add-max"><i class="fas fa-less-than"></i>Максимум</a></li>
                        <li><a href="#" data-action="constraint-add-enum"><i class="fas fa-list-ul"></i>Перечисление</a></li>
                        <li><a href="#" data-action="constraint-add-regex"><i class="fas fa-code"></i>Регулярное выражение</a></li>
                    </ul>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="acl">
                        <i class="fas fa-lock"></i>
                        <span>ACL управление</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-section="acl-users"><i class="fas fa-users"></i>Пользователи</a></li>
                        <li><a href="#" data-section="acl-roles"><i class="fas fa-user-tag"></i>Роли</a></li>
                        <li><a href="#" data-section="acl-permissions"><i class="fas fa-key"></i>Разрешения</a></li>
                        <li><a href="#" data-action="acl-create-user"><i class="fas fa-user-plus"></i>Создать пользователя</a></li>
                        <li><a href="#" data-action="acl-create-role"><i class="fas fa-plus-circle"></i>Создать роль</a></li>
                    </ul>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="transactions">
                        <i class="fas fa-exchange-alt"></i>
                        <span>Транзакции</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-action="tx-start-session"><i class="fas fa-play"></i>Начать сессию</a></li>
                        <li><a href="#" data-action="tx-start"><i class="fas fa-play-circle"></i>Начать транзакцию</a></li>
                        <li><a href="#" data-action="tx-commit"><i class="fas fa-check-circle"></i>Зафиксировать</a></li>
                        <li><a href="#" data-action="tx-abort"><i class="fas fa-times-circle"></i>Отменить</a></li>
                        <li><a href="#" data-section="tx-list"><i class="fas fa-list"></i>Список транзакций</a></li>
                    </ul>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="indexes">
                        <i class="fas fa-search"></i>
                        <span>Индексы</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-section="indexes-list"><i class="fas fa-list"></i>Список индексов</a></li>
                        <li><a href="#" data-action="index-create"><i class="fas fa-plus"></i>Создать индекс</a></li>
                        <li><a href="#" data-action="index-drop"><i class="fas fa-trash"></i>Удалить индекс</a></li>
                    </ul>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="triggers">
                        <i class="fas fa-bolt"></i>
                        <span>Триггеры</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-section="triggers-list"><i class="fas fa-list"></i>Список триггеров</a></li>
                        <li><a href="#" data-action="trigger-create"><i class="fas fa-plus"></i>Создать триггер</a></li>
                        <li><a href="#" data-section="trigger-log"><i class="fas fa-history"></i>Лог выполнения</a></li>
                    </ul>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="import-export">
                        <i class="fas fa-database"></i>
                        <span>Импорт/Экспорт</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-section="export-data"><i class="fas fa-upload"></i>Экспорт данных</a></li>
                        <li><a href="#" data-section="import-data"><i class="fas fa-download"></i>Импорт данных</a></li>
                    </ul>
                </li>
                <li class="nav-item">
                    <a href="#" class="nav-link" data-section="cluster">
                        <i class="fas fa-network-wired"></i>
                        <span>Управление кластером</span>
                    </a>
                </li>
                <li class="nav-item">
                    <a href="#" class="nav-link" data-section="logs">
                        <i class="fas fa-history"></i>
                        <span>Логи операций</span>
                    </a>
                </li>
                <li class="nav-item has-submenu">
                    <a href="#" class="nav-link" data-submenu="plugins">
                        <i class="fas fa-puzzle-piece"></i>
                        <span>Плагины</span>
                        <i class="fas fa-chevron-down"></i>
                    </a>
                    <ul class="submenu">
                        <li><a href="#" data-section="plugins-list"><i class="fas fa-list"></i>Список плагинов</a></li>
                        <li><a href="#" data-action="plugin-upload"><i class="fas fa-upload"></i>Загрузить плагин</a></li>
                    </ul>
                </li>
                <li class="nav-item">
                    <a href="#" class="nav-link" data-section="settings">
                        <i class="fas fa-cog"></i>
                        <span>Настройки</span>
                    </a>
                </li>
            </ul>
            
            <div class="sidebar-footer">
                <div class="user-info" id="userInfo">
                    <div class="user-avatar" id="userAvatar">
                        <i class="fas fa-user-circle" style="font-size: 40px;"></i>
                    </div>
                    <div class="user-details">
                        <span id="userName">Гость</span>
                        <span id="userRole" class="user-role"></span>
                    </div>
                    <button class="change-password-icon" id="changePasswordIcon" title="Сменить пароль">
                        <i class="fas fa-key"></i>
                    </button>
                </div>
                <button class="logout-btn" id="logoutBtn">
                    <i class="fas fa-sign-out-alt"></i>
                    <span>Выход</span>
                </button>
            </div>
        </nav>
        
        <main class="main-content">
            <header class="top-bar">
                <h1 id="pageTitle">Панель управления <span class="version-badge">futriis 3i² (02.04.2026)</span></h1>
                <div class="connection-status" id="connectionStatus">
                    <span>Подключено</span>
                </div>
            </header>
            
            <div class="content-area" id="contentArea">
                <div class="loading-spinner">
                    <i class="fas fa-spinner fa-pulse"></i>
                    <p>Загрузка...</p>
                </div>
            </div>
        </main>
    </div>
    
    <div id="modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 id="modalTitle">Заголовок</h2>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body" id="modalBody"></div>
            <div class="modal-footer">
                <button class="btn btn-secondary modal-close">Отмена</button>
                <button class="btn btn-primary" id="modalConfirm">Подтвердить</button>
            </div>
        </div>
    </div>
    
    <div id="avatarUploadModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>Загрузить аватар</h2>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label>Выберите изображение (JPEG, PNG, GIF, до 2MB)</label>
                    <input type="file" id="avatarFile" class="form-control" accept="image/jpeg,image/png,image/gif">
                </div>
                <div id="avatarPreview" class="avatar-preview" style="text-align: center; margin-top: 16px;"></div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary modal-close">Отмена</button>
                <button class="btn btn-primary" id="uploadAvatarBtn">Загрузить</button>
            </div>
        </div>
    </div>
    
    <div id="changePasswordModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>Смена пароля</h2>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label>Текущий пароль</label>
                    <input type="password" id="currentPassword" class="form-control" placeholder="Введите текущий пароль">
                </div>
                <div class="form-group">
                    <label>Новый пароль</label>
                    <input type="password" id="newPassword" class="form-control" placeholder="Введите новый пароль">
                </div>
                <div class="form-group">
                    <label>Подтверждение нового пароля</label>
                    <input type="password" id="confirmPassword" class="form-control" placeholder="Подтвердите новый пароль">
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary modal-close">Отмена</button>
                <button class="btn btn-primary" id="changePasswordBtn">Сменить пароль</button>
            </div>
        </div>
    </div>
    
    <div id="notificationContainer" class="notification-container"></div>
    
    <script src="/static/app.js"></script>
</body>
</html>`

	wr.Header().Set("Content-Type", "text/html; charset=utf-8")
	wr.Write([]byte(html))
}

// ========== Observability Handlers ==========

// handleMetrics возвращает метрики системы
func (w *WebUIServer) handleMetrics(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	metrics := w.metrics.GetMetrics()

	if w.coordinator != nil {
		metrics["pipeline_stats"] = w.coordinator.GetPipelineStats()
		metrics["batch_commit_stats"] = w.coordinator.GetBatchCommitStats()
		metrics["resharding_stats"] = w.coordinator.GetReshardingStats()
		metrics["joint_consensus_status"] = w.coordinator.GetJointConsensusStatus()
	}

	metrics["rate_limiter_stats"] = w.rateLimiter.GetStats()

	w.sendJSONSuccess(wr, metrics)
}

// handleHealth возвращает статус здоровья
func (w *WebUIServer) handleHealth(wr http.ResponseWriter, r *http.Request) {
	status := w.healthChecker.Check()
	w.sendJSONSuccess(wr, status)
}

// handlePipelineStatus возвращает статус пайплайна
func (w *WebUIServer) handlePipelineStatus(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if w.coordinator == nil {
		w.sendJSONError(wr, "Cluster not available", http.StatusServiceUnavailable)
		return
	}

	w.sendJSONSuccess(wr, map[string]interface{}{
		"pipeline":        w.coordinator.GetPipelineStats(),
		"batch_commit":    w.coordinator.GetBatchCommitStats(),
		"resharding":      w.coordinator.GetReshardingStats(),
		"joint_consensus": w.coordinator.GetJointConsensusStatus(),
	})
}

// handleReshardingTrigger запускает перераспределение шардов
func (w *WebUIServer) handleReshardingTrigger(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if w.coordinator == nil {
		w.sendJSONError(wr, "Cluster not available", http.StatusServiceUnavailable)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if !w.credentialMgr.IsAdmin(username) {
		w.sendJSONError(wr, "Forbidden", http.StatusForbidden)
		return
	}

	var req struct {
		Reason string `json:"reason"`
	}

	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	if req.Reason == "" {
		req.Reason = "manual_trigger"
	}

	if err := w.coordinator.TriggerResharding(req.Reason); err != nil {
		w.logOperation("RESHARDING_TRIGGER", "", "error", err.Error(), nil)
		w.sendJSONError(wr, err.Error(), http.StatusInternalServerError)
		return
	}

	w.logOperation("RESHARDING_TRIGGER", "", "success", "", map[string]interface{}{
		"reason": req.Reason,
	})

	w.sendJSONSuccess(wr, map[string]interface{}{
		"status": "resharding_triggered",
		"reason": req.Reason,
	})
}

// ========== Authentication Handlers ==========

// handleWebLogin обрабатывает вход в веб-интерфейс
func (w *WebUIServer) handleWebLogin(wr http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
		return
	}

	w.logger.Debug(fmt.Sprintf("Login attempt: username=%s", creds.Username))

	if !w.credentialMgr.Validate(creds.Username, creds.Password) {
		w.logger.Debug(fmt.Sprintf("Authentication failed for %s", creds.Username))
		w.logOperation("LOGIN", creds.Username, "error", "Invalid credentials", nil)
		w.sendJSONError(wr, "Неверный логин и/или пароль", http.StatusUnauthorized)
		return
	}

	sessionID := generateSecureSessionID(creds.Username)

	w.credentialMgr.SetCurrentUsername(creds.Username)
	w.credentialMgr.UpdateLastLogin(creds.Username)

	w.logger.Debug(fmt.Sprintf("Authentication successful for %s, sessionID=%s", creds.Username, sessionID[:20]+"..."))
	w.logOperation("LOGIN", creds.Username, "success", "", nil)

	http.SetCookie(wr, &http.Cookie{
		Name:     "session_id",
		Value:    sessionID,
		Path:     "/",
		MaxAge:   86400,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})

	avatarData := ""
	if avatar, err := w.credentialMgr.GetAvatar(creds.Username); err == nil && avatar != "" {
		avatarData = avatar
	}

	w.sendJSONSuccess(wr, map[string]interface{}{
		"session_id": sessionID,
		"username":   creds.Username,
		"avatar":     avatarData,
		"is_admin":   w.credentialMgr.IsAdmin(creds.Username),
	})
}

// handleWebLogout обрабатывает выход из веб-интерфейса
func (w *WebUIServer) handleWebLogout(wr http.ResponseWriter, r *http.Request) {
	username := w.credentialMgr.GetCurrentUsername()
	w.logOperation("LOGOUT", username, "success", "", nil)

	http.SetCookie(wr, &http.Cookie{
		Name:     "session_id",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
	})

	w.sendJSONSuccess(wr, map[string]interface{}{
		"status": "logged out",
	})
}

// handleSessionCheck проверяет активность сессии
func (w *WebUIServer) handleSessionCheck(wr http.ResponseWriter, r *http.Request) {
	sessionID := w.getSessionID(r)
	if sessionID == "" {
		w.sendJSONError(wr, "No session", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if username == "" {
		username = "admin"
	}

	connectionStatus := "connected"
	if w.coordinator == nil {
		connectionStatus = "disconnected"
	} else if status := w.coordinator.GetClusterStatus(); status.Health == "critical" {
		connectionStatus = "disconnected"
	}

	avatarData := ""
	if avatar, err := w.credentialMgr.GetAvatar(username); err == nil && avatar != "" {
		avatarData = avatar
	}

	w.sendJSONSuccess(wr, map[string]interface{}{
		"authenticated":     true,
		"username":          username,
		"avatar":            avatarData,
		"is_admin":          w.credentialMgr.IsAdmin(username),
		"connection_status": connectionStatus,
	})
}

// handleChangePassword обрабатывает смену пароля
func (w *WebUIServer) handleChangePassword(wr http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := w.getSessionID(r)
	if sessionID == "" {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.CurrentPassword == "" || req.NewPassword == "" {
		w.sendJSONError(wr, "Current and new password are required", http.StatusBadRequest)
		return
	}

	if len(req.NewPassword) < 4 {
		w.sendJSONError(wr, "New password must be at least 4 characters", http.StatusBadRequest)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if username == "" {
		username = "admin"
	}

	if err := w.credentialMgr.ChangePassword(username, req.CurrentPassword, req.NewPassword); err != nil {
		w.logOperation("CHANGE_PASSWORD", username, "error", err.Error(), nil)
		w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
		return
	}

	w.logger.Info(fmt.Sprintf("Password changed for user: %s", username))
	w.logOperation("CHANGE_PASSWORD", username, "success", "", nil)
	w.sendJSONSuccess(wr, map[string]interface{}{
		"status":  "password_changed",
		"message": "Пароль успешно изменён",
	})
}

// handleUserAvatar обрабатывает загрузку и получение аватара пользователя
func (w *WebUIServer) handleUserAvatar(wr http.ResponseWriter, r *http.Request) {
	sessionID := w.getSessionID(r)
	if sessionID == "" {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if username == "" {
		username = "admin"
	}

	switch r.Method {
	case http.MethodGet:
		avatar, err := w.credentialMgr.GetAvatar(username)
		if err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusNotFound)
			return
		}
		w.sendJSONSuccess(wr, map[string]interface{}{
			"avatar": avatar,
		})

	case http.MethodPost:
		if err := r.ParseMultipartForm(2 << 20); err != nil {
			w.sendJSONError(wr, "Failed to parse form: "+err.Error(), http.StatusBadRequest)
			return
		}

		file, handler, err := r.FormFile("avatar")
		if err != nil {
			w.sendJSONError(wr, "Failed to get avatar file: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		if handler.Size > 2<<20 {
			w.sendJSONError(wr, "Avatar file too large (max 2MB)", http.StatusBadRequest)
			return
		}

		contentType := handler.Header.Get("Content-Type")
		if contentType != "image/jpeg" && contentType != "image/png" && contentType != "image/gif" {
			w.sendJSONError(wr, "Invalid image type. Use JPEG, PNG or GIF", http.StatusBadRequest)
			return
		}

		fileData := make([]byte, handler.Size)
		if _, err := file.Read(fileData); err != nil {
			w.sendJSONError(wr, "Failed to read file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		avatarBase64 := "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(fileData)

		if err := w.credentialMgr.SetAvatar(username, avatarBase64); err != nil {
			w.sendJSONError(wr, "Failed to save avatar: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.logger.Info(fmt.Sprintf("Avatar uploaded for user: %s", username))
		w.logOperation("AVATAR_UPLOAD", username, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "avatar_uploaded",
			"avatar": avatarBase64,
		})

	case http.MethodDelete:
		if err := w.credentialMgr.DeleteAvatar(username); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusNotFound)
			return
		}
		w.logOperation("AVATAR_DELETE", username, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "avatar_deleted",
		})

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleUserInfo возвращает информацию о пользователе
func (w *WebUIServer) handleUserInfo(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if username == "" {
		username = "admin"
	}

	avatar, _ := w.credentialMgr.GetAvatar(username)

	w.sendJSONSuccess(wr, map[string]interface{}{
		"username": username,
		"avatar":   avatar,
		"is_admin": w.credentialMgr.IsAdmin(username),
	})
}

// ========== Database Handlers ==========

// handleGetDatabases возвращает список баз данных
func (w *WebUIServer) handleGetDatabases(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	databases := w.store.ListDatabases()

	dbInfo := make([]map[string]interface{}, 0)
	for _, dbName := range databases {
		db, err := w.store.GetDatabase(dbName)
		if err == nil {
			collections := db.ListCollections()
			dbInfo = append(dbInfo, map[string]interface{}{
				"name":             dbName,
				"collections":      len(collections),
				"collections_list": collections,
			})
		}
	}

	w.sendJSONSuccess(wr, dbInfo)
}

// handleGetCollections возвращает список коллекций в базе данных
func (w *WebUIServer) handleGetCollections(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/collections/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 || parts[0] == "" {
		w.sendJSONError(wr, "Database name required", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	db, err := w.store.GetDatabase(dbName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	collections := db.ListCollections()
	collectionsInfo := make([]map[string]interface{}, 0)

	for _, collName := range collections {
		coll, err := db.GetCollection(collName)
		if err == nil {
			collectionsInfo = append(collectionsInfo, map[string]interface{}{
				"name":    collName,
				"count":   coll.Count(),
				"size":    coll.Size(),
				"indexes": coll.GetIndexes(),
			})
		}
	}

	w.sendJSONSuccess(wr, map[string]interface{}{
		"database":    dbName,
		"collections": collectionsInfo,
	})
}

// handleDocuments обрабатывает CRUD операции с документами
func (w *WebUIServer) handleDocuments(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/documents/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		w.sendJSONError(wr, "Invalid path. Use /api/webui/documents/{database}/{collection}", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	collName := parts[1]

	db, err := w.store.GetDatabase(dbName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	coll, err := db.GetCollection(collName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		limit := 50
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 500 {
				limit = l
			}
		}

		offset := 0
		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
				offset = o
			}
		}

		allDocs := coll.GetAllDocuments()
		start := offset
		end := offset + limit
		if start > len(allDocs) {
			start = len(allDocs)
		}
		if end > len(allDocs) {
			end = len(allDocs)
		}

		docs := make([]map[string]interface{}, 0)
		for _, doc := range allDocs[start:end] {
			docs = append(docs, map[string]interface{}{
				"id":         doc.ID,
				"fields":     doc.GetFields(),
				"created_at": doc.CreatedAt,
				"updated_at": doc.UpdatedAt,
				"version":    doc.Version,
			})
		}

		w.sendJSONSuccess(wr, map[string]interface{}{
			"documents": docs,
			"total":     len(allDocs),
			"limit":     limit,
			"offset":    offset,
		})

	case http.MethodPost:
		var docData map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&docData); err != nil {
			w.sendJSONError(wr, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if err := coll.InsertFromMap(docData); err != nil {
			w.logOperation("INSERT_DOCUMENT", fmt.Sprintf("%s.%s", dbName, collName), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}

		w.logOperation("INSERT_DOCUMENT", fmt.Sprintf("%s.%s/%v", dbName, collName, docData["_id"]), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "inserted",
			"id":     docData["_id"],
		})

	case http.MethodPut:
		docID := r.URL.Query().Get("id")
		if docID == "" {
			w.sendJSONError(wr, "Document ID required", http.StatusBadRequest)
			return
		}

		var updates map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
			w.sendJSONError(wr, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if err := coll.Update(docID, updates); err != nil {
			w.logOperation("UPDATE_DOCUMENT", fmt.Sprintf("%s.%s/%s", dbName, collName, docID), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}

		w.logOperation("UPDATE_DOCUMENT", fmt.Sprintf("%s.%s/%s", dbName, collName, docID), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "updated",
			"id":     docID,
		})

	case http.MethodDelete:
		docID := r.URL.Query().Get("id")
		if docID == "" {
			w.sendJSONError(wr, "Document ID required", http.StatusBadRequest)
			return
		}

		if err := coll.Delete(docID); err != nil {
			w.logOperation("DELETE_DOCUMENT", fmt.Sprintf("%s.%s/%s", dbName, collName, docID), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusNotFound)
			return
		}

		w.logOperation("DELETE_DOCUMENT", fmt.Sprintf("%s.%s/%s", dbName, collName, docID), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "deleted",
			"id":     docID,
		})

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ========== Cluster Handlers ==========

// handleClusterStatus возвращает статус кластера
func (w *WebUIServer) handleClusterStatus(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if w.coordinator == nil {
		w.sendJSONError(wr, "Cluster not available", http.StatusServiceUnavailable)
		return
	}

	status := w.coordinator.GetClusterStatus()
	w.sendJSONSuccess(wr, status)
}

// handleClusterNodes возвращает список узлов кластера
func (w *WebUIServer) handleClusterNodes(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if w.coordinator == nil {
		w.sendJSONError(wr, "Cluster not available", http.StatusServiceUnavailable)
		return
	}

	nodes := w.coordinator.GetAllNodes()
	w.sendJSONSuccess(wr, nodes)
}

// handleStats возвращает статистику системы
func (w *WebUIServer) handleStats(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	databases := w.store.ListDatabases()
	totalDocs := int64(0)
	totalCollections := 0

	for _, dbName := range databases {
		db, _ := w.store.GetDatabase(dbName)
		if db != nil {
			collections := db.ListCollections()
			totalCollections += len(collections)
			for _, collName := range collections {
				coll, _ := db.GetCollection(collName)
				if coll != nil {
					totalDocs += coll.Count()
				}
			}
		}
	}

	stats := map[string]interface{}{
		"databases":          len(databases),
		"collections":        totalCollections,
		"documents":          totalDocs,
		"storage_used_mb":    float64(w.store.GetPageSize()*int64(len(databases))) / (1024 * 1024),
		"uptime_seconds":     (time.Now().UnixMilli() - w.metrics.StartTime) / 1000,
		"cluster_enabled":    w.coordinator != nil,
		"replication_factor": 0,
	}

	if w.coordinator != nil {
		stats["replication_factor"] = w.coordinator.GetReplicationFactor()
		stats["cluster_health"] = w.coordinator.GetClusterStatus().Health
		stats["pipeline_enabled"] = true
		stats["batch_commit_enabled"] = true
		stats["resharding_enabled"] = true
	}

	w.sendJSONSuccess(wr, stats)
}

// ========== Logs Handlers ==========

// handleWebLogs возвращает логи веб-интерфейса
func (w *WebUIServer) handleWebLogs(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if !w.credentialMgr.IsAdmin(username) {
		w.sendJSONError(wr, "Forbidden", http.StatusForbidden)
		return
	}

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 5000 {
			limit = l
		}
	}

	entries := w.logBuffer.GetEntries()
	if len(entries) > limit {
		entries = entries[len(entries)-limit:]
	}

	w.sendJSONSuccess(wr, entries)
}

// ========== Plugin Handlers ==========

// handlePlugins возвращает список плагинов
func (w *WebUIServer) handlePlugins(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if !w.credentialMgr.IsAdmin(username) {
		w.sendJSONError(wr, "Forbidden", http.StatusForbidden)
		return
	}

	switch r.Method {
	case http.MethodGet:
		plugins := w.pluginManager.ListPlugins()
		pluginList := make([]map[string]interface{}, 0, len(plugins))
		for _, p := range plugins {
			pluginList = append(pluginList, map[string]interface{}{
				"name":        p.Name,
				"version":     p.Version(),
				"author":      p.Author(),
				"description": p.Description(),
				"file_path":   p.FilePath,
				"loaded_at":   p.LoadedAt(),
			})
		}
		w.sendJSONSuccess(wr, pluginList)

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePluginOperation обрабатывает операции с плагинами
func (w *WebUIServer) handlePluginOperation(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if !w.credentialMgr.IsAdmin(username) {
		w.sendJSONError(wr, "Forbidden", http.StatusForbidden)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/plugin/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		w.sendJSONError(wr, "Plugin name and action required", http.StatusBadRequest)
		return
	}

	pluginName := parts[0]
	action := parts[1]

	switch action {
	case "start":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := w.pluginManager.StartPlugin(pluginName); err != nil {
			w.logOperation("PLUGIN_START", pluginName, "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("PLUGIN_START", pluginName, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "started"})

	case "stop":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := w.pluginManager.StopPlugin(pluginName); err != nil {
			w.logOperation("PLUGIN_STOP", pluginName, "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("PLUGIN_STOP", pluginName, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "stopped"})

	case "reload":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := w.pluginManager.UnloadPlugin(pluginName); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("PLUGIN_RELOAD", pluginName, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "reloaded"})

	case "delete":
		if r.Method != http.MethodDelete {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := w.pluginManager.UnloadPlugin(pluginName); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		pluginPath := filepath.Join(w.pluginManager.GetPluginsDir(), pluginName+".lua")
		if err := os.Remove(pluginPath); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("PLUGIN_DELETE", pluginName, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "deleted"})

	case "upload":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseMultipartForm(1024 * 1024); err != nil {
			w.sendJSONError(wr, "Failed to parse form", http.StatusBadRequest)
			return
		}

		file, handler, err := r.FormFile("plugin")
		if err != nil {
			w.sendJSONError(wr, "Failed to get plugin file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		if !strings.HasSuffix(handler.Filename, ".lua") {
			w.sendJSONError(wr, "Only .lua files are allowed", http.StatusBadRequest)
			return
		}

		pluginPath := filepath.Join(w.pluginManager.GetPluginsDir(), handler.Filename)

		dst, err := os.Create(pluginPath)
		if err != nil {
			w.sendJSONError(wr, "Failed to create plugin file", http.StatusInternalServerError)
			return
		}
		defer dst.Close()

		if _, err := file.Seek(0, 0); err != nil {
			w.sendJSONError(wr, "Failed to read file", http.StatusInternalServerError)
			return
		}

		if _, err := dst.ReadFrom(file); err != nil {
			w.sendJSONError(wr, "Failed to write plugin file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.logOperation("PLUGIN_UPLOAD", handler.Filename, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "uploaded", "filename": handler.Filename})

	default:
		w.sendJSONError(wr, "Unknown action", http.StatusBadRequest)
	}
}

// ========== Admin Handlers ==========

func (w *WebUIServer) handleAdminUsers(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	username := w.credentialMgr.GetCurrentUsername()
	if !w.credentialMgr.IsAdmin(username) {
		w.sendJSONError(wr, "Forbidden", http.StatusForbidden)
		return
	}

	switch r.Method {
	case http.MethodGet:
		users := w.credentialMgr.ListUsers()
		w.sendJSONSuccess(wr, users)

	case http.MethodPost:
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			IsAdmin  bool   `json:"is_admin"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := w.credentialMgr.CreateUser(req.Username, req.Password, req.IsAdmin); err != nil {
			w.logOperation("ADMIN_CREATE_USER", req.Username, "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}

		w.logOperation("ADMIN_CREATE_USER", req.Username, "success", "", map[string]interface{}{
			"is_admin": req.IsAdmin,
		})
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "created"})

	case http.MethodDelete:
		usernameToDelete := r.URL.Query().Get("username")
		if usernameToDelete == "" {
			w.sendJSONError(wr, "Username required", http.StatusBadRequest)
			return
		}

		if err := w.credentialMgr.DeleteUser(usernameToDelete); err != nil {
			w.logOperation("ADMIN_DELETE_USER", usernameToDelete, "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}

		w.logOperation("ADMIN_DELETE_USER", usernameToDelete, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "deleted"})

	case http.MethodPut:
		var req struct {
			Username    string `json:"username"`
			NewPassword string `json:"new_password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := w.credentialMgr.AdminChangePassword(req.Username, req.NewPassword); err != nil {
			w.logOperation("ADMIN_CHANGE_PASSWORD", req.Username, "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}

		w.logOperation("ADMIN_CHANGE_PASSWORD", req.Username, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "password_changed"})

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ========== ACL Handlers ==========

func (w *WebUIServer) handleACLUsers(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	users := w.aclManager.ListUsers()
	usersInfo := make([]map[string]interface{}, 0)

	for _, username := range users {
		user, err := w.aclManager.GetUserInfo(username)
		if err == nil {
			usersInfo = append(usersInfo, map[string]interface{}{
				"username":   user.Username,
				"roles":      user.Roles,
				"active":     user.Active,
				"created_at": user.CreatedAt,
				"last_login": user.LastLogin,
			})
		}
	}

	w.sendJSONSuccess(wr, usersInfo)
}

func (w *WebUIServer) handleACLRoles(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	roles := w.aclManager.ListRoles()
	rolesInfo := make([]map[string]interface{}, 0)

	for _, roleName := range roles {
		perms, err := w.aclManager.GetRolePermissions(roleName)
		if err == nil {
			rolesInfo = append(rolesInfo, map[string]interface{}{
				"name":        roleName,
				"permissions": perms,
			})
		}
	}

	w.sendJSONSuccess(wr, rolesInfo)
}

func (w *WebUIServer) handleACLPermissions(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	roles := w.aclManager.ListRoles()
	allPermissions := make(map[string][]string)

	for _, roleName := range roles {
		perms, _ := w.aclManager.GetRolePermissions(roleName)
		allPermissions[roleName] = perms
	}

	w.sendJSONSuccess(wr, allPermissions)
}

func (w *WebUIServer) handleACLUser(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/acl/user/")
	username := path

	if username == "" {
		w.sendJSONError(wr, "Username required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		var req struct {
			Password string   `json:"password"`
			Roles    []string `json:"roles"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := w.aclManager.CreateUser(username, req.Password, req.Roles); err != nil {
			w.logOperation("ACL_CREATE_USER", username, "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("ACL_CREATE_USER", username, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "created"})

	case http.MethodPut:
		var req struct {
			AddRole    string `json:"add_role"`
			RemoveRole string `json:"remove_role"`
			Password   string `json:"password"`
			Disable    bool   `json:"disable"`
			Enable     bool   `json:"enable"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.AddRole != "" {
			if err := w.aclManager.AddUserRole(username, req.AddRole); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		}

		if req.RemoveRole != "" {
			if err := w.aclManager.RemoveUserRole(username, req.RemoveRole); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		}

		if req.Password != "" {
			if err := w.aclManager.ChangePassword(username, req.Password); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		}

		if req.Disable {
			if err := w.aclManager.DisableUser(username); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		}

		if req.Enable {
			if err := w.aclManager.EnableUser(username); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		}

		w.logOperation("ACL_UPDATE_USER", username, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "updated"})

	case http.MethodDelete:
		if err := w.aclManager.DeleteUser(username); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("ACL_DELETE_USER", username, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "deleted"})

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (w *WebUIServer) handleACLRole(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/acl/role/")
	parts := strings.Split(path, "/")

	roleName := parts[0]
	if roleName == "" {
		w.sendJSONError(wr, "Role name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if err := w.aclManager.CreateRole(roleName); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("ACL_CREATE_ROLE", roleName, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "created"})

	case http.MethodPut:
		if len(parts) < 3 {
			w.sendJSONError(wr, "Action and permission required", http.StatusBadRequest)
			return
		}

		action := parts[1]
		permission := strings.Join(parts[2:], "/")

		switch action {
		case "grant":
			if err := w.aclManager.GrantPermission(roleName, permission); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		case "revoke":
			if err := w.aclManager.RevokePermission(roleName, permission); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}
		default:
			w.sendJSONError(wr, "Unknown action", http.StatusBadRequest)
			return
		}
		w.logOperation("ACL_UPDATE_ROLE", roleName, "success", "", map[string]interface{}{"action": action, "permission": permission})
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "updated"})

	case http.MethodDelete:
		if err := w.aclManager.DeleteRole(roleName); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("ACL_DELETE_ROLE", roleName, "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "deleted"})

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ========== Transaction Handlers ==========

func (w *WebUIServer) handleTransactions(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case http.MethodGet:
		txList := storage.GetActiveTransactions()
		w.sendJSONSuccess(wr, txList)

	case http.MethodPost:
		var req struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		switch req.Action {
		case "start_session":
			if err := storage.InitTransactionManager("futriis.wal"); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusInternalServerError)
				return
			}
			w.logOperation("TRANSACTION_START_SESSION", "", "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{"status": "session_started"})

		case "start_transaction":
			databases := w.store.ListDatabases()
			if len(databases) == 0 {
				w.sendJSONError(wr, "No database available", http.StatusBadRequest)
				return
			}

			db, err := w.store.GetDatabase(databases[0])
			if err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}

			collections := db.ListCollections()
			if len(collections) == 0 {
				w.sendJSONError(wr, "No collection available", http.StatusBadRequest)
				return
			}

			coll, err := db.GetCollection(collections[0])
			if err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
				return
			}

			if err := storage.BeginTransactionOnCollection(coll); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusInternalServerError)
				return
			}
			w.logOperation("TRANSACTION_START", "", "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{"status": "transaction_started"})

		default:
			w.sendJSONError(wr, "Unknown action", http.StatusBadRequest)
		}

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (w *WebUIServer) handleTransactionAction(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/transaction/")

	switch r.Method {
	case http.MethodPost:
		switch path {
		case "commit":
			if err := storage.CommitCurrentTransaction(); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusInternalServerError)
				return
			}
			w.logOperation("TRANSACTION_COMMIT", "", "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{"status": "committed"})

		case "abort":
			if err := storage.AbortCurrentTransaction(); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusInternalServerError)
				return
			}
			w.logOperation("TRANSACTION_ABORT", "", "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{"status": "aborted"})

		default:
			w.sendJSONError(wr, "Unknown action", http.StatusBadRequest)
		}

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// ========== Index Handlers ==========

func (w *WebUIServer) handleIndexesList(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/indexes/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		w.sendJSONError(wr, "Database and collection required", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	collName := parts[1]

	db, err := w.store.GetDatabase(dbName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	coll, err := db.GetCollection(collName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		indexes := coll.GetIndexesInfo()
		w.sendJSONSuccess(wr, indexes)

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (w *WebUIServer) handleIndexOperation(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/index/")
	parts := strings.Split(path, "/")

	if len(parts) < 3 {
		w.sendJSONError(wr, "Database, collection and action required", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	collName := parts[1]
	action := parts[2]

	db, err := w.store.GetDatabase(dbName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	coll, err := db.GetCollection(collName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	switch action {
	case "create":
		var req struct {
			Name   string   `json:"name"`
			Fields []string `json:"fields"`
			Unique bool     `json:"unique"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := coll.CreateIndex(req.Name, req.Fields, req.Unique); err != nil {
			w.logOperation("INDEX_CREATE", fmt.Sprintf("%s.%s.%s", dbName, collName, req.Name), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("INDEX_CREATE", fmt.Sprintf("%s.%s.%s", dbName, collName, req.Name), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "created"})

	case "drop":
		if len(parts) < 4 {
			w.sendJSONError(wr, "Index name required", http.StatusBadRequest)
			return
		}
		indexName := parts[3]
		if err := coll.DropIndex(indexName); err != nil {
			w.logOperation("INDEX_DROP", fmt.Sprintf("%s.%s.%s", dbName, collName, indexName), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}
		w.logOperation("INDEX_DROP", fmt.Sprintf("%s.%s.%s", dbName, collName, indexName), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "dropped"})

	default:
		w.sendJSONError(wr, "Unknown action", http.StatusBadRequest)
	}
}

// ========== Import/Export Handlers ==========

func (w *WebUIServer) handleExportData(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodPost {
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database string `json:"database"`
		Filename string `json:"filename"`
		Format   string `json:"format"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Database == "" {
		w.sendJSONError(wr, "Database name required", http.StatusBadRequest)
		return
	}

	if req.Filename == "" {
		req.Filename = req.Database + "_export_" + strconv.FormatInt(time.Now().Unix(), 10)
		if req.Format == "msgpack" {
			req.Filename += ".msgpack"
		} else {
			req.Filename += ".json"
		}
	}

	if req.Format == "" {
		req.Format = "json"
	}

	db, err := w.store.GetDatabase(req.Database)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	exportData := make(map[string]interface{})
	collections := db.ListCollections()
	totalDocuments := 0

	for _, collName := range collections {
		coll, err := db.GetCollection(collName)
		if err != nil {
			continue
		}

		docs := coll.GetAllDocuments()
		totalDocuments += len(docs)
		collData := make([]map[string]interface{}, 0, len(docs))
		for _, doc := range docs {
			docData := map[string]interface{}{
				"_id":        doc.ID,
				"fields":     doc.GetFields(),
				"created_at": doc.CreatedAt,
				"updated_at": doc.UpdatedAt,
				"version":    doc.Version,
			}
			if doc.DeletedAt > 0 {
				docData["deleted_at"] = doc.DeletedAt
			}
			collData = append(collData, docData)
		}
		exportData[collName] = collData
	}

	exportData["_metadata"] = map[string]interface{}{
		"database":        req.Database,
		"export_time":     time.Now().Unix(),
		"export_time_str": time.Now().Format("2006-01-02 15:04:05.000"),
		"version":         "1.0",
		"collections":     len(collections),
		"total_documents": totalDocuments,
		"format":          req.Format,
		"exported_by":     w.credentialMgr.GetCurrentUsername(),
	}

	w.logOperation("EXPORT", req.Database, "success", "", map[string]interface{}{
		"collections":     len(collections),
		"total_documents": totalDocuments,
		"format":          req.Format,
	})

	w.sendJSONSuccess(wr, map[string]interface{}{
		"status":          "export_prepared",
		"database":        req.Database,
		"filename":        req.Filename,
		"format":          req.Format,
		"collections":     len(collections),
		"total_documents": totalDocuments,
		"data":            exportData,
	})
}

func (w *WebUIServer) handleImportData(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodPost {
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database  string                 `json:"database"`
		Data      map[string]interface{} `json:"data"`
		Overwrite bool                   `json:"overwrite"`
		Format    string                 `json:"format"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Database == "" {
		w.sendJSONError(wr, "Database name required", http.StatusBadRequest)
		return
	}

	if req.Data == nil {
		w.sendJSONError(wr, "Import data required", http.StatusBadRequest)
		return
	}

	if req.Format == "" {
		req.Format = "json"
	}

	if !w.store.ExistsDatabase(req.Database) {
		if err := w.store.CreateDatabase(req.Database); err != nil {
			w.sendJSONError(wr, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	db, err := w.store.GetDatabase(req.Database)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	importedCollections := 0
	importedDocuments := 0
	skippedDocuments := 0
	failedDocuments := 0

	for key, value := range req.Data {
		if key == "_metadata" {
			continue
		}

		collName := key
		collData, ok := value.([]interface{})
		if !ok {
			continue
		}

		if _, err := db.GetCollection(collName); err != nil {
			if err := db.CreateCollection(collName); err != nil {
				w.logOperation("IMPORT_COLLECTION_CREATE", collName, "error", err.Error(), nil)
				continue
			}
		}

		coll, err := db.GetCollection(collName)
		if err != nil {
			continue
		}

		for _, docRaw := range collData {
			docMap, ok := docRaw.(map[string]interface{})
			if !ok {
				failedDocuments++
				continue
			}

			var docID string
			if id, ok := docMap["_id"].(string); ok {
				docID = id
			} else {
				failedDocuments++
				continue
			}

			if existingDoc, _ := coll.Find(docID); existingDoc != nil && !req.Overwrite {
				skippedDocuments++
				continue
			}

			doc := storage.NewDocumentWithID(docID)

			if fields, ok := docMap["fields"].(map[string]interface{}); ok {
				for k, v := range fields {
					doc.SetField(k, v)
				}
			}

			if createdAt, ok := docMap["created_at"]; ok {
				switch v := createdAt.(type) {
				case float64:
					doc.CreatedAt = int64(v)
				case int64:
					doc.CreatedAt = v
				}
			}

			if updatedAt, ok := docMap["updated_at"]; ok {
				switch v := updatedAt.(type) {
				case float64:
					doc.UpdatedAt = int64(v)
				case int64:
					doc.UpdatedAt = v
				}
			}

			if deletedAt, ok := docMap["deleted_at"]; ok {
				switch v := deletedAt.(type) {
				case float64:
					doc.DeletedAt = int64(v)
				case int64:
					doc.DeletedAt = v
				}
			}

			if version, ok := docMap["version"]; ok {
				switch v := version.(type) {
				case float64:
					doc.Version = uint64(v)
				case int64:
					doc.Version = uint64(v)
				case uint64:
					doc.Version = v
				}
			}

			if err := coll.Insert(doc); err != nil {
				failedDocuments++
				continue
			}
			importedDocuments++
		}
		importedCollections++
	}

	w.logOperation("IMPORT", req.Database, "success", "", map[string]interface{}{
		"collections": importedCollections,
		"documents":   importedDocuments,
		"skipped":     skippedDocuments,
		"failed":      failedDocuments,
		"overwrite":   req.Overwrite,
		"format":      req.Format,
	})

	w.sendJSONSuccess(wr, map[string]interface{}{
		"status":      "imported",
		"database":    req.Database,
		"collections": importedCollections,
		"documents":   importedDocuments,
		"skipped":     skippedDocuments,
		"failed":      failedDocuments,
		"format":      req.Format,
	})
}

// ========== Trigger Handlers ==========

func (w *WebUIServer) handleTriggers(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/triggers/")
	parts := strings.Split(path, "/")

	tm := storage.GetTriggerManager()
	if tm == nil {
		storage.InitTriggerManager(w.logger)
		tm = storage.GetTriggerManager()
	}

	switch r.Method {
	case http.MethodGet:
		if len(parts) >= 2 && parts[0] != "" && parts[1] != "" {
			dbName := parts[0]
			collName := parts[1]

			db, err := w.store.GetDatabase(dbName)
			if err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusNotFound)
				return
			}

			if _, err := db.GetCollection(collName); err != nil {
				w.sendJSONError(wr, err.Error(), http.StatusNotFound)
				return
			}

			triggers := tm.ListTriggers(collName)

			triggerList := make([]map[string]interface{}, 0, len(triggers))
			for _, trigger := range triggers {
				triggerList = append(triggerList, map[string]interface{}{
					"name":            trigger.Name,
					"collection":      trigger.Collection,
					"event":           trigger.Event,
					"action":          trigger.Action,
					"enabled":         trigger.Enabled,
					"description":     trigger.Description,
					"created_at":      trigger.CreatedAt,
					"updated_at":      trigger.UpdatedAt,
					"has_condition":   trigger.Condition != nil,
					"operations_count": len(trigger.Operations),
				})
			}
			w.sendJSONSuccess(wr, triggerList)
		} else {
			triggers := tm.ListTriggers("")
			triggerList := make([]map[string]interface{}, 0, len(triggers))
			for _, trigger := range triggers {
				triggerList = append(triggerList, map[string]interface{}{
					"name":        trigger.Name,
					"collection":  trigger.Collection,
					"event":       trigger.Event,
					"action":      trigger.Action,
					"enabled":     trigger.Enabled,
					"description": trigger.Description,
					"created_at":  trigger.CreatedAt,
				})
			}
			w.sendJSONSuccess(wr, triggerList)
		}

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (w *WebUIServer) handleTriggerOperation(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/trigger/")
	parts := strings.Split(path, "/")

	if len(parts) < 3 {
		w.sendJSONError(wr, "Invalid path. Use /api/webui/trigger/{db}/{collection}/{action}", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	collName := parts[1]
	action := parts[2]

	tm := storage.GetTriggerManager()
	if tm == nil {
		storage.InitTriggerManager(w.logger)
		tm = storage.GetTriggerManager()
	}

	switch action {
	case "create":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Name        string                   `json:"name"`
			Event       string                   `json:"event"`
			Action      string                   `json:"action"`
			Condition   map[string]interface{}   `json:"condition"`
			Operations  []map[string]interface{} `json:"operations"`
			Description string                   `json:"description"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.sendJSONError(wr, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.Name == "" {
			w.sendJSONError(wr, "Trigger name required", http.StatusBadRequest)
			return
		}

		config := make(map[string]interface{})
		config["action"] = req.Action
		config["description"] = req.Description

		if req.Condition != nil {
			config["condition"] = req.Condition
		}

		if len(req.Operations) > 0 {
			ops := make([]interface{}, len(req.Operations))
			for i, op := range req.Operations {
				ops[i] = op
			}
			config["operations"] = ops
		}

		event := storage.TriggerEvent(req.Event)
		if event == "" {
			event = storage.TriggerAfterInsert
		}

		if err := tm.CreateTrigger(dbName, collName, req.Name, event, config); err != nil {
			w.logOperation("TRIGGER_CREATE", fmt.Sprintf("%s.%s.%s", dbName, collName, req.Name), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusBadRequest)
			return
		}

		w.logOperation("TRIGGER_CREATE", fmt.Sprintf("%s.%s.%s", dbName, collName, req.Name), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "created",
			"name":   req.Name,
		})

	case "enable":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if len(parts) < 4 {
			w.sendJSONError(wr, "Trigger name required", http.StatusBadRequest)
			return
		}
		triggerName := parts[3]

		if err := tm.EnableTrigger(collName, "", triggerName); err != nil {
			w.logOperation("TRIGGER_ENABLE", fmt.Sprintf("%s.%s.%s", dbName, collName, triggerName), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusNotFound)
			return
		}
		w.logOperation("TRIGGER_ENABLE", fmt.Sprintf("%s.%s.%s", dbName, collName, triggerName), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "enabled", "name": triggerName})

	case "disable":
		if r.Method != http.MethodPost {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if len(parts) < 4 {
			w.sendJSONError(wr, "Trigger name required", http.StatusBadRequest)
			return
		}
		triggerName := parts[3]

		if err := tm.DisableTrigger(collName, "", triggerName); err != nil {
			w.logOperation("TRIGGER_DISABLE", fmt.Sprintf("%s.%s.%s", dbName, collName, triggerName), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusNotFound)
			return
		}
		w.logOperation("TRIGGER_DISABLE", fmt.Sprintf("%s.%s.%s", dbName, collName, triggerName), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "disabled", "name": triggerName})

	case "delete":
		if r.Method != http.MethodDelete {
			w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if len(parts) < 4 {
			w.sendJSONError(wr, "Trigger name required", http.StatusBadRequest)
			return
		}
		triggerName := parts[3]
		triggerEvent := ""
		if len(parts) > 4 {
			triggerEvent = parts[4]
		}

		if err := tm.DropTrigger(collName, triggerEvent, triggerName); err != nil {
			w.logOperation("TRIGGER_DELETE", fmt.Sprintf("%s.%s.%s", dbName, collName, triggerName), "error", err.Error(), nil)
			w.sendJSONError(wr, err.Error(), http.StatusNotFound)
			return
		}
		w.logOperation("TRIGGER_DELETE", fmt.Sprintf("%s.%s.%s", dbName, collName, triggerName), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{"status": "deleted", "name": triggerName})

	default:
		w.sendJSONError(wr, "Unknown action", http.StatusBadRequest)
	}
}

func (w *WebUIServer) handleTriggerLog(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodGet {
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tm := storage.GetTriggerManager()
	if tm == nil {
		w.sendJSONSuccess(wr, []interface{}{})
		return
	}

	logEntries := tm.GetTriggerExecutionLog()

	formattedLog := make([]map[string]interface{}, 0, len(logEntries))
	for _, entry := range logEntries {
		formattedLog = append(formattedLog, map[string]interface{}{
			"trigger_name": entry.TriggerName,
			"event":        entry.Event,
			"collection":   entry.Collection,
			"database":     entry.Database,
			"document_id":  entry.DocumentID,
			"operation":    entry.Operation,
			"timestamp":    entry.Timestamp.UnixMilli(),
			"user":         entry.User,
			"role":         entry.Role,
			"custom_data":  entry.CustomData,
		})
	}

	w.sendJSONSuccess(wr, formattedLog)
}

// ========== Constraint Handlers ==========

func (w *WebUIServer) handleConstraintsList(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodGet {
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/constraints/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		w.sendJSONError(wr, "Database and collection required", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	collName := parts[1]

	db, err := w.store.GetDatabase(dbName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	coll, err := db.GetCollection(collName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	constraintsList := make([]map[string]interface{}, 0)

	requiredFields := coll.GetRequiredFields()
	for _, field := range requiredFields {
		constraintsList = append(constraintsList, map[string]interface{}{
			"type":  "required",
			"field": field,
		})
	}

	uniqueFields := coll.GetUniqueConstraints()
	for _, field := range uniqueFields {
		constraintsList = append(constraintsList, map[string]interface{}{
			"type":  "unique",
			"field": field,
		})
	}

	minConstraints := coll.GetMinConstraints()
	for field, value := range minConstraints {
		constraintsList = append(constraintsList, map[string]interface{}{
			"type":  "min",
			"field": field,
			"value": value,
		})
	}

	maxConstraints := coll.GetMaxConstraints()
	for field, value := range maxConstraints {
		constraintsList = append(constraintsList, map[string]interface{}{
			"type":  "max",
			"field": field,
			"value": value,
		})
	}

	enumConstraints := coll.GetEnumConstraints()
	for field, values := range enumConstraints {
		constraintsList = append(constraintsList, map[string]interface{}{
			"type":   "enum",
			"field":  field,
			"values": values,
		})
	}

	regexConstraints := coll.GetRegexConstraints()
	for field, pattern := range regexConstraints {
		constraintsList = append(constraintsList, map[string]interface{}{
			"type":    "regex",
			"field":   field,
			"pattern": pattern,
		})
	}

	w.sendJSONSuccess(wr, constraintsList)
}

func (w *WebUIServer) handleConstraintOperation(wr http.ResponseWriter, r *http.Request) {
	if !w.checkAuth(r) {
		w.sendJSONError(wr, "Unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/webui/constraint/")
	parts := strings.Split(path, "/")

	if len(parts) < 3 {
		w.sendJSONError(wr, "Invalid path. Use /api/webui/constraint/{db}/{collection}/{action}", http.StatusBadRequest)
		return
	}

	dbName := parts[0]
	collName := parts[1]
	action := parts[2]

	db, err := w.store.GetDatabase(dbName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	coll, err := db.GetCollection(collName)
	if err != nil {
		w.sendJSONError(wr, err.Error(), http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPost:
		switch action {
		case "required":
			if len(parts) < 4 {
				w.sendJSONError(wr, "Field name required", http.StatusBadRequest)
				return
			}
			field := parts[3]
			coll.AddRequiredField(field)
			w.logOperation("CONSTRAINT_ADD", fmt.Sprintf("%s.%s.required.%s", dbName, collName, field), "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{
				"status": "added",
				"type":   "required",
				"field":  field,
			})

		case "unique":
			if len(parts) < 4 {
				w.sendJSONError(wr, "Field name required", http.StatusBadRequest)
				return
			}
			field := parts[3]
			coll.AddUniqueConstraint(field)
			w.logOperation("CONSTRAINT_ADD", fmt.Sprintf("%s.%s.unique.%s", dbName, collName, field), "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{
				"status": "added",
				"type":   "unique",
				"field":  field,
			})

		case "min":
			if len(parts) < 5 {
				w.sendJSONError(wr, "Field name and value required", http.StatusBadRequest)
				return
			}
			field := parts[3]
			minVal, err := strconv.ParseFloat(parts[4], 64)
			if err != nil {
				w.sendJSONError(wr, "Invalid minimum value", http.StatusBadRequest)
				return
			}
			coll.AddMinConstraint(field, minVal)
			w.logOperation("CONSTRAINT_ADD", fmt.Sprintf("%s.%s.min.%s", dbName, collName, field), "success", "", map[string]interface{}{"value": minVal})
			w.sendJSONSuccess(wr, map[string]interface{}{
				"status": "added",
				"type":   "min",
				"field":  field,
				"value":  minVal,
			})

		case "max":
			if len(parts) < 5 {
				w.sendJSONError(wr, "Field name and value required", http.StatusBadRequest)
				return
			}
			field := parts[3]
			maxVal, err := strconv.ParseFloat(parts[4], 64)
			if err != nil {
				w.sendJSONError(wr, "Invalid maximum value", http.StatusBadRequest)
				return
			}
			coll.AddMaxConstraint(field, maxVal)
			w.logOperation("CONSTRAINT_ADD", fmt.Sprintf("%s.%s.max.%s", dbName, collName, field), "success", "", map[string]interface{}{"value": maxVal})
			w.sendJSONSuccess(wr, map[string]interface{}{
				"status": "added",
				"type":   "max",
				"field":  field,
				"value":  maxVal,
			})

		case "enum":
			if len(parts) < 5 {
				w.sendJSONError(wr, "Field name and values required", http.StatusBadRequest)
				return
			}
			field := parts[3]
			values := make([]interface{}, len(parts)-4)
			for i := 4; i < len(parts); i++ {
				if num, err := strconv.ParseFloat(parts[i], 64); err == nil {
					values[i-4] = num
				} else {
					values[i-4] = parts[i]
				}
			}
			coll.AddEnumConstraint(field, values)
			w.logOperation("CONSTRAINT_ADD", fmt.Sprintf("%s.%s.enum.%s", dbName, collName, field), "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{
				"status": "added",
				"type":   "enum",
				"field":  field,
				"values": values,
			})

		case "regex":
			if len(parts) < 5 {
				w.sendJSONError(wr, "Field name and pattern required", http.StatusBadRequest)
				return
			}
			field := parts[3]
			pattern := parts[4]
			coll.AddRegexConstraint(field, pattern)
			w.logOperation("CONSTRAINT_ADD", fmt.Sprintf("%s.%s.regex.%s", dbName, collName, field), "success", "", nil)
			w.sendJSONSuccess(wr, map[string]interface{}{
				"status":  "added",
				"type":    "regex",
				"field":   field,
				"pattern": pattern,
			})

		default:
			w.sendJSONError(wr, "Unknown constraint type", http.StatusBadRequest)
		}

	case http.MethodDelete:
		if len(parts) < 4 {
			w.sendJSONError(wr, "Constraint type and field required", http.StatusBadRequest)
			return
		}
		constraintType := parts[2]
		field := parts[3]

		switch constraintType {
		case "required":
			coll.RemoveRequiredField(field)
		case "unique":
			coll.RemoveUniqueConstraint(field)
		case "min":
			coll.RemoveMinConstraint(field)
		case "max":
			coll.RemoveMaxConstraint(field)
		case "enum":
			coll.RemoveEnumConstraint(field)
		case "regex":
			coll.RemoveRegexConstraint(field)
		default:
			w.sendJSONError(wr, "Unknown constraint type", http.StatusBadRequest)
			return
		}

		w.logOperation("CONSTRAINT_REMOVE", fmt.Sprintf("%s.%s.%s.%s", dbName, collName, constraintType, field), "success", "", nil)
		w.sendJSONSuccess(wr, map[string]interface{}{
			"status": "removed",
			"type":   constraintType,
			"field":  field,
		})

	default:
		w.sendJSONError(wr, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
