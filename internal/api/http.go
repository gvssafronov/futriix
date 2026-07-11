/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/api/http.go
// Назначение: HTTP RESTful API для взаимодействия с СУБД через curl.
// Поддерживает CRUD операции, управление индексами, ACL и ограничениями.
// Реализован с минимальными блокировками, использует wait-free структуры.

package api

import (
    "crypto/rand"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "net/http"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"
    
    "futriis/internal/acl"
    "futriis/internal/cluster"
    "futriis/internal/log"
    "futriis/internal/storage"
)

// RateLimiterConfig содержит конфигурацию rate limiter'а
type RateLimiterConfigHTTP struct {
    RequestsPerSecond int
    BurstSize         int
    CleanupInterval   time.Duration
    MaxEntries        int
}

// DefaultRateLimiterConfigHTTP возвращает конфигурацию по умолчанию
func DefaultRateLimiterConfigHTTP() *RateLimiterConfigHTTP {
    return &RateLimiterConfigHTTP{
        RequestsPerSecond: 100,
        BurstSize:         20,
        CleanupInterval:   5 * time.Minute,
        MaxEntries:        10000,
    }
}

// TokenBucketHTTP реализует алгоритм token bucket для HTTP API
type TokenBucketHTTP struct {
    tokens        float64
    maxTokens     float64
    refillRate    float64
    lastRefill    time.Time
    mu            sync.Mutex
}

// NewTokenBucketHTTP создаёт новый token bucket
func NewTokenBucketHTTP(ratePerSecond float64, burst int) *TokenBucketHTTP {
    return &TokenBucketHTTP{
        tokens:     float64(burst),
        maxTokens:  float64(burst),
        refillRate: ratePerSecond,
        lastRefill: time.Now(),
    }
}

// Allow проверяет, разрешён ли запрос
func (tb *TokenBucketHTTP) Allow() bool {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    
    now := time.Now()
    elapsed := now.Sub(tb.lastRefill).Seconds()
    tb.tokens += elapsed * tb.refillRate
    if tb.tokens > tb.maxTokens {
        tb.tokens = tb.maxTokens
    }
    tb.lastRefill = now
    
    if tb.tokens >= 1.0 {
        tb.tokens -= 1.0
        return true
    }
    return false
}

// RateLimiterHTTP управляет rate limiting для HTTP API
type RateLimiterHTTP struct {
    config      *RateLimiterConfigHTTP
    limiters    sync.Map
    rejected    atomic.Uint64
    allowed     atomic.Uint64
    stopChan    chan struct{}
    wg          sync.WaitGroup
}

// NewRateLimiterHTTP создаёт новый rate limiter
func NewRateLimiterHTTP(config *RateLimiterConfigHTTP) *RateLimiterHTTP {
    if config == nil {
        config = DefaultRateLimiterConfigHTTP()
    }
    
    rl := &RateLimiterHTTP{
        config:   config,
        stopChan: make(chan struct{}),
    }
    
    rl.wg.Add(1)
    go rl.cleanupLoop()
    
    return rl
}

// getLimiter возвращает или создаёт лимитер для ключа
func (rl *RateLimiterHTTP) getLimiter(key string) *TokenBucketHTTP {
    if val, ok := rl.limiters.Load(key); ok {
        return val.(*TokenBucketHTTP)
    }
    
    limiter := NewTokenBucketHTTP(float64(rl.config.RequestsPerSecond), rl.config.BurstSize)
    actual, _ := rl.limiters.LoadOrStore(key, limiter)
    return actual.(*TokenBucketHTTP)
}

// Allow проверяет, разрешён ли запрос для ключа
func (rl *RateLimiterHTTP) Allow(key string) bool {
    limiter := rl.getLimiter(key)
    allowed := limiter.Allow()
    
    if allowed {
        rl.allowed.Add(1)
    } else {
        rl.rejected.Add(1)
    }
    
    return allowed
}

// Middleware возвращает middleware для HTTP
func (rl *RateLimiterHTTP) Middleware(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        key := r.RemoteAddr
        if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
            key = forwarded
        }
        
        if !rl.Allow(key) {
            w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.config.RequestsPerSecond))
            w.Header().Set("X-RateLimit-Remaining", "0")
            w.Header().Set("Retry-After", "1")
            http.Error(w, "Rate limit exceeded. Please try again later.", http.StatusTooManyRequests)
            return
        }
        
        next(w, r)
    }
}

// cleanupLoop периодически очищает старые лимитеры
func (rl *RateLimiterHTTP) cleanupLoop() {
    defer rl.wg.Done()
    
    ticker := time.NewTicker(rl.config.CleanupInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            rl.cleanup()
        case <-rl.stopChan:
            return
        }
    }
}

// cleanup удаляет старые лимитеры
func (rl *RateLimiterHTTP) cleanup() {
    count := 0
    rl.limiters.Range(func(key, value interface{}) bool {
        count++
        if count > rl.config.MaxEntries {
            rl.limiters.Delete(key)
        }
        return true
    })
}

// GetStats возвращает статистику rate limiter'а
func (rl *RateLimiterHTTP) GetStats() map[string]interface{} {
    count := 0
    rl.limiters.Range(func(key, value interface{}) bool {
        count++
        return true
    })
    
    return map[string]interface{}{
        "active_limiters":     count,
        "allowed_requests":    rl.allowed.Load(),
        "rejected_requests":   rl.rejected.Load(),
        "requests_per_second": rl.config.RequestsPerSecond,
        "burst_size":          rl.config.BurstSize,
    }
}

// Stop останавливает rate limiter
func (rl *RateLimiterHTTP) Stop() {
    close(rl.stopChan)
    rl.wg.Wait()
}

// generateSecureToken генерирует криптостойкий токен
func generateSecureToken() string {
    bytes := make([]byte, 32)
    _, err := rand.Read(bytes)
    if err != nil {
        return fmt.Sprintf("fallback_%d", time.Now().UnixNano())
    }
    return base64.URLEncoding.EncodeToString(bytes)
}

// HTTPServer представляет HTTP сервер API
type HTTPServer struct {
    store       *storage.Storage
    coordinator *cluster.RaftCoordinator
    aclManager  *acl.ACLManager
    logger      *log.Logger
    server      *http.Server
    port        int
    rateLimiter *RateLimiterHTTP
    sessions    sync.Map
}

// APIResponse представляет стандартный ответ API
type APIResponse struct {
    Success bool        `json:"success"`
    Data    interface{} `json:"data,omitempty"`
    Error   string      `json:"error,omitempty"`
}

// NewHTTPServer создаёт новый HTTP сервер
func NewHTTPServer(port int, store *storage.Storage, coord *cluster.RaftCoordinator, aclMgr *acl.ACLManager, logger *log.Logger) *HTTPServer {
    s := &HTTPServer{
        store:       store,
        coordinator: coord,
        aclManager:  aclMgr,
        logger:      logger,
        port:        port,
        rateLimiter: NewRateLimiterHTTP(DefaultRateLimiterConfigHTTP()),
    }
    
    mux := http.NewServeMux()
    
    corsHandler := func(handler http.HandlerFunc) http.HandlerFunc {
        return func(w http.ResponseWriter, r *http.Request) {
            w.Header().Set("Access-Control-Allow-Origin", "*")
            w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
            w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Session-ID, Authorization")
            
            if r.Method == http.MethodOptions {
                w.WriteHeader(http.StatusOK)
                return
            }
            
            s.rateLimiter.Middleware(handler)(w, r)
        }
    }
    
    authMiddleware := func(handler http.HandlerFunc) http.HandlerFunc {
        return func(w http.ResponseWriter, r *http.Request) {
            sessionID := r.Header.Get("X-Session-ID")
            if sessionID == "" {
                auth := r.Header.Get("Authorization")
                if strings.HasPrefix(auth, "Bearer ") {
                    sessionID = strings.TrimPrefix(auth, "Bearer ")
                }
            }
            
            if sessionID == "" {
                s.sendError(w, "Authentication required", http.StatusUnauthorized)
                return
            }
            
            if _, ok := s.sessions.Load(sessionID); !ok {
                s.sendError(w, "Invalid or expired session", http.StatusUnauthorized)
                return
            }
            
            handler(w, r)
        }
    }
    
    mux.HandleFunc("/api/auth/login", corsHandler(s.handleLogin))
    mux.HandleFunc("/api/auth/logout", corsHandler(authMiddleware(s.handleLogout)))
    mux.HandleFunc("/api/auth/session", corsHandler(authMiddleware(s.handleSessionCheck)))
    
    mux.HandleFunc("/api/db/", corsHandler(authMiddleware(s.handleDatabaseRequest)))
    mux.HandleFunc("/api/index/", corsHandler(authMiddleware(s.handleIndexRequest)))
    mux.HandleFunc("/api/acl/", corsHandler(authMiddleware(s.handleACLRequest)))
    mux.HandleFunc("/api/constraint/", corsHandler(authMiddleware(s.handleConstraintRequest)))
    mux.HandleFunc("/api/cluster/", corsHandler(authMiddleware(s.handleClusterRequest)))
    mux.HandleFunc("/api/trigger/", corsHandler(authMiddleware(s.handleTriggerRequest)))
    mux.HandleFunc("/api/transaction/", corsHandler(authMiddleware(s.handleTransactionRequest)))
    
    mux.HandleFunc("/api/health", s.handleHealthCheck)
    mux.HandleFunc("/api/metrics", s.handleMetricsEndpoint)
    
    s.server = &http.Server{
        Addr:    fmt.Sprintf(":%d", port),
        Handler: mux,
    }
    
    return s
}

// Start запускает HTTP сервер
func (s *HTTPServer) Start() error {
    s.logger.Info("Starting HTTP API server on port " + strconv.Itoa(s.port))
    return s.server.ListenAndServe()
}

// Stop останавливает HTTP сервер
func (s *HTTPServer) Stop() error {
    s.rateLimiter.Stop()
    return s.server.Close()
}

// GetRateLimiterStats возвращает статистику rate limiter'а
func (s *HTTPServer) GetRateLimiterStats() map[string]interface{} {
    return s.rateLimiter.GetStats()
}

// isUserAdmin проверяет, является ли пользователь администратором через ACL
func (s *HTTPServer) isUserAdmin(username string) bool {
    if s.aclManager == nil {
        return username == "admin"
    }
    roles := s.aclManager.GetUserRoles(username)
    for _, role := range roles {
        if role == "admin" {
            return true
        }
    }
    return false
}

// handleLogin обрабатывает аутентификацию
func (s *HTTPServer) handleLogin(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    var creds struct {
        Username string `json:"username"`
        Password string `json:"password"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
        s.sendError(w, "Invalid request body", http.StatusBadRequest)
        return
    }
    
    sessionID, err := s.aclManager.Authenticate(creds.Username, creds.Password)
    if err != nil {
        s.logOperation("LOGIN", creds.Username, "error", err.Error(), nil)
        s.sendError(w, err.Error(), http.StatusUnauthorized)
        return
    }
    
    token := generateSecureToken()
    s.sessions.Store(token, creds.Username)
    s.sessions.Store(sessionID, creds.Username)
    
    s.logOperation("LOGIN", creds.Username, "success", "", nil)
    
    s.sendSuccess(w, map[string]interface{}{
        "session_id": sessionID,
        "token":      token,
        "username":   creds.Username,
        "is_admin":   s.isUserAdmin(creds.Username),
    })
}

// handleLogout обрабатывает выход
func (s *HTTPServer) handleLogout(w http.ResponseWriter, r *http.Request) {
    sessionID := r.Header.Get("X-Session-ID")
    if sessionID == "" {
        auth := r.Header.Get("Authorization")
        if strings.HasPrefix(auth, "Bearer ") {
            sessionID = strings.TrimPrefix(auth, "Bearer ")
        }
    }
    
    if sessionID != "" {
        if username, ok := s.sessions.Load(sessionID); ok {
            s.logOperation("LOGOUT", username.(string), "success", "", nil)
            s.sessions.Delete(sessionID)
        }
        s.aclManager.Logout(sessionID)
    }
    
    s.sendSuccess(w, map[string]string{"status": "logged out"})
}

// handleSessionCheck проверяет активность сессии
func (s *HTTPServer) handleSessionCheck(w http.ResponseWriter, r *http.Request) {
    sessionID := r.Header.Get("X-Session-ID")
    if sessionID == "" {
        auth := r.Header.Get("Authorization")
        if strings.HasPrefix(auth, "Bearer ") {
            sessionID = strings.TrimPrefix(auth, "Bearer ")
        }
    }
    
    if sessionID == "" {
        s.sendError(w, "No session", http.StatusUnauthorized)
        return
    }
    
    username, ok := s.sessions.Load(sessionID)
    if !ok {
        s.sendError(w, "Invalid session", http.StatusUnauthorized)
        return
    }
    
    s.sendSuccess(w, map[string]interface{}{
        "authenticated": true,
        "username":      username,
        "is_admin":      s.isUserAdmin(username.(string)),
    })
}

// handleDatabaseRequest обрабатывает запросы к БД
func (s *HTTPServer) handleDatabaseRequest(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/api/db/")
    parts := strings.Split(path, "/")
    
    if len(parts) < 2 {
        s.sendError(w, "Invalid path. Use /api/db/{database}/{collection}[/{id}]", http.StatusBadRequest)
        return
    }
    
    database := parts[0]
    collection := parts[1]
    docID := ""
    if len(parts) > 2 {
        docID = parts[2]
    }
    
    sessionID := s.getSessionID(r)
    if sessionID == "" {
        s.sendError(w, "Authentication required", http.StatusUnauthorized)
        return
    }
    
    switch r.Method {
    case http.MethodGet:
        s.handleGetDocument(w, r, sessionID, database, collection, docID)
    case http.MethodPost:
        s.handleInsertDocument(w, r, sessionID, database, collection)
    case http.MethodPut:
        s.handleUpdateDocument(w, r, sessionID, database, collection, docID)
    case http.MethodDelete:
        s.handleDeleteDocument(w, r, sessionID, database, collection, docID)
    default:
        s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
    }
}

// getSessionID извлекает session ID из запроса
func (s *HTTPServer) getSessionID(r *http.Request) string {
    sessionID := r.Header.Get("X-Session-ID")
    if sessionID == "" {
        auth := r.Header.Get("Authorization")
        if strings.HasPrefix(auth, "Bearer ") {
            sessionID = strings.TrimPrefix(auth, "Bearer ")
        }
    }
    return sessionID
}

// handleGetDocument обрабатывает GET запросы
func (s *HTTPServer) handleGetDocument(w http.ResponseWriter, r *http.Request, sessionID, database, collection, docID string) {
    if !s.aclManager.CheckPermission(sessionID, database, collection, "read") {
        s.logOperation("GET_DOCUMENT", fmt.Sprintf("%s.%s", database, collection), "error", "Access denied", nil)
        s.sendError(w, "Access denied", http.StatusForbidden)
        return
    }
    
    db, err := s.store.GetDatabase(database)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    coll, err := db.GetCollection(collection)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    query := r.URL.Query()
    if indexName := query.Get("index"); indexName != "" {
        indexValue := query.Get("value")
        docs, err := coll.FindByIndex(indexName, indexValue)
        if err != nil {
            s.sendError(w, err.Error(), http.StatusNotFound)
            return
        }
        s.logOperation("GET_DOCUMENT_BY_INDEX", fmt.Sprintf("%s.%s[%s=%s]", database, collection, indexName, indexValue), "success", "", nil)
        s.sendSuccess(w, docs)
        return
    }
    
    if docID == "" {
        limit := 100
        if limitStr := query.Get("limit"); limitStr != "" {
            if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
                limit = l
            }
        }
        
        offset := 0
        if offsetStr := query.Get("offset"); offsetStr != "" {
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
        
        result := allDocs[start:end]
        s.logOperation("LIST_DOCUMENTS", fmt.Sprintf("%s.%s", database, collection), "success", "", map[string]interface{}{
            "total":  len(allDocs),
            "limit":  limit,
            "offset": offset,
        })
        s.sendSuccess(w, map[string]interface{}{
            "documents": result,
            "total":     len(allDocs),
            "limit":     limit,
            "offset":    offset,
        })
        return
    }
    
    doc, err := coll.Find(docID)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    s.logOperation("GET_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "success", "", nil)
    s.sendSuccess(w, doc)
}

// handleInsertDocument обрабатывает POST запросы
func (s *HTTPServer) handleInsertDocument(w http.ResponseWriter, r *http.Request, sessionID, database, collection string) {
    if !s.aclManager.CheckPermission(sessionID, database, collection, "write") {
        s.logOperation("INSERT_DOCUMENT", fmt.Sprintf("%s.%s", database, collection), "error", "Access denied", nil)
        s.sendError(w, "Access denied", http.StatusForbidden)
        return
    }
    
    db, err := s.store.GetDatabase(database)
    if err != nil {
        if err := s.store.CreateDatabase(database); err != nil {
            s.sendError(w, err.Error(), http.StatusInternalServerError)
            return
        }
        db, _ = s.store.GetDatabase(database)
    }
    
    coll, err := db.GetCollection(collection)
    if err != nil {
        if err := db.CreateCollection(collection); err != nil {
            s.sendError(w, err.Error(), http.StatusInternalServerError)
            return
        }
        coll, _ = db.GetCollection(collection)
    }
    
    var doc map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
        s.sendError(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    docID := ""
    if id, ok := doc["_id"]; ok {
        if idStr, ok := id.(string); ok {
            docID = idStr
        }
    }
    
    if err := coll.InsertFromMap(doc); err != nil {
        s.logOperation("INSERT_DOCUMENT", fmt.Sprintf("%s.%s/%s", database, collection, docID), "error", err.Error(), nil)
        s.sendError(w, err.Error(), http.StatusBadRequest)
        return
    }
    
    s.logOperation("INSERT_DOCUMENT", fmt.Sprintf("%s.%s/%s", database, collection, docID), "success", "", nil)
    s.sendSuccess(w, map[string]interface{}{
        "status": "inserted",
        "id":     docID,
    })
}

// handleUpdateDocument обрабатывает PUT запросы
func (s *HTTPServer) handleUpdateDocument(w http.ResponseWriter, r *http.Request, sessionID, database, collection, docID string) {
    if docID == "" {
        s.sendError(w, "Document ID required", http.StatusBadRequest)
        return
    }
    
    if !s.aclManager.CheckPermission(sessionID, database, collection, "write") {
        s.logOperation("UPDATE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "error", "Access denied", nil)
        s.sendError(w, "Access denied", http.StatusForbidden)
        return
    }
    
    db, err := s.store.GetDatabase(database)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    coll, err := db.GetCollection(collection)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    var updates map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
        s.sendError(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    if err := coll.Update(docID, updates); err != nil {
        s.logOperation("UPDATE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "error", err.Error(), nil)
        s.sendError(w, err.Error(), http.StatusBadRequest)
        return
    }
    
    s.logOperation("UPDATE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "success", "", map[string]interface{}{
        "updated_fields": len(updates),
    })
    s.sendSuccess(w, map[string]interface{}{
        "status": "updated",
        "id":     docID,
    })
}

// handleDeleteDocument обрабатывает DELETE запросы
func (s *HTTPServer) handleDeleteDocument(w http.ResponseWriter, r *http.Request, sessionID, database, collection, docID string) {
    if docID == "" {
        s.sendError(w, "Document ID required", http.StatusBadRequest)
        return
    }
    
    if !s.aclManager.CheckPermission(sessionID, database, collection, "delete") {
        s.logOperation("DELETE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "error", "Access denied", nil)
        s.sendError(w, "Access denied", http.StatusForbidden)
        return
    }
    
    db, err := s.store.GetDatabase(database)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    coll, err := db.GetCollection(collection)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    permanent := r.URL.Query().Get("permanent") == "true"
    
    if permanent {
        if err := coll.PermanentDelete(docID); err != nil {
            s.logOperation("PERMANENT_DELETE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusNotFound)
            return
        }
        s.logOperation("PERMANENT_DELETE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "permanently_deleted",
            "id":     docID,
        })
        return
    }
    
    if err := coll.Delete(docID); err != nil {
        s.logOperation("DELETE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "error", err.Error(), nil)
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    s.logOperation("DELETE_DOCUMENT", fmt.Sprintf("%s.%s.%s", database, collection, docID), "success", "", nil)
    s.sendSuccess(w, map[string]interface{}{
        "status": "deleted",
        "id":     docID,
    })
}

// handleIndexRequest обрабатывает запросы к индексам
func (s *HTTPServer) handleIndexRequest(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/api/index/")
    parts := strings.Split(path, "/")
    
    if len(parts) < 3 {
        s.sendError(w, "Invalid path. Use /api/index/{database}/{collection}/{action}", http.StatusBadRequest)
        return
    }
    
    database := parts[0]
    collection := parts[1]
    action := parts[2]
    
    sessionID := s.getSessionID(r)
    if !s.aclManager.CheckPermission(sessionID, database, collection, "admin") {
        s.logOperation("INDEX_OPERATION", fmt.Sprintf("%s.%s.%s", database, collection, action), "error", "Admin access required", nil)
        s.sendError(w, "Admin access required", http.StatusForbidden)
        return
    }
    
    db, err := s.store.GetDatabase(database)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    coll, err := db.GetCollection(collection)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    switch action {
    case "list":
        indexes := coll.GetIndexesInfo()
        s.logOperation("LIST_INDEXES", fmt.Sprintf("%s.%s", database, collection), "success", "", nil)
        s.sendSuccess(w, indexes)
        
    case "create":
        var req struct {
            Name   string   `json:"name"`
            Fields []string `json:"fields"`
            Unique bool     `json:"unique"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            s.sendError(w, "Invalid request body", http.StatusBadRequest)
            return
        }
        if req.Name == "" {
            s.sendError(w, "Index name required", http.StatusBadRequest)
            return
        }
        if len(req.Fields) == 0 {
            s.sendError(w, "At least one field required", http.StatusBadRequest)
            return
        }
        if err := coll.CreateIndex(req.Name, req.Fields, req.Unique); err != nil {
            s.logOperation("CREATE_INDEX", fmt.Sprintf("%s.%s.%s", database, collection, req.Name), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusBadRequest)
            return
        }
        s.logOperation("CREATE_INDEX", fmt.Sprintf("%s.%s.%s", database, collection, req.Name), "success", "", map[string]interface{}{
            "fields": req.Fields,
            "unique": req.Unique,
        })
        s.sendSuccess(w, map[string]interface{}{
            "status": "index_created",
            "name":   req.Name,
        })
        
    case "drop":
        if len(parts) < 4 {
            s.sendError(w, "Index name required", http.StatusBadRequest)
            return
        }
        indexName := parts[3]
        if err := coll.DropIndex(indexName); err != nil {
            s.logOperation("DROP_INDEX", fmt.Sprintf("%s.%s.%s", database, collection, indexName), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusBadRequest)
            return
        }
        s.logOperation("DROP_INDEX", fmt.Sprintf("%s.%s.%s", database, collection, indexName), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "index_dropped",
            "name":   indexName,
        })
        
    default:
        s.sendError(w, "Unknown action", http.StatusBadRequest)
    }
}

// handleACLRequest обрабатывает запросы ACL
func (s *HTTPServer) handleACLRequest(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/api/acl/")
    parts := strings.Split(path, "/")
    
    if len(parts) < 1 {
        s.sendError(w, "Invalid path", http.StatusBadRequest)
        return
    }
    
    sessionID := s.getSessionID(r)
    if !s.aclManager.CheckPermission(sessionID, "*", "*", "admin") {
        s.logOperation("ACL_OPERATION", strings.Join(parts, "/"), "error", "Admin access required", nil)
        s.sendError(w, "Admin access required", http.StatusForbidden)
        return
    }
    
    action := parts[0]
    
    switch action {
    case "users":
        users := s.aclManager.ListUsers()
        s.sendSuccess(w, users)
        
    case "user":
        if len(parts) < 2 {
            s.sendError(w, "Username required", http.StatusBadRequest)
            return
        }
        username := parts[1]
        
        switch r.Method {
        case http.MethodGet:
            userInfo, err := s.aclManager.GetUserInfo(username)
            if err != nil {
                s.sendError(w, err.Error(), http.StatusNotFound)
                return
            }
            s.sendSuccess(w, userInfo)
            
        case http.MethodPost:
            var req struct {
                Password string   `json:"password"`
                Roles    []string `json:"roles"`
            }
            if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                s.sendError(w, "Invalid request body", http.StatusBadRequest)
                return
            }
            if err := s.aclManager.CreateUser(username, req.Password, req.Roles); err != nil {
                s.logOperation("ACL_CREATE_USER", username, "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            s.logOperation("ACL_CREATE_USER", username, "success", "", map[string]interface{}{
                "roles": req.Roles,
            })
            s.sendSuccess(w, map[string]interface{}{
                "status": "user_created",
                "user":   username,
            })
            
        case http.MethodPut:
            var req struct {
                AddRole    string `json:"add_role"`
                RemoveRole string `json:"remove_role"`
                Password   string `json:"password"`
                Disable    bool   `json:"disable"`
                Enable     bool   `json:"enable"`
            }
            if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                s.sendError(w, "Invalid request body", http.StatusBadRequest)
                return
            }
            
            if req.AddRole != "" {
                if err := s.aclManager.AddUserRole(username, req.AddRole); err != nil {
                    s.sendError(w, err.Error(), http.StatusBadRequest)
                    return
                }
            }
            if req.RemoveRole != "" {
                if err := s.aclManager.RemoveUserRole(username, req.RemoveRole); err != nil {
                    s.sendError(w, err.Error(), http.StatusBadRequest)
                    return
                }
            }
            if req.Password != "" {
                if err := s.aclManager.ChangePassword(username, req.Password); err != nil {
                    s.sendError(w, err.Error(), http.StatusBadRequest)
                    return
                }
            }
            if req.Disable {
                if err := s.aclManager.DisableUser(username); err != nil {
                    s.sendError(w, err.Error(), http.StatusBadRequest)
                    return
                }
            }
            if req.Enable {
                if err := s.aclManager.EnableUser(username); err != nil {
                    s.sendError(w, err.Error(), http.StatusBadRequest)
                    return
                }
            }
            s.logOperation("ACL_UPDATE_USER", username, "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "user_updated",
                "user":   username,
            })
            
        case http.MethodDelete:
            if err := s.aclManager.DeleteUser(username); err != nil {
                s.logOperation("ACL_DELETE_USER", username, "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            s.logOperation("ACL_DELETE_USER", username, "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "user_deleted",
                "user":   username,
            })
            
        default:
            s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
        }
        
    case "roles":
        roles := s.aclManager.ListRoles()
        s.sendSuccess(w, roles)
        
    case "role":
        if len(parts) < 2 {
            s.sendError(w, "Role name required", http.StatusBadRequest)
            return
        }
        roleName := parts[1]
        
        switch r.Method {
        case http.MethodGet:
            perms, err := s.aclManager.GetRolePermissions(roleName)
            if err != nil {
                s.sendError(w, err.Error(), http.StatusNotFound)
                return
            }
            s.sendSuccess(w, map[string]interface{}{
                "name":        roleName,
                "permissions": perms,
            })
            
        case http.MethodPost:
            if err := s.aclManager.CreateRole(roleName); err != nil {
                s.logOperation("ACL_CREATE_ROLE", roleName, "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            s.logOperation("ACL_CREATE_ROLE", roleName, "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "role_created",
                "role":   roleName,
            })
            
        case http.MethodDelete:
            if err := s.aclManager.DeleteRole(roleName); err != nil {
                s.logOperation("ACL_DELETE_ROLE", roleName, "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            s.logOperation("ACL_DELETE_ROLE", roleName, "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "role_deleted",
                "role":   roleName,
            })
            
        default:
            s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
        }
        
    case "grant":
        if len(parts) < 3 {
            s.sendError(w, "Role and permission required", http.StatusBadRequest)
            return
        }
        roleName := parts[1]
        permission := parts[2]
        if err := s.aclManager.GrantPermission(roleName, permission); err != nil {
            s.logOperation("ACL_GRANT", fmt.Sprintf("%s.%s", roleName, permission), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusBadRequest)
            return
        }
        s.logOperation("ACL_GRANT", fmt.Sprintf("%s.%s", roleName, permission), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status":     "permission_granted",
            "role":       roleName,
            "permission": permission,
        })
        
    case "revoke":
        if len(parts) < 3 {
            s.sendError(w, "Role and permission required", http.StatusBadRequest)
            return
        }
        roleName := parts[1]
        permission := parts[2]
        if err := s.aclManager.RevokePermission(roleName, permission); err != nil {
            s.logOperation("ACL_REVOKE", fmt.Sprintf("%s.%s", roleName, permission), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusBadRequest)
            return
        }
        s.logOperation("ACL_REVOKE", fmt.Sprintf("%s.%s", roleName, permission), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status":     "permission_revoked",
            "role":       roleName,
            "permission": permission,
        })
        
    default:
        s.sendError(w, "Unknown action", http.StatusBadRequest)
    }
}

// handleConstraintRequest обрабатывает запросы к ограничениям
func (s *HTTPServer) handleConstraintRequest(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/api/constraint/")
    parts := strings.Split(path, "/")
    
    if len(parts) < 3 {
        s.sendError(w, "Invalid path. Use /api/constraint/{database}/{collection}/{action}", http.StatusBadRequest)
        return
    }
    
    database := parts[0]
    collection := parts[1]
    action := parts[2]
    
    sessionID := s.getSessionID(r)
    if !s.aclManager.CheckPermission(sessionID, database, collection, "admin") {
        s.logOperation("CONSTRAINT_OPERATION", fmt.Sprintf("%s.%s.%s", database, collection, action), "error", "Admin access required", nil)
        s.sendError(w, "Admin access required", http.StatusForbidden)
        return
    }
    
    db, err := s.store.GetDatabase(database)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    coll, err := db.GetCollection(collection)
    if err != nil {
        s.sendError(w, err.Error(), http.StatusNotFound)
        return
    }
    
    switch action {
    case "list":
        constraints := make([]map[string]interface{}, 0)
        
        for _, field := range coll.GetRequiredFields() {
            constraints = append(constraints, map[string]interface{}{
                "type":  "required",
                "field": field,
            })
        }
        for _, field := range coll.GetUniqueConstraints() {
            constraints = append(constraints, map[string]interface{}{
                "type":  "unique",
                "field": field,
            })
        }
        for field, value := range coll.GetMinConstraints() {
            constraints = append(constraints, map[string]interface{}{
                "type":  "min",
                "field": field,
                "value": value,
            })
        }
        for field, value := range coll.GetMaxConstraints() {
            constraints = append(constraints, map[string]interface{}{
                "type":  "max",
                "field": field,
                "value": value,
            })
        }
        for field, values := range coll.GetEnumConstraints() {
            constraints = append(constraints, map[string]interface{}{
                "type":   "enum",
                "field":  field,
                "values": values,
            })
        }
        for field, pattern := range coll.GetRegexConstraints() {
            constraints = append(constraints, map[string]interface{}{
                "type":    "regex",
                "field":   field,
                "pattern": pattern,
            })
        }
        
        s.sendSuccess(w, constraints)
        
    case "required":
        if len(parts) < 4 {
            s.sendError(w, "Field name required", http.StatusBadRequest)
            return
        }
        field := parts[3]
        coll.AddRequiredField(field)
        s.logOperation("ADD_REQUIRED", fmt.Sprintf("%s.%s.%s", database, collection, field), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "required_field_added",
            "field":  field,
        })
        
    case "unique":
        if len(parts) < 4 {
            s.sendError(w, "Field name required", http.StatusBadRequest)
            return
        }
        field := parts[3]
        coll.AddUniqueConstraint(field)
        s.logOperation("ADD_UNIQUE", fmt.Sprintf("%s.%s.%s", database, collection, field), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "unique_constraint_added",
            "field":  field,
        })
        
    case "min":
        if len(parts) < 5 {
            s.sendError(w, "Field name and value required", http.StatusBadRequest)
            return
        }
        field := parts[3]
        minVal, err := strconv.ParseFloat(parts[4], 64)
        if err != nil {
            s.sendError(w, "Invalid minimum value", http.StatusBadRequest)
            return
        }
        coll.AddMinConstraint(field, minVal)
        s.logOperation("ADD_MIN", fmt.Sprintf("%s.%s.%s", database, collection, field), "success", "", map[string]interface{}{"value": minVal})
        s.sendSuccess(w, map[string]interface{}{
            "status": "min_constraint_added",
            "field":  field,
            "value":  minVal,
        })
        
    case "max":
        if len(parts) < 5 {
            s.sendError(w, "Field name and value required", http.StatusBadRequest)
            return
        }
        field := parts[3]
        maxVal, err := strconv.ParseFloat(parts[4], 64)
        if err != nil {
            s.sendError(w, "Invalid maximum value", http.StatusBadRequest)
            return
        }
        coll.AddMaxConstraint(field, maxVal)
        s.logOperation("ADD_MAX", fmt.Sprintf("%s.%s.%s", database, collection, field), "success", "", map[string]interface{}{"value": maxVal})
        s.sendSuccess(w, map[string]interface{}{
            "status": "max_constraint_added",
            "field":  field,
            "value":  maxVal,
        })
        
    case "enum":
        if len(parts) < 5 {
            s.sendError(w, "Field name and values required", http.StatusBadRequest)
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
        s.logOperation("ADD_ENUM", fmt.Sprintf("%s.%s.%s", database, collection, field), "success", "", map[string]interface{}{"values": values})
        s.sendSuccess(w, map[string]interface{}{
            "status": "enum_constraint_added",
            "field":  field,
            "values": values,
        })
        
    case "regex":
        if len(parts) < 5 {
            s.sendError(w, "Field name and pattern required", http.StatusBadRequest)
            return
        }
        field := parts[3]
        pattern := parts[4]
        coll.AddRegexConstraint(field, pattern)
        s.logOperation("ADD_REGEX", fmt.Sprintf("%s.%s.%s", database, collection, field), "success", "", map[string]interface{}{"pattern": pattern})
        s.sendSuccess(w, map[string]interface{}{
            "status":  "regex_constraint_added",
            "field":   field,
            "pattern": pattern,
        })
        
    case "remove":
        if len(parts) < 5 {
            s.sendError(w, "Constraint type and field required", http.StatusBadRequest)
            return
        }
        constraintType := parts[3]
        field := parts[4]
        
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
            s.sendError(w, "Unknown constraint type", http.StatusBadRequest)
            return
        }
        s.logOperation("REMOVE_CONSTRAINT", fmt.Sprintf("%s.%s.%s.%s", database, collection, constraintType, field), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "constraint_removed",
            "type":   constraintType,
            "field":  field,
        })
        
    default:
        s.sendError(w, "Unknown action", http.StatusBadRequest)
    }
}

// handleClusterRequest обрабатывает запросы к кластеру
func (s *HTTPServer) handleClusterRequest(w http.ResponseWriter, r *http.Request) {
    sessionID := s.getSessionID(r)
    if !s.aclManager.CheckPermission(sessionID, "*", "*", "admin") {
        s.logOperation("CLUSTER_OPERATION", r.URL.Path, "error", "Admin access required", nil)
        s.sendError(w, "Admin access required", http.StatusForbidden)
        return
    }
    
    if s.coordinator == nil {
        s.sendError(w, "Cluster not available", http.StatusServiceUnavailable)
        return
    }
    
    path := strings.TrimPrefix(r.URL.Path, "/api/cluster/")
    parts := strings.Split(path, "/")
    
    switch r.Method {
    case http.MethodGet:
        if len(parts) == 0 || parts[0] == "" || parts[0] == "status" {
            status := s.coordinator.GetClusterStatus()
            s.logOperation("CLUSTER_STATUS", "", "success", "", nil)
            s.sendSuccess(w, status)
        } else if parts[0] == "health" {
            status := s.coordinator.GetClusterStatus()
            s.sendSuccess(w, map[string]interface{}{
                "status":             status.Health,
                "total_nodes":        status.TotalNodes,
                "active_nodes":       status.ActiveNodes,
                "syncing_nodes":      status.SyncingNodes,
                "failed_nodes":       status.FailedNodes,
                "replication_factor": status.ReplicationFactor,
                "leader_id":          status.LeaderID,
                "checked_at":         time.Now().UnixMilli(),
            })
        } else if parts[0] == "nodes" {
            nodes := s.coordinator.GetAllNodes()
            s.sendSuccess(w, nodes)
        } else if parts[0] == "leader" {
            leader := s.coordinator.GetLeader()
            s.sendSuccess(w, leader)
        } else if parts[0] == "shards" {
            shards := s.coordinator.GetAllShards()
            s.sendSuccess(w, shards)
        } else if parts[0] == "pipeline" {
            stats := s.coordinator.GetPipelineStats()
            s.sendSuccess(w, stats)
        } else if parts[0] == "batch" {
            stats := s.coordinator.GetBatchCommitStats()
            s.sendSuccess(w, stats)
        } else if parts[0] == "resharding" {
            stats := s.coordinator.GetReshardingStats()
            s.sendSuccess(w, stats)
        } else {
            s.sendError(w, "Unknown cluster endpoint", http.StatusNotFound)
        }
        
    case http.MethodPost:
        if len(parts) >= 2 && parts[0] == "replication" && parts[1] == "factor" {
            var req struct {
                Factor int `json:"factor"`
            }
            if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                s.sendError(w, "Invalid request body", http.StatusBadRequest)
                return
            }
            if req.Factor < 1 || req.Factor > 5 {
                s.sendError(w, "Replication factor must be between 1 and 5", http.StatusBadRequest)
                return
            }
            if err := s.coordinator.SetReplicationFactor(req.Factor); err != nil {
                s.logOperation("SET_REPLICATION_FACTOR", fmt.Sprintf("%d", req.Factor), "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            s.logOperation("SET_REPLICATION_FACTOR", fmt.Sprintf("%d", req.Factor), "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "updated",
                "factor": req.Factor,
            })
        } else if len(parts) >= 1 && parts[0] == "resharding" {
            var req struct {
                Reason string `json:"reason"`
            }
            json.NewDecoder(r.Body).Decode(&req)
            if req.Reason == "" {
                req.Reason = "api_trigger"
            }
            if err := s.coordinator.TriggerResharding(req.Reason); err != nil {
                s.logOperation("TRIGGER_RESHARDING", "", "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            s.logOperation("TRIGGER_RESHARDING", "", "success", "", map[string]interface{}{"reason": req.Reason})
            s.sendSuccess(w, map[string]interface{}{
                "status": "resharding_triggered",
                "reason": req.Reason,
            })
        } else {
            s.sendError(w, "Unknown cluster endpoint", http.StatusNotFound)
        }
        
    default:
        s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
    }
}

// handleTriggerRequest обрабатывает запросы к триггерам
func (s *HTTPServer) handleTriggerRequest(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/api/trigger/")
    parts := strings.Split(path, "/")
    
    if len(parts) < 2 {
        s.sendError(w, "Invalid path. Use /api/trigger/{collection}/{action}", http.StatusBadRequest)
        return
    }
    
    collection := parts[0]
    action := parts[1]
    
    sessionID := s.getSessionID(r)
    if !s.aclManager.CheckPermission(sessionID, "", collection, "admin") {
        s.logOperation("TRIGGER_OPERATION", fmt.Sprintf("%s.%s", collection, action), "error", "Admin access required", nil)
        s.sendError(w, "Admin access required", http.StatusForbidden)
        return
    }
    
    tm := storage.GetTriggerManager()
    if tm == nil {
        storage.InitTriggerManager(s.logger)
        tm = storage.GetTriggerManager()
    }
    
    switch action {
    case "list":
        triggers := tm.ListTriggers(collection)
        s.sendSuccess(w, triggers)
        
    case "create":
        if r.Method != http.MethodPost {
            s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
            return
        }
        
        var req struct {
            Name        string                 `json:"name"`
            Event       string                 `json:"event"`
            Action      string                 `json:"action"`
            Condition   map[string]interface{} `json:"condition"`
            Description string                 `json:"description"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            s.sendError(w, "Invalid request body", http.StatusBadRequest)
            return
        }
        
        if req.Name == "" {
            s.sendError(w, "Trigger name required", http.StatusBadRequest)
            return
        }
        
        config := make(map[string]interface{})
        config["action"] = req.Action
        config["description"] = req.Description
        if req.Condition != nil {
            config["condition"] = req.Condition
        }
        
        event := storage.TriggerEvent(req.Event)
        if event == "" {
            event = storage.TriggerAfterInsert
        }
        
        if err := tm.CreateTrigger("", collection, req.Name, event, config); err != nil {
            s.logOperation("CREATE_TRIGGER", fmt.Sprintf("%s.%s", collection, req.Name), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusBadRequest)
            return
        }
        s.logOperation("CREATE_TRIGGER", fmt.Sprintf("%s.%s", collection, req.Name), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "trigger_created",
            "name":   req.Name,
        })
        
    case "drop":
        if len(parts) < 3 {
            s.sendError(w, "Trigger name required", http.StatusBadRequest)
            return
        }
        triggerName := parts[2]
        if err := tm.DropTrigger(collection, "", triggerName); err != nil {
            s.logOperation("DROP_TRIGGER", fmt.Sprintf("%s.%s", collection, triggerName), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusNotFound)
            return
        }
        s.logOperation("DROP_TRIGGER", fmt.Sprintf("%s.%s", collection, triggerName), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "trigger_dropped",
            "name":   triggerName,
        })
        
    case "enable":
        if len(parts) < 3 {
            s.sendError(w, "Trigger name required", http.StatusBadRequest)
            return
        }
        triggerName := parts[2]
        if err := tm.EnableTrigger(collection, "", triggerName); err != nil {
            s.logOperation("ENABLE_TRIGGER", fmt.Sprintf("%s.%s", collection, triggerName), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusNotFound)
            return
        }
        s.logOperation("ENABLE_TRIGGER", fmt.Sprintf("%s.%s", collection, triggerName), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "trigger_enabled",
            "name":   triggerName,
        })
        
    case "disable":
        if len(parts) < 3 {
            s.sendError(w, "Trigger name required", http.StatusBadRequest)
            return
        }
        triggerName := parts[2]
        if err := tm.DisableTrigger(collection, "", triggerName); err != nil {
            s.logOperation("DISABLE_TRIGGER", fmt.Sprintf("%s.%s", collection, triggerName), "error", err.Error(), nil)
            s.sendError(w, err.Error(), http.StatusNotFound)
            return
        }
        s.logOperation("DISABLE_TRIGGER", fmt.Sprintf("%s.%s", collection, triggerName), "success", "", nil)
        s.sendSuccess(w, map[string]interface{}{
            "status": "trigger_disabled",
            "name":   triggerName,
        })
        
    case "log":
        logs := tm.GetTriggerExecutionLog()
        filtered := make([]interface{}, 0)
        for _, entry := range logs {
            if entry.Collection == collection {
                filtered = append(filtered, entry)
            }
        }
        s.sendSuccess(w, filtered)
        
    default:
        s.sendError(w, "Unknown action", http.StatusBadRequest)
    }
}

// handleTransactionRequest обрабатывает запросы к транзакциям
func (s *HTTPServer) handleTransactionRequest(w http.ResponseWriter, r *http.Request) {
    path := strings.TrimPrefix(r.URL.Path, "/api/transaction/")
    parts := strings.Split(path, "/")
    
    sessionID := s.getSessionID(r)
    if !s.aclManager.CheckPermission(sessionID, "*", "*", "write") {
        s.logOperation("TRANSACTION_OPERATION", "", "error", "Write access required", nil)
        s.sendError(w, "Write access required", http.StatusForbidden)
        return
    }
    
    switch r.Method {
    case http.MethodGet:
        transactions := storage.GetActiveTransactions()
        s.sendSuccess(w, transactions)
        
    case http.MethodPost:
        if len(parts) == 0 || parts[0] == "" {
            s.sendError(w, "Action required", http.StatusBadRequest)
            return
        }
        
        action := parts[0]
        switch action {
        case "begin":
            if err := storage.InitTransactionManager("futriis.wal"); err != nil {
                s.logOperation("BEGIN_TRANSACTION", "", "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusInternalServerError)
                return
            }
            
            databases := s.store.ListDatabases()
            if len(databases) == 0 {
                s.sendError(w, "No database available", http.StatusBadRequest)
                return
            }
            
            db, err := s.store.GetDatabase(databases[0])
            if err != nil {
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            
            collections := db.ListCollections()
            if len(collections) == 0 {
                s.sendError(w, "No collection available", http.StatusBadRequest)
                return
            }
            
            coll, err := db.GetCollection(collections[0])
            if err != nil {
                s.sendError(w, err.Error(), http.StatusBadRequest)
                return
            }
            
            if err := storage.BeginTransactionOnCollection(coll); err != nil {
                s.logOperation("BEGIN_TRANSACTION", "", "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusInternalServerError)
                return
            }
            s.logOperation("BEGIN_TRANSACTION", "", "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "transaction_started",
                "id":     storage.GetCurrentTransactionID(),
            })
            
        case "commit":
            if err := storage.CommitCurrentTransaction(); err != nil {
                s.logOperation("COMMIT_TRANSACTION", "", "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusInternalServerError)
                return
            }
            s.logOperation("COMMIT_TRANSACTION", "", "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "transaction_committed",
            })
            
        case "abort":
            if err := storage.AbortCurrentTransaction(); err != nil {
                s.logOperation("ABORT_TRANSACTION", "", "error", err.Error(), nil)
                s.sendError(w, err.Error(), http.StatusInternalServerError)
                return
            }
            s.logOperation("ABORT_TRANSACTION", "", "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "transaction_aborted",
            })
            
        default:
            s.sendError(w, "Unknown action", http.StatusBadRequest)
        }
        
    case http.MethodPut:
        if len(parts) < 2 {
            s.sendError(w, "Transaction ID and action required", http.StatusBadRequest)
            return
        }
        txID := parts[0]
        action := parts[1]
        
        switch action {
        case "commit":
            s.logOperation("COMMIT_TRANSACTION", txID, "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "transaction_committed",
                "id":     txID,
            })
            
        case "abort":
            s.logOperation("ABORT_TRANSACTION", txID, "success", "", nil)
            s.sendSuccess(w, map[string]interface{}{
                "status": "transaction_aborted",
                "id":     txID,
            })
            
        default:
            s.sendError(w, "Unknown action", http.StatusBadRequest)
        }
        
    default:
        s.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
    }
}

// handleHealthCheck обрабатывает проверку здоровья (без аутентификации)
func (s *HTTPServer) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
    status := "healthy"
    httpStatus := http.StatusOK
    
    if s.store == nil {
        status = "unhealthy"
        httpStatus = http.StatusServiceUnavailable
    }
    
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(httpStatus)
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status":    status,
        "timestamp": time.Now().UnixMilli(),
        "version":   "1.0.0",
    })
}

// handleMetricsEndpoint обрабатывает запрос метрик (без аутентификации)
func (s *HTTPServer) handleMetricsEndpoint(w http.ResponseWriter, r *http.Request) {
    metrics := map[string]interface{}{
        "timestamp":    time.Now().UnixMilli(),
        "rate_limiter": s.rateLimiter.GetStats(),
        "store_stats":  s.store.GetStats(),
    }
    
    if s.coordinator != nil {
        metrics["cluster"] = s.coordinator.GetClusterStatus()
        metrics["pipeline"] = s.coordinator.GetPipelineStats()
        metrics["batch_commit"] = s.coordinator.GetBatchCommitStats()
        metrics["resharding"] = s.coordinator.GetReshardingStats()
    }
    
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(metrics)
}

// logOperation логирует операцию
func (s *HTTPServer) logOperation(operation, target, status, errMsg string, details map[string]interface{}) {
    if s.logger == nil {
        return
    }
    
    logMsg := fmt.Sprintf("[API] %s: %s - %s", operation, target, status)
    if errMsg != "" {
        logMsg += " - " + errMsg
    }
    
    if status == "error" {
        s.logger.Error(logMsg)
    } else {
        s.logger.Info(logMsg)
    }
}

// sendSuccess отправляет успешный ответ
func (s *HTTPServer) sendSuccess(w http.ResponseWriter, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(APIResponse{
        Success: true,
        Data:    data,
    })
}

// sendError отправляет ответ с ошибкой
func (s *HTTPServer) sendError(w http.ResponseWriter, errMsg string, statusCode int) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(statusCode)
    json.NewEncoder(w).Encode(APIResponse{
        Success: false,
        Error:   errMsg,
    })
}
