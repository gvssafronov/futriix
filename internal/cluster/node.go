/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/node.go
// Назначение: Реализация узла кластера (node) для распределённой СУБД с поддержкой временных меток.

package cluster

import (
    "crypto/rand"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "io"
    "net"
    "runtime/debug"
    "sync"
    "sync/atomic"
    "time"
    
    "futriis/internal/log"
    "futriis/internal/storage"
    "github.com/google/uuid"
)

// =============================================================================
// NETWORK REPLICATOR - ЗАГЛУШКА
// =============================================================================

type NetworkReplicator struct{}

func NewNetworkReplicator(config *ReplicationRetryConfig, workerPool *WorkerPool, logger *log.Logger) *NetworkReplicator {
    return &NetworkReplicator{}
}

type ReplicationRetryConfig struct {
    MaxRetries     int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
    BackoffFactor  float64
    JitterEnabled  bool
}

func DefaultReplicationRetryConfig() *ReplicationRetryConfig {
    return &ReplicationRetryConfig{
        MaxRetries:     5,
        InitialBackoff: 100 * time.Millisecond,
        MaxBackoff:     10 * time.Second,
        BackoffFactor:  2.0,
        JitterEnabled:  true,
    }
}

func (nr *NetworkReplicator) Close() error {
    return nil
}

func (nr *NetworkReplicator) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "enabled": false,
        "message": "NetworkReplicator is a stub - full implementation pending",
    }
}

func (nr *NetworkReplicator) Replicate(targetNodeID, targetAddress string, data []byte) error {
    return nil
}

// =============================================================================
// ОСНОВНЫЕ ТИПЫ
// =============================================================================

// LoggerInterface определяет интерфейс для логирования
type LoggerInterface interface {
    Debug(msg string)
    Info(msg string)
    Warn(msg string)
    Error(msg string)
    Debugf(format string, args ...interface{})
    Infof(format string, args ...interface{})
    Warnf(format string, args ...interface{})
    Errorf(format string, args ...interface{})
}

// NodeStatus представляет состояние узла кластера
type NodeStatus int32

const (
    StatusOffline NodeStatus = iota
    StatusActive
    StatusSyncing
    StatusFailed
)

// NodeRequest представляет запрос между узлами
type NodeRequest struct {
    Type      string          `json:"type"`
    FromNode  string          `json:"from_node"`
    Data      json.RawMessage `json:"data"`
    Timestamp int64           `json:"timestamp"`
}

// NodeInfo представляет информацию об узле в кластере
type NodeInfo struct {
    ID        string `json:"id"`
    IP        string `json:"ip"`
    Port      int    `json:"port"`
    Status    string `json:"status"`
    LastSeen  int64  `json:"last_seen"`
    JoinedAt  int64  `json:"joined_at"`
    UpdatedAt int64  `json:"updated_at"`
    Version   int    `json:"version"`
}

// ShardInfo представляет информацию о шарде
type ShardInfo struct {
    ID             string   `json:"id"`
    Name           string   `json:"name"`
    Nodes          []string `json:"nodes"`
    LeaderNode     string   `json:"leader_node"`
    Status         string   `json:"status"`
    CreatedAt      int64    `json:"created_at"`
    UpdatedAt      int64    `json:"updated_at"`
    LastRebalanced int64    `json:"last_rebalanced"`
    DocumentCount  int64    `json:"document_count"`
    SizeBytes      int64    `json:"size_bytes"`
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

func SafeGoWithLogger(fn func(), logger *log.Logger, name string) {
    go func() {
        defer func() {
            if r := recover(); r != nil {
                if logger != nil {
                    logger.Error(fmt.Sprintf("Goroutine %s panicked: %v\n%s", name, r, debug.Stack()))
                }
                time.Sleep(5 * time.Second)
                SafeGoWithLogger(fn, logger, name)
            }
        }()
        fn()
    }()
}

// =============================================================================
// NODE - ОСНОВНАЯ СТРУКТУРА УЗЛА
// =============================================================================

type Node struct {
    ID           string
    IP           string
    Port         int
    Status       atomic.Int32
    Storage      *storage.Storage
    logger       *log.Logger
    coordinator  *RaftCoordinator
    lastSeen     atomic.Int64
    joinedAt     atomic.Int64
    createdAt    int64
    startedAt    int64
    stoppedAt    int64
    incomingConn chan net.Conn
    stopChan     chan struct{}
    requestCount atomic.Uint64
    bytesRx      atomic.Uint64
    bytesTx      atomic.Uint64
    mu           sync.RWMutex
    
    workerPool   *WorkerPool
    replicator   *NetworkReplicator
    connPool     sync.Map
    
    panicRecoveryMgr *PanicRecoveryManager
    recoverableRoutines map[string]*RecoverableRoutine
    routinesMu         sync.RWMutex
}

type NodeConfig struct {
    IP          string
    Port        int
    Storage     *storage.Storage
    Logger      *log.Logger
    Coordinator *RaftCoordinator
    PanicRecoveryMgr *PanicRecoveryManager
}

// =============================================================================
// СОЗДАНИЕ УЗЛА
// =============================================================================

func NewNode(ip string, port int, store *storage.Storage, logger *log.Logger) *Node {
    return NewNodeWithRecovery(ip, port, store, logger, nil)
}

func NewNodeWithRecovery(ip string, port int, store *storage.Storage, logger *log.Logger, panicRecoveryMgr *PanicRecoveryManager) *Node {
    now := time.Now().UnixMilli()
    
    workerPool := NewWorkerPool(500, logger)
    replicator := NewNetworkReplicator(DefaultReplicationRetryConfig(), workerPool, logger)
    
    node := &Node{
        ID:           uuid.New().String(),
        IP:           ip,
        Port:         port,
        Storage:      store,
        logger:       logger,
        incomingConn: make(chan net.Conn, 10000),
        stopChan:     make(chan struct{}),
        createdAt:    now,
        startedAt:    now,
        workerPool:   workerPool,
        replicator:   replicator,
        panicRecoveryMgr: panicRecoveryMgr,
        recoverableRoutines: make(map[string]*RecoverableRoutine),
    }
    node.Status.Store(int32(StatusActive))
    node.lastSeen.Store(now)
    node.joinedAt.Store(0)
    
    if panicRecoveryMgr != nil {
        node.startRecoverableRoutine("TCPServer", node.startTCPServer)
        node.startRecoverableRoutine("IncomingConnections", node.handleIncomingConnections)
        node.startRecoverableRoutine("HeartbeatLoop", node.heartbeatLoop)
        node.startRecoverableRoutine("ConnectionHealthMonitor", node.connectionHealthMonitor)
        logger.Info(fmt.Sprintf("Node %s created with panic recovery (max workers: 500)", node.ID))
    } else {
        SafeGoWithLogger(node.startTCPServer, logger, "TCPServer")
        SafeGoWithLogger(node.handleIncomingConnections, logger, "IncomingConnections")
        SafeGoWithLogger(node.heartbeatLoop, logger, "HeartbeatLoop")
        SafeGoWithLogger(node.connectionHealthMonitor, logger, "ConnectionHealthMonitor")
        logger.Info(fmt.Sprintf("Node %s created at %s with lock-free worker pool (max: 500)", node.ID, node.GetCreatedAtStr()))
    }
    
    return node
}

// =============================================================================
// ВОССТАНАВЛИВАЕМЫЕ ГОРУТИНЫ
// =============================================================================

func (n *Node) startRecoverableRoutine(name string, fn func()) {
    if n.panicRecoveryMgr == nil {
        SafeGoWithLogger(fn, n.logger, name)
        return
    }
    
    n.routinesMu.Lock()
    defer n.routinesMu.Unlock()
    
    routine := NewRecoverableRoutine(name, func() error {
        fn()
        return nil
    }, n.panicRecoveryMgr, 10)
    
    n.recoverableRoutines[name] = routine
    routine.Start()
    
    if n.logger != nil {
        n.logger.Debug(fmt.Sprintf("Started recoverable routine: %s", name))
    }
}

func (n *Node) stopRecoverableRoutine(name string) {
    n.routinesMu.RLock()
    routine, ok := n.recoverableRoutines[name]
    n.routinesMu.RUnlock()
    
    if ok && routine != nil {
        routine.Stop()
        if n.logger != nil {
            n.logger.Debug(fmt.Sprintf("Stopped recoverable routine: %s", name))
        }
    }
}

// =============================================================================
// TCP СЕРВЕР
// =============================================================================

func (n *Node) startTCPServer() {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("TCP server panicked: %v\n%s", r, debug.Stack()))
            }
            time.Sleep(5 * time.Second)
            if n.panicRecoveryMgr != nil {
                n.startRecoverableRoutine("TCPServer", n.startTCPServer)
            } else {
                SafeGoWithLogger(n.startTCPServer, n.logger, "TCPServer")
            }
        }
    }()
    
    addr := fmt.Sprintf("%s:%d", n.IP, n.Port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to start TCP server: %v", n.ID, err))
        }
        n.Status.Store(int32(StatusFailed))
        return
    }
    defer listener.Close()
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s listening on %s (started at %s)", n.ID, addr, n.GetStartedAtStr()))
    }
    
    for {
        select {
        case <-n.stopChan:
            if n.logger != nil {
                n.logger.Info(fmt.Sprintf("Node %s TCP server stopped", n.ID))
            }
            return
        default:
            if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
                continue
            }
            
            conn, err := listener.Accept()
            if err != nil {
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    continue
                }
                if n.logger != nil {
                    n.logger.Error(fmt.Sprintf("Node %s accept error: %v", n.ID, err))
                }
                continue
            }
            
            conn.SetReadDeadline(time.Now().Add(30 * time.Second))
            conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
            
            select {
            case n.incomingConn <- conn:
                n.bytesRx.Add(1)
            default:
                if n.logger != nil {
                    n.logger.Warn(fmt.Sprintf("Node %s incoming connection queue full, dropping connection", n.ID))
                }
                conn.Close()
            }
        }
    }
}

// =============================================================================
// ОБРАБОТКА ВХОДЯЩИХ СОЕДИНЕНИЙ
// =============================================================================

func (n *Node) handleIncomingConnections() {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Incoming connections handler panicked: %v\n%s", r, debug.Stack()))
            }
            time.Sleep(5 * time.Second)
            if n.panicRecoveryMgr != nil {
                n.startRecoverableRoutine("IncomingConnections", n.handleIncomingConnections)
            } else {
                SafeGoWithLogger(n.handleIncomingConnections, n.logger, "IncomingConnections")
            }
        }
    }()
    
    for {
        select {
        case <-n.stopChan:
            return
        case conn := <-n.incomingConn:
            n.requestCount.Add(1)
            taskID := fmt.Sprintf("handle_conn_%d_%s", time.Now().UnixNano(), conn.RemoteAddr().String())
            err := n.workerPool.SubmitFunc(taskID, func() error {
                n.handleNodeRequest(conn)
                return nil
            })
            if err != nil {
                if n.logger != nil {
                    n.logger.Warn(fmt.Sprintf("Failed to submit connection task: %v", err))
                }
                conn.Close()
            }
        }
    }
}

// =============================================================================
// ОБРАБОТКА ЗАПРОСОВ
// =============================================================================

func (n *Node) handleNodeRequest(conn net.Conn) {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Request handler panicked: %v\n%s", r, debug.Stack()))
            }
        }
        conn.Close()
    }()
    
    conn.SetReadDeadline(time.Now().Add(30 * time.Second))
    
    decoder := json.NewDecoder(conn)
    var req NodeRequest
    if err := decoder.Decode(&req); err != nil {
        if err != io.EOF && n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to decode request: %v", n.ID, err))
        }
        return
    }
    
    n.lastSeen.Store(time.Now().UnixMilli())
    
    if n.logger != nil {
        n.logger.Debug(fmt.Sprintf("Node %s received request type %s from %s at %s", 
            n.ID, req.Type, req.FromNode, time.UnixMilli(req.Timestamp).Format("15:04:05.000")))
    }
    
    switch req.Type {
    case "replicate":
        n.handleReplicateRequest(req.Data)
    case "query":
        n.handleQueryRequest(req.Data, conn)
    case "sync":
        n.handleSyncRequest(req.Data, conn)
    case "heartbeat":
        n.handleHeartbeatRequest(req, conn)
    case "status_sync":
        n.handleStatusSyncRequest(req, conn)
    default:
        if n.logger != nil {
            n.logger.Warn(fmt.Sprintf("Node %s unknown request type: %s", n.ID, req.Type))
        }
    }
}

func (n *Node) handleReplicateRequest(data []byte) {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Replicate request handler panicked: %v\n%s", r, debug.Stack()))
            }
        }
    }()
    
    startTime := time.Now().UnixMilli()
    
    var repData struct {
        Database   string                 `json:"database"`
        Collection string                 `json:"collection"`
        Document   map[string]interface{} `json:"document"`
        SourceNode string                 `json:"source_node"`
        ReplicaID  string                 `json:"replica_id"`
    }
    
    if err := json.Unmarshal(data, &repData); err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to unmarshal replicate data: %v", n.ID, err))
        }
        return
    }
    
    db, err := n.Storage.GetDatabase(repData.Database)
    if err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s database not found for replication: %s", n.ID, repData.Database))
        }
        return
    }
    
    coll, err := db.GetCollection(repData.Collection)
    if err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s collection not found for replication: %s", n.ID, repData.Collection))
        }
        return
    }
    
    docID, ok := repData.Document["_id"].(string)
    if !ok {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s document missing _id field", n.ID))
        }
        return
    }
    
    doc := storage.NewDocumentWithID(docID)
    for k, v := range repData.Document {
        doc.SetField(k, v)
    }
    
    if err := coll.Insert(doc); err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to replicate document: %v", n.ID, err))
        }
    } else {
        duration := time.Now().UnixMilli() - startTime
        if n.logger != nil {
            n.logger.Debug(fmt.Sprintf("Node %s replicated document %s from %s (took %d ms)", 
                n.ID, doc.ID, repData.SourceNode, duration))
        }
    }
}

func (n *Node) handleQueryRequest(data []byte, conn net.Conn) {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Query request handler panicked: %v\n%s", r, debug.Stack()))
            }
            n.sendErrorResponse(conn, "Internal server error")
        }
    }()
    
    startTime := time.Now().UnixMilli()
    
    var queryData struct {
        Database   string `json:"database"`
        Collection string `json:"collection"`
        DocumentID string `json:"document_id"`
        RequestID  string `json:"request_id"`
    }
    
    if err := json.Unmarshal(data, &queryData); err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    db, err := n.Storage.GetDatabase(queryData.Database)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    coll, err := db.GetCollection(queryData.Collection)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    doc, err := coll.Find(queryData.DocumentID)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    duration := time.Now().UnixMilli() - startTime
    
    response := map[string]interface{}{
        "status":      "success",
        "data":        doc,
        "node_id":     n.ID,
        "request_id":  queryData.RequestID,
        "duration_ms": duration,
        "timestamp":   time.Now().UnixMilli(),
    }
    
    conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err == nil {
        responseData, _ := json.Marshal(response)
        n.bytesTx.Add(uint64(len(responseData)))
    }
    
    if n.logger != nil {
        n.logger.Debug(fmt.Sprintf("Node %s handled query for %s.%s:%s (took %d ms)", 
            n.ID, queryData.Database, queryData.Collection, queryData.DocumentID, duration))
    }
}

func (n *Node) handleSyncRequest(data []byte, conn net.Conn) {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Sync request handler panicked: %v\n%s", r, debug.Stack()))
            }
            n.sendErrorResponse(conn, "Internal server error")
        }
    }()
    
    startTime := time.Now().UnixMilli()
    
    var syncData struct {
        Database   string `json:"database"`
        Collection string `json:"collection"`
        RequestID  string `json:"request_id"`
        Since      int64  `json:"since"`
    }
    
    if err := json.Unmarshal(data, &syncData); err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    db, err := n.Storage.GetDatabase(syncData.Database)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    coll, err := db.GetCollection(syncData.Collection)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    docs := coll.GetAllDocuments()
    
    if syncData.Since > 0 {
        filtered := make([]*storage.Document, 0)
        for _, doc := range docs {
            if doc.UpdatedAt > syncData.Since {
                filtered = append(filtered, doc)
            }
        }
        docs = filtered
    }
    
    duration := time.Now().UnixMilli() - startTime
    
    response := map[string]interface{}{
        "status":           "success",
        "docs":             docs,
        "count":            len(docs),
        "node_id":          n.ID,
        "request_id":       syncData.RequestID,
        "duration_ms":      duration,
        "timestamp":        time.Now().UnixMilli(),
        "sync_duration_ms": duration,
    }
    
    conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
    encoder := json.NewEncoder(conn)
    if err := encoder.Encode(response); err != nil && n.logger != nil {
        n.logger.Error(fmt.Sprintf("Failed to send sync response: %v", err))
    }
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s synced %d documents from %s.%s (took %d ms)", 
            n.ID, len(docs), syncData.Database, syncData.Collection, duration))
    }
}

func (n *Node) handleHeartbeatRequest(req NodeRequest, conn net.Conn) {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Heartbeat handler panicked: %v\n%s", r, debug.Stack()))
            }
        }
    }()
    
    n.lastSeen.Store(time.Now().UnixMilli())
    
    response := map[string]interface{}{
        "status":     "alive",
        "node_id":    n.ID,
        "timestamp":  time.Now().UnixMilli(),
        "uptime_ms":  time.Now().UnixMilli() - n.startedAt,
    }
    
    conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
    encoder := json.NewEncoder(conn)
    encoder.Encode(response)
}

func (n *Node) handleStatusSyncRequest(req NodeRequest, conn net.Conn) {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Status sync handler panicked: %v\n%s", r, debug.Stack()))
            }
            n.sendErrorResponse(conn, "Internal server error")
        }
    }()
    
    var syncStatus struct {
        LeaderID    string `json:"leader_id"`
        Term        uint64 `json:"term"`
        ClusterSize int    `json:"cluster_size"`
    }
    
    if err := json.Unmarshal(req.Data, &syncStatus); err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    if n.coordinator != nil {
        n.coordinator.HandleStatusSync(syncStatus.LeaderID, syncStatus.Term, syncStatus.ClusterSize)
    }
    
    response := map[string]interface{}{
        "status":    "synced",
        "node_id":   n.ID,
        "term":      n.coordinator.GetCurrentTerm(),
        "is_leader": n.coordinator.IsLeader(),
        "timestamp": time.Now().UnixMilli(),
    }
    
    conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
    encoder := json.NewEncoder(conn)
    encoder.Encode(response)
}

func (n *Node) sendErrorResponse(conn net.Conn, errMsg string) {
    response := map[string]interface{}{
        "status":    "error",
        "error":     errMsg,
        "node_id":   n.ID,
        "timestamp": time.Now().UnixMilli(),
    }
    
    conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
    encoder := json.NewEncoder(conn)
    encoder.Encode(response)
}

// =============================================================================
// HEARTBEAT И МОНИТОРИНГ
// =============================================================================

func (n *Node) heartbeatLoop() {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Heartbeat loop panicked: %v\n%s", r, debug.Stack()))
            }
            time.Sleep(5 * time.Second)
            if n.panicRecoveryMgr != nil {
                n.startRecoverableRoutine("HeartbeatLoop", n.heartbeatLoop)
            } else {
                SafeGoWithLogger(n.heartbeatLoop, n.logger, "HeartbeatLoop")
            }
        }
    }()
    
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-n.stopChan:
            return
        case <-ticker.C:
            if n.coordinator != nil {
                n.coordinator.SendHeartbeat(n.ID)
                n.lastSeen.Store(time.Now().UnixMilli())
                if n.logger != nil {
                    n.logger.Debug(fmt.Sprintf("Node %s sent heartbeat at %s", n.ID, n.GetLastSeenStr()))
                }
            }
        }
    }
}

func (n *Node) connectionHealthMonitor() {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Connection health monitor panicked: %v\n%s", r, debug.Stack()))
            }
            SafeGoWithLogger(n.connectionHealthMonitor, n.logger, "ConnectionHealthMonitor")
        }
    }()
    
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-n.stopChan:
            return
        case <-ticker.C:
            n.cleanupStaleConnections()
        }
    }
}

func (n *Node) cleanupStaleConnections() {
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Cleanup connections panicked: %v", r))
            }
        }
    }()
    
    n.connPool.Range(func(key, value interface{}) bool {
        if conn, ok := value.(net.Conn); ok {
            conn.SetReadDeadline(time.Now().Add(1 * time.Second))
            buf := make([]byte, 1)
            _, err := conn.Read(buf)
            if err != nil {
                conn.Close()
                n.connPool.Delete(key)
                if n.logger != nil {
                    n.logger.Debug(fmt.Sprintf("Cleaned up stale connection for %v", key))
                }
            }
        }
        return true
    })
}

// =============================================================================
// УПРАВЛЕНИЕ СТАТУСОМ УЗЛА
// =============================================================================

func (n *Node) GetNodeStatus() NodeStatus {
    return NodeStatus(n.Status.Load())
}

func (n *Node) IsActive() bool {
    return NodeStatus(n.Status.Load()) == StatusActive
}

func (n *Node) SetStatus(status NodeStatus) error {
    n.mu.Lock()
    defer n.mu.Unlock()
    
    oldStatus := n.Status.Load()
    if oldStatus == int32(status) {
        return nil
    }
    
    if n.coordinator != nil && n.coordinator.IsLeader() {
        if err := n.coordinator.UpdateNodeStatus(n.ID, status); err != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Failed to update node status via Raft: %v", err))
            }
            return err
        }
    }
    
    n.Status.Store(int32(status))
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s status changed from %d to %d at %s", 
            n.ID, oldStatus, status, time.Now().Format("2006-01-02 15:04:05.000")))
    }
    
    return nil
}

// =============================================================================
// УПРАВЛЕНИЕ КЛАСТЕРОМ
// =============================================================================

func (n *Node) SetCoordinator(coord *RaftCoordinator) {
    n.coordinator = coord
    now := time.Now().UnixMilli()
    n.joinedAt.Store(now)
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s joined cluster at %s", n.ID, n.GetJoinedAtStr()))
    }
}

func (n *Node) JoinCluster(coord *RaftCoordinator) error {
    if n.coordinator != nil {
        return fmt.Errorf("node already joined to cluster")
    }
    
    n.SetCoordinator(coord)
    
    if err := coord.RegisterNode(n); err != nil {
        return fmt.Errorf("failed to register node: %v", err)
    }
    
    if err := n.SetStatus(StatusActive); err != nil {
        return fmt.Errorf("failed to set active status: %v", err)
    }
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s successfully joined cluster at %s", n.ID, n.GetJoinedAtStr()))
    }
    
    return nil
}

func (n *Node) LeaveCluster() error {
    if n.coordinator == nil {
        return fmt.Errorf("node not in cluster")
    }
    
    if err := n.SetStatus(StatusOffline); err != nil {
        n.logger.Warn(fmt.Sprintf("Failed to set offline status: %v", err))
    }
    
    if err := n.coordinator.RemoveNode(n.ID); err != nil {
        n.logger.Warn(fmt.Sprintf("Failed to remove node from coordinator: %v", err))
    }
    
    n.coordinator = nil
    n.joinedAt.Store(0)
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s left cluster at %s", n.ID, time.Now().Format("2006-01-02 15:04:05.000")))
    }
    
    return nil
}

// =============================================================================
// ИНФОРМАЦИЯ О ВРЕМЕНИ
// =============================================================================

func (n *Node) GetLastSeen() int64 {
    return n.lastSeen.Load()
}

func (n *Node) GetLastSeenStr() string {
    lastSeen := n.lastSeen.Load()
    if lastSeen == 0 {
        return "never"
    }
    return time.UnixMilli(lastSeen).Format("2006-01-02 15:04:05.000")
}

func (n *Node) GetJoinedAt() int64 {
    return n.joinedAt.Load()
}

func (n *Node) GetJoinedAtStr() string {
    joinedAt := n.joinedAt.Load()
    if joinedAt == 0 {
        return "not joined"
    }
    return time.UnixMilli(joinedAt).Format("2006-01-02 15:04:05.000")
}

func (n *Node) GetStartedAt() int64 {
    return n.startedAt
}

func (n *Node) GetStartedAtStr() string {
    return time.UnixMilli(n.startedAt).Format("2006-01-02 15:04:05.000")
}

func (n *Node) GetCreatedAt() int64 {
    return n.createdAt
}

func (n *Node) GetCreatedAtStr() string {
    return time.UnixMilli(n.createdAt).Format("2006-01-02 15:04:05.000")
}

func (n *Node) GetUptime() time.Duration {
    if n.startedAt == 0 {
        return 0
    }
    return time.Duration(time.Now().UnixMilli()-n.startedAt) * time.Millisecond
}

func (n *Node) GetAddress() string {
    return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

// =============================================================================
// СТАТИСТИКА
// =============================================================================

func (n *Node) GetStats() map[string]interface{} {
    stats := map[string]interface{}{
        "id":            n.ID,
        "ip":            n.IP,
        "port":          n.Port,
        "status":        n.GetNodeStatus(),
        "created_at":    n.GetCreatedAtStr(),
        "started_at":    n.GetStartedAtStr(),
        "joined_at":     n.GetJoinedAtStr(),
        "last_seen":     n.GetLastSeenStr(),
        "uptime":        n.GetUptime().String(),
        "request_count": n.requestCount.Load(),
        "bytes_rx":      n.bytesRx.Load(),
        "bytes_tx":      n.bytesTx.Load(),
    }
    
    if n.workerPool != nil {
        stats["worker_pool"] = n.workerPool.GetStats()
    }
    
    if n.replicator != nil {
        stats["replication"] = n.replicator.GetStats()
    }
    
    if n.panicRecoveryMgr != nil {
        n.routinesMu.RLock()
        routineStatus := make(map[string]bool)
        for name, routine := range n.recoverableRoutines {
            routineStatus[name] = routine.IsRunning()
        }
        n.routinesMu.RUnlock()
        stats["recoverable_routines"] = routineStatus
        stats["panic_recovery_stats"] = n.panicRecoveryMgr.GetStats()
    }
    
    return stats
}

func (n *Node) GetWorkerPoolStats() map[string]interface{} {
    if n.workerPool == nil {
        return map[string]interface{}{"enabled": false}
    }
    return n.workerPool.GetStats()
}

func (n *Node) GetReplicationStats() map[string]interface{} {
    if n.replicator == nil {
        return map[string]interface{}{"enabled": false}
    }
    return n.replicator.GetStats()
}

func (n *Node) GetPanicRecoveryStats() map[string]interface{} {
    if n.panicRecoveryMgr == nil {
        return map[string]interface{}{"enabled": false}
    }
    return n.panicRecoveryMgr.GetStats()
}

// =============================================================================
// РЕПЛИКАЦИЯ ДОКУМЕНТОВ
// =============================================================================

func generateReplicationID() string {
    bytes := make([]byte, 16)
    rand.Read(bytes)
    return base64.URLEncoding.EncodeToString(bytes)
}

func (n *Node) ReplicateDocument(database, collection string, doc *storage.Document) error {
    if n.coordinator == nil {
        if n.logger != nil {
            n.logger.Warn("No coordinator set, skipping replication")
        }
        return fmt.Errorf("no coordinator set")
    }
    
    nodes := n.coordinator.GetActiveNodes()
    if len(nodes) <= 1 {
        if n.logger != nil {
            n.logger.Debug("No other nodes for replication")
        }
        return nil
    }
    
    repData := struct {
        Database   string                 `json:"database"`
        Collection string                 `json:"collection"`
        Document   map[string]interface{} `json:"document"`
        SourceNode string                 `json:"source_node"`
        ReplicaID  string                 `json:"replica_id"`
    }{
        Database:   database,
        Collection: collection,
        Document:   doc.GetFields(),
        SourceNode: n.ID,
        ReplicaID:  generateReplicationID(),
    }
    
    data, err := json.Marshal(repData)
    if err != nil {
        return fmt.Errorf("failed to marshal replication data: %v", err)
    }
    
    startTime := time.Now().UnixMilli()
    
    var wg sync.WaitGroup
    var failedCount atomic.Int32
    var successCount atomic.Int32
    
    for _, nodeInfo := range nodes {
        if nodeInfo.ID == n.ID {
            continue
        }
        
        wg.Add(1)
        targetNodeID := nodeInfo.ID
        targetAddress := fmt.Sprintf("%s:%d", nodeInfo.IP, nodeInfo.Port)
        
        taskID := fmt.Sprintf("replicate_%s_to_%s_%s", doc.ID, targetNodeID, repData.ReplicaID)
        err := n.workerPool.SubmitFunc(taskID, func() error {
            defer wg.Done()
            
            if n.replicator == nil {
                failedCount.Add(1)
                return fmt.Errorf("replicator not initialized")
            }
            
            err := n.replicator.Replicate(targetNodeID, targetAddress, data)
            if err != nil {
                failedCount.Add(1)
                if n.logger != nil {
                    n.logger.Error(fmt.Sprintf("Failed to replicate document %s to node %s after retries: %v", 
                        doc.ID, targetNodeID, err))
                }
                return err
            }
            
            successCount.Add(1)
            if n.logger != nil {
                n.logger.Debug(fmt.Sprintf("Successfully replicated document %s to node %s", doc.ID, targetNodeID))
            }
            return nil
        })
        
        if err != nil {
            wg.Done()
            failedCount.Add(1)
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Failed to submit replication task for %s to %s: %v", 
                    doc.ID, targetNodeID, err))
            }
        }
    }
    
    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        duration := time.Now().UnixMilli() - startTime
        if n.logger != nil {
            n.logger.Info(fmt.Sprintf("Replicated document %s to %d/%d nodes (took %d ms)", 
                doc.ID, successCount.Load(), len(nodes)-1, duration))
        }
    case <-time.After(30 * time.Second):
        if n.logger != nil {
            n.logger.Warn(fmt.Sprintf("Replication timeout for document %s after %d ms", doc.ID, time.Now().UnixMilli()-startTime))
        }
        return fmt.Errorf("replication timeout")
    }
    
    if failedCount.Load() > 0 {
        return fmt.Errorf("replication partially failed: %d of %d nodes failed", failedCount.Load(), len(nodes)-1)
    }
    
    return nil
}

// =============================================================================
// УПРАВЛЕНИЕ PANIC RECOVERY
// =============================================================================

func (n *Node) SetPanicRecoveryManager(mgr *PanicRecoveryManager) {
    n.panicRecoveryMgr = mgr
    if n.logger != nil {
        n.logger.Debug("Panic recovery manager set for node")
    }
}

// =============================================================================
// ОСТАНОВКА УЗЛА
// =============================================================================

func (n *Node) Stop() {
    if n.coordinator != nil && n.coordinator.IsLeader() {
        n.SetStatus(StatusOffline)
    }
    n.Status.Store(int32(StatusOffline))
    n.stoppedAt = time.Now().UnixMilli()
    
    n.routinesMu.RLock()
    for name, routine := range n.recoverableRoutines {
        routine.Stop()
        if n.logger != nil {
            n.logger.Debug(fmt.Sprintf("Stopped recoverable routine: %s", name))
        }
    }
    n.routinesMu.RUnlock()
    
    close(n.stopChan)
    
    if n.workerPool != nil {
        n.workerPool.Stop()
    }
    
    if n.replicator != nil {
        n.replicator.Close()
    }
    
    n.connPool.Range(func(key, value interface{}) bool {
        if conn, ok := value.(net.Conn); ok {
            conn.Close()
        }
        return true
    })
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s stopped at %s", n.ID, time.UnixMilli(n.stoppedAt).Format("2006-01-02 15:04:05.000")))
    }
}
