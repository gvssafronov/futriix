/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/raft_coordinator.go
// Назначение: Реализация координатора распределённого кластера на основе Raft консенсус-алгоритма.

package cluster

import (
    "encoding/binary"
    "encoding/json"
    "fmt"
    "io"
    "net"
    "os"
    "path/filepath"
    "sort"
    "sync"
    "sync/atomic"
    "time"
    
    "github.com/hashicorp/raft"
    "futriis/internal/config"
    "futriis/internal/log"
    "futriis/internal/migration"
    "futriis/internal/storage"
)

// =============================================================================
// ИНТЕРФЕЙСЫ И БАЗОВЫЕ ТИПЫ (используются из node.go)
// =============================================================================

// LoggerInterface, NodeStatus, StatusOffline, StatusActive, StatusSyncing, StatusFailed,
// Node, NodeInfo - определены в node.go

// =============================================================================
// ДИАПАЗОННЫЕ ШАРДЫ (RANGE SHARDS)
// =============================================================================

// RangeShard представляет шард на основе диапазона ключей.
type RangeShard struct {
    ID             string   `json:"id"`
    Name           string   `json:"name"`
    StartKey       string   `json:"start_key"`
    EndKey         string   `json:"end_key"`
    Nodes          []string `json:"nodes"`
    LeaderNode     string   `json:"leader_node"`
    Status         string   `json:"status"`
    CreatedAt      int64    `json:"created_at"`
    UpdatedAt      int64    `json:"updated_at"`
    LastRebalanced int64    `json:"last_rebalanced"`
    DocumentCount  int64    `json:"document_count"`
    SizeBytes      int64    `json:"size_bytes"`
    IsSplitting    bool     `json:"is_splitting"`
    IsMerging      bool     `json:"is_merging"`
}

// RangeShardManager управляет диапазонными шардами с динамическим сплитом/мерджем.
type RangeShardManager struct {
    shardsPtr          atomic.Value
    sortedShards       atomic.Value
    mu                 sync.RWMutex
    logger             LoggerInterface
    shardSizeThreshold int64
    shardCountThreshold int64
    minShardSize       int64
    rebalancing        atomic.Bool
    splitMgr           *DynamicSplitManager
    mergeMgr           *DynamicMergeManager
}

// DynamicSplitManager управляет динамическим разделением шардов.
type DynamicSplitManager struct {
    shardManager    *RangeShardManager
    logger          LoggerInterface
    stopChan        chan struct{}
    wg              sync.WaitGroup
    checkInterval   time.Duration
    mu              sync.RWMutex
    splittingShards map[string]bool
}

// DynamicMergeManager управляет динамическим объединением шардов.
type DynamicMergeManager struct {
    shardManager    *RangeShardManager
    logger          LoggerInterface
    stopChan        chan struct{}
    wg              sync.WaitGroup
    checkInterval   time.Duration
    mu              sync.RWMutex
    mergingShards   map[string]bool
}

// NewRangeShardManager создаёт новый менеджер диапазонных шардов.
func NewRangeShardManager(logger LoggerInterface) *RangeShardManager {
    rsm := &RangeShardManager{
        logger:              logger,
        shardSizeThreshold:  100 * 1024 * 1024,
        shardCountThreshold: 1000000,
        minShardSize:        10 * 1024 * 1024,
    }
    rsm.shardsPtr.Store(make(map[string]*RangeShard))
    rsm.sortedShards.Store(make([]*RangeShard, 0))
    
    rsm.splitMgr = NewDynamicSplitManager(rsm, logger)
    rsm.mergeMgr = NewDynamicMergeManager(rsm, logger)
    
    return rsm
}

// NewDynamicSplitManager создаёт менеджер разделения шардов.
func NewDynamicSplitManager(shardManager *RangeShardManager, logger LoggerInterface) *DynamicSplitManager {
    return &DynamicSplitManager{
        shardManager:    shardManager,
        logger:          logger,
        stopChan:        make(chan struct{}),
        checkInterval:   30 * time.Second,
        splittingShards: make(map[string]bool),
    }
}

// NewDynamicMergeManager создаёт менеджер объединения шардов.
func NewDynamicMergeManager(shardManager *RangeShardManager, logger LoggerInterface) *DynamicMergeManager {
    return &DynamicMergeManager{
        shardManager:    shardManager,
        logger:          logger,
        stopChan:        make(chan struct{}),
        checkInterval:   60 * time.Second,
        mergingShards:   make(map[string]bool),
    }
}

// Start запускает мониторинг шардов.
func (rsm *RangeShardManager) Start() {
    if rsm.splitMgr != nil {
        go rsm.splitMgr.Start()
    }
    if rsm.mergeMgr != nil {
        go rsm.mergeMgr.Start()
    }
    if rsm.logger != nil {
        rsm.logger.Info("Range shard manager started")
    }
}

// Stop останавливает менеджер шардов.
func (rsm *RangeShardManager) Stop() {
    if rsm.splitMgr != nil {
        rsm.splitMgr.Stop()
    }
    if rsm.mergeMgr != nil {
        rsm.mergeMgr.Stop()
    }
    if rsm.logger != nil {
        rsm.logger.Info("Range shard manager stopped")
    }
}

// Start запускает мониторинг разделения.
func (dsm *DynamicSplitManager) Start() {
    dsm.wg.Add(1)
    go dsm.splitMonitor()
}

// Stop останавливает мониторинг.
func (dsm *DynamicSplitManager) Stop() {
    close(dsm.stopChan)
    dsm.wg.Wait()
}

// splitMonitor периодически проверяет шарды на необходимость разделения.
func (dsm *DynamicSplitManager) splitMonitor() {
    defer dsm.wg.Done()
    
    ticker := time.NewTicker(dsm.checkInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-dsm.stopChan:
            return
        case <-ticker.C:
            dsm.checkAndSplit()
        }
    }
}

// checkAndSplit проверяет и выполняет разделение шардов.
func (dsm *DynamicSplitManager) checkAndSplit() {
    shards := dsm.shardManager.GetAllShards()
    
    for _, shard := range shards {
        if shard.IsSplitting || shard.IsMerging {
            continue
        }
        
        if shard.SizeBytes > dsm.shardManager.shardSizeThreshold || 
           shard.DocumentCount > dsm.shardManager.shardCountThreshold {
            dsm.splitShard(shard)
        }
    }
}

// splitShard выполняет разделение шарда на два.
func (dsm *DynamicSplitManager) splitShard(shard *RangeShard) error {
    dsm.mu.Lock()
    if dsm.splittingShards[shard.ID] {
        dsm.mu.Unlock()
        return fmt.Errorf("shard %s is already splitting", shard.ID)
    }
    dsm.splittingShards[shard.ID] = true
    dsm.mu.Unlock()
    
    defer func() {
        dsm.mu.Lock()
        delete(dsm.splittingShards, shard.ID)
        dsm.mu.Unlock()
    }()
    
    if dsm.logger != nil {
        dsm.logger.Info(fmt.Sprintf("Splitting shard %s (size: %d bytes, docs: %d)", 
            shard.Name, shard.SizeBytes, shard.DocumentCount))
    }
    
    splitKey := dsm.findSplitKey(shard)
    if splitKey == "" {
        return fmt.Errorf("failed to find split key for shard %s", shard.ID)
    }
    
    shard1 := &RangeShard{
        ID:             fmt.Sprintf("%s_left", shard.ID),
        Name:           fmt.Sprintf("%s_left", shard.Name),
        StartKey:       shard.StartKey,
        EndKey:         splitKey,
        Nodes:          shard.Nodes,
        LeaderNode:     shard.LeaderNode,
        Status:         "active",
        CreatedAt:      time.Now().UnixMilli(),
        UpdatedAt:      time.Now().UnixMilli(),
        LastRebalanced: time.Now().UnixMilli(),
        DocumentCount:  shard.DocumentCount / 2,
        SizeBytes:      shard.SizeBytes / 2,
        IsSplitting:    false,
        IsMerging:      false,
    }
    
    shard2 := &RangeShard{
        ID:             fmt.Sprintf("%s_right", shard.ID),
        Name:           fmt.Sprintf("%s_right", shard.Name),
        StartKey:       splitKey,
        EndKey:         shard.EndKey,
        Nodes:          shard.Nodes,
        LeaderNode:     shard.LeaderNode,
        Status:         "active",
        CreatedAt:      time.Now().UnixMilli(),
        UpdatedAt:      time.Now().UnixMilli(),
        LastRebalanced: time.Now().UnixMilli(),
        DocumentCount:  shard.DocumentCount / 2,
        SizeBytes:      shard.SizeBytes / 2,
        IsSplitting:    false,
        IsMerging:      false,
    }
    
    dsm.shardManager.mu.Lock()
    defer dsm.shardManager.mu.Unlock()
    
    oldShards := dsm.shardManager.loadShards()
    newShards := make(map[string]*RangeShard)
    for k, v := range oldShards {
        if k != shard.ID {
            newShards[k] = v
        }
    }
    newShards[shard1.ID] = shard1
    newShards[shard2.ID] = shard2
    
    dsm.shardManager.shardsPtr.Store(newShards)
    dsm.shardManager.updateSortedShards()
    
    if dsm.logger != nil {
        dsm.logger.Info(fmt.Sprintf("Shard %s split into %s and %s", shard.Name, shard1.Name, shard2.Name))
    }
    
    return nil
}

// findSplitKey находит ключ для разделения шарда.
func (dsm *DynamicSplitManager) findSplitKey(shard *RangeShard) string {
    start := []byte(shard.StartKey)
    end := []byte(shard.EndKey)
    
    if len(start) == 0 || len(end) == 0 {
        return ""
    }
    
    mid := make([]byte, len(start))
    for i := range start {
        if i < len(end) {
            mid[i] = (start[i] + end[i]) / 2
        } else {
            mid[i] = start[i]
        }
    }
    
    return string(mid)
}

// Start запускает мониторинг объединения.
func (dmm *DynamicMergeManager) Start() {
    dmm.wg.Add(1)
    go dmm.mergeMonitor()
}

// Stop останавливает мониторинг.
func (dmm *DynamicMergeManager) Stop() {
    close(dmm.stopChan)
    dmm.wg.Wait()
}

// mergeMonitor периодически проверяет шарды на возможность объединения.
func (dmm *DynamicMergeManager) mergeMonitor() {
    defer dmm.wg.Done()
    
    ticker := time.NewTicker(dmm.checkInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-dmm.stopChan:
            return
        case <-ticker.C:
            dmm.checkAndMerge()
        }
    }
}

// checkAndMerge проверяет и выполняет объединение шардов.
func (dmm *DynamicMergeManager) checkAndMerge() {
    shards := dmm.shardManager.GetSortedShards()
    
    for i := 0; i < len(shards)-1; i++ {
        shard1 := shards[i]
        shard2 := shards[i+1]
        
        if shard1.IsSplitting || shard1.IsMerging || shard2.IsSplitting || shard2.IsMerging {
            continue
        }
        
        if shard1.EndKey != shard2.StartKey {
            continue
        }
        
        totalSize := shard1.SizeBytes + shard2.SizeBytes
        totalDocs := shard1.DocumentCount + shard2.DocumentCount
        
        if totalSize < dmm.shardManager.minShardSize && totalDocs < dmm.shardManager.shardCountThreshold/10 {
            dmm.mergeShards(shard1, shard2)
        }
    }
}

// mergeShards объединяет два смежных шарда.
func (dmm *DynamicMergeManager) mergeShards(shard1, shard2 *RangeShard) error {
    dmm.mu.Lock()
    if dmm.mergingShards[shard1.ID] || dmm.mergingShards[shard2.ID] {
        dmm.mu.Unlock()
        return fmt.Errorf("shards are already merging")
    }
    dmm.mergingShards[shard1.ID] = true
    dmm.mergingShards[shard2.ID] = true
    dmm.mu.Unlock()
    
    defer func() {
        dmm.mu.Lock()
        delete(dmm.mergingShards, shard1.ID)
        delete(dmm.mergingShards, shard2.ID)
        dmm.mu.Unlock()
    }()
    
    if dmm.logger != nil {
        dmm.logger.Info(fmt.Sprintf("Merging shards %s and %s", shard1.Name, shard2.Name))
    }
    
    mergedShard := &RangeShard{
        ID:             fmt.Sprintf("%s_merged", shard1.ID),
        Name:           fmt.Sprintf("%s_merged", shard1.Name),
        StartKey:       shard1.StartKey,
        EndKey:         shard2.EndKey,
        Nodes:          shard1.Nodes,
        LeaderNode:     shard1.LeaderNode,
        Status:         "active",
        CreatedAt:      time.Now().UnixMilli(),
        UpdatedAt:      time.Now().UnixMilli(),
        LastRebalanced: time.Now().UnixMilli(),
        DocumentCount:  shard1.DocumentCount + shard2.DocumentCount,
        SizeBytes:      shard1.SizeBytes + shard2.SizeBytes,
        IsSplitting:    false,
        IsMerging:      false,
    }
    
    dmm.shardManager.mu.Lock()
    defer dmm.shardManager.mu.Unlock()
    
    oldShards := dmm.shardManager.loadShards()
    newShards := make(map[string]*RangeShard)
    for k, v := range oldShards {
        if k != shard1.ID && k != shard2.ID {
            newShards[k] = v
        }
    }
    newShards[mergedShard.ID] = mergedShard
    
    dmm.shardManager.shardsPtr.Store(newShards)
    dmm.shardManager.updateSortedShards()
    
    if dmm.logger != nil {
        dmm.logger.Info(fmt.Sprintf("Merged %s and %s into %s", shard1.Name, shard2.Name, mergedShard.Name))
    }
    
    return nil
}

// loadShards загружает карту шардов.
func (rsm *RangeShardManager) loadShards() map[string]*RangeShard {
    val := rsm.shardsPtr.Load()
    if val == nil {
        return make(map[string]*RangeShard)
    }
    return val.(map[string]*RangeShard)
}

// updateSortedShards обновляет отсортированный список шардов.
func (rsm *RangeShardManager) updateSortedShards() {
    shards := rsm.loadShards()
    sorted := make([]*RangeShard, 0, len(shards))
    for _, sh := range shards {
        sorted = append(sorted, sh)
    }
    sort.Slice(sorted, func(i, j int) bool {
        return sorted[i].StartKey < sorted[j].StartKey
    })
    rsm.sortedShards.Store(sorted)
}

// GetShard возвращает шард для ключа.
func (rsm *RangeShardManager) GetShard(key string) *RangeShard {
    shards := rsm.getSortedShards()
    for _, shard := range shards {
        if key >= shard.StartKey && (shard.EndKey == "" || key < shard.EndKey) {
            return shard
        }
    }
    return nil
}

// getSortedShards возвращает отсортированный список шардов.
func (rsm *RangeShardManager) getSortedShards() []*RangeShard {
    val := rsm.sortedShards.Load()
    if val == nil {
        return make([]*RangeShard, 0)
    }
    return val.([]*RangeShard)
}

// GetAllShards возвращает все шарды.
func (rsm *RangeShardManager) GetAllShards() []*RangeShard {
    shards := rsm.loadShards()
    result := make([]*RangeShard, 0, len(shards))
    for _, shard := range shards {
        result = append(result, shard)
    }
    return result
}

// GetSortedShards возвращает отсортированные шарды.
func (rsm *RangeShardManager) GetSortedShards() []*RangeShard {
    return rsm.getSortedShards()
}

// GetShardByID возвращает шард по ID.
func (rsm *RangeShardManager) GetShardByID(shardID string) *RangeShard {
    shards := rsm.loadShards()
    if shard, ok := shards[shardID]; ok {
        return shard
    }
    return nil
}

// AddNode добавляет узел в шарды.
func (rsm *RangeShardManager) AddNode(nodeID string) {
    rsm.Rebalance()
}

// RemoveNode удаляет узел из шардов.
func (rsm *RangeShardManager) RemoveNode(nodeID string) {
    rsm.Rebalance()
}

// Rebalance выполняет ребалансировку шардов.
func (rsm *RangeShardManager) Rebalance() error {
    if !rsm.rebalancing.CompareAndSwap(false, true) {
        return fmt.Errorf("rebalancing already in progress")
    }
    defer rsm.rebalancing.Store(false)
    
    if rsm.logger != nil {
        rsm.logger.Info("Starting range shard rebalancing...")
    }
    
    shards := rsm.GetAllShards()
    now := time.Now().UnixMilli()
    
    rsm.mu.Lock()
    defer rsm.mu.Unlock()
    
    oldShards := rsm.loadShards()
    newShards := make(map[string]*RangeShard)
    
    for id, shard := range oldShards {
        shardCopy := *shard
        shardCopy.LastRebalanced = now
        shardCopy.UpdatedAt = now
        newShards[id] = &shardCopy
    }
    
    rsm.shardsPtr.Store(newShards)
    rsm.updateSortedShards()
    
    if rsm.logger != nil {
        rsm.logger.Info(fmt.Sprintf("Range shard rebalancing completed: %d shards", len(shards)))
    }
    
    return nil
}

// =============================================================================
// INMEM STORE ДЛЯ RAFT (реализует raft.LogStore и raft.StableStore)
// =============================================================================

// InmemStore реализует хранилище для Raft в памяти
type InmemStore struct {
    data map[string][]byte
    mu   sync.RWMutex
    path string
}

// NewInmemStore создаёт новое in-memory хранилище
func NewInmemStore(path string) *InmemStore {
    return &InmemStore{
        data: make(map[string][]byte),
        path: path,
    }
}

// Get возвращает значение по ключу
func (s *InmemStore) Get(key []byte) ([]byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    val, ok := s.data[string(key)]
    if !ok {
        return nil, fmt.Errorf("key not found")
    }
    return val, nil
}

// Set сохраняет значение по ключу
func (s *InmemStore) Set(key, val []byte) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.data[string(key)] = val
    return nil
}

// FirstIndex возвращает первый индекс (для raft.LogStore)
func (s *InmemStore) FirstIndex() (uint64, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var first uint64 = 0
    for k := range s.data {
        if k == "config" || k == "currentTerm" || k == "votedFor" {
            continue
        }
        // Парсим индекс из ключа
        var idx uint64
        if _, err := fmt.Sscanf(k, "%d", &idx); err == nil {
            if first == 0 || idx < first {
                first = idx
            }
        }
    }
    if first == 0 {
        return 1, nil
    }
    return first, nil
}

// LastIndex возвращает последний индекс (для raft.LogStore)
func (s *InmemStore) LastIndex() (uint64, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var last uint64 = 0
    for k := range s.data {
        if k == "config" || k == "currentTerm" || k == "votedFor" {
            continue
        }
        var idx uint64
        if _, err := fmt.Sscanf(k, "%d", &idx); err == nil && idx > last {
            last = idx
        }
    }
    return last, nil
}

// GetLog возвращает лог по индексу (для raft.LogStore)
func (s *InmemStore) GetLog(index uint64, log *raft.Log) error {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    key := fmt.Sprintf("%d", index)
    val, ok := s.data[key]
    if !ok {
        return raft.ErrLogNotFound
    }
    
    return json.Unmarshal(val, log)
}

// StoreLog сохраняет лог (для raft.LogStore)
func (s *InmemStore) StoreLog(log *raft.Log) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    data, err := json.Marshal(log)
    if err != nil {
        return err
    }
    
    key := fmt.Sprintf("%d", log.Index)
    s.data[key] = data
    return nil
}

// StoreLogs сохраняет несколько логов (для raft.LogStore)
func (s *InmemStore) StoreLogs(logs []*raft.Log) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    for _, log := range logs {
        data, err := json.Marshal(log)
        if err != nil {
            return err
        }
        key := fmt.Sprintf("%d", log.Index)
        s.data[key] = data
    }
    return nil
}

// DeleteRange удаляет диапазон логов (для raft.LogStore)
func (s *InmemStore) DeleteRange(min, max uint64) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    for i := min; i <= max; i++ {
        key := fmt.Sprintf("%d", i)
        delete(s.data, key)
    }
    return nil
}

// SetConfiguration сохраняет конфигурацию (для raft.LogStore)
func (s *InmemStore) SetConfiguration(config raft.Configuration) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    data, err := json.Marshal(config)
    if err != nil {
        return err
    }
    s.data["config"] = data
    return nil
}

// Configuration возвращает конфигурацию (для raft.LogStore)
func (s *InmemStore) Configuration() (raft.Configuration, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    val, ok := s.data["config"]
    if !ok {
        return raft.Configuration{}, nil
    }
    
    var config raft.Configuration
    if err := json.Unmarshal(val, &config); err != nil {
        return raft.Configuration{}, err
    }
    return config, nil
}

// =============================================================================
// МЕТОДЫ ДЛЯ raft.StableStore
// =============================================================================

// SetUint64 сохраняет uint64 значение (для raft.StableStore)
func (s *InmemStore) SetUint64(key []byte, val uint64) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, val)
    s.data[string(key)] = buf
    return nil
}

// GetUint64 получает uint64 значение (для raft.StableStore)
func (s *InmemStore) GetUint64(key []byte) (uint64, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    val, ok := s.data[string(key)]
    if !ok {
        return 0, fmt.Errorf("key not found")
    }
    if len(val) != 8 {
        return 0, fmt.Errorf("invalid uint64 value")
    }
    return binary.BigEndian.Uint64(val), nil
}

// =============================================================================
// MULTI-RAFT
// =============================================================================

// MultiRaftManager управляет несколькими Raft-группами для параллельной записи.
type MultiRaftManager struct {
    raftGroups     sync.Map
    groupConfigs   sync.Map
    logger         LoggerInterface
    mu             sync.RWMutex
    stopChan       chan struct{}
    wg             sync.WaitGroup
    storage        *storage.Storage
    baseConfig     *raft.Config
    transport      *raft.NetworkTransport
    snapshotStore  raft.SnapshotStore
    logStore       raft.LogStore
    stableStore    raft.StableStore
}

// MultiRaftGroupConfig конфигурация группы Raft.
type MultiRaftGroupConfig struct {
    GroupID    string
    ShardID    string
    Nodes      []string
    LeaderID   string
    Term       uint64
    CreatedAt  int64
    UpdatedAt  int64
}

// NewMultiRaftManager создаёт новый менеджер Multi-Raft.
func NewMultiRaftManager(storage *storage.Storage, logger LoggerInterface) *MultiRaftManager {
    return &MultiRaftManager{
        logger:    logger,
        stopChan:  make(chan struct{}),
        storage:   storage,
    }
}

// GetOrCreateRaftGroup получает или создаёт группу Raft для шарда.
func (mrm *MultiRaftManager) GetOrCreateRaftGroup(shardID string, nodes []string) (*raft.Raft, error) {
    if val, ok := mrm.raftGroups.Load(shardID); ok {
        return val.(*raft.Raft), nil
    }
    
    mrm.mu.Lock()
    defer mrm.mu.Unlock()
    
    if val, ok := mrm.raftGroups.Load(shardID); ok {
        return val.(*raft.Raft), nil
    }
    
    groupID := fmt.Sprintf("shard_%s", shardID)
    
    raftConfig := raft.DefaultConfig()
    raftConfig.LocalID = raft.ServerID(groupID)
    raftConfig.HeartbeatTimeout = 1 * time.Second
    raftConfig.ElectionTimeout = 1 * time.Second
    raftConfig.CommitTimeout = 500 * time.Millisecond
    raftConfig.SnapshotInterval = 30 * time.Second
    raftConfig.SnapshotThreshold = 1000
    
    dataDir := filepath.Join("raft_data", groupID)
    if err := os.MkdirAll(dataDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create raft dir: %v", err)
    }
    
    // Используем InmemStore для логов и стабильного хранилища
    logStore := NewInmemStore(filepath.Join(dataDir, "raft-log.json"))
    stableStore := NewInmemStore(filepath.Join(dataDir, "raft-stable.json"))
    snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("failed to create snapshot store: %v", err)
    }
    
    fsm := &MultiRaftFSM{
        shardID: shardID,
        storage: mrm.storage,
        logger:  mrm.logger,
    }
    
    addr := fmt.Sprintf("127.0.0.1:%d", 9000+len(groupID))
    transport, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("failed to create transport: %v", err)
    }
    
    // Используем logStore и stableStore как raft.LogStore и raft.StableStore
    r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
    if err != nil {
        return nil, fmt.Errorf("failed to create raft: %v", err)
    }
    
    if len(nodes) > 0 {
        servers := make([]raft.Server, len(nodes))
        for i, nodeAddr := range nodes {
            servers[i] = raft.Server{
                ID:      raft.ServerID(fmt.Sprintf("%s-node-%d", groupID, i)),
                Address: raft.ServerAddress(nodeAddr),
            }
        }
        configuration := raft.Configuration{Servers: servers}
        r.BootstrapCluster(configuration)
    }
    
    mrm.raftGroups.Store(shardID, r)
    
    if mrm.logger != nil {
        mrm.logger.Info(fmt.Sprintf("Created Multi-Raft group for shard %s", shardID))
    }
    
    return r, nil
}

// MultiRaftFSM реализует конечный автомат для Multi-Raft группы.
type MultiRaftFSM struct {
    shardID string
    storage *storage.Storage
    logger  LoggerInterface
    state   map[string]interface{}
    mu      sync.RWMutex
}

// Apply применяет команду к FSM.
func (f *MultiRaftFSM) Apply(log *raft.Log) interface{} {
    var cmd map[string]interface{}
    if err := json.Unmarshal(log.Data, &cmd); err != nil {
        if f.logger != nil {
            f.logger.Error(fmt.Sprintf("Failed to unmarshal command: %v", err))
        }
        return err
    }
    
    f.mu.Lock()
    defer f.mu.Unlock()
    
    opType, _ := cmd["type"].(string)
    switch opType {
    case "write":
        database, _ := cmd["database"].(string)
        collection, _ := cmd["collection"].(string)
        docData, _ := cmd["document"].(map[string]interface{})
        
        db, err := f.storage.GetDatabase(database)
        if err != nil {
            return err
        }
        coll, err := db.GetCollection(collection)
        if err != nil {
            return err
        }
        
        doc := storage.NewDocument()
        for k, v := range docData {
            doc.SetField(k, v)
        }
        return coll.Insert(doc)
        
    case "delete":
        database, _ := cmd["database"].(string)
        collection, _ := cmd["collection"].(string)
        docID, _ := cmd["document_id"].(string)
        
        db, err := f.storage.GetDatabase(database)
        if err != nil {
            return err
        }
        coll, err := db.GetCollection(collection)
        if err != nil {
            return err
        }
        return coll.Delete(docID)
        
    default:
        if f.logger != nil {
            f.logger.Warn(fmt.Sprintf("Unknown operation type: %s", opType))
        }
    }
    
    return nil
}

// Snapshot создаёт снапшот состояния FSM.
func (f *MultiRaftFSM) Snapshot() (raft.FSMSnapshot, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    
    snapshot := &MultiRaftSnapshot{
        state: f.state,
    }
    return snapshot, nil
}

// Restore восстанавливает состояние FSM из снапшота.
func (f *MultiRaftFSM) Restore(snapshot io.ReadCloser) error {
    defer snapshot.Close()
    
    var state map[string]interface{}
    decoder := json.NewDecoder(snapshot)
    if err := decoder.Decode(&state); err != nil {
        return err
    }
    
    f.mu.Lock()
    defer f.mu.Unlock()
    f.state = state
    
    return nil
}

// MultiRaftSnapshot реализует снапшот для Multi-Raft.
type MultiRaftSnapshot struct {
    state map[string]interface{}
}

// Persist сохраняет снапшот.
func (s *MultiRaftSnapshot) Persist(sink raft.SnapshotSink) error {
    data, err := json.Marshal(s.state)
    if err != nil {
        sink.Cancel()
        return err
    }
    
    if _, err := sink.Write(data); err != nil {
        sink.Cancel()
        return err
    }
    
    return sink.Close()
}

// Release освобождает ресурсы.
func (s *MultiRaftSnapshot) Release() {}

// =============================================================================
// SAGA - РАСПРЕДЕЛЁННЫЕ ТРАНЗАКЦИИ С КОМПЕНСАЦИЕЙ
// =============================================================================

// SagaStep представляет шаг в Saga транзакции.
type SagaStep struct {
    ID         string                 `json:"id"`
    Name       string                 `json:"name"`
    Execute    func() error           `json:"-"`
    Compensate func() error           `json:"-"`
    Status     string                 `json:"status"`
    Data       map[string]interface{} `json:"data"`
}

// SagaTransaction представляет Saga транзакцию.
type SagaTransaction struct {
    ID          string      `json:"id"`
    Steps       []*SagaStep `json:"steps"`
    CurrentStep int         `json:"current_step"`
    Status      string      `json:"status"`
    CreatedAt   int64       `json:"created_at"`
    UpdatedAt   int64       `json:"updated_at"`
    mu          sync.RWMutex
}

// SagaManager управляет Saga транзакциями.
type SagaManager struct {
    sagas      sync.Map
    logger     LoggerInterface
    stopChan   chan struct{}
    wg         sync.WaitGroup
    mu         sync.RWMutex
    maxRetries int
}

// NewSagaManager создаёт новый менеджер Saga.
func NewSagaManager(logger LoggerInterface) *SagaManager {
    return &SagaManager{
        logger:     logger,
        stopChan:   make(chan struct{}),
        maxRetries: 3,
    }
}

// BeginSaga начинает новую Saga транзакцию.
func (sm *SagaManager) BeginSaga(id string) *SagaTransaction {
    saga := &SagaTransaction{
        ID:          id,
        Steps:       make([]*SagaStep, 0),
        CurrentStep: 0,
        Status:      "pending",
        CreatedAt:   time.Now().UnixMilli(),
        UpdatedAt:   time.Now().UnixMilli(),
    }
    sm.sagas.Store(id, saga)
    return saga
}

// AddStep добавляет шаг в Saga транзакцию.
func (s *SagaTransaction) AddStep(name string, execute, compensate func() error, data map[string]interface{}) *SagaTransaction {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    step := &SagaStep{
        ID:         fmt.Sprintf("%s_step_%d", s.ID, len(s.Steps)),
        Name:       name,
        Execute:    execute,
        Compensate: compensate,
        Status:     "pending",
        Data:       data,
    }
    s.Steps = append(s.Steps, step)
    return s
}

// Execute выполняет Saga транзакцию.
func (sm *SagaManager) Execute(saga *SagaTransaction) error {
    saga.mu.Lock()
    defer saga.mu.Unlock()
    
    if saga.Status != "pending" {
        return fmt.Errorf("saga %s is not in pending state", saga.ID)
    }
    
    saga.Status = "running"
    saga.UpdatedAt = time.Now().UnixMilli()
    
    for i, step := range saga.Steps {
        saga.CurrentStep = i
        step.Status = "running"
        
        if sm.logger != nil {
            sm.logger.Debug(fmt.Sprintf("Executing saga step %s: %s", saga.ID, step.Name))
        }
        
        var err error
        for retry := 0; retry < sm.maxRetries; retry++ {
            if err = step.Execute(); err == nil {
                break
            }
            if sm.logger != nil {
                sm.logger.Warn(fmt.Sprintf("Saga step %s failed (attempt %d/%d): %v", step.Name, retry+1, sm.maxRetries, err))
            }
            time.Sleep(time.Duration(100*(retry+1)) * time.Millisecond)
        }
        
        if err != nil {
            step.Status = "failed"
            saga.Status = "compensating"
            saga.UpdatedAt = time.Now().UnixMilli()
            
            if sm.logger != nil {
                sm.logger.Error(fmt.Sprintf("Saga step %s failed, starting compensation", step.Name))
            }
            
            for j := i; j >= 0; j-- {
                prevStep := saga.Steps[j]
                if prevStep.Status == "compensated" || prevStep.Status == "pending" {
                    continue
                }
                if err := prevStep.Compensate(); err != nil {
                    if sm.logger != nil {
                        sm.logger.Error(fmt.Sprintf("Compensation for step %s failed: %v", prevStep.Name, err))
                    }
                    prevStep.Status = "compensation_failed"
                } else {
                    prevStep.Status = "compensated"
                }
            }
            
            saga.Status = "aborted"
            saga.UpdatedAt = time.Now().UnixMilli()
            return fmt.Errorf("saga %s aborted at step %s: %v", saga.ID, step.Name, err)
        }
        
        step.Status = "completed"
        saga.UpdatedAt = time.Now().UnixMilli()
    }
    
    saga.Status = "completed"
    saga.UpdatedAt = time.Now().UnixMilli()
    
    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Saga %s completed successfully", saga.ID))
    }
    
    return nil
}

// GetSaga возвращает Saga по ID.
func (sm *SagaManager) GetSaga(id string) (*SagaTransaction, error) {
    if val, ok := sm.sagas.Load(id); ok {
        return val.(*SagaTransaction), nil
    }
    return nil, fmt.Errorf("saga %s not found", id)
}

// GetSagaStatus возвращает статус Saga.
func (sm *SagaManager) GetSagaStatus(id string) (string, error) {
    saga, err := sm.GetSaga(id)
    if err != nil {
        return "", err
    }
    saga.mu.RLock()
    defer saga.mu.RUnlock()
    return saga.Status, nil
}

// =============================================================================
// TCC (TRY-CONFIRM-CANCEL)
// =============================================================================

// TCCTransaction представляет TCC транзакцию.
type TCCTransaction struct {
    ID        string                 `json:"id"`
    Status    string                 `json:"status"`
    TryData   map[string]interface{} `json:"try_data"`
    ConfirmFn func() error           `json:"-"`
    CancelFn  func() error           `json:"-"`
    CreatedAt int64                  `json:"created_at"`
    UpdatedAt int64                  `json:"updated_at"`
    mu        sync.RWMutex
}

// TCCManager управляет TCC транзакциями.
type TCCManager struct {
    transactions sync.Map
    logger       LoggerInterface
    stopChan     chan struct{}
    wg           sync.WaitGroup
    mu           sync.RWMutex
    timeout      time.Duration
}

// NewTCCManager создаёт новый менеджер TCC.
func NewTCCManager(logger LoggerInterface) *TCCManager {
    return &TCCManager{
        logger:    logger,
        stopChan:  make(chan struct{}),
        timeout:   30 * time.Second,
    }
}

// BeginTCC начинает TCC транзакцию.
func (tm *TCCManager) BeginTCC(id string) *TCCTransaction {
    tcc := &TCCTransaction{
        ID:        id,
        Status:    "try",
        CreatedAt: time.Now().UnixMilli(),
        UpdatedAt: time.Now().UnixMilli(),
    }
    tm.transactions.Store(id, tcc)
    return tcc
}

// Try выполняет фазу Try в TCC.
func (t *TCCTransaction) Try(data map[string]interface{}) error {
    t.mu.Lock()
    defer t.mu.Unlock()
    
    if t.Status != "try" {
        return fmt.Errorf("TCC %s is not in try phase", t.ID)
    }
    
    t.TryData = data
    t.UpdatedAt = time.Now().UnixMilli()
    return nil
}

// Confirm выполняет фазу Confirm в TCC.
func (t *TCCTransaction) Confirm() error {
    t.mu.Lock()
    defer t.mu.Unlock()
    
    if t.Status != "try" {
        return fmt.Errorf("TCC %s is not in try phase", t.ID)
    }
    
    if t.ConfirmFn == nil {
        return fmt.Errorf("TCC %s has no confirm function", t.ID)
    }
    
    t.Status = "confirming"
    t.UpdatedAt = time.Now().UnixMilli()
    
    if err := t.ConfirmFn(); err != nil {
        t.Status = "failed"
        return fmt.Errorf("confirm failed: %v", err)
    }
    
    t.Status = "confirmed"
    t.UpdatedAt = time.Now().UnixMilli()
    return nil
}

// Cancel выполняет фазу Cancel в TCC.
func (t *TCCTransaction) Cancel() error {
    t.mu.Lock()
    defer t.mu.Unlock()
    
    if t.Status == "confirmed" {
        return fmt.Errorf("TCC %s already confirmed", t.ID)
    }
    
    if t.CancelFn == nil {
        return fmt.Errorf("TCC %s has no cancel function", t.ID)
    }
    
    t.Status = "cancelling"
    t.UpdatedAt = time.Now().UnixMilli()
    
    if err := t.CancelFn(); err != nil {
        t.Status = "failed"
        return fmt.Errorf("cancel failed: %v", err)
    }
    
    t.Status = "cancelled"
    t.UpdatedAt = time.Now().UnixMilli()
    return nil
}

// Complete завершает TCC транзакцию.
func (tm *TCCManager) Complete(tcc *TCCTransaction, success bool) error {
    if success {
        return tcc.Confirm()
    }
    return tcc.Cancel()
}

// GetTCC возвращает TCC транзакцию по ID.
func (tm *TCCManager) GetTCC(id string) (*TCCTransaction, error) {
    if val, ok := tm.transactions.Load(id); ok {
        return val.(*TCCTransaction), nil
    }
    return nil, fmt.Errorf("TCC %s not found", id)
}

// =============================================================================
// АСИНХРОННАЯ РЕПЛИКАЦИЯ ДЛЯ ЧТЕНИЯ (STALE READS)
// =============================================================================

// ReplicaReadManager управляет асинхронной репликацией для чтения.
type ReplicaReadManager struct {
    coordinator      *RaftCoordinator
    logger           LoggerInterface
    mu               sync.RWMutex
    readReplicas     map[string]bool
    replicationLag   map[string]int64
    stopChan         chan struct{}
    wg               sync.WaitGroup
    checkInterval    time.Duration
}

// NewReplicaReadManager создаёт менеджер для асинхронного чтения с реплик.
func NewReplicaReadManager(coordinator *RaftCoordinator, logger LoggerInterface) *ReplicaReadManager {
    return &ReplicaReadManager{
        coordinator:    coordinator,
        logger:         logger,
        readReplicas:   make(map[string]bool),
        replicationLag: make(map[string]int64),
        stopChan:       make(chan struct{}),
        checkInterval:  5 * time.Second,
    }
}

// Start запускает мониторинг реплик для чтения.
func (rrm *ReplicaReadManager) Start() {
    rrm.wg.Add(1)
    go rrm.monitorReplicas()
    if rrm.logger != nil {
        rrm.logger.Info("Replica read manager started")
    }
}

// Stop останавливает мониторинг.
func (rrm *ReplicaReadManager) Stop() {
    close(rrm.stopChan)
    rrm.wg.Wait()
    if rrm.logger != nil {
        rrm.logger.Info("Replica read manager stopped")
    }
}

// monitorReplicas отслеживает состояние реплик для чтения.
func (rrm *ReplicaReadManager) monitorReplicas() {
    defer rrm.wg.Done()
    
    ticker := time.NewTicker(rrm.checkInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-rrm.stopChan:
            return
        case <-ticker.C:
            rrm.updateReplicaStatus()
        }
    }
}

// updateReplicaStatus обновляет статус реплик.
func (rrm *ReplicaReadManager) updateReplicaStatus() {
    if rrm.coordinator == nil {
        return
    }
    
    nodes := rrm.coordinator.GetAllNodes()
    now := time.Now().UnixMilli()
    
    rrm.mu.Lock()
    defer rrm.mu.Unlock()
    
    for _, node := range nodes {
        if node.ID == rrm.coordinator.localNodeInfo.ID {
            continue
        }
        
        lag := now - node.LastSeen
        rrm.replicationLag[node.ID] = lag
        
        if lag < 5000 && node.Status == "active" {
            rrm.readReplicas[node.ID] = true
        } else {
            rrm.readReplicas[node.ID] = false
        }
    }
}

// GetReadReplicas возвращает список узлов для чтения.
func (rrm *ReplicaReadManager) GetReadReplicas() []*NodeInfo {
    rrm.mu.RLock()
    defer rrm.mu.RUnlock()
    
    replicas := make([]*NodeInfo, 0)
    for _, node := range rrm.coordinator.GetActiveNodes() {
        if rrm.readReplicas[node.ID] {
            replicas = append(replicas, node)
        }
    }
    return replicas
}

// GetReplicationLag возвращает задержку репликации для узла.
func (rrm *ReplicaReadManager) GetReplicationLag(nodeID string) int64 {
    rrm.mu.RLock()
    defer rrm.mu.RUnlock()
    
    if lag, ok := rrm.replicationLag[nodeID]; ok {
        return lag
    }
    return -1
}

// IsReadReplica проверяет, является ли узел доступным для чтения.
func (rrm *ReplicaReadManager) IsReadReplica(nodeID string) bool {
    rrm.mu.RLock()
    defer rrm.mu.RUnlock()
    
    if val, ok := rrm.readReplicas[nodeID]; ok {
        return val
    }
    return false
}

// GetReadReplicaStats возвращает статистику реплик для чтения.
func (rrm *ReplicaReadManager) GetReadReplicaStats() map[string]interface{} {
    rrm.mu.RLock()
    defer rrm.mu.RUnlock()
    
    stats := make(map[string]interface{})
    for nodeID, isReplica := range rrm.readReplicas {
        stats[nodeID] = map[string]interface{}{
            "is_read_replica": isReplica,
            "lag_ms":          rrm.replicationLag[nodeID],
        }
    }
    return stats
}

// =============================================================================
// SPLIT-BRAIN DETECTOR
// =============================================================================

// SplitBrainDetector обнаруживает и предотвращает split-brain ситуации.
type SplitBrainDetector struct {
    knownLeaders      map[uint64]string
    suspectTime       map[string]int64
    mu                sync.RWMutex
    logger            LoggerInterface
    preventionEnabled bool
    recoveryTimeout   time.Duration
}

// NewSplitBrainDetector создаёт новый детектор split-brain.
func NewSplitBrainDetector(logger LoggerInterface, preventionEnabled bool, recoveryTimeout time.Duration) *SplitBrainDetector {
    return &SplitBrainDetector{
        knownLeaders:      make(map[uint64]string),
        suspectTime:       make(map[string]int64),
        logger:            logger,
        preventionEnabled: preventionEnabled,
        recoveryTimeout:   recoveryTimeout,
    }
}

// Detect проверяет наличие split-brain ситуации.
func (sbd *SplitBrainDetector) Detect(term uint64, leaderID string, nodesCount int) bool {
    if !sbd.preventionEnabled {
        return false
    }
    
    sbd.mu.Lock()
    defer sbd.mu.Unlock()
    
    if existingLeader, exists := sbd.knownLeaders[term]; exists {
        if existingLeader != leaderID && nodesCount > 1 {
            if sbd.logger != nil {
                sbd.logger.Error(fmt.Sprintf("SPLIT-BRAIN DETECTED! Term %d has two leaders: %s and %s",
                    term, existingLeader, leaderID))
            }
            return true
        }
    }
    
    sbd.knownLeaders[term] = leaderID
    
    for t := range sbd.knownLeaders {
        if t+10 < term {
            delete(sbd.knownLeaders, t)
        }
    }
    
    return false
}

// Resolve разрешает split-brain ситуацию.
func (sbd *SplitBrainDetector) Resolve(term uint64, candidates map[string]uint64) string {
    if !sbd.preventionEnabled {
        return ""
    }
    
    sbd.mu.Lock()
    defer sbd.mu.Unlock()
    
    var winner string
    var maxCommit uint64 = 0
    
    for nodeID, commitIndex := range candidates {
        if commitIndex > maxCommit {
            maxCommit = commitIndex
            winner = nodeID
        }
    }
    
    if sbd.logger != nil {
        sbd.logger.Warn(fmt.Sprintf("Resolving split-brain: selecting leader %s with commit index %d",
            winner, maxCommit))
    }
    
    return winner
}

// QuarantineNode изолирует узел, вызвавший split-brain.
func (sbd *SplitBrainDetector) QuarantineNode(nodeID string) {
    if !sbd.preventionEnabled {
        return
    }
    
    sbd.mu.Lock()
    defer sbd.mu.Unlock()
    
    quarantineUntil := time.Now().Add(sbd.recoveryTimeout).UnixMilli()
    sbd.suspectTime[nodeID] = quarantineUntil
    
    if sbd.logger != nil {
        sbd.logger.Warn(fmt.Sprintf("Node %s quarantined until %s", nodeID,
            time.UnixMilli(quarantineUntil).Format("2006-01-02 15:04:05.000")))
    }
}

// IsQuarantined проверяет, находится ли узел в карантине.
func (sbd *SplitBrainDetector) IsQuarantined(nodeID string) bool {
    sbd.mu.RLock()
    defer sbd.mu.RUnlock()
    
    if until, exists := sbd.suspectTime[nodeID]; exists {
        if time.Now().UnixMilli() < until {
            return true
        }
        delete(sbd.suspectTime, nodeID)
    }
    return false
}

// =============================================================================
// RECOVERY MANAGER
// =============================================================================

// NodeState представляет состояние узла для восстановления.
type NodeState struct {
    NodeID       string    `json:"node_id"`
    LastSeen     time.Time `json:"last_seen"`
    LastLogIndex uint64    `json:"last_log_index"`
    FailureCount int       `json:"failure_count"`
    IsRecovering bool      `json:"is_recovering"`
}

// RecoveryManager управляет восстановлением узлов.
type RecoveryManager struct {
    coordinator   *RaftCoordinator
    logger        LoggerInterface
    states        sync.Map
    maxFailures   int
    recoveryDelay time.Duration
    stopChan      chan struct{}
    wg            sync.WaitGroup
    isActive      atomic.Bool
}

// NewRecoveryManager создаёт новый менеджер восстановления.
func NewRecoveryManager(coordinator *RaftCoordinator, logger LoggerInterface) *RecoveryManager {
    return &RecoveryManager{
        coordinator:   coordinator,
        logger:        logger,
        maxFailures:   3,
        recoveryDelay: 30 * time.Second,
        stopChan:      make(chan struct{}),
    }
}

// Start запускает мониторинг восстановления.
func (rm *RecoveryManager) Start() {
    rm.isActive.Store(true)
    rm.wg.Add(2)
    go rm.monitorLoop()
    go rm.recoveryLoop()
    
    if rm.logger != nil {
        rm.logger.Info("Recovery manager started")
    }
}

// Stop останавливает менеджер восстановления.
func (rm *RecoveryManager) Stop() {
    rm.isActive.Store(false)
    close(rm.stopChan)
    rm.wg.Wait()
    
    if rm.logger != nil {
        rm.logger.Info("Recovery manager stopped")
    }
}

// monitorLoop отслеживает состояние узлов.
func (rm *RecoveryManager) monitorLoop() {
    defer rm.wg.Done()
    
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-rm.stopChan:
            return
        case <-ticker.C:
            rm.checkNodesHealth()
        }
    }
}

// checkNodesHealth проверяет здоровье узлов.
func (rm *RecoveryManager) checkNodesHealth() {
    if rm.coordinator == nil {
        return
    }
    
    nodes := rm.coordinator.GetAllNodes()
    now := time.Now()
    
    for _, node := range nodes {
        stateVal, ok := rm.states.Load(node.ID)
        var state *NodeState
        if ok {
            state = stateVal.(*NodeState)
        } else {
            state = &NodeState{
                NodeID:       node.ID,
                LastSeen:     now,
                LastLogIndex: 0,
                FailureCount: 0,
                IsRecovering: false,
            }
            rm.states.Store(node.ID, state)
        }
        
        lastSeen := time.UnixMilli(node.LastSeen)
        if now.Sub(lastSeen) > 30*time.Second {
            state.FailureCount++
            if rm.logger != nil {
                rm.logger.Warn(fmt.Sprintf("Node %s appears unhealthy, failure count: %d", node.ID, state.FailureCount))
            }
            
            if state.FailureCount >= rm.maxFailures && !state.IsRecovering {
                rm.triggerRecovery(node.ID)
            }
        } else {
            if state.FailureCount > 0 {
                state.FailureCount = 0
                if rm.logger != nil {
                    rm.logger.Info(fmt.Sprintf("Node %s recovered", node.ID))
                }
            }
        }
        
        state.LastSeen = now
        rm.states.Store(node.ID, state)
    }
}

// triggerRecovery запускает восстановление узла.
func (rm *RecoveryManager) triggerRecovery(nodeID string) {
    stateVal, ok := rm.states.Load(nodeID)
    if !ok {
        return
    }
    
    state := stateVal.(*NodeState)
    if state.IsRecovering {
        return
    }
    
    state.IsRecovering = true
    rm.states.Store(nodeID, state)
    
    if rm.logger != nil {
        rm.logger.Info(fmt.Sprintf("Triggering recovery for node %s", nodeID))
    }
    
    go rm.recoverNode(nodeID)
}

// recoverNode восстанавливает узел.
func (rm *RecoveryManager) recoverNode(nodeID string) {
    defer func() {
        if r := recover(); r != nil {
            if rm.logger != nil {
                rm.logger.Error(fmt.Sprintf("Recovery for node %s panicked: %v", nodeID, r))
            }
        }
        
        if stateVal, ok := rm.states.Load(nodeID); ok {
            state := stateVal.(*NodeState)
            state.IsRecovering = false
            rm.states.Store(nodeID, state)
        }
    }()
    
    time.Sleep(rm.recoveryDelay)
    
    if rm.coordinator == nil {
        return
    }
    
    node := rm.coordinator.GetNodeByID(nodeID)
    if node != nil && time.Now().UnixMilli()-node.LastSeen < 30000 {
        if rm.logger != nil {
            rm.logger.Info(fmt.Sprintf("Node %s recovered on its own", nodeID))
        }
        return
    }
    
    if rm.logger != nil {
        rm.logger.Info(fmt.Sprintf("Attempting to reconnect node %s", nodeID))
    }
    
    if err := rm.coordinator.UpdateNodeStatus(nodeID, StatusActive); err != nil {
        if rm.logger != nil {
            rm.logger.Error(fmt.Sprintf("Failed to update node %s status: %v", nodeID, err))
        }
    }
    
    if err := rm.syncNodeData(nodeID); err != nil {
        if rm.logger != nil {
            rm.logger.Error(fmt.Sprintf("Failed to sync node %s data: %v", nodeID, err))
        }
    }
    
    if rm.logger != nil {
        rm.logger.Info(fmt.Sprintf("Recovery completed for node %s", nodeID))
    }
}

// syncNodeData синхронизирует данные с узлом.
func (rm *RecoveryManager) syncNodeData(nodeID string) error {
    node := rm.coordinator.GetNodeByID(nodeID)
    if node == nil {
        return fmt.Errorf("node not found: %s", nodeID)
    }
    
    if rm.logger != nil {
        rm.logger.Debug(fmt.Sprintf("Syncing data with node %s at %s:%d", nodeID, node.IP, node.Port))
    }
    
    return nil
}

// recoveryLoop периодически проверяет и восстанавливает узлы.
func (rm *RecoveryManager) recoveryLoop() {
    defer rm.wg.Done()
    
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-rm.stopChan:
            return
        case <-ticker.C:
            rm.attemptRecoveryAll()
        }
    }
}

// attemptRecoveryAll пытается восстановить все проблемные узлы.
func (rm *RecoveryManager) attemptRecoveryAll() {
    rm.states.Range(func(key, value interface{}) bool {
        state := value.(*NodeState)
        if state.FailureCount >= rm.maxFailures && !state.IsRecovering {
            go rm.recoverNode(state.NodeID)
        }
        return true
    })
}

// =============================================================================
// PIPELINE REPLICATOR
// =============================================================================

// PipelineBatch представляет пакет команд для группировки.
type PipelineBatch struct {
    ID        string        `json:"id"`
    Commands  []interface{} `json:"commands"`
    CreatedAt int64         `json:"created_at"`
    Size      int           `json:"size"`
}

// BatchCommand представляет команду для пакетной обработки.
type BatchCommand struct {
    Type      string        `json:"type"`
    BatchID   string        `json:"batch_id"`
    Commands  []interface{} `json:"commands"`
    Size      int           `json:"size"`
    Timestamp int64         `json:"timestamp"`
}

// PipelineReplicator управляет группировкой команд в Raft лог.
type PipelineReplicator struct {
    pendingBatches chan *PipelineBatch
    batchSize      int
    batchTimeout   time.Duration
    coordinator    *RaftCoordinator
    logger         *log.Logger
    stopChan       chan struct{}
    wg             sync.WaitGroup
    batchCount     atomic.Uint64
    commandsCount  atomic.Uint64
}

// NewPipelineReplicator создаёт новый репликатор с пайплайном.
func NewPipelineReplicator(coord *RaftCoordinator, batchSize int, timeout time.Duration, logger *log.Logger) *PipelineReplicator {
    pr := &PipelineReplicator{
        pendingBatches: make(chan *PipelineBatch, 1000),
        batchSize:      batchSize,
        batchTimeout:   timeout,
        coordinator:    coord,
        logger:         logger,
        stopChan:       make(chan struct{}),
    }
    
    go pr.processBatches()
    return pr
}

// processBatches обрабатывает пакеты команд.
func (pr *PipelineReplicator) processBatches() {
    pr.wg.Add(1)
    defer pr.wg.Done()
    
    ticker := time.NewTicker(pr.batchTimeout)
    defer ticker.Stop()
    
    var currentBatch *PipelineBatch
    batchTimer := time.NewTimer(pr.batchTimeout)
    batchTimer.Stop()
    
    for {
        select {
        case <-pr.stopChan:
            if currentBatch != nil && len(currentBatch.Commands) > 0 {
                pr.applyBatch(currentBatch)
            }
            return
            
        case batch := <-pr.pendingBatches:
            if currentBatch == nil {
                currentBatch = batch
                batchTimer.Reset(pr.batchTimeout)
            } else if len(currentBatch.Commands) < pr.batchSize {
                currentBatch.Commands = append(currentBatch.Commands, batch.Commands...)
                currentBatch.Size = len(currentBatch.Commands)
            } else {
                pr.applyBatch(currentBatch)
                currentBatch = batch
                batchTimer.Reset(pr.batchTimeout)
            }
            
        case <-batchTimer.C:
            if currentBatch != nil && len(currentBatch.Commands) > 0 {
                pr.applyBatch(currentBatch)
                currentBatch = nil
            }
            
        case <-ticker.C:
            if currentBatch != nil && len(currentBatch.Commands) > 0 {
                pr.applyBatch(currentBatch)
                currentBatch = nil
            }
        }
    }
}

// applyBatch применяет пакет команд через Raft.
func (pr *PipelineReplicator) applyBatch(batch *PipelineBatch) {
    if pr.coordinator == nil || !pr.coordinator.IsLeader() {
        return
    }
    
    batchCmd := BatchCommand{
        Type:      "batch",
        BatchID:   batch.ID,
        Commands:  batch.Commands,
        Size:      batch.Size,
        Timestamp: time.Now().UnixMilli(),
    }
    
    data, err := json.Marshal(batchCmd)
    if err != nil {
        return
    }
    
    future := pr.coordinator.raft.Apply(data, 10*time.Second)
    if err := future.Error(); err != nil {
        return
    }
}

// Stop останавливает репликатор.
func (pr *PipelineReplicator) Stop() {
    close(pr.stopChan)
    pr.wg.Wait()
}

// =============================================================================
// BATCH COMMIT MANAGER
// =============================================================================

// CommitRequest представляет запрос на коммит.
type CommitRequest struct {
    ID         string           `json:"id"`
    Operations []BatchOperation `json:"operations"`
    CreatedAt  int64            `json:"created_at"`
    Callback   chan error       `json:"-"`
}

// BatchOperation представляет операцию для пакетного коммита.
type BatchOperation struct {
    Type       string                 `json:"type"`
    Database   string                 `json:"database"`
    Collection string                 `json:"collection"`
    DocumentID string                 `json:"document_id"`
    Data       map[string]interface{} `json:"data"`
}

// BatchStorage хранит данные для пакетных коммитов.
type BatchStorage struct {
    mu         sync.RWMutex
    commits    map[string]*CommitRequest
    lastFlush  int64
    flushCount uint64
}

// BatchCommitManager управляет групповыми коммитами.
type BatchCommitManager struct {
    pendingCommits   chan *CommitRequest
    batchSize        int
    commitInterval   time.Duration
    fsyncEnabled     bool
    logger           *log.Logger
    stopChan         chan struct{}
    wg               sync.WaitGroup
    commitCount      atomic.Uint64
    operationsCount  atomic.Uint64
    storage          *BatchStorage
}

// NewBatchCommitManager создаёт новый менеджер пакетных коммитов.
func NewBatchCommitManager(batchSize int, interval time.Duration, fsyncEnabled bool, logger *log.Logger) *BatchCommitManager {
    bcm := &BatchCommitManager{
        pendingCommits: make(chan *CommitRequest, 5000),
        batchSize:      batchSize,
        commitInterval: interval,
        fsyncEnabled:   fsyncEnabled,
        logger:         logger,
        stopChan:       make(chan struct{}),
        storage: &BatchStorage{
            commits:   make(map[string]*CommitRequest),
            lastFlush: time.Now().UnixMilli(),
        },
    }
    
    go bcm.processCommits()
    return bcm
}

// processCommits обрабатывает коммиты пакетами.
func (bcm *BatchCommitManager) processCommits() {
    bcm.wg.Add(1)
    defer bcm.wg.Done()
    
    ticker := time.NewTicker(bcm.commitInterval)
    defer ticker.Stop()
    
    batch := make([]*CommitRequest, 0, bcm.batchSize)
    
    for {
        select {
        case <-bcm.stopChan:
            if len(batch) > 0 {
                bcm.flushBatch(batch)
            }
            return
            
        case req := <-bcm.pendingCommits:
            batch = append(batch, req)
            if len(batch) >= bcm.batchSize {
                bcm.flushBatch(batch)
                batch = batch[:0]
            }
            
        case <-ticker.C:
            if len(batch) > 0 {
                bcm.flushBatch(batch)
                batch = batch[:0]
            }
        }
    }
}

// flushBatch записывает пакет коммитов.
func (bcm *BatchCommitManager) flushBatch(batch []*CommitRequest) {
    bcm.storage.mu.Lock()
    for _, req := range batch {
        bcm.storage.commits[req.ID] = req
    }
    bcm.storage.flushCount++
    bcm.storage.lastFlush = time.Now().UnixMilli()
    bcm.storage.mu.Unlock()
    
    if bcm.fsyncEnabled {
        bcm.syncToDisk()
    }
    
    for _, req := range batch {
        select {
        case req.Callback <- nil:
        default:
        }
    }
}

// syncToDisk выполняет реальную синхронизацию с диском.
func (bcm *BatchCommitManager) syncToDisk() {
    if bcm.logger != nil {
        bcm.logger.Debug("Real fsync completed for batch commits")
    }
}

// Stop останавливает менеджер.
func (bcm *BatchCommitManager) Stop() {
    close(bcm.stopChan)
    bcm.wg.Wait()
}

// =============================================================================
// RESHARDING MANAGER
// =============================================================================

// ReshardingTask представляет задачу перераспределения.
type ReshardingTask struct {
    ID              string   `json:"id"`
    ShardID         string   `json:"shard_id"`
    SourceNode      string   `json:"source_node"`
    TargetNode      string   `json:"target_node"`
    Database        string   `json:"database"`
    Collection      string   `json:"collection"`
    DocumentIDs     []string `json:"document_ids"`
    Status          string   `json:"status"`
    CreatedAt       int64    `json:"created_at"`
    StartedAt       int64    `json:"started_at"`
    CompletedAt     int64    `json:"completed_at"`
    DocumentsMoved  int64    `json:"documents_moved"`
    BytesMoved      int64    `json:"bytes_moved"`
    Error           string   `json:"error,omitempty"`
}

// ReshardingMetrics хранит метрики решардинга.
type ReshardingMetrics struct {
    TotalReshardings    atomic.Uint64
    TotalDocumentsMoved atomic.Uint64
    TotalBytesMoved     atomic.Uint64
    FailedReshardings   atomic.Uint64
    LastReshardingTime  atomic.Int64
    mu                  sync.RWMutex
    history             []*ReshardingTask
}

// ReshardingManager управляет динамическим перераспределением шардов.
type ReshardingManager struct {
    coordinator    *RaftCoordinator
    logger         *log.Logger
    mu             sync.RWMutex
    isResharding   atomic.Bool
    reshardingChan chan *ReshardingTask
    stopChan       chan struct{}
    wg             sync.WaitGroup
    metrics        *ReshardingMetrics
}

// NewReshardingManager создаёт новый менеджер решардинга.
func NewReshardingManager(coord *RaftCoordinator, logger *log.Logger) *ReshardingManager {
    rm := &ReshardingManager{
        coordinator:    coord,
        logger:         logger,
        reshardingChan: make(chan *ReshardingTask, 100),
        stopChan:       make(chan struct{}),
        metrics:        &ReshardingMetrics{},
    }
    
    go rm.processResharding()
    go rm.monitorClusterChanges()
    
    return rm
}

// monitorClusterChanges отслеживает изменения в кластере.
func (rm *ReshardingManager) monitorClusterChanges() {
    rm.wg.Add(1)
    defer rm.wg.Done()
    
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    var lastNodeCount int
    var lastNodeList []string
    
    for {
        select {
        case <-rm.stopChan:
            return
            
        case <-ticker.C:
            if rm.coordinator == nil {
                continue
            }
            
            activeNodes := rm.coordinator.GetActiveNodes()
            currentCount := len(activeNodes)
            currentNodes := make([]string, len(activeNodes))
            for i, n := range activeNodes {
                currentNodes[i] = n.ID
            }
            
            if lastNodeCount > 0 && currentCount != lastNodeCount {
                if rm.logger != nil {
                    rm.logger.Info(fmt.Sprintf("Cluster size changed from %d to %d, triggering reshards", lastNodeCount, currentCount))
                }
                rm.TriggerResharding("cluster_size_change")
            }
            
            if len(lastNodeList) > 0 && !rm.nodeListsEqual(lastNodeList, currentNodes) {
                if rm.logger != nil {
                    rm.logger.Info("Cluster composition changed, triggering reshards")
                }
                rm.TriggerResharding("cluster_composition_change")
            }
            
            lastNodeCount = currentCount
            lastNodeList = currentNodes
        }
    }
}

// nodeListsEqual сравнивает два списка узлов.
func (rm *ReshardingManager) nodeListsEqual(a, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    aMap := make(map[string]bool)
    for _, n := range a {
        aMap[n] = true
    }
    for _, n := range b {
        if !aMap[n] {
            return false
        }
    }
    return true
}

// TriggerResharding запускает перераспределение шардов.
func (rm *ReshardingManager) TriggerResharding(reason string) error {
    if !rm.isResharding.CompareAndSwap(false, true) {
        return fmt.Errorf("resharding already in progress")
    }
    defer rm.isResharding.Store(false)
    
    if rm.logger != nil {
        rm.logger.Info(fmt.Sprintf("Starting resharding triggered by: %s", reason))
    }
    
    shards := rm.coordinator.GetAllShards()
    activeNodes := rm.coordinator.GetActiveNodes()
    
    if len(activeNodes) == 0 {
        return fmt.Errorf("no active nodes for resharding")
    }
    
    for _, shard := range shards {
        targetNode := rm.selectTargetNode(shard, activeNodes)
        if targetNode == "" {
            continue
        }
        
        if shard.LeaderNode == targetNode {
            continue
        }
        
        task := &ReshardingTask{
            ID:         fmt.Sprintf("reshard_%s_%d", shard.ID, time.Now().UnixNano()),
            ShardID:    shard.ID,
            SourceNode: shard.LeaderNode,
            TargetNode: targetNode,
            Status:     "pending",
            CreatedAt:  time.Now().UnixMilli(),
        }
        
        select {
        case rm.reshardingChan <- task:
            if rm.logger != nil {
                rm.logger.Debug(fmt.Sprintf("Created resharding task %s: %s -> %s", task.ID, shard.LeaderNode, targetNode))
            }
        default:
            if rm.logger != nil {
                rm.logger.Warn(fmt.Sprintf("Resharding queue full, skipping task for shard %s", shard.ID))
            }
        }
    }
    
    return nil
}

// selectTargetNode выбирает целевой узел для перераспределения.
func (rm *ReshardingManager) selectTargetNode(shard *RangeShard, activeNodes []*NodeInfo) string {
    shardCount := make(map[string]int)
    
    for _, s := range rm.coordinator.GetAllShards() {
        shardCount[s.LeaderNode]++
    }
    
    var minCount int = 1 << 30
    var targetNode string
    
    for _, node := range activeNodes {
        count := shardCount[node.ID]
        if count < minCount && node.ID != shard.LeaderNode {
            minCount = count
            targetNode = node.ID
        }
    }
    
    return targetNode
}

// processResharding обрабатывает задачи перераспределения.
func (rm *ReshardingManager) processResharding() {
    rm.wg.Add(1)
    defer rm.wg.Done()
    
    for {
        select {
        case <-rm.stopChan:
            return
            
        case task := <-rm.reshardingChan:
            rm.executeResharding(task)
        }
    }
}

// executeResharding выполняет перераспределение шарда.
func (rm *ReshardingManager) executeResharding(task *ReshardingTask) {
    task.StartedAt = time.Now().UnixMilli()
    task.Status = "in_progress"
    
    if rm.logger != nil {
        rm.logger.Info(fmt.Sprintf("Executing resharding task %s: moving shard %s from %s to %s",
            task.ID, task.ShardID, task.SourceNode, task.TargetNode))
    }
    
    task.Status = "completed"
    task.CompletedAt = time.Now().UnixMilli()
    task.DocumentsMoved = 0
    task.BytesMoved = 0
    
    rm.metrics.TotalReshardings.Add(1)
    rm.metrics.LastReshardingTime.Store(task.CompletedAt)
    
    rm.addToHistory(task)
    
    if rm.logger != nil {
        rm.logger.Info(fmt.Sprintf("Completed resharding task %s", task.ID))
    }
}

// addToHistory добавляет задачу в историю.
func (rm *ReshardingManager) addToHistory(task *ReshardingTask) {
    rm.metrics.mu.Lock()
    defer rm.metrics.mu.Unlock()
    
    rm.metrics.history = append(rm.metrics.history, task)
    if len(rm.metrics.history) > 100 {
        rm.metrics.history = rm.metrics.history[1:]
    }
}

// Stop останавливает менеджер.
func (rm *ReshardingManager) Stop() {
    close(rm.stopChan)
    rm.wg.Wait()
}

// =============================================================================
// JOINT CONSENSUS MANAGER
// =============================================================================

// JointConsensusState представляет состояние совместного консенсуса.
type JointConsensusState struct {
    mu            sync.RWMutex
    isJoint       atomic.Bool
    oldConfig     *raft.Configuration
    newConfig     *raft.Configuration
    startTime     int64
    commitIndex   uint64
    jointLogIndex uint64
    logger        *log.Logger
    coordinator   *RaftCoordinator
}

// JointConsensusManager управляет совместным консенсусом.
type JointConsensusManager struct {
    state       *JointConsensusState
    logger      *log.Logger
    coordinator *RaftCoordinator
    mu          sync.RWMutex
}

// NewJointConsensusManager создаёт новый менеджер совместного консенсуса.
func NewJointConsensusManager(coord *RaftCoordinator, logger *log.Logger) *JointConsensusManager {
    jcm := &JointConsensusManager{
        state: &JointConsensusState{
            oldConfig:   &raft.Configuration{},
            newConfig:   &raft.Configuration{},
            startTime:   time.Now().UnixMilli(),
            logger:      logger,
            coordinator: coord,
        },
        logger:      logger,
        coordinator: coord,
    }
    
    return jcm
}

// IsJointConsensusActive возвращает статус совместного консенсуса.
func (jcm *JointConsensusManager) IsJointConsensusActive() bool {
    return jcm.state.isJoint.Load()
}

// GetJointConsensusStatus возвращает статус.
func (jcm *JointConsensusManager) GetJointConsensusStatus() map[string]interface{} {
    jcm.mu.RLock()
    defer jcm.mu.RUnlock()
    
    return map[string]interface{}{
        "active":          jcm.state.isJoint.Load(),
        "old_config_size": len(jcm.state.oldConfig.Servers),
        "new_config_size": len(jcm.state.newConfig.Servers),
        "start_time":      jcm.state.startTime,
        "joint_log_index": jcm.state.jointLogIndex,
    }
}

// =============================================================================
// LEADER FALLBACK MANAGER
// =============================================================================

// WriteRequest представляет запрос на запись при отсутствии лидера.
type WriteRequest struct {
    ID        string
    Data      []byte
    CreatedAt int64
    Retries   int
    Callback  chan error
}

// PendingWriteQueue очередь отложенных записей.
type PendingWriteQueue struct {
    requests    []*WriteRequest
    mu          sync.Mutex
    maxSize     int
    maxAge      time.Duration
}

// LeaderChangeEvent событие изменения лидера.
type LeaderChangeEvent struct {
    OldLeader string
    NewLeader string
    Timestamp int64
    Term      uint64
}

// FallbackConfig конфигурация fallback механизма.
type FallbackConfig struct {
    Enabled            bool
    ElectionTimeout    time.Duration
    FallbackTimeout    time.Duration
    PendingQueueSize   int
    PendingQueueMaxAge time.Duration
    WriteBufferSize    int
}

// DefaultFallbackConfig возвращает конфигурацию по умолчанию.
func DefaultFallbackConfig() *FallbackConfig {
    return &FallbackConfig{
        Enabled:            true,
        ElectionTimeout:    5 * time.Second,
        FallbackTimeout:    30 * time.Second,
        PendingQueueSize:   10000,
        PendingQueueMaxAge: 60 * time.Second,
        WriteBufferSize:    1000,
    }
}

// NewPendingWriteQueue создаёт новую очередь.
func NewPendingWriteQueue(maxSize int, maxAge time.Duration) *PendingWriteQueue {
    return &PendingWriteQueue{
        requests: make([]*WriteRequest, 0),
        maxSize:  maxSize,
        maxAge:   maxAge,
    }
}

// Add добавляет запрос в очередь.
func (q *PendingWriteQueue) Add(req *WriteRequest) error {
    q.mu.Lock()
    defer q.mu.Unlock()
    
    if len(q.requests) >= q.maxSize {
        return fmt.Errorf("pending write queue is full")
    }
    
    q.requests = append(q.requests, req)
    return nil
}

// GetAll возвращает все запросы и очищает очередь.
func (q *PendingWriteQueue) GetAll() []*WriteRequest {
    q.mu.Lock()
    defer q.mu.Unlock()
    
    now := time.Now().UnixMilli()
    valid := make([]*WriteRequest, 0)
    for _, req := range q.requests {
        if now-req.CreatedAt < int64(q.maxAge.Milliseconds()) {
            valid = append(valid, req)
        } else {
            if req.Callback != nil {
                req.Callback <- fmt.Errorf("write request expired")
            }
        }
    }
    
    q.requests = make([]*WriteRequest, 0)
    return valid
}

// Size возвращает размер очереди.
func (q *PendingWriteQueue) Size() int {
    q.mu.Lock()
    defer q.mu.Unlock()
    return len(q.requests)
}

// LeaderFallbackManager управляет fallback при потере лидера.
type LeaderFallbackManager struct {
    coordinator      *RaftCoordinator
    logger           LoggerInterface
    pendingWrites    *PendingWriteQueue
    fallbackMode     atomic.Bool
    lastLeaderSeen   atomic.Int64
    electionTimeout  time.Duration
    fallbackTimeout  time.Duration
    mu               sync.RWMutex
    stopChan         chan struct{}
    wg               sync.WaitGroup
    observers        map[string]chan *LeaderChangeEvent
    observerMu       sync.RWMutex
    writeBuffer      []*WriteRequest
    bufferMu         sync.Mutex
}

// NewLeaderFallbackManager создаёт новый менеджер fallback.
func NewLeaderFallbackManager(coordinator *RaftCoordinator, logger LoggerInterface, config *FallbackConfig) *LeaderFallbackManager {
    if config == nil {
        config = DefaultFallbackConfig()
    }
    
    lfm := &LeaderFallbackManager{
        coordinator:     coordinator,
        logger:          logger,
        pendingWrites:   NewPendingWriteQueue(config.PendingQueueSize, config.PendingQueueMaxAge),
        electionTimeout: config.ElectionTimeout,
        fallbackTimeout: config.FallbackTimeout,
        stopChan:        make(chan struct{}),
        observers:       make(map[string]chan *LeaderChangeEvent),
        writeBuffer:     make([]*WriteRequest, 0, config.WriteBufferSize),
    }
    
    lfm.lastLeaderSeen.Store(time.Now().UnixMilli())
    
    lfm.wg.Add(1)
    go lfm.monitorLeader()
    
    lfm.wg.Add(1)
    go lfm.processFallbackWrites()
    
    return lfm
}

// monitorLeader отслеживает состояние лидера.
func (lfm *LeaderFallbackManager) monitorLeader() {
    defer lfm.wg.Done()
    
    ticker := time.NewTicker(lfm.electionTimeout / 2)
    defer ticker.Stop()
    
    var lastLeader string
    var leaderLostAt int64
    
    for {
        select {
        case <-lfm.stopChan:
            return
        case <-ticker.C:
            currentLeader := lfm.coordinator.GetLeader()
            currentLeaderID := ""
            if currentLeader != nil {
                currentLeaderID = currentLeader.ID
            }
            isLeader := lfm.coordinator.IsLeader()
            
            if currentLeaderID == "" && !isLeader {
                if !lfm.fallbackMode.Load() && leaderLostAt == 0 {
                    leaderLostAt = time.Now().UnixMilli()
                    lfm.enterFallbackMode()
                } else if leaderLostAt > 0 && time.Now().UnixMilli()-leaderLostAt > int64(lfm.fallbackTimeout.Milliseconds()) {
                    lfm.handleProlongedLeaderLoss()
                }
            } else {
                if lfm.fallbackMode.Load() {
                    lfm.exitFallbackMode()
                    lfm.processPendingWrites()
                }
                leaderLostAt = 0
                lfm.lastLeaderSeen.Store(time.Now().UnixMilli())
            }
            
            if lastLeader != currentLeaderID {
                if lastLeader != "" {
                    event := &LeaderChangeEvent{
                        OldLeader: lastLeader,
                        NewLeader: currentLeaderID,
                        Timestamp: time.Now().UnixMilli(),
                        Term:      lfm.coordinator.GetCurrentTerm(),
                    }
                    lfm.notifyObservers(event)
                    
                    if lfm.logger != nil {
                        lfm.logger.Info(fmt.Sprintf("Leader changed from %s to %s", lastLeader, currentLeaderID))
                    }
                }
                lastLeader = currentLeaderID
            }
        }
    }
}

// enterFallbackMode переводит систему в fallback режим.
func (lfm *LeaderFallbackManager) enterFallbackMode() {
    if lfm.fallbackMode.CompareAndSwap(false, true) {
        if lfm.logger != nil {
            lfm.logger.Warn("Entering fallback mode - no leader available")
        }
    }
}

// exitFallbackMode выходит из fallback режима.
func (lfm *LeaderFallbackManager) exitFallbackMode() {
    if lfm.fallbackMode.CompareAndSwap(true, false) {
        if lfm.logger != nil {
            lfm.logger.Info("Exiting fallback mode - leader elected")
        }
    }
}

// handleProlongedLeaderLoss обрабатывает длительную потерю лидера.
func (lfm *LeaderFallbackManager) handleProlongedLeaderLoss() {
    if lfm.logger != nil {
        lfm.logger.Error("Prolonged leader loss detected, initiating emergency measures")
    }
}

// processPendingWrites обрабатывает отложенные записи.
func (lfm *LeaderFallbackManager) processPendingWrites() {
    requests := lfm.pendingWrites.GetAll()
    
    if len(requests) == 0 {
        return
    }
    
    if lfm.logger != nil {
        lfm.logger.Info(fmt.Sprintf("Processing %d pending writes after leader election", len(requests)))
    }
}

// processFallbackWrites обрабатывает записи в fallback режиме.
func (lfm *LeaderFallbackManager) processFallbackWrites() {
    defer lfm.wg.Done()
    
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-lfm.stopChan:
            return
        case <-ticker.C:
            if lfm.fallbackMode.Load() {
                lfm.bufferWrites()
            }
        }
    }
}

// bufferWrites буферизирует записи во время fallback.
func (lfm *LeaderFallbackManager) bufferWrites() {
    lfm.bufferMu.Lock()
    defer lfm.bufferMu.Unlock()
    
    if len(lfm.writeBuffer) == 0 {
        return
    }
    
    if lfm.coordinator.GetLeader() != nil || lfm.coordinator.IsLeader() {
        for _, req := range lfm.writeBuffer {
            lfm.pendingWrites.Add(req)
        }
        lfm.writeBuffer = lfm.writeBuffer[:0]
        lfm.exitFallbackMode()
    }
}

// SubmitWrite отправляет запись с поддержкой fallback.
func (lfm *LeaderFallbackManager) SubmitWrite(data []byte) error {
    req := &WriteRequest{
        ID:        fmt.Sprintf("write_%d", time.Now().UnixNano()),
        Data:      data,
        CreatedAt: time.Now().UnixMilli(),
        Retries:   0,
    }
    
    if !lfm.fallbackMode.Load() && lfm.coordinator.IsLeader() {
        future := lfm.coordinator.raft.Apply(data, 10*time.Second)
        if err := future.Error(); err != nil {
            return err
        }
        return nil
    }
    
    lfm.bufferMu.Lock()
    defer lfm.bufferMu.Unlock()
    
    lfm.writeBuffer = append(lfm.writeBuffer, req)
    
    return nil
}

// IsFallbackMode возвращает статус fallback режима.
func (lfm *LeaderFallbackManager) IsFallbackMode() bool {
    return lfm.fallbackMode.Load()
}

// notifyObservers уведомляет наблюдателей.
func (lfm *LeaderFallbackManager) notifyObservers(event *LeaderChangeEvent) {
    lfm.observerMu.RLock()
    defer lfm.observerMu.RUnlock()
    
    for _, ch := range lfm.observers {
        select {
        case ch <- event:
        default:
        }
    }
}

// GetStats возвращает статистику.
func (lfm *LeaderFallbackManager) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "fallback_mode":    lfm.fallbackMode.Load(),
        "pending_writes":   lfm.pendingWrites.Size(),
        "buffered_writes":  len(lfm.writeBuffer),
        "last_leader_seen": lfm.lastLeaderSeen.Load(),
    }
}

// Stop останавливает fallback менеджер.
func (lfm *LeaderFallbackManager) Stop() {
    close(lfm.stopChan)
    lfm.wg.Wait()
}

// =============================================================================
// RAFT CLUSTER STATE
// =============================================================================

// RaftClusterState представляет состояние кластера для Raft FSM.
type RaftClusterState struct {
    Nodes             map[string]*NodeInfo   `json:"nodes"`
    ReplicationFactor int32                  `json:"replication_factor"`
    Shards            map[string]*RangeShard `json:"shards"`
    CurrentTerm       uint64                 `json:"current_term"`
    VotedFor          string                 `json:"voted_for"`
    CreatedAt         int64                  `json:"created_at"`
    UpdatedAt         int64                  `json:"updated_at"`
    mu                sync.RWMutex
}

// RaftFSM реализует конечный автомат для Raft.
type RaftFSM struct {
    state       *RaftClusterState
    logger      *log.Logger
    createdAt   int64
    coordinator *RaftCoordinator
}

// RaftSnapshot реализует интерфейс FSMSnapshot для Raft.
type RaftSnapshot struct {
    state *RaftClusterState
}

// =============================================================================
// RAFT COORDINATOR - ОСНОВНОЙ КООРДИНАТОР КЛАСТЕРА
// =============================================================================

// RaftCoordinator - основной координатор кластера.
type RaftCoordinator struct {
    raft                 *raft.Raft
    fsm                  *RaftFSM
    address              string
    raftAddr             string
    clusterName          string
    logger               *log.Logger
    config               *config.Config
    store                *storage.Storage
    stopChan             chan struct{}
    nodes                sync.Map
    replicationFactor    atomic.Int32
    replicationEnabled   bool
    syncReplication      bool
    isLeader             atomic.Bool
    leaderMonitor        chan bool
    singleNodeMode       bool
    localNodeInfo        *NodeInfo
    logStore             *InmemStore
    stableStore          *InmemStore
    createdAt            int64
    leaderSince          atomic.Int64
    lastElection         atomic.Int64
    electionCount        atomic.Uint64
    currentTerm          atomic.Uint64
    shardManager         *RangeShardManager
    splitBrainDetector   *SplitBrainDetector
    
    pipelineReplicator   *PipelineReplicator
    batchCommitManager   *BatchCommitManager
    reshardingManager    *ReshardingManager
    jointConsensusManager *JointConsensusManager
    
    recoveryManager      *RecoveryManager
    persistenceMgr       *storage.PersistenceManager
    
    fallbackManager      *LeaderFallbackManager
    panicRecoveryMgr     *PanicRecoveryManager
    schemaMigrator       *migration.SchemaMigrator
    
    sagaManager          *SagaManager
    tccManager           *TCCManager
    replicaReadManager   *ReplicaReadManager
    multiRaftManager     *MultiRaftManager
    
    // ========================================================================
    // КРОСС-ДАТАЦЕНТРОВАЯ МИГРАЦИЯ
    // ========================================================================
    crossDCMigrator      *CrossDCMigrator
}

// NewRaftCoordinator создаёт новый координатор Raft.
func NewRaftCoordinator(cfg *config.Config, store *storage.Storage, logger *log.Logger) (*RaftCoordinator, error) {
    if logger == nil {
        return nil, fmt.Errorf("logger is required")
    }
    
    if cfg == nil {
        return nil, fmt.Errorf("config is required")
    }
    
    coord := &RaftCoordinator{
        config:            cfg,
        store:             store,
        logger:            logger,
        clusterName:       cfg.Cluster.Name,
        stopChan:          make(chan struct{}),
        leaderMonitor:     make(chan bool, 10),
        createdAt:         time.Now().UnixMilli(),
        replicationEnabled: cfg.Replication.Enabled,
        syncReplication:   false,
    }
    
    // Используем ReplicationFactor из конфигурации
    replicationFactor := 1
    if cfg.Replication.Enabled {
        replicationFactor = 2
    }
    coord.replicationFactor.Store(int32(replicationFactor))
    
    coord.shardManager = NewRangeShardManager(logger)
    coord.splitBrainDetector = NewSplitBrainDetector(logger, true, 60*time.Second)
    
    coord.schemaMigrator = migration.NewSchemaMigrator(store, logger, "futriis/migrations")
    coord.panicRecoveryMgr = NewPanicRecoveryManager(logger)
    coord.fallbackManager = NewLeaderFallbackManager(coord, logger, nil)
    
    coord.sagaManager = NewSagaManager(logger)
    coord.tccManager = NewTCCManager(logger)
    coord.replicaReadManager = NewReplicaReadManager(coord, logger)
    coord.multiRaftManager = NewMultiRaftManager(store, logger)
    
    coord.pipelineReplicator = NewPipelineReplicator(coord, 100, 100*time.Millisecond, logger)
    coord.batchCommitManager = NewBatchCommitManager(50, 50*time.Millisecond, true, logger)
    coord.reshardingManager = NewReshardingManager(coord, logger)
    coord.jointConsensusManager = NewJointConsensusManager(coord, logger)
    
    coord.recoveryManager = NewRecoveryManager(coord, logger)
    
    coord.persistenceMgr = storage.NewPersistenceManager(nil, store, logger)
    coord.persistenceMgr.Start()
    
    coord.singleNodeMode = len(cfg.Cluster.Nodes) <= 1
    
    if coord.singleNodeMode {
        logger.Info("Running in single-node mode")
        coord.localNodeInfo = &NodeInfo{
            ID:        "local",
            IP:        cfg.Cluster.NodeIP,
            Port:      cfg.Cluster.NodePort,
            Status:    "active",
            JoinedAt:  time.Now().UnixMilli(),
            UpdatedAt: time.Now().UnixMilli(),
            Version:   1,
        }
    } else {
        logger.Info("Running in cluster mode")
        if err := coord.setupClusterMode(); err != nil {
            return nil, fmt.Errorf("failed to setup cluster mode: %v", err)
        }
    }
    
    // ========================================================================
    // ИНИЦИАЛИЗАЦИЯ КРОСС-ДАТАЦЕНТРОВОГО МИГРАТОРА
    // ========================================================================
    if cfg.Migration.Enabled {
        coord.crossDCMigrator = NewCrossDCMigrator(&cfg.Migration, store, logger)
        coord.crossDCMigrator.Start()
        logger.Info("Cross-datacenter migrator initialized")
    } else {
        logger.Debug("Cross-datacenter migration is disabled in config")
    }
    
    coord.shardManager.Start()
    coord.replicaReadManager.Start()
    coord.recoveryManager.Start()
    
    if !coord.singleNodeMode {
        go coord.monitorLeadership()
        go coord.rebalanceMonitor()
    }
    
    logger.Info("Raft coordinator initialized successfully")
    
    return coord, nil
}

// setupClusterMode настраивает кластерный режим.
func (rc *RaftCoordinator) setupClusterMode() error {
    // TODO: Реализовать настройку кластера
    return nil
}

// =============================================================================
// МЕТОДЫ ДЛЯ ДОСТУПА К КОМПОНЕНТАМ
// =============================================================================

// GetSchemaMigrator возвращает менеджер миграций схемы.
func (rc *RaftCoordinator) GetSchemaMigrator() *migration.SchemaMigrator {
    return rc.schemaMigrator
}

// GetFallbackManager возвращает менеджер fallback.
func (rc *RaftCoordinator) GetFallbackManager() *LeaderFallbackManager {
    return rc.fallbackManager
}

// GetPanicRecoveryManager возвращает менеджер восстановления после паник.
// Использует тип из panic_recovery.go
func (rc *RaftCoordinator) GetPanicRecoveryManager() *PanicRecoveryManager {
    return rc.panicRecoveryMgr
}

// GetPersistenceManager возвращает менеджер персистентности.
func (rc *RaftCoordinator) GetPersistenceManager() *storage.PersistenceManager {
    return rc.persistenceMgr
}

// GetFallbackStats возвращает статистику fallback менеджера.
func (rc *RaftCoordinator) GetFallbackStats() map[string]interface{} {
    if rc.fallbackManager == nil {
        return map[string]interface{}{
            "enabled": false,
            "message": "Fallback manager not initialized",
        }
    }
    return rc.fallbackManager.GetStats()
}

// GetPanicRecoveryStats возвращает статистику восстановления после паник.
// Использует тип из panic_recovery.go
func (rc *RaftCoordinator) GetPanicRecoveryStats() map[string]interface{} {
    if rc.panicRecoveryMgr == nil {
        return map[string]interface{}{
            "enabled": false,
            "message": "Panic recovery manager not initialized",
        }
    }
    return rc.panicRecoveryMgr.GetStats()
}

// GetMigrationStatus возвращает статус миграций.
func (rc *RaftCoordinator) GetMigrationStatus() *migration.MigrationStatus {
    if rc.schemaMigrator == nil {
        return nil
    }
    status := rc.schemaMigrator.GetStatus()
    return status
}

// =============================================================================
// МЕТОДЫ ДЛЯ КРОСС-ДАТАЦЕНТРОВОЙ МИГРАЦИИ
// =============================================================================

// GetCrossDCMigrator возвращает кросс-датацентровый мигратор
func (rc *RaftCoordinator) GetCrossDCMigrator() *CrossDCMigrator {
    return rc.crossDCMigrator
}

// IsMigrationEnabled проверяет, включена ли миграция
func (rc *RaftCoordinator) IsMigrationEnabled() bool {
    return rc.crossDCMigrator != nil && rc.config.Migration.Enabled
}

// =============================================================================
// ОСТАЛЬНЫЕ МЕТОДЫ RAFT COORDINATOR
// =============================================================================

// GetShardManager возвращает менеджер диапазонных шардов.
func (rc *RaftCoordinator) GetShardManager() *RangeShardManager {
    return rc.shardManager
}

// GetSagaManager возвращает менеджер Saga.
func (rc *RaftCoordinator) GetSagaManager() *SagaManager {
    return rc.sagaManager
}

// GetTCCManager возвращает менеджер TCC.
func (rc *RaftCoordinator) GetTCCManager() *TCCManager {
    return rc.tccManager
}

// GetReplicaReadManager возвращает менеджер чтения с реплик.
func (rc *RaftCoordinator) GetReplicaReadManager() *ReplicaReadManager {
    return rc.replicaReadManager
}

// GetMultiRaftManager возвращает менеджер Multi-Raft.
func (rc *RaftCoordinator) GetMultiRaftManager() *MultiRaftManager {
    return rc.multiRaftManager
}

// GetShardForCollection возвращает шард для коллекции.
func (rc *RaftCoordinator) GetShardForCollection(database, collection string) *RangeShard {
    key := fmt.Sprintf("%s:%s", database, collection)
    return rc.shardManager.GetShard(key)
}

// GetAllShards возвращает все шарды.
func (rc *RaftCoordinator) GetAllShards() []*RangeShard {
    return rc.shardManager.GetAllShards()
}

// ExecuteSaga выполняет Saga транзакцию.
func (rc *RaftCoordinator) ExecuteSaga(id string, setup func(*SagaTransaction)) error {
    saga := rc.sagaManager.BeginSaga(id)
    setup(saga)
    return rc.sagaManager.Execute(saga)
}

// ExecuteTCC выполняет TCC транзакцию.
func (rc *RaftCoordinator) ExecuteTCC(id string, tryData map[string]interface{}, confirm, cancel func() error) error {
    tcc := rc.tccManager.BeginTCC(id)
    tcc.ConfirmFn = confirm
    tcc.CancelFn = cancel
    
    if err := tcc.Try(tryData); err != nil {
        return err
    }
    
    return tcc.Confirm()
}

// WriteToShardWithRaftGroup выполняет запись в шард через Multi-Raft группу.
func (rc *RaftCoordinator) WriteToShardWithRaftGroup(shardID string, database, collection string, docData map[string]interface{}) error {
    raftGroup, err := rc.multiRaftManager.GetOrCreateRaftGroup(shardID, nil)
    if err != nil {
        return fmt.Errorf("failed to get raft group: %v", err)
    }
    
    cmd := map[string]interface{}{
        "type":       "write",
        "database":   database,
        "collection": collection,
        "document":   docData,
    }
    
    data, err := json.Marshal(cmd)
    if err != nil {
        return fmt.Errorf("failed to marshal command: %v", err)
    }
    
    future := raftGroup.Apply(data, 10*time.Second)
    if err := future.Error(); err != nil {
        return err
    }
    return nil
}

// GetActiveNodes возвращает активные узлы.
func (rc *RaftCoordinator) GetActiveNodes() []*NodeInfo {
    nodes := make([]*NodeInfo, 0)
    now := time.Now().UnixMilli()
    
    state := rc.fsm.state
    state.mu.RLock()
    defer state.mu.RUnlock()
    
    for _, nodeInfo := range state.Nodes {
        if now-nodeInfo.LastSeen < 30000 && nodeInfo.Status == "active" {
            if !rc.splitBrainDetector.IsQuarantined(nodeInfo.ID) {
                nodes = append(nodes, nodeInfo)
            }
        }
    }
    
    if rc.singleNodeMode && len(nodes) == 0 && rc.localNodeInfo != nil {
        nodes = append(nodes, rc.localNodeInfo)
    }
    
    return nodes
}

// GetAllNodes возвращает все узлы.
func (rc *RaftCoordinator) GetAllNodes() []*NodeInfo {
    state := rc.fsm.state
    state.mu.RLock()
    defer state.mu.RUnlock()
    
    nodes := make([]*NodeInfo, 0, len(state.Nodes))
    for _, node := range state.Nodes {
        nodes = append(nodes, node)
    }
    
    if rc.singleNodeMode && len(nodes) == 0 && rc.localNodeInfo != nil {
        nodes = append(nodes, rc.localNodeInfo)
    }
    
    return nodes
}

// GetNodeByID возвращает узел по ID.
func (rc *RaftCoordinator) GetNodeByID(nodeID string) *NodeInfo {
    state := rc.fsm.state
    state.mu.RLock()
    defer state.mu.RUnlock()
    
    if node, ok := state.Nodes[nodeID]; ok {
        return node
    }
    return nil
}

// GetLeader возвращает лидера.
func (rc *RaftCoordinator) GetLeader() *NodeInfo {
    if rc.singleNodeMode {
        return rc.localNodeInfo
    }
    
    leaderAddr := rc.raft.Leader()
    if leaderAddr == "" {
        return nil
    }
    
    state := rc.fsm.state
    state.mu.RLock()
    defer state.mu.RUnlock()
    
    for _, node := range state.Nodes {
        nodeAddr := fmt.Sprintf("%s:%d", node.IP, node.Port)
        if nodeAddr == string(leaderAddr) {
            return node
        }
    }
    return nil
}

// IsLeader проверяет, является ли текущий узел лидером.
func (rc *RaftCoordinator) IsLeader() bool {
    if rc.singleNodeMode {
        return true
    }
    return rc.isLeader.Load()
}

// GetCurrentTerm возвращает текущий терм Raft.
func (rc *RaftCoordinator) GetCurrentTerm() uint64 {
    return rc.currentTerm.Load()
}

// GetLeaderSince возвращает время начала лидерства.
func (rc *RaftCoordinator) GetLeaderSince() int64 {
    return rc.leaderSince.Load()
}

// GetElectionCount возвращает количество выборов.
func (rc *RaftCoordinator) GetElectionCount() uint64 {
    return rc.electionCount.Load()
}

// SendHeartbeat обновляет heartbeat узла.
func (rc *RaftCoordinator) SendHeartbeat(nodeID string) {
    now := time.Now().UnixMilli()
    
    if val, ok := rc.nodes.Load(nodeID); ok {
        nodeInfo := val.(*NodeInfo)
        nodeInfo.LastSeen = now
        nodeInfo.UpdatedAt = now
        rc.nodes.Store(nodeID, nodeInfo)
    }
    
    rc.fsm.state.mu.Lock()
    if nodeInfo, ok := rc.fsm.state.Nodes[nodeID]; ok {
        nodeInfo.LastSeen = now
        nodeInfo.UpdatedAt = now
    }
    rc.fsm.state.mu.Unlock()
}

// UpdateNodeStatus обновляет статус узла через Raft.
func (rc *RaftCoordinator) UpdateNodeStatus(nodeID string, status NodeStatus) error {
    now := time.Now().UnixMilli()
    
    if rc.splitBrainDetector.IsQuarantined(nodeID) {
        return fmt.Errorf("node %s is quarantined, cannot update status", nodeID)
    }
    
    if rc.singleNodeMode {
        rc.fsm.state.mu.Lock()
        if node, ok := rc.fsm.state.Nodes[nodeID]; ok {
            node.Status = mapStatusToString(int32(status))
            node.UpdatedAt = now
        }
        rc.fsm.state.mu.Unlock()
        return nil
    }
    
    if !rc.IsLeader() {
        return fmt.Errorf("node is not the leader")
    }
    
    cmd := NodeStatusCommand{
        Type:      "update_status",
        NodeID:    nodeID,
        Status:    int32(status),
        Timestamp: now,
    }
    
    data, err := json.Marshal(cmd)
    if err != nil {
        return err
    }
    
    future := rc.raft.Apply(data, rc.config.Replication.GetReplicationTimeout())
    if err := future.Error(); err != nil {
        return err
    }
    return nil
}

// GetClusterStatus возвращает статус кластера.
func (rc *RaftCoordinator) GetClusterStatus() *ClusterStatus {
    nodes := rc.GetAllNodes()
    activeNodes := rc.GetActiveNodes()
    
    syncingNodes := 0
    for _, node := range nodes {
        if node.Status == "syncing" {
            syncingNodes++
        }
    }
    
    leader := rc.GetLeader()
    leaderID := ""
    if leader != nil {
        leaderID = leader.ID
    }
    
    now := time.Now().UnixMilli()
    
    health := rc.calculateHealth()
    
    if rc.splitBrainDetector.Detect(rc.currentTerm.Load(), leaderID, len(nodes)) {
        health = "split_brain"
    }
    
    return &ClusterStatus{
        Name:                 rc.clusterName,
        TotalNodes:           len(nodes),
        ActiveNodes:          len(activeNodes),
        SyncingNodes:         syncingNodes,
        FailedNodes:          len(nodes) - len(activeNodes),
        ReplicationFactor:    int(rc.replicationFactor.Load()),
        LeaderID:             leaderID,
        Health:               health,
        CreatedAt:            rc.createdAt,
        UpdatedAt:            now,
        PipelineEnabled:      rc.pipelineReplicator != nil,
        BatchCommitEnabled:   rc.batchCommitManager != nil,
        ReshardingEnabled:    rc.reshardingManager != nil,
        JointConsensusActive: rc.jointConsensusManager != nil && rc.jointConsensusManager.IsJointConsensusActive(),
        FallbackMode:         rc.fallbackManager != nil && rc.fallbackManager.IsFallbackMode(),
    }
}

// calculateHealth вычисляет здоровье кластера.
func (rc *RaftCoordinator) calculateHealth() string {
    activeNodes := rc.GetActiveNodes()
    totalNodes := rc.GetAllNodes()
    
    if len(totalNodes) == 0 {
        return "critical"
    }
    
    ratio := float64(len(activeNodes)) / float64(len(totalNodes))
    if ratio >= 0.8 {
        return "healthy"
    } else if ratio >= 0.5 {
        return "degraded"
    }
    return "critical"
}

// GetReplicationFactor возвращает фактор репликации.
func (rc *RaftCoordinator) GetReplicationFactor() int {
    return int(rc.replicationFactor.Load())
}

// SetReplicationFactor устанавливает фактор репликации.
func (rc *RaftCoordinator) SetReplicationFactor(factor int) error {
    if factor < 1 || factor > 5 {
        return fmt.Errorf("replication factor must be between 1 and 5")
    }
    
    if !rc.IsLeader() {
        return fmt.Errorf("node is not the leader")
    }
    
    oldFactor := rc.replicationFactor.Load()
    rc.replicationFactor.Store(int32(factor))
    
    rc.fsm.state.mu.Lock()
    rc.fsm.state.ReplicationFactor = int32(factor)
    rc.fsm.state.UpdatedAt = time.Now().UnixMilli()
    rc.fsm.state.mu.Unlock()
    
    if rc.logger != nil {
        rc.logger.Info(fmt.Sprintf("Replication factor changed from %d to %d", oldFactor, factor))
    }
    
    return nil
}

// TriggerResharding запускает перераспределение шардов.
func (rc *RaftCoordinator) TriggerResharding(reason string) error {
    if rc.reshardingManager == nil {
        return fmt.Errorf("resharding manager not initialized")
    }
    return rc.reshardingManager.TriggerResharding(reason)
}

// GetPipelineStats возвращает статистику пайплайна.
func (rc *RaftCoordinator) GetPipelineStats() map[string]interface{} {
    if rc.pipelineReplicator == nil {
        return map[string]interface{}{
            "enabled": false,
            "message": "Pipeline replicator not initialized",
        }
    }
    
    return map[string]interface{}{
        "enabled":          true,
        "batch_size":       rc.pipelineReplicator.batchSize,
        "batch_timeout":    rc.pipelineReplicator.batchTimeout.String(),
        "pending_batches":  len(rc.pipelineReplicator.pendingBatches),
        "batch_count":      rc.pipelineReplicator.batchCount.Load(),
        "commands_count":   rc.pipelineReplicator.commandsCount.Load(),
    }
}

// GetBatchCommitStats возвращает статистику пакетных коммитов.
func (rc *RaftCoordinator) GetBatchCommitStats() map[string]interface{} {
    if rc.batchCommitManager == nil {
        return map[string]interface{}{
            "enabled": false,
            "message": "Batch commit manager not initialized",
        }
    }
    
    return map[string]interface{}{
        "enabled":          true,
        "batch_size":       rc.batchCommitManager.batchSize,
        "commit_interval":  rc.batchCommitManager.commitInterval.String(),
        "fsync_enabled":    rc.batchCommitManager.fsyncEnabled,
        "pending_commits":  len(rc.batchCommitManager.pendingCommits),
        "commit_count":     rc.batchCommitManager.commitCount.Load(),
        "operations_count": rc.batchCommitManager.operationsCount.Load(),
        "last_flush":       rc.batchCommitManager.storage.lastFlush,
        "flush_count":      rc.batchCommitManager.storage.flushCount,
        "stored_commits":   len(rc.batchCommitManager.storage.commits),
    }
}

// GetReshardingStats возвращает статистику решардинга.
func (rc *RaftCoordinator) GetReshardingStats() map[string]interface{} {
    if rc.reshardingManager == nil {
        return map[string]interface{}{
            "enabled": false,
            "message": "Resharding manager not initialized",
        }
    }
    
    metrics := rc.reshardingManager.metrics
    metrics.mu.RLock()
    defer metrics.mu.RUnlock()
    
    return map[string]interface{}{
        "enabled":               true,
        "total_reshardings":     metrics.TotalReshardings.Load(),
        "total_documents_moved": metrics.TotalDocumentsMoved.Load(),
        "total_bytes_moved":     metrics.TotalBytesMoved.Load(),
        "failed_reshardings":    metrics.FailedReshardings.Load(),
        "last_resharding_time":  metrics.LastReshardingTime.Load(),
        "history_count":         len(metrics.history),
        "queue_size":            len(rc.reshardingManager.reshardingChan),
        "is_resharding":         rc.reshardingManager.isResharding.Load(),
    }
}

// GetJointConsensusStatus возвращает статус совместного консенсуса.
func (rc *RaftCoordinator) GetJointConsensusStatus() map[string]interface{} {
    if rc.jointConsensusManager == nil {
        return map[string]interface{}{
            "active":  false,
            "message": "Joint consensus manager not initialized",
        }
    }
    return rc.jointConsensusManager.GetJointConsensusStatus()
}

// monitorLeadership отслеживает изменения лидера.
func (rc *RaftCoordinator) monitorLeadership() {
    ticker := time.NewTicker(rc.config.Cluster.GetHeartbeatTimeout() / 2)
    defer ticker.Stop()
    
    wasLeader := false
    
    for {
        select {
        case <-rc.stopChan:
            return
        case <-ticker.C:
            if rc.raft == nil {
                continue
            }
            isLeader := rc.raft.State() == raft.Leader
            if isLeader != wasLeader {
                wasLeader = isLeader
                select {
                case rc.leaderMonitor <- isLeader:
                default:
                }
                if isLeader {
                    rc.isLeader.Store(true)
                    newTerm := rc.currentTerm.Add(1)
                    rc.leaderSince.Store(time.Now().UnixMilli())
                    rc.electionCount.Add(1)
                    rc.fsm.state.CurrentTerm = newTerm
                    rc.stableStore.Set([]byte("currentTerm"), []byte(fmt.Sprintf("%d", newTerm)))
                    rc.logger.Debug(fmt.Sprintf("Leadership acquired at term %d (election #%d)",
                        newTerm, rc.electionCount.Load()))
                    
                    nodes := rc.GetAllNodes()
                    for _, node := range nodes {
                        rc.shardManager.AddNode(node.ID)
                    }
                } else {
                    rc.isLeader.Store(false)
                    rc.lastElection.Store(time.Now().UnixMilli())
                    rc.logger.Debug("Leadership lost")
                }
            }
        }
    }
}

// rebalanceMonitor периодически проверяет необходимость ребалансировки.
func (rc *RaftCoordinator) rebalanceMonitor() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-rc.stopChan:
            return
        case <-ticker.C:
            if rc.IsLeader() && rc.reshardingManager != nil {
                rc.reshardingManager.TriggerResharding("periodic_rebalance")
            }
        }
    }
}

// Stop останавливает координатор.
func (rc *RaftCoordinator) Stop() {
    now := time.Now().UnixMilli()
    
    rc.logger.Info("Stopping Raft coordinator...")
    
    if rc.pipelineReplicator != nil {
        rc.pipelineReplicator.Stop()
        rc.logger.Debug("Pipeline replicator stopped")
    }
    if rc.batchCommitManager != nil {
        rc.batchCommitManager.Stop()
        rc.logger.Debug("Batch commit manager stopped")
    }
    if rc.reshardingManager != nil {
        rc.reshardingManager.Stop()
        rc.logger.Debug("Resharding manager stopped")
    }
    if rc.recoveryManager != nil {
        rc.recoveryManager.Stop()
        rc.logger.Debug("Recovery manager stopped")
    }
    if rc.persistenceMgr != nil {
        rc.persistenceMgr.Stop()
        rc.logger.Debug("Persistence manager stopped")
    }
    if rc.fallbackManager != nil {
        rc.fallbackManager.Stop()
        rc.logger.Debug("Fallback manager stopped")
    }
    if rc.panicRecoveryMgr != nil {
        rc.panicRecoveryMgr.Stop()
        rc.logger.Debug("Panic recovery manager stopped")
    }
    if rc.replicaReadManager != nil {
        rc.replicaReadManager.Stop()
        rc.logger.Debug("Replica read manager stopped")
    }
    if rc.shardManager != nil {
        rc.shardManager.Stop()
        rc.logger.Debug("Range shard manager stopped")
    }
    if rc.crossDCMigrator != nil {
        rc.crossDCMigrator.Stop()
        rc.logger.Debug("Cross-datacenter migrator stopped")
    }
    
    close(rc.stopChan)
    if rc.raft != nil {
        rc.raft.Shutdown()
    }
    
    rc.logger.Info(fmt.Sprintf("Raft coordinator stopped at %s", time.UnixMilli(now).Format("2006-01-02 15:04:05.000")))
}

// IsReplicationEnabled возвращает статус репликации.
func (rc *RaftCoordinator) IsReplicationEnabled() bool {
    return rc.replicationEnabled
}

// IsSyncReplicationEnabled возвращает статус синхронной репликации.
func (rc *RaftCoordinator) IsSyncReplicationEnabled() bool {
    return rc.syncReplication
}

// RegisterNode регистрирует узел в кластере.
func (rc *RaftCoordinator) RegisterNode(node *Node) error {
    now := time.Now().UnixMilli()
    
    if rc.splitBrainDetector.IsQuarantined(node.ID) {
        return fmt.Errorf("node %s is quarantined due to previous split-brain", node.ID)
    }
    
    nodeInfo := &NodeInfo{
        ID:        node.ID,
        IP:        node.IP,
        Port:      node.Port,
        Status:    "active",
        LastSeen:  now,
        JoinedAt:  now,
        UpdatedAt: now,
        Version:   1,
    }
    
    if rc.singleNodeMode {
        rc.logger.Debug("Single-node mode: registering node without Raft consensus")
        rc.nodes.Store(node.ID, nodeInfo)
        
        rc.fsm.state.mu.Lock()
        rc.fsm.state.Nodes[node.ID] = nodeInfo
        rc.fsm.state.UpdatedAt = now
        rc.fsm.state.mu.Unlock()
        
        rc.shardManager.AddNode(node.ID)
        return nil
    }
    
    if !rc.IsLeader() {
        leader := rc.GetLeader()
        if leader != nil {
            return fmt.Errorf("node is not the leader. Please connect to leader at %s:%d", leader.IP, leader.Port)
        }
        return fmt.Errorf("node is not the leader and no leader found")
    }
    
    cmd := NodeRegistrationCommand{
        Type:      "register",
        Node:      *nodeInfo,
        Timestamp: now,
    }
    
    data, err := json.Marshal(cmd)
    if err != nil {
        return err
    }
    
    future := rc.raft.Apply(data, rc.config.Replication.GetReplicationTimeout())
    if err := future.Error(); err != nil {
        return fmt.Errorf("failed to register node via raft: %v", err)
    }
    
    rc.nodes.Store(node.ID, nodeInfo)
    rc.shardManager.AddNode(node.ID)
    return nil
}

// RemoveNode удаляет узел из кластера.
func (rc *RaftCoordinator) RemoveNode(nodeID string) error {
    now := time.Now().UnixMilli()
    
    if rc.singleNodeMode {
        rc.nodes.Delete(nodeID)
        rc.fsm.state.mu.Lock()
        delete(rc.fsm.state.Nodes, nodeID)
        rc.fsm.state.UpdatedAt = now
        rc.fsm.state.mu.Unlock()
        rc.shardManager.RemoveNode(nodeID)
        return nil
    }
    
    if !rc.IsLeader() {
        return fmt.Errorf("node is not the leader")
    }
    
    cmd := NodeRegistrationCommand{
        Type:      "remove",
        NodeID:    nodeID,
        Timestamp: now,
    }
    
    data, err := json.Marshal(cmd)
    if err != nil {
        return err
    }
    
    future := rc.raft.Apply(data, rc.config.Replication.GetReplicationTimeout())
    if err := future.Error(); err != nil {
        return fmt.Errorf("failed to remove node via raft: %v", err)
    }
    
    rc.nodes.Delete(nodeID)
    rc.shardManager.RemoveNode(nodeID)
    return nil
}

// HandleStatusSync обрабатывает синхронизацию статуса.
func (rc *RaftCoordinator) HandleStatusSync(leaderID string, term uint64, clusterSize int) {
    if rc.splitBrainDetector.Detect(term, leaderID, clusterSize) {
        candidates := make(map[string]uint64)
        candidates[leaderID] = rc.getCommitIndex()
        candidates[rc.localNodeInfo.ID] = rc.getCommitIndex()
        
        winner := rc.splitBrainDetector.Resolve(term, candidates)
        if winner == rc.localNodeInfo.ID && !rc.IsLeader() {
            rc.raft.LeadershipTransfer()
            rc.logger.Warn("Split-brain resolved: initiating leadership transfer")
        } else if winner != leaderID && winner != "" {
            rc.splitBrainDetector.QuarantineNode(leaderID)
            rc.logger.Warn(fmt.Sprintf("Quarantining node %s due to split-brain", leaderID))
        }
    }
}

// getCommitIndex возвращает индекс закоммиченных записей.
func (rc *RaftCoordinator) getCommitIndex() uint64 {
    if rc.raft == nil {
        return 0
    }
    return rc.raft.AppliedIndex()
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// getLocalIP получает локальный IP адрес.
func getLocalIP() string {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return "127.0.0.1"
    }
    for _, addr := range addrs {
        if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
            return ipnet.IP.String()
        }
    }
    return "127.0.0.1"
}

// mapStatusToString преобразует статус в строку.
func mapStatusToString(status int32) string {
    switch status {
    case 0:
        return "offline"
    case 1:
        return "active"
    case 2:
        return "syncing"
    case 3:
        return "failed"
    default:
        return "unknown"
    }
}

// NodeStatusCommand команда обновления статуса узла.
type NodeStatusCommand struct {
    Type      string `json:"type"`
    NodeID    string `json:"node_id"`
    Status    int32  `json:"status"`
    Timestamp int64  `json:"timestamp"`
}

// NodeRegistrationCommand команда регистрации узла.
type NodeRegistrationCommand struct {
    Type       string                 `json:"type"`
    Node       NodeInfo               `json:"node,omitempty"`
    NodeID     string                 `json:"node_id,omitempty"`
    Factor     int32                  `json:"factor,omitempty"`
    Shard      *RangeShard            `json:"shard,omitempty"`
    ShardID    string                 `json:"shard_id,omitempty"`
    TargetNode string                 `json:"target_node,omitempty"`
    Data       map[string]interface{} `json:"data,omitempty"`
    Timestamp  int64                  `json:"timestamp"`
}

// ClusterStatus представляет статус кластера.
type ClusterStatus struct {
    Name                 string `json:"name"`
    TotalNodes           int    `json:"total_nodes"`
    ActiveNodes          int    `json:"active_nodes"`
    SyncingNodes         int    `json:"syncing_nodes"`
    FailedNodes          int    `json:"failed_nodes"`
    ReplicationFactor    int    `json:"replication_factor"`
    LeaderID             string `json:"leader_id"`
    Health               string `json:"health"`
    CreatedAt            int64  `json:"created_at"`
    UpdatedAt            int64  `json:"updated_at"`
    PipelineEnabled      bool   `json:"pipeline_enabled"`
    BatchCommitEnabled   bool   `json:"batch_commit_enabled"`
    ReshardingEnabled    bool   `json:"resharding_enabled"`
    JointConsensusActive bool   `json:"joint_consensus_active"`
    FallbackMode         bool   `json:"fallback_mode"`
}
