/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/gossip.go
// Назначение: Gossip Protocol для автоматического обнаружения узлов
// и обмена информацией о состоянии кластера.

package cluster

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"futriis/internal/log"
)

// =============================================================================
// ТИПЫ ДЛЯ GOSSIP PROTOCOL
// =============================================================================

// GossipMessageType представляет тип сообщения протокола сплетен
type GossipMessageType int

const (
	GossipTypeMembership GossipMessageType = iota // Обновление членства
	GossipTypeHealth                              // Информация о здоровье
	GossipTypeLoad                                // Информация о нагрузке
	GossipTypeConfig                              // Обновление конфигурации
	GossipTypeSync                                // Синхронизация состояния
)

// GossipMessage представляет сообщение протокола сплетен
type GossipMessage struct {
	Type      GossipMessageType       `json:"type"`
	SenderID  string                  `json:"sender_id"`
	SenderIP  string                  `json:"sender_ip"`
	SenderPort int                    `json:"sender_port"`
	Timestamp int64                   `json:"timestamp"`
	Payload   json.RawMessage         `json:"payload"`
	TTL       int                     `json:"ttl"`
	Version   uint64                  `json:"version"`
}

// NodeState представляет состояние узла в протоколе сплетен
type NodeState struct {
	ID           string    `json:"id"`
	IP           string    `json:"ip"`
	Port         int       `json:"port"`
	RaftPort     int       `json:"raft_port"`
	Status       string    `json:"status"`
	LastSeen     time.Time `json:"last_seen"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Incarnation  uint64    `json:"incarnation"`
	LoadCPU      float64   `json:"load_cpu"`
	LoadMemory   float64   `json:"load_memory"`
	LoadQueue    int64     `json:"load_queue"`
	Connections  int       `json:"connections"`
	Version      string    `json:"version"`
	IsAlive      bool      `json:"is_alive"`
	IsSuspect    bool      `json:"is_suspect"`
}

// GossipConfig содержит настройки протокола сплетен
type GossipConfig struct {
	// Интервал отправки gossip сообщений (сек)
	BroadcastIntervalSec int `json:"broadcast_interval_sec"`
	// Интервал проверки состояния узлов (сек)
	MonitorIntervalSec int `json:"monitor_interval_sec"`
	// Количество узлов для случайного распространения
	Fanout int `json:"fanout"`
	// Максимальное TTL сообщений
	MaxTTL int `json:"max_ttl"`
	// Время ожидания подтверждения (сек)
	TimeoutSec int `json:"timeout_sec"`
	// Время до перевода в подозрительные (сек)
	SuspectTimeoutSec int `json:"suspect_timeout_sec"`
	// Время до удаления узла (сек)
	DeadTimeoutSec int `json:"dead_timeout_sec"`
	// Максимальный размер буфера сообщений
	BufferSize int `json:"buffer_size"`
}

// DefaultGossipConfig возвращает конфигурацию gossip по умолчанию
func DefaultGossipConfig() *GossipConfig {
	return &GossipConfig{
		BroadcastIntervalSec: 5,
		MonitorIntervalSec:   2,
		Fanout:               3,
		MaxTTL:               3,
		TimeoutSec:           10,
		SuspectTimeoutSec:    15,
		DeadTimeoutSec:       30,
		BufferSize:           10000,
	}
}

// =============================================================================
// GOSSIP MANAGER
// =============================================================================

// GossipManager управляет протоколом сплетен для обнаружения узлов
type GossipManager struct {
	config       *GossipConfig
	logger       *log.Logger
	coordinator  *RaftCoordinator
	localNode    *NodeState
	nodes        sync.Map // map[string]*NodeState
	membership   atomic.Value // []*NodeState
	subscribers  map[string]chan *GossipMessage
	subscriberMu sync.RWMutex
	stopChan     chan struct{}
	wg           sync.WaitGroup
	udpConn      *net.UDPConn
	msgCounter   atomic.Uint64
	incarnation  atomic.Uint64
	mu           sync.RWMutex
	seeds        []string
	metrics      *GossipMetrics
}

// GossipMetrics хранит метрики gossip протокола
type GossipMetrics struct {
	MessagesSent     atomic.Uint64
	MessagesReceived atomic.Uint64
	MessagesDropped  atomic.Uint64
	NodesDiscovered  atomic.Uint64
	NodesRemoved     atomic.Uint64
	LastBroadcast    atomic.Int64
	LastSync         atomic.Int64
}

// NewGossipManager создаёт новый менеджер gossip протокола
func NewGossipManager(config *GossipConfig, coordinator *RaftCoordinator, logger *log.Logger) *GossipManager {
	if config == nil {
		config = DefaultGossipConfig()
	}

	gm := &GossipManager{
		config:      config,
		logger:      logger,
		coordinator: coordinator,
		subscribers: make(map[string]chan *GossipMessage),
		stopChan:    make(chan struct{}),
		metrics:     &GossipMetrics{},
		seeds:       make([]string, 0),
	}

	gm.membership.Store(make([]*NodeState, 0))

	return gm
}

// Start запускает gossip протокол
func (gm *GossipManager) Start() error {
	gm.localNode = &NodeState{
		ID:           gm.coordinator.localNodeInfo.ID,
		IP:           gm.coordinator.localNodeInfo.IP,
		Port:         gm.coordinator.localNodeInfo.Port,
		RaftPort:     gm.coordinator.config.Cluster.RaftPort,
		Status:       "active",
		LastSeen:     time.Now(),
		LastHeartbeat: time.Now(),
		Incarnation:  0,
		IsAlive:      true,
		IsSuspect:    false,
		Version:      "1.0",
	}

	// Запускаем UDP сервер для приёма gossip сообщений
	if err := gm.startUDPServer(); err != nil {
		return fmt.Errorf("failed to start UDP server: %v", err)
	}

	// Запускаем горутины
	gm.wg.Add(4)
	go gm.broadcastLoop()
	go gm.monitorLoop()
	go gm.receiveLoop()
	go gm.syncLoop()

	gm.logger.Info(fmt.Sprintf("Gossip protocol started on port %d", gm.coordinator.config.Cluster.RaftPort+1))

	return nil
}

// Stop останавливает gossip протокол
func (gm *GossipManager) Stop() {
	close(gm.stopChan)
	gm.wg.Wait()

	if gm.udpConn != nil {
		gm.udpConn.Close()
	}

	gm.logger.Info("Gossip protocol stopped")
}

// startUDPServer запускает UDP сервер для приёма сообщений
func (gm *GossipManager) startUDPServer() error {
	port := gm.coordinator.config.Cluster.RaftPort + 1
	addr := fmt.Sprintf("%s:%d", gm.localNode.IP, port)

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}

	gm.udpConn = conn
	return nil
}

// broadcastLoop периодически рассылает gossip сообщения
func (gm *GossipManager) broadcastLoop() {
	defer gm.wg.Done()

	ticker := time.NewTicker(time.Duration(gm.config.BroadcastIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-gm.stopChan:
			return
		case <-ticker.C:
			gm.broadcast()
		}
	}
}

// broadcast рассылает сообщение случайным узлам
func (gm *GossipManager) broadcast() {
	// Формируем сообщение с состоянием узла
	payload, _ := json.Marshal(gm.localNode)
	msg := &GossipMessage{
		Type:      GossipTypeMembership,
		SenderID:  gm.localNode.ID,
		SenderIP:  gm.localNode.IP,
		SenderPort: gm.localNode.Port,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
		TTL:       gm.config.MaxTTL,
		Version:   gm.incarnation.Add(1),
	}

	// Получаем список активных узлов
	nodes := gm.getActiveNodes()

	if len(nodes) == 0 {
		// Если нет известных узлов, пробуем связаться с seed узлами
		gm.broadcastToSeeds(msg)
		return
	}

	// Выбираем случайные узлы для рассылки
	fanout := gm.config.Fanout
	if fanout > len(nodes) {
		fanout = len(nodes)
	}

	selected := gm.selectRandomNodes(nodes, fanout)

	// Рассылаем сообщения
	for _, node := range selected {
		gm.sendMessage(msg, node.IP, node.Port)
	}

	gm.metrics.MessagesSent.Add(uint64(len(selected)))
	gm.metrics.LastBroadcast.Store(time.Now().UnixMilli())
}

// broadcastToSeeds рассылает сообщения seed узлам
func (gm *GossipManager) broadcastToSeeds(msg *GossipMessage) {
	gm.mu.RLock()
	seeds := make([]string, len(gm.seeds))
	copy(seeds, gm.seeds)
	gm.mu.RUnlock()

	for _, seed := range seeds {
		// Парсим seed адрес
		host, port, err := net.SplitHostPort(seed)
		if err != nil {
			continue
		}
		var portInt int
		fmt.Sscanf(port, "%d", &portInt)
		gm.sendMessage(msg, host, portInt)
	}
}

// monitorLoop периодически проверяет состояние узлов
func (gm *GossipManager) monitorLoop() {
	defer gm.wg.Done()

	ticker := time.NewTicker(time.Duration(gm.config.MonitorIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-gm.stopChan:
			return
		case <-ticker.C:
			gm.monitorNodes()
		}
	}
}

// monitorNodes проверяет состояние всех известных узлов
func (gm *GossipManager) monitorNodes() {
	now := time.Now()
	deadTimeout := time.Duration(gm.config.DeadTimeoutSec) * time.Second
	suspectTimeout := time.Duration(gm.config.SuspectTimeoutSec) * time.Second

	var nodesToRemove []string

	gm.nodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		node := value.(*NodeState)

		if nodeID == gm.localNode.ID {
			return true
		}

		if !node.IsAlive {
			return true
		}

		lastSeen := now.Sub(node.LastSeen)

		if lastSeen > deadTimeout {
			// Узел считается мёртвым
			node.IsAlive = false
			node.IsSuspect = false
			gm.nodes.Store(nodeID, node)
			nodesToRemove = append(nodesToRemove, nodeID)
			gm.metrics.NodesRemoved.Add(1)

			if gm.logger != nil {
				gm.logger.Warn(fmt.Sprintf("Node %s considered dead (last seen %v ago)", nodeID, lastSeen))
			}

			// Уведомляем координатор об удалении узла
			if gm.coordinator != nil {
				gm.coordinator.RemoveNode(nodeID)
			}
		} else if lastSeen > suspectTimeout {
			// Узел под подозрением
			if !node.IsSuspect {
				node.IsSuspect = true
				gm.nodes.Store(nodeID, node)

				if gm.logger != nil {
					gm.logger.Warn(fmt.Sprintf("Node %s marked as suspect (last seen %v ago)", nodeID, lastSeen))
				}

				// Отправляем запрос на подтверждение
				gm.requestConfirmation(node)
			}
		}

		return true
	})

	// Удаляем мёртвые узлы
	for _, nodeID := range nodesToRemove {
		gm.nodes.Delete(nodeID)
	}

	// Обновляем список членства
	gm.updateMembership()
}

// requestConfirmation запрашивает подтверждение от узла
func (gm *GossipManager) requestConfirmation(node *NodeState) {
	msg := &GossipMessage{
		Type:      GossipTypeHealth,
		SenderID:  gm.localNode.ID,
		SenderIP:  gm.localNode.IP,
		SenderPort: gm.localNode.Port,
		Timestamp: time.Now().UnixMilli(),
		Payload:   json.RawMessage(`{"action":"ping"}`),
		TTL:       1,
	}

	gm.sendMessage(msg, node.IP, node.Port)
}

// receiveLoop принимает входящие gossip сообщения
func (gm *GossipManager) receiveLoop() {
	defer gm.wg.Done()

	buffer := make([]byte, 65536)

	for {
		select {
		case <-gm.stopChan:
			return
		default:
			n, addr, err := gm.udpConn.ReadFromUDP(buffer)
			if err != nil {
				continue
			}

			if n > 0 {
				go gm.handleMessage(buffer[:n], addr)
			}
		}
	}
}

// handleMessage обрабатывает полученное gossip сообщение
func (gm *GossipManager) handleMessage(data []byte, addr *net.UDPAddr) {
	var msg GossipMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	gm.metrics.MessagesReceived.Add(1)

	// Обновляем информацию об отправителе
	gm.updateNodeFromMessage(&msg)

	// Обрабатываем сообщение в зависимости от типа
	switch msg.Type {
	case GossipTypeMembership:
		gm.handleMembershipMessage(&msg)
	case GossipTypeHealth:
		gm.handleHealthMessage(&msg)
	case GossipTypeLoad:
		gm.handleLoadMessage(&msg)
	case GossipTypeConfig:
		gm.handleConfigMessage(&msg)
	case GossipTypeSync:
		gm.handleSyncMessage(&msg)
	}

	// Пересылаем сообщение дальше (если TTL > 0)
	if msg.TTL > 0 && msg.SenderID != gm.localNode.ID {
		msg.TTL--
		gm.forwardMessage(&msg)
	}
}

// updateNodeFromMessage обновляет информацию об узле из сообщения
func (gm *GossipManager) updateNodeFromMessage(msg *GossipMessage) {
	// Проверяем, не от себя ли сообщение
	if msg.SenderID == gm.localNode.ID {
		return
	}

	var nodeState NodeState
	if msg.Type == GossipTypeMembership {
		if err := json.Unmarshal(msg.Payload, &nodeState); err != nil {
			return
		}
	} else {
		// Для других типов создаём базовое состояние
		nodeState = NodeState{
			ID:       msg.SenderID,
			IP:       msg.SenderIP,
			Port:     msg.SenderPort,
			LastSeen: time.Now(),
			IsAlive:  true,
		}
	}

	// Обновляем или создаём узел
	if existing, ok := gm.nodes.Load(msg.SenderID); ok {
		existingNode := existing.(*NodeState)

		// Обновляем только если новее
		if nodeState.Incarnation > existingNode.Incarnation {
			existingNode.IP = nodeState.IP
			existingNode.Port = nodeState.Port
			existingNode.LastSeen = time.Now()
			existingNode.Incarnation = nodeState.Incarnation
			existingNode.IsAlive = true
			existingNode.IsSuspect = false
			if nodeState.Status != "" {
				existingNode.Status = nodeState.Status
			}
			gm.nodes.Store(msg.SenderID, existingNode)
		} else {
			// Обновляем только время последнего контакта
			existingNode.LastSeen = time.Now()
			if existingNode.IsSuspect {
				existingNode.IsSuspect = false
			}
			gm.nodes.Store(msg.SenderID, existingNode)
		}
	} else {
		// Новый узел
		gm.nodes.Store(msg.SenderID, &nodeState)
		gm.metrics.NodesDiscovered.Add(1)

		if gm.logger != nil {
			gm.logger.Info(fmt.Sprintf("Discovered new node: %s (%s:%d)", msg.SenderID, msg.SenderIP, msg.SenderPort))
		}
	}

	gm.updateMembership()
}

// handleMembershipMessage обрабатывает сообщение о членстве
func (gm *GossipManager) handleMembershipMessage(msg *GossipMessage) {
	// Обновление уже выполнено в updateNodeFromMessage
}

// handleHealthMessage обрабатывает сообщение о здоровье
func (gm *GossipManager) handleHealthMessage(msg *GossipMessage) {
	// Отвечаем на ping запросы
	var payload map[string]string
	if err := json.Unmarshal(msg.Payload, &payload); err == nil {
		if action, ok := payload["action"]; ok && action == "ping" {
			// Отправляем pong
			response := &GossipMessage{
				Type:      GossipTypeHealth,
				SenderID:  gm.localNode.ID,
				SenderIP:  gm.localNode.IP,
				SenderPort: gm.localNode.Port,
				Timestamp: time.Now().UnixMilli(),
				Payload:   json.RawMessage(`{"action":"pong"}`),
				TTL:       1,
			}
			// Отправляем обратно отправителю
			host := msg.SenderIP
			port := msg.SenderPort
			gm.sendMessage(response, host, port)
		}
	}
}

// handleLoadMessage обрабатывает сообщение о нагрузке
func (gm *GossipManager) handleLoadMessage(msg *GossipMessage) {
	// TODO: Обновляем информацию о нагрузке узла
}

// handleConfigMessage обрабатывает сообщение об изменении конфигурации
func (gm *GossipManager) handleConfigMessage(msg *GossipMessage) {
	// TODO: Применяем изменения конфигурации
}

// handleSyncMessage обрабатывает сообщение синхронизации
func (gm *GossipManager) handleSyncMessage(msg *GossipMessage) {
	// Отправляем полное состояние кластера
	if msg.SenderID != gm.localNode.ID {
		gm.sendFullState(msg.SenderIP, msg.SenderPort)
	}
}

// sendMessage отправляет gossip сообщение указанному узлу
func (gm *GossipManager) sendMessage(msg *GossipMessage, ip string, port int) {
	if gm.udpConn == nil {
		return
	}

	// Сериализуем сообщение
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	// Отправляем
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return
	}

	gm.udpConn.WriteToUDP(data, addr)
}

// forwardMessage пересылает сообщение дальше
func (gm *GossipManager) forwardMessage(msg *GossipMessage) {
	// Если TTL истёк, не пересылаем
	if msg.TTL <= 0 {
		return
	}

	// Получаем активные узлы
	nodes := gm.getActiveNodes()
	if len(nodes) == 0 {
		return
	}

	// Выбираем случайные узлы для пересылки
	selected := gm.selectRandomNodes(nodes, gm.config.Fanout)
	for _, node := range selected {
		if node.ID != msg.SenderID && node.ID != gm.localNode.ID {
			gm.sendMessage(msg, node.IP, node.Port)
		}
	}
}

// sendFullState отправляет полное состояние кластера
func (gm *GossipManager) sendFullState(ip string, port int) {
	nodes := gm.getAllNodes()
	payload, _ := json.Marshal(nodes)

	msg := &GossipMessage{
		Type:      GossipTypeSync,
		SenderID:  gm.localNode.ID,
		SenderIP:  gm.localNode.IP,
		SenderPort: gm.localNode.Port,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
		TTL:       1,
	}

	gm.sendMessage(msg, ip, port)
}

// syncLoop периодически синхронизирует состояние с другими узлами
func (gm *GossipManager) syncLoop() {
	defer gm.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-gm.stopChan:
			return
		case <-ticker.C:
			gm.syncWithCluster()
		}
	}
}

// syncWithCluster синхронизирует состояние с кластером
func (gm *GossipManager) syncWithCluster() {
	// Выбираем случайный узел для синхронизации
	nodes := gm.getActiveNodes()
	if len(nodes) == 0 {
		return
	}

	// Выбираем случайный узел, кроме себя
	var target *NodeState
	for _, node := range nodes {
		if node.ID != gm.localNode.ID {
			target = node
			break
		}
	}

	if target == nil {
		return
	}

	// Запрашиваем полное состояние
	gm.sendFullState(target.IP, target.Port)
	gm.metrics.LastSync.Store(time.Now().UnixMilli())
}

// getActiveNodes возвращает список активных узлов
func (gm *GossipManager) getActiveNodes() []*NodeState {
	nodes := make([]*NodeState, 0)
	gm.nodes.Range(func(key, value interface{}) bool {
		node := value.(*NodeState)
		if node.IsAlive && node.ID != gm.localNode.ID {
			nodes = append(nodes, node)
		}
		return true
	})
	return nodes
}

// getAllNodes возвращает список всех узлов
func (gm *GossipManager) getAllNodes() []*NodeState {
	nodes := make([]*NodeState, 0)
	gm.nodes.Range(func(key, value interface{}) bool {
		nodes = append(nodes, value.(*NodeState))
		return true
	})
	return nodes
}

// selectRandomNodes выбирает случайные узлы из списка
func (gm *GossipManager) selectRandomNodes(nodes []*NodeState, count int) []*NodeState {
	if len(nodes) == 0 {
		return nil
	}

	if count > len(nodes) {
		count = len(nodes)
	}

	// Создаём копию и перемешиваем
	shuffled := make([]*NodeState, len(nodes))
	copy(shuffled, nodes)

	// Перемешиваем с использованием rand
	for i := len(shuffled) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled[:count]
}

// updateMembership обновляет список членства
func (gm *GossipManager) updateMembership() {
	nodes := gm.getAllNodes()
	gm.membership.Store(nodes)
}

// GetMembership возвращает текущий список членства
func (gm *GossipManager) GetMembership() []*NodeState {
	return gm.membership.Load().([]*NodeState)
}

// GetNodeByID возвращает узел по ID
func (gm *GossipManager) GetNodeByID(id string) *NodeState {
	if val, ok := gm.nodes.Load(id); ok {
		return val.(*NodeState)
	}
	return nil
}

// AddSeed добавляет seed узел
func (gm *GossipManager) AddSeed(addr string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	// Проверяем, что адрес не дублируется
	for _, existing := range gm.seeds {
		if existing == addr {
			return
		}
	}
	gm.seeds = append(gm.seeds, addr)
}

// Subscribe подписывается на события gossip
func (gm *GossipManager) Subscribe(id string) <-chan *GossipMessage {
	gm.subscriberMu.Lock()
	defer gm.subscriberMu.Unlock()

	ch := make(chan *GossipMessage, gm.config.BufferSize)
	gm.subscribers[id] = ch
	return ch
}

// Unsubscribe отписывается от событий gossip
func (gm *GossipManager) Unsubscribe(id string) {
	gm.subscriberMu.Lock()
	defer gm.subscriberMu.Unlock()

	if ch, ok := gm.subscribers[id]; ok {
		close(ch)
		delete(gm.subscribers, id)
	}
}

// GetMetrics возвращает метрики gossip протокола
func (gm *GossipManager) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"messages_sent":      gm.metrics.MessagesSent.Load(),
		"messages_received":  gm.metrics.MessagesReceived.Load(),
		"messages_dropped":   gm.metrics.MessagesDropped.Load(),
		"nodes_discovered":   gm.metrics.NodesDiscovered.Load(),
		"nodes_removed":      gm.metrics.NodesRemoved.Load(),
		"last_broadcast":     gm.metrics.LastBroadcast.Load(),
		"last_sync":          gm.metrics.LastSync.Load(),
		"known_nodes":        gm.getNodeCount(),
		"active_nodes":       len(gm.getActiveNodes()),
	}
}

// getNodeCount возвращает количество известных узлов
func (gm *GossipManager) getNodeCount() int {
	count := 0
	gm.nodes.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// UpdateLocalNode обновляет локальное состояние узла
func (gm *GossipManager) UpdateLocalNode(status string, loadCPU float64, loadMemory float64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if gm.localNode != nil {
		gm.localNode.Status = status
		gm.localNode.LoadCPU = loadCPU
		gm.localNode.LoadMemory = loadMemory
		gm.localNode.LastHeartbeat = time.Now()
		gm.localNode.Incarnation++
	}
}
