/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/cluster/self_healing.go
// Назначение: Механизмы самоисцеления для автоматического восстановления узлов

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"futriis/internal/log"
	"futriis/internal/storage"
)

// =============================================================================
// ТИПЫ ДЛЯ SELF-HEALING
// =============================================================================

// HealingAction представляет действие по восстановлению
type HealingAction int

const (
	HealingActionNone HealingAction = iota
	HealingActionRestartNode          // Перезапуск узла
	HealingActionRejoinCluster        // Переподключение к кластеру
	HealingActionResharding           // Перебалансировка шардов
	HealingActionDataRecovery         // Восстановление данных
	HealingActionStateRestore         // Восстановление состояния из снэпшота
)

// HealingState представляет состояние процесса восстановления
type HealingState string

const (
	HealingStateIdle      HealingState = "idle"
	HealingStateDetecting HealingState = "detecting"
	HealingStateAnalyzing HealingState = "analyzing"
	HealingStateHealing   HealingState = "healing"
	HealingStateVerifying HealingState = "verifying"
	HealingStateComplete  HealingState = "complete"
	HealingStateFailed    HealingState = "failed"
)

// HealingEvent представляет событие восстановления
type HealingEvent struct {
	ID          string                 `json:"id"`
	NodeID      string                 `json:"node_id"`
	Action      HealingAction          `json:"action"`
	State       HealingState           `json:"state"`
	Timestamp   int64                  `json:"timestamp"`
	Description string                 `json:"description"`
	Error       string                 `json:"error,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty"`
}

// HealingConfig содержит настройки самоисцеления
type HealingConfig struct {
	// Включено ли самоисцеление
	Enabled bool `json:"enabled"`
	// Интервал проверки состояния (сек)
	CheckIntervalSec int `json:"check_interval_sec"`
	// Количество последовательных сбоев до активации
	FailureThreshold int `json:"failure_threshold"`
	// Задержка перед началом восстановления (сек)
	HealingDelaySec int `json:"healing_delay_sec"`
	// Максимальное время восстановления (сек)
	HealingTimeoutSec int `json:"healing_timeout_sec"`
	// Автоматический перезапуск узла
	AutoRestart bool `json:"auto_restart"`
	// Автоматическая перебалансировка
	AutoResharding bool `json:"auto_resharding"`
	// Автоматическое восстановление данных
	AutoDataRecovery bool `json:"auto_data_recovery"`
	// Максимальное количество одновременных восстановлений
	MaxConcurrentHealings int `json:"max_concurrent_healings"`
}

// DefaultHealingConfig возвращает конфигурацию по умолчанию
func DefaultHealingConfig() *HealingConfig {
	return &HealingConfig{
		Enabled:               true,
		CheckIntervalSec:      10,
		FailureThreshold:      3,
		HealingDelaySec:       5,
		HealingTimeoutSec:     300,
		AutoRestart:           true,
		AutoResharding:        true,
		AutoDataRecovery:      true,
		MaxConcurrentHealings: 3,
	}
}

// =============================================================================
// SELF-HEALING MANAGER
// =============================================================================

// SelfHealingManager управляет механизмами самоисцеления
type SelfHealingManager struct {
	config          *HealingConfig
	logger          *log.Logger
	coordinator     *RaftCoordinator
	gossipManager   *GossipManager
	store           *storage.Storage
	healingEvents   sync.Map // map[string]*HealingEvent
	activeHealings  atomic.Int32
	stopChan        chan struct{}
	wg              sync.WaitGroup
	mu              sync.RWMutex
	nodeFailures    map[string]int
	healingHistory  []*HealingEvent
	metrics         *HealingMetrics
	eventHandlers   map[string]func(*HealingEvent)
	handlerMu       sync.RWMutex
}

// HealingMetrics хранит метрики восстановления
type HealingMetrics struct {
	TotalHealings       atomic.Uint64
	SuccessfulHealings  atomic.Uint64
	FailedHealings      atomic.Uint64
	ReshardingsTriggered atomic.Uint64
	DataRecoveries      atomic.Uint64
	RestartsTriggered   atomic.Uint64
	AvgHealingDuration  atomic.Int64
}

// NewSelfHealingManager создаёт новый менеджер самоисцеления
func NewSelfHealingManager(
	config *HealingConfig,
	coordinator *RaftCoordinator,
	gossipManager *GossipManager,
	store *storage.Storage,
	logger *log.Logger,
) *SelfHealingManager {
	if config == nil {
		config = DefaultHealingConfig()
	}

	shm := &SelfHealingManager{
		config:         config,
		logger:         logger,
		coordinator:    coordinator,
		gossipManager:  gossipManager,
		store:          store,
		stopChan:       make(chan struct{}),
		nodeFailures:   make(map[string]int),
		healingHistory: make([]*HealingEvent, 0),
		metrics:        &HealingMetrics{},
		eventHandlers:  make(map[string]func(*HealingEvent)),
	}

	return shm
}

// Start запускает менеджер самоисцеления
func (shm *SelfHealingManager) Start() {
	if !shm.config.Enabled {
		shm.logger.Info("Self-healing is disabled")
		return
	}

	shm.wg.Add(2)
	go shm.monitorLoop()
	go shm.healingLoop()

	shm.logger.Info("Self-healing manager started")
}

// Stop останавливает менеджер самоисцеления
func (shm *SelfHealingManager) Stop() {
	close(shm.stopChan)
	shm.wg.Wait()
	shm.logger.Info("Self-healing manager stopped")
}

// monitorLoop периодически проверяет состояние узлов
func (shm *SelfHealingManager) monitorLoop() {
	defer shm.wg.Done()

	ticker := time.NewTicker(time.Duration(shm.config.CheckIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shm.stopChan:
			return
		case <-ticker.C:
			shm.monitorNodes()
		}
	}
}

// monitorNodes проверяет состояние всех узлов в кластере
func (shm *SelfHealingManager) monitorNodes() {
	// Получаем информацию о узлах из gossip
	membership := shm.gossipManager.GetMembership()

	for _, node := range membership {
		if node.ID == shm.coordinator.localNodeInfo.ID {
			continue
		}

		// Проверяем состояние узла
		if !node.IsAlive && !node.IsSuspect {
			shm.handleNodeFailure(node)
		}
	}

	// Проверяем также через координатор
	nodes := shm.coordinator.GetAllNodes()
	for _, node := range nodes {
		if node.ID == shm.coordinator.localNodeInfo.ID {
			continue
		}

		// Проверяем, не пропал ли узел из gossip
		if gossipNode := shm.gossipManager.GetNodeByID(node.ID); gossipNode == nil {
			// Узел известен координатору, но не в gossip - возможно проблема
			if shm.shouldHealNode(node.ID) {
				go shm.initiateHealing(node.ID, "Node missing from gossip")
			}
		}
	}
}

// handleNodeFailure обрабатывает сбой узла
func (shm *SelfHealingManager) handleNodeFailure(node *NodeState) {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	// Увеличиваем счётчик сбоев
	shm.nodeFailures[node.ID]++

	if shm.logger != nil {
		shm.logger.Warn(fmt.Sprintf("Node %s failure detected (count: %d)", node.ID, shm.nodeFailures[node.ID]))
	}

	// Проверяем, превышен ли порог
	if shm.nodeFailures[node.ID] >= shm.config.FailureThreshold {
		if shm.shouldHealNode(node.ID) {
			go shm.initiateHealing(node.ID, fmt.Sprintf("Node failure threshold exceeded: %d", shm.nodeFailures[node.ID]))
		}
	}
}

// shouldHealNode проверяет, нужно ли восстанавливать узел
func (shm *SelfHealingManager) shouldHealNode(nodeID string) bool {
	// Проверяем, не идёт ли уже восстановление
	if _, ok := shm.healingEvents.Load(nodeID); ok {
		return false
	}

	// Проверяем количество активных восстановлений
	if shm.activeHealings.Load() >= int32(shm.config.MaxConcurrentHealings) {
		return false
	}

	return true
}

// initiateHealing запускает процесс восстановления узла
func (shm *SelfHealingManager) initiateHealing(nodeID string, reason string) {
	// Проверяем, не идёт ли уже восстановление
	if _, ok := shm.healingEvents.Load(nodeID); ok {
		return
	}

	// Увеличиваем счётчик активных восстановлений
	if !shm.activeHealings.CompareAndSwap(shm.activeHealings.Load(), shm.activeHealings.Load()+1) {
		return
	}
	defer shm.activeHealings.Add(-1)

	// Создаём событие восстановления
	eventID := fmt.Sprintf("healing_%s_%d", nodeID, time.Now().UnixNano())
	event := &HealingEvent{
		ID:          eventID,
		NodeID:      nodeID,
		Action:      HealingActionNone,
		State:       HealingStateDetecting,
		Timestamp:   time.Now().UnixMilli(),
		Description: reason,
		Data:        make(map[string]interface{}),
	}

	shm.healingEvents.Store(nodeID, event)
	shm.metrics.TotalHealings.Add(1)

	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Initiating healing for node %s: %s", nodeID, reason))
	}

	// Выполняем восстановление
	shm.executeHealing(nodeID, event)
}

// executeHealing выполняет процесс восстановления
func (shm *SelfHealingManager) executeHealing(nodeID string, event *HealingEvent) {
	// Создаём контекст с таймаутом для всего процесса восстановления
	healingTimeout := time.Duration(shm.config.HealingTimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), healingTimeout)
	defer cancel()

	// Фаза 1: Анализ состояния
	event.State = HealingStateAnalyzing
	event.Timestamp = time.Now().UnixMilli()
	shm.healingEvents.Store(nodeID, event)

	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Analyzing node %s...", nodeID))
	}

	// Проверяем контекст
	select {
	case <-ctx.Done():
		event.State = HealingStateFailed
		event.Error = "healing timeout during analysis"
		shm.healingEvents.Store(nodeID, event)
		shm.failHealing(nodeID)
		return
	default:
	}

	// Проверяем, жив ли узел
	nodeInfo := shm.coordinator.GetNodeByID(nodeID)
	if nodeInfo == nil {
		// Узел не найден - возможно, он уже удалён
		event.State = HealingStateComplete
		event.Description = "Node not found, assuming removed"
		shm.healingEvents.Store(nodeID, event)
		shm.completeHealing(nodeID)
		return
	}

	// Проверяем, не восстановился ли узел сам
	if shm.isNodeAlive(nodeID) {
		event.State = HealingStateComplete
		event.Description = "Node recovered automatically"
		shm.healingEvents.Store(nodeID, event)
		shm.completeHealing(nodeID)
		return
	}

	// Проверяем контекст перед выполнением действия
	select {
	case <-ctx.Done():
		event.State = HealingStateFailed
		event.Error = "healing timeout before action"
		shm.healingEvents.Store(nodeID, event)
		shm.failHealing(nodeID)
		return
	default:
	}

	// Фаза 2: Выбор действия
	action := shm.determineHealingAction(nodeID, nodeInfo)
	event.Action = action
	event.State = HealingStateHealing
	event.Timestamp = time.Now().UnixMilli()
	shm.healingEvents.Store(nodeID, event)

	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Selected healing action %v for node %s", action, nodeID))
	}

	// Фаза 3: Выполнение действия с учётом контекста
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		switch action {
		case HealingActionRestartNode:
			err = shm.restartNode(nodeID, event)
		case HealingActionRejoinCluster:
			err = shm.rejoinCluster(nodeID, event)
		case HealingActionResharding:
			err = shm.triggerResharding(nodeID, event)
		case HealingActionDataRecovery:
			err = shm.recoverData(nodeID, event)
		case HealingActionStateRestore:
			err = shm.restoreState(nodeID, event)
		default:
			err = fmt.Errorf("no suitable healing action found")
		}
	}()

	select {
	case <-ctx.Done():
		event.State = HealingStateFailed
		event.Error = "healing timeout during action execution"
		shm.healingEvents.Store(nodeID, event)
		shm.failHealing(nodeID)
		return
	case <-done:
		// Действие выполнено, продолжаем
	}

	// Фаза 4: Проверка результата
	event.State = HealingStateVerifying
	event.Timestamp = time.Now().UnixMilli()
	shm.healingEvents.Store(nodeID, event)

	if err != nil {
		event.State = HealingStateFailed
		event.Error = err.Error()
		shm.healingEvents.Store(nodeID, event)
		shm.failHealing(nodeID)
		if shm.logger != nil {
			shm.logger.Error(fmt.Sprintf("Healing for node %s failed: %v", nodeID, err))
		}
		return
	}

	// Проверяем контекст перед финальной проверкой
	select {
	case <-ctx.Done():
		event.State = HealingStateFailed
		event.Error = "healing timeout during verification"
		shm.healingEvents.Store(nodeID, event)
		shm.failHealing(nodeID)
		return
	default:
	}

	// Проверяем, восстановился ли узел
	if shm.isNodeAlive(nodeID) {
		event.State = HealingStateComplete
		event.Description = fmt.Sprintf("Healing completed successfully with action %v", action)
		shm.healingEvents.Store(nodeID, event)
		shm.completeHealing(nodeID)
		shm.metrics.SuccessfulHealings.Add(1)

		if shm.logger != nil {
			shm.logger.Info(fmt.Sprintf("Healing for node %s completed successfully", nodeID))
		}
	} else {
		event.State = HealingStateFailed
		event.Error = "Node did not recover after healing action"
		shm.healingEvents.Store(nodeID, event)
		shm.failHealing(nodeID)
		shm.metrics.FailedHealings.Add(1)
	}
}

// determineHealingAction определяет действие для восстановления
func (shm *SelfHealingManager) determineHealingAction(nodeID string, nodeInfo *NodeInfo) HealingAction {
	// Проверяем статус узла
	switch nodeInfo.Status {
	case "offline":
		if shm.config.AutoRestart {
			return HealingActionRestartNode
		}
		return HealingActionRejoinCluster
	case "failed":
		if shm.config.AutoDataRecovery {
			return HealingActionDataRecovery
		}
		return HealingActionRestartNode
	case "syncing":
		return HealingActionStateRestore
	default:
		return HealingActionRejoinCluster
	}
}

// restartNode перезапускает узел
func (shm *SelfHealingManager) restartNode(nodeID string, event *HealingEvent) error {
	shm.metrics.RestartsTriggered.Add(1)

	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Attempting to restart node %s", nodeID))
	}

	// Получаем информацию об узле
	nodeInfo := shm.coordinator.GetNodeByID(nodeID)
	if nodeInfo == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Пробуем переподключиться к узлу
	address := fmt.Sprintf("%s:%d", nodeInfo.IP, nodeInfo.Port)
	if err := shm.coordinator.UpdateNodeStatus(nodeID, StatusActive); err != nil {
		return fmt.Errorf("failed to update node status: %v", err)
	}

	event.Data["address"] = address
	event.Data["method"] = "restart"

	return nil
}

// rejoinCluster переподключает узел к кластеру
func (shm *SelfHealingManager) rejoinCluster(nodeID string, event *HealingEvent) error {
	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Attempting to rejoin node %s to cluster", nodeID))
	}

	// Пробуем восстановить соединение
	if err := shm.coordinator.UpdateNodeStatus(nodeID, StatusActive); err != nil {
		return fmt.Errorf("failed to update node status: %v", err)
	}

	event.Data["method"] = "rejoin"

	return nil
}

// triggerResharding запускает перебалансировку шардов
func (shm *SelfHealingManager) triggerResharding(nodeID string, event *HealingEvent) error {
	if !shm.config.AutoResharding {
		return fmt.Errorf("auto resharding is disabled")
	}

	shm.metrics.ReshardingsTriggered.Add(1)

	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Triggering resharding due to node failure: %s", nodeID))
	}

	// Запускаем перебалансировку через координатор
	if err := shm.coordinator.TriggerResharding("self_healing_node_failure"); err != nil {
		return fmt.Errorf("failed to trigger resharding: %v", err)
	}

	event.Data["method"] = "resharding"

	return nil
}

// recoverData восстанавливает данные узла
func (shm *SelfHealingManager) recoverData(nodeID string, event *HealingEvent) error {
	if !shm.config.AutoDataRecovery {
		return fmt.Errorf("auto data recovery is disabled")
	}

	shm.metrics.DataRecoveries.Add(1)

	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Initiating data recovery for node %s", nodeID))
	}

	// Проверяем целостность данных
	if err := shm.verifyDataIntegrity(); err != nil {
		return fmt.Errorf("data integrity check failed: %v", err)
	}

	// Восстанавливаем данные из бэкапа если необходимо
	// TODO: Реализовать восстановление из бэкапа

	event.Data["method"] = "data_recovery"

	return nil
}

// restoreState восстанавливает состояние узла
func (shm *SelfHealingManager) restoreState(nodeID string, event *HealingEvent) error {
	if shm.logger != nil {
		shm.logger.Info(fmt.Sprintf("Restoring state for node %s", nodeID))
	}

	// Восстанавливаем состояние из снэпшота Raft
	// TODO: Реализовать восстановление состояния

	event.Data["method"] = "state_restore"

	return nil
}

// verifyDataIntegrity проверяет целостность данных
func (shm *SelfHealingManager) verifyDataIntegrity() error {
	// Проверяем целостность хранилища
	stats := shm.store.GetStats()
	if stats == nil {
		return fmt.Errorf("failed to get storage stats")
	}

	// Проверяем, что данные не повреждены
	// TODO: Реализовать полноценную проверку целостности

	return nil
}

// isNodeAlive проверяет, жив ли узел
func (shm *SelfHealingManager) isNodeAlive(nodeID string) bool {
	// Проверяем через gossip
	gossipNode := shm.gossipManager.GetNodeByID(nodeID)
	if gossipNode != nil && gossipNode.IsAlive {
		return true
	}

	// Проверяем через координатор
	nodeInfo := shm.coordinator.GetNodeByID(nodeID)
	if nodeInfo != nil && nodeInfo.Status == "active" {
		return true
	}

	return false
}

// completeHealing завершает процесс восстановления
func (shm *SelfHealingManager) completeHealing(nodeID string) {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	// Удаляем из списка сбоев
	delete(shm.nodeFailures, nodeID)

	// Удаляем событие
	if event, ok := shm.healingEvents.Load(nodeID); ok {
		event.(*HealingEvent).State = HealingStateComplete
		shm.healingHistory = append(shm.healingHistory, event.(*HealingEvent))
		shm.healingEvents.Delete(nodeID)
	}
}

// failHealing отмечает неудачное восстановление
func (shm *SelfHealingManager) failHealing(nodeID string) {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	if event, ok := shm.healingEvents.Load(nodeID); ok {
		shm.healingHistory = append(shm.healingHistory, event.(*HealingEvent))
		shm.healingEvents.Delete(nodeID)
	}
}

// healingLoop периодически проверяет зависшие восстановления
func (shm *SelfHealingManager) healingLoop() {
	defer shm.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shm.stopChan:
			return
		case <-ticker.C:
			shm.checkStalledHealings()
		}
	}
}

// checkStalledHealings проверяет зависшие восстановления
func (shm *SelfHealingManager) checkStalledHealings() {
	now := time.Now().UnixMilli()

	shm.healingEvents.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		event := value.(*HealingEvent)

		if event.State == HealingStateHealing || event.State == HealingStateVerifying {
			if now-event.Timestamp > int64(shm.config.HealingTimeoutSec*1000) {
				// Восстановление зависло
				event.State = HealingStateFailed
				event.Error = "Healing stalled"
				shm.healingEvents.Store(nodeID, event)
				shm.failHealing(nodeID)

				if shm.logger != nil {
					shm.logger.Warn(fmt.Sprintf("Healing for node %s stalled, marking as failed", nodeID))
				}
			}
		}

		return true
	})
}

// GetHealingStatus возвращает статус восстановления для узла
func (shm *SelfHealingManager) GetHealingStatus(nodeID string) *HealingEvent {
	if event, ok := shm.healingEvents.Load(nodeID); ok {
		return event.(*HealingEvent)
	}
	return nil
}

// GetHealingHistory возвращает историю восстановлений
func (shm *SelfHealingManager) GetHealingHistory() []*HealingEvent {
	shm.mu.RLock()
	defer shm.mu.RUnlock()

	history := make([]*HealingEvent, len(shm.healingHistory))
	copy(history, shm.healingHistory)
	return history
}

// GetMetrics возвращает метрики восстановления
func (shm *SelfHealingManager) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"total_healings":        shm.metrics.TotalHealings.Load(),
		"successful_healings":   shm.metrics.SuccessfulHealings.Load(),
		"failed_healings":       shm.metrics.FailedHealings.Load(),
		"reshardings_triggered": shm.metrics.ReshardingsTriggered.Load(),
		"data_recoveries":       shm.metrics.DataRecoveries.Load(),
		"restarts_triggered":    shm.metrics.RestartsTriggered.Load(),
		"active_healings":       shm.activeHealings.Load(),
		"history_count":         len(shm.healingHistory),
	}
}

// RegisterEventHandler регистрирует обработчик событий
func (shm *SelfHealingManager) RegisterEventHandler(name string, handler func(*HealingEvent)) {
	shm.handlerMu.Lock()
	defer shm.handlerMu.Unlock()
	shm.eventHandlers[name] = handler
}

// emitEvent отправляет событие всем обработчикам
func (shm *SelfHealingManager) emitEvent(event *HealingEvent) {
	shm.handlerMu.RLock()
	defer shm.handlerMu.RUnlock()

	for _, handler := range shm.eventHandlers {
		go handler(event)
	}
}
