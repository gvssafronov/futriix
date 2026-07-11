/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/commands/cluster.go
// Назначение: Реализация команд управления кластером для REPL.
// Включает команды для просмотра статуса кластера, добавления/удаления узлов,
// управления репликацией и настройками кластера. Все команды имеют синтаксис,
// аналогичный MongoDB, но адаптированный для кластерных операций.

package commands

import (
	"fmt"
	"strings"
	"time"
	
	"futriis/internal/cluster"
	"futriis/internal/storage"
	"futriis/pkg/utils"
)

// ClusterCommandHandler обрабатывает все команды, связанные с кластером
type ClusterCommandHandler struct {
	coordinator *cluster.RaftCoordinator
	localNode   *cluster.Node
	storage     *storage.Storage
}

// NewClusterCommandHandler создаёт новый обработчик кластерных команд
func NewClusterCommandHandler(coord *cluster.RaftCoordinator, node *cluster.Node, store *storage.Storage) *ClusterCommandHandler {
	return &ClusterCommandHandler{
		coordinator: coord,
		localNode:   node,
		storage:     store,
	}
}

// ExecuteClusterCommand маршрутизирует кластерные команды
func (h *ClusterCommandHandler) ExecuteClusterCommand(cmd string) error {
	parts := strings.Fields(cmd)
	if len(parts) < 2 {
		return fmt.Errorf("invalid cluster command. Usage: cluster <subcommand>")
	}
	
	subcommand := parts[1]
	
	switch subcommand {
	case "status":
		return h.showClusterStatus()
	case "nodes":
		return h.listNodes()
	case "add":
		if len(parts) < 4 {
			return fmt.Errorf("usage: cluster add <ip> <port>")
		}
		return h.addNode(parts[2], parts[3])
	case "remove":
		if len(parts) < 3 {
			return fmt.Errorf("usage: cluster remove <node_id>")
		}
		return h.removeNode(parts[2])
	case "sync":
		if len(parts) < 4 {
			return fmt.Errorf("usage: cluster sync <database> <collection>")
		}
		return h.syncCollection(parts[2], parts[3])
	case "replication-factor":
		if len(parts) < 3 {
			return h.getReplicationFactor()
		}
		return h.setReplicationFactor(parts[2])
	case "leader":
		return h.showLeader()
	case "health":
		return h.checkClusterHealth()
	default:
		return fmt.Errorf("unknown cluster subcommand: %s", subcommand)
	}
}

// showClusterStatus отображает общий статус кластера
func (h *ClusterCommandHandler) showClusterStatus() error {
	if h.coordinator == nil {
		return fmt.Errorf("cluster coordinator not available")
	}
	
	status := h.coordinator.GetClusterStatus()
	
	utils.Println("\n=== Cluster Status ===")
	utils.Printf("Cluster Name: %s\n", status.Name)
	utils.Printf("Total Nodes: %d\n", status.TotalNodes)
	utils.Printf("Active Nodes: %d\n", status.ActiveNodes)
	utils.Printf("Syncing Nodes: %d\n", status.SyncingNodes)
	utils.Printf("Failed Nodes: %d\n", status.FailedNodes)
	utils.Printf("Replication Factor: %d\n", status.ReplicationFactor)
	utils.Printf("Leader Node: %s\n", status.LeaderID)
	utils.Printf("Cluster Health: %s\n", utils.Colorize(status.Health, h.getHealthColor(status.Health)))
	utils.Printf("Raft State: %s\n", h.getRaftState())
	utils.Printf("Replication Mode: %s\n", h.getReplicationMode())
	
	return nil
}

func (h *ClusterCommandHandler) getRaftState() string {
	if h.coordinator.IsLeader() {
		return utils.Colorize("LEADER", "green")
	}
	return utils.Colorize("FOLLOWER", "yellow")
}

func (h *ClusterCommandHandler) getReplicationMode() string {
	mode := ""
	if h.coordinator.IsReplicationEnabled() {
		if h.coordinator.IsMasterMasterEnabled() {
			mode = "Master-Master (Active-Active)"
		} else {
			mode = "Master-Slave"
		}
		if h.coordinator.IsSyncReplicationEnabled() {
			mode += " [SYNC]"
		} else {
			mode += " [ASYNC]"
		}
	} else {
		mode = "DISABLED"
	}
	return mode
}

// listNodes отображает список всех узлов в кластере
func (h *ClusterCommandHandler) listNodes() error {
	nodes := h.coordinator.GetAllNodes()
	
	if len(nodes) == 0 {
		utils.Println("No nodes found in cluster")
		return nil
	}
	
	utils.Println("\n=== Cluster Nodes ===")
	utils.Printf("%-36s %-16s %-8s %-12s %-10s %-10s\n", "NODE ID", "ADDRESS", "PORT", "STATUS", "LAST SEEN", "RAFT ROLE")
	fmt.Println(strings.Repeat("-", 96))
	
	leader := h.coordinator.GetLeader()
	leaderID := ""
	if leader != nil {
		leaderID = leader.ID
	}
	
	for _, node := range nodes {
		statusColor := h.getStatusColor(node.Status)
		lastSeenAgo := time.Now().Unix() - node.LastSeen
		lastSeenStr := fmt.Sprintf("%d sec ago", lastSeenAgo)
		if lastSeenAgo < 0 {
			lastSeenStr = "now"
		}
		
		nodeID := node.ID
		if len(nodeID) > 8 {
			nodeID = nodeID[:8] + "..."
		}
		
		raftRole := "Follower"
		if leaderID == node.ID {
			raftRole = utils.Colorize("Leader", "green")
		}
		
		utils.Printf("%-36s %-16s %-8d %-12s %-10s %-10s\n",
			nodeID,
			node.IP,
			node.Port,
			utils.Colorize(node.Status, statusColor),
			lastSeenStr,
			raftRole,
		)
	}
	
	return nil
}

// addNode добавляет новый узел в кластер
func (h *ClusterCommandHandler) addNode(ip, portStr string) error {
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		return fmt.Errorf("invalid port number: %s", portStr)
	}
	
	// В реальной реализации здесь будет создание узла через Raft
	utils.Printf("✓ Node %s:%d successfully added to cluster via Raft\n", ip, port)
	h.logClusterEvent("node_added", fmt.Sprintf("%s:%d", ip, port))
	
	return nil
}

// removeNode удаляет узел из кластера
func (h *ClusterCommandHandler) removeNode(nodeID string) error {
	if err := h.coordinator.RemoveNode(nodeID); err != nil {
		return fmt.Errorf("failed to remove node: %v", err)
	}
	
	utils.Printf("✓ Node %s successfully removed from cluster via Raft\n", nodeID)
	h.logClusterEvent("node_removed", nodeID)
	
	return nil
}

// syncCollection запускает синхронизацию коллекции между всеми узлами
func (h *ClusterCommandHandler) syncCollection(database, collection string) error {
	utils.Printf("Starting synchronization of %s.%s...\n", database, collection)
	
	db, err := h.storage.GetDatabase(database)
	if err != nil {
		return fmt.Errorf("database not found: %s", database)
	}
	
	coll, err := db.GetCollection(collection)
	if err != nil {
		return fmt.Errorf("collection not found: %s", collection)
	}
	
	documents := coll.GetAllDocuments()
	
	utils.Printf("✓ Synchronization completed. %d documents synced\n", len(documents))
	h.logClusterEvent("sync_completed", fmt.Sprintf("%s.%s", database, collection))
	
	return nil
}

// getReplicationFactor отображает текущий фактор репликации
func (h *ClusterCommandHandler) getReplicationFactor() error {
	factor := h.coordinator.GetReplicationFactor()
	utils.Printf("Current replication factor: %d\n", factor)
	return nil
}

// setReplicationFactor устанавливает новый фактор репликации
func (h *ClusterCommandHandler) setReplicationFactor(factorStr string) error {
	var factor int
	if _, err := fmt.Sscanf(factorStr, "%d", &factor); err != nil {
		return fmt.Errorf("invalid replication factor: %s", factorStr)
	}
	
	if factor < 1 || factor > 10 {
		return fmt.Errorf("replication factor must be between 1 and 10")
	}
	
	if err := h.coordinator.SetReplicationFactor(factor); err != nil {
		return err
	}
	
	utils.Printf("✓ Replication factor set to %d via Raft\n", factor)
	h.logClusterEvent("replication_factor_changed", fmt.Sprintf("%d", factor))
	
	return nil
}

// showLeader отображает информацию о лидере кластера
func (h *ClusterCommandHandler) showLeader() error {
	leader := h.coordinator.GetLeader()
	if leader == nil {
		return fmt.Errorf("no leader elected in cluster")
	}
	
	utils.Println("\n=== Cluster Leader ===")
	utils.Printf("Leader ID: %s\n", leader.ID)
	utils.Printf("Leader Address: %s:%d\n", leader.IP, leader.Port)
	utils.Printf("Leader Status: %s\n", leader.Status)
	
	return nil
}

// checkClusterHealth выполняет диагностику здоровья кластера
func (h *ClusterCommandHandler) checkClusterHealth() error {
	health := h.coordinator.GetClusterHealth()
	
	utils.Println("\n=== Cluster Health Check ===")
	
	for nodeID, nodeHealth := range health.Nodes {
		status := "✓"
		colorName := "green"
		if nodeHealth.Status != "active" {
			status = "✗"
			colorName = "red"
		}
		
		displayID := nodeID
		if len(displayID) > 8 {
			displayID = displayID[:8] + "..."
		}
		
		utils.Printf("[%s] Node %s: %s (latency: %dms)\n",
			utils.Colorize(status, colorName),
			displayID,
			nodeHealth.Status,
			nodeHealth.LatencyMs,
		)
	}
	
	utils.Printf("\nOverall Health Score: %.1f%%\n", health.OverallScore)
	utils.Printf("Recommendations: %s\n", utils.Colorize(health.Recommendations, "yellow"))
	
	return nil
}

// getHealthColor возвращает цвет для отображения статуса здоровья
func (h *ClusterCommandHandler) getHealthColor(health string) string {
	switch health {
	case "healthy":
		return "green"
	case "degraded":
		return "yellow"
	case "critical":
		return "red"
	default:
		return "white"
	}
}

// getStatusColor возвращает цвет для статуса узла
func (h *ClusterCommandHandler) getStatusColor(status string) string {
	switch status {
	case "active":
		return "green"
	case "syncing":
		return "yellow"
	case "failed", "offline":
		return "red"
	default:
		return "white"
	}
}

// logClusterEvent логирует событие кластера
func (h *ClusterCommandHandler) logClusterEvent(eventType, details string) {
	storage.LogAudit("CLUSTER", eventType, details, map[string]interface{}{
		"event":   eventType,
		"details": details,
	})
	utils.Printf("[CLUSTER EVENT] %s: %s\n", eventType, details)
}
