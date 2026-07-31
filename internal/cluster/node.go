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
//
// ОСНОВНЫЕ ФУНКЦИИ:
// 1. Управление жизненным циклом узла (создание, запуск, остановка)
// 2. TCP-сервер для приёма входящих соединений от других узлов
// 3. Обработка различных типов запросов (репликация, запросы, синхронизация, heartbeat)
// 4. Управление состоянием узла (active, syncing, offline, failed)
// 5. Интеграция с Raft-координатором для управления кластером
// 6. Пул воркеров для асинхронной обработки запросов
// 7. Механизм восстановления после паники (panic recovery)
// 8. Репликация документов между узлами кластера
// 9. Сбор статистики и мониторинг состояния

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
    "futriis/internal/config" 
)

// =============================================================================
// NETWORK REPLICATOR - ЗАГЛУШКА
// =============================================================================

// NetworkReplicator - заглушка для сетевой репликации.
// В реальной реализации этот компонент отвечал бы за отправку данных на другие узлы,  с поддержкой повторных попыток, backoff и джиттера.
// Полная реализация с поддержкой протокола gRPC или HTTP/2
type NetworkReplicator struct{}

// NewNetworkReplicator создаёт новый экземпляр NetworkReplicator.
// Принимает конфигурацию повторных попыток, пул воркеров и логгер.
// Возвращает заглушку, которая не выполняет реальной репликации.
func NewNetworkReplicator(config *ReplicationRetryConfig, workerPool *WorkerPool, logger *log.Logger) *NetworkReplicator {
    return &NetworkReplicator{}
}

// ReplicationRetryConfig определяет конфигурацию повторных попыток для репликации.
// Содержит параметры экспоненциальной задержки и джиттера.
type ReplicationRetryConfig struct {
    MaxRetries     int           // Максимальное количество попыток
    InitialBackoff time.Duration // Начальная задержка между попытками
    MaxBackoff     time.Duration // Максимальная задержка
    BackoffFactor  float64       // Множитель для экспоненциального увеличения задержки
    JitterEnabled  bool          // Включение случайного джиттера для предотвращения "thundering herd"
}

// DefaultReplicationRetryConfig возвращает конфигурацию по умолчанию.
// Настройки подобраны для баланса между надёжностью и производительностью.
func DefaultReplicationRetryConfig() *ReplicationRetryConfig {
    return &ReplicationRetryConfig{
        MaxRetries:     5,
        InitialBackoff: 100 * time.Millisecond,
        MaxBackoff:     10 * time.Second,
        BackoffFactor:  2.0,
        JitterEnabled:  true,
    }
}

// Close закрывает репликатор и освобождает ресурсы.
// В заглушке ничего не делает.
func (nr *NetworkReplicator) Close() error {
    return nil
}

// GetStats возвращает статистику работы репликатора.
// В заглушке возвращает информацию о том, что репликация отключена.
func (nr *NetworkReplicator) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "enabled": false,
        "message": "NetworkReplicator is a stub - full implementation pending",
    }
}

// Replicate выполняет репликацию данных на целевой узел.
// В заглушке всегда возвращает nil (успех).
func (nr *NetworkReplicator) Replicate(targetNodeID, targetAddress string, data []byte) error {
    return nil
}

// =============================================================================
// ОСНОВНЫЕ ТИПЫ
// =============================================================================

// LoggerInterface определяет интерфейс для логирования.
// Используется для абстракции от конкретной реализации логгера.
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

// NodeStatus представляет состояние узла кластера.
// Использует атомарные операции для потокобезопасного доступа.
type NodeStatus int32

const (
    StatusOffline NodeStatus = iota // Узел отключён или недоступен
    StatusActive                    // Узел активен и готов к работе
    StatusSyncing                   // Узел синхронизируется с кластером
    StatusFailed                    // Узел в состоянии ошибки
)

// NodeRequest представляет запрос между узлами кластера.
// Используется для передачи данных в формате JSON через TCP.
type NodeRequest struct {
    Type      string          `json:"type"`       // Тип запроса: replicate, query, sync, heartbeat, status_sync
    FromNode  string          `json:"from_node"`  // ID узла-отправителя
    Data      json.RawMessage `json:"data"`       // Данные запроса в формате JSON
    Timestamp int64           `json:"timestamp"`  // Временная метка запроса (Unix millis)
}

// NodeInfo представляет информацию об узле в кластере.
// Используется для обмена метаданными между узлами.
type NodeInfo struct {
    ID        string `json:"id"`         // Уникальный идентификатор узла
    IP        string `json:"ip"`         // IP-адрес узла
    Port      int    `json:"port"`       // Порт для TCP-соединений
    Status    string `json:"status"`     // Текущий статус узла
    LastSeen  int64  `json:"last_seen"`  // Время последнего контакта (Unix millis)
    JoinedAt  int64  `json:"joined_at"`  // Время присоединения к кластеру
    UpdatedAt int64  `json:"updated_at"` // Время последнего обновления информации
    Version   int    `json:"version"`    // Версия узла для обнаружения изменений
}

// ShardInfo представляет информацию о шарде.
// Используется для управления распределением данных между узлами.
type ShardInfo struct {
    ID             string   `json:"id"`               // Уникальный идентификатор шарда
    Name           string   `json:"name"`             // Имя шарда
    Nodes          []string `json:"nodes"`            // Список узлов, хранящих шард
    LeaderNode     string   `json:"leader_node"`      // Лидер шарда
    Status         string   `json:"status"`           // Статус шарда
    CreatedAt      int64    `json:"created_at"`       // Время создания
    UpdatedAt      int64    `json:"updated_at"`       // Время последнего обновления
    LastRebalanced int64    `json:"last_rebalanced"`  // Время последней перебалансировки
    DocumentCount  int64    `json:"document_count"`   // Количество документов в шарде
    SizeBytes      int64    `json:"size_bytes"`       // Размер шарда в байтах
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// SafeGoWithLogger запускает горутину с защитой от паники.
// При панике горутина перезапускается с задержкой 5 секунд.
// Это обеспечивает самовосстановление критических компонентов.
//
// Параметры:
//   - fn: функция для выполнения в горутине
//   - logger: логгер для записи ошибок
//   - name: имя горутины для идентификации в логах
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

// Node представляет узел в распределённом кластере.
// Каждый узел имеет свой TCP-сервер, хранилище данных и интеграцию с Raft-координатором.
//
// ПОТОКОВАЯ БЕЗОПАСНОСТЬ:
// - Status, lastSeen, joinedAt используют atomic для потокобезопасного доступа
// - mu защищает операции, изменяющие состояние узла
// - connPool использует sync.Map для потокобезопасного хранения соединений
// - routinesMu защищает доступ к recoverableRoutines
type Node struct {
    // Основная информация об узле
    ID           string // Уникальный идентификатор узла (UUID)
    IP           string // IP-адрес для приёма соединений
    Port         int    // Порт для TCP-сервера
    
    // Состояние узла (атомарное для потокобезопасности)
    Status       atomic.Int32 // Текущий статус (NodeStatus)
    
    // Хранилище и компоненты
    Storage      *storage.Storage // Хранилище данных
    logger       *log.Logger      // Логгер для записи событий
    coordinator  *RaftCoordinator // Координатор для управления кластером (Raft)
    
    // Временные метки (атомарные)
    lastSeen     atomic.Int64 // Время последнего полученного heartbeat
    joinedAt     atomic.Int64 // Время присоединения к кластеру
    
    // Время жизненного цикла
    createdAt    int64 // Время создания узла
    startedAt    int64 // Время запуска TCP-сервера
    stoppedAt    int64 // Время остановки узла
    
    // Сетевые компоненты
    incomingConn chan net.Conn // Канал для входящих TCP-соединений (буфер 10000)
    stopChan     chan struct{} // Канал для остановки всех горутин
    
    // Статистика (атомарная)
    requestCount atomic.Uint64 // Счётчик обработанных запросов
    bytesRx      atomic.Uint64 // Количество полученных байт
    bytesTx      atomic.Uint64 // Количество отправленных байт
    
    // Защита состояния
    mu           sync.RWMutex // Блокировка для операций изменения состояния
    
    // Компоненты обработки
    workerPool   *WorkerPool         // Пул воркеров для асинхронной обработки
    replicator   *NetworkReplicator  // Репликатор для отправки данных
    connPool     sync.Map            // Пул активных TCP-соединений
    
    // Механизм восстановления после паники
    panicRecoveryMgr *PanicRecoveryManager        // Менеджер восстановления
    recoverableRoutines map[string]*RecoverableRoutine // Зарегистрированные восстанавливаемые горутины
    routinesMu         sync.RWMutex               // Защита доступа к recoverableRoutines
}

// NodeConfig представляет конфигурацию для создания узла.
// Используется для передачи зависимостей при создании.
type NodeConfig struct {
    IP          string                 // IP-адрес узла
    Port        int                    // Порт для TCP-сервера
    Storage     *storage.Storage       // Хранилище данных
    Logger      *log.Logger            // Логгер
    Coordinator *RaftCoordinator       // Raft-координатор
    PanicRecoveryMgr *PanicRecoveryManager // Менеджер восстановления
}

// =============================================================================
// СОЗДАНИЕ УЗЛА
// =============================================================================

// NewNode создаёт новый узел с механизмом восстановления по умолчанию.
// Это упрощённый конструктор для случаев, когда менеджер восстановления не требуется.
//
// Параметры:
//   - ip: IP-адрес для приёма соединений
//   - port: порт для TCP-сервера
//   - store: хранилище данных
//   - logger: логгер
//
// Возвращает: указатель на созданный узел
func NewNode(ip string, port int, store *storage.Storage, logger *log.Logger) *Node {
    return NewNodeWithRecovery(ip, port, store, logger, nil)
}

// NewNodeWithRecovery создаёт новый узел с поддержкой восстановления после паники.
// Это основной конструктор, который инициализирует все компоненты узла.
//
// ПОРЯДОК ИНИЦИАЛИЗАЦИИ:
// 1. Создание пула воркеров и репликатора
// 2. Инициализация структуры Node
// 3. Запуск восстанавливаемых горутин (если есть менеджер)
// 4. Запуск горутин с защитой от паники (если нет менеджера)
//
// Параметры:
//   - ip: IP-адрес для приёма соединений
//   - port: порт для TCP-сервера
//   - store: хранилище данных
//   - logger: логгер
//   - panicRecoveryMgr: менеджер восстановления (может быть nil)
//
// Возвращает: указатель на созданный узел
func NewNodeWithRecovery(ip string, port int, store *storage.Storage, logger *log.Logger, panicRecoveryMgr *PanicRecoveryManager) *Node {
    now := time.Now().UnixMilli()
    
    // Создаём пул воркеров с максимальным количеством 500 задач
    workerPool := NewWorkerPool(500, logger)
    // Создаём репликатор с настройками по умолчанию
    replicator := NewNetworkReplicator(DefaultReplicationRetryConfig(), workerPool, logger)
    
    // Инициализируем структуру узла
    node := &Node{
        ID:           uuid.New().String(), // Генерируем уникальный ID
        IP:           ip,
        Port:         port,
        Storage:      store,
        logger:       logger,
        incomingConn: make(chan net.Conn, 10000), // Буфер на 10000 соединений
        stopChan:     make(chan struct{}),
        createdAt:    now,
        startedAt:    now,
        workerPool:   workerPool,
        replicator:   replicator,
        panicRecoveryMgr: panicRecoveryMgr,
        recoverableRoutines: make(map[string]*RecoverableRoutine),
    }
    
    // Устанавливаем начальный статус и временные метки
    node.Status.Store(int32(StatusActive))
    node.lastSeen.Store(now)
    node.joinedAt.Store(0) // 0 означает "не присоединён"
    
    // Запускаем горутины с механизмом восстановления или без него
    if panicRecoveryMgr != nil {
        // Используем восстанавливаемые горутины для критических компонентов
        node.startRecoverableRoutine("TCPServer", node.startTCPServer)
        node.startRecoverableRoutine("IncomingConnections", node.handleIncomingConnections)
        node.startRecoverableRoutine("HeartbeatLoop", node.heartbeatLoop)
        node.startRecoverableRoutine("ConnectionHealthMonitor", node.connectionHealthMonitor)
        logger.Info(fmt.Sprintf("Node %s created with panic recovery (max workers: 500)", node.ID))
    } else {
        // Используем базовую защиту от паники с перезапуском
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

// startRecoverableRoutine запускает горутину с поддержкой восстановления.
// Если panicRecoveryMgr доступен, горутина будет автоматически перезапущена при панике.
// В противном случае используется базовый механизм SafeGoWithLogger.
//
// Параметры:
//   - name: имя горутины для идентификации
//   - fn: функция для выполнения
func (n *Node) startRecoverableRoutine(name string, fn func()) {
    // Если менеджер восстановления недоступен, используем базовый механизм
    if n.panicRecoveryMgr == nil {
        SafeGoWithLogger(fn, n.logger, name)
        return
    }
    
    // Блокируем доступ к карте восстанавливаемых горутин
    n.routinesMu.Lock()
    defer n.routinesMu.Unlock()
    
    // Создаём новую восстанавливаемую горутину
    routine := NewRecoverableRoutine(name, func() error {
        fn()
        return nil
    }, n.panicRecoveryMgr, 10) // Максимум 10 перезапусков
    
    // Сохраняем и запускаем
    n.recoverableRoutines[name] = routine
    routine.Start()
    
    if n.logger != nil {
        n.logger.Debug(fmt.Sprintf("Started recoverable routine: %s", name))
    }
}

// stopRecoverableRoutine останавливает восстанавливаемую горутину.
// Используется при остановке узла для корректного завершения.
//
// Параметры:
//   - name: имя горутины для остановки
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

// startTCPServer запускает TCP-сервер для приёма входящих соединений.
// Это основная точка входа для межузлового взаимодействия.
//
// ОСОБЕННОСТИ:
// - Использует deadline для предотвращения блокировки accept
// - Автоматически перезапускается при панике
// - Обрабатывает ошибки таймаута без остановки сервера
// - Передаёт соединения в канал incomingConn для асинхронной обработки
func (n *Node) startTCPServer() {
    // Защита от паники с автоматическим перезапуском
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
    
    // Формируем адрес и начинаем прослушивание
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
    
    // Основной цикл приёма соединений
    for {
        select {
        case <-n.stopChan:
            // Сигнал остановки получен
            if n.logger != nil {
                n.logger.Info(fmt.Sprintf("Node %s TCP server stopped", n.ID))
            }
            return
        default:
            // Устанавливаем deadline для таймаута accept (5 секунд)
            // Это позволяет периодически проверять stopChan
            if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
                continue
            }
            
            // Принимаем новое соединение
            conn, err := listener.Accept()
            if err != nil {
                // Проверяем, не является ли ошибка таймаутом (это нормально)
                if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
                    continue
                }
                if n.logger != nil {
                    n.logger.Error(fmt.Sprintf("Node %s accept error: %v", n.ID, err))
                }
                continue
            }
            
            // Устанавливаем таймауты для соединения
            conn.SetReadDeadline(time.Now().Add(30 * time.Second))
            conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
            
            // Передаём соединение в канал для асинхронной обработки
            select {
            case n.incomingConn <- conn:
                n.bytesRx.Add(1)
            default:
                // Канал переполнен - закрываем соединение
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

// handleIncomingConnections обрабатывает входящие соединения из канала.
// Каждое соединение обрабатывается асинхронно через пул воркеров.
//
// РАБОТА С ПУЛОМ ВОРКЕРОВ:
// 1. Получение соединения из канала
// 2. Создание задачи с уникальным ID
// 3. Отправка задачи в пул воркеров
// 4. При ошибке отправки - закрытие соединения
func (n *Node) handleIncomingConnections() {
    // Защита от паники с автоматическим перезапуском
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
            // Увеличиваем счётчик запросов
            n.requestCount.Add(1)
            
            // Создаём задачу для обработки соединения
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

// handleNodeRequest обрабатывает один запрос от узла.
// Декодирует JSON-запрос и направляет его соответствующему обработчику.
//
// ТИПЫ ЗАПРОСОВ:
//   - replicate: репликация документа
//   - query: запрос документа
//   - sync: синхронизация коллекции
//   - heartbeat: проверка доступности узла
//   - status_sync: синхронизация статуса кластера
func (n *Node) handleNodeRequest(conn net.Conn) {
    // Защита от паники - всегда закрываем соединение
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Request handler panicked: %v\n%s", r, debug.Stack()))
            }
        }
        conn.Close()
    }()
    
    // Устанавливаем таймаут на чтение
    conn.SetReadDeadline(time.Now().Add(30 * time.Second))
    
    // Декодируем запрос из JSON
    decoder := json.NewDecoder(conn)
    var req NodeRequest
    if err := decoder.Decode(&req); err != nil {
        if err != io.EOF && n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to decode request: %v", n.ID, err))
        }
        return
    }
    
    // Обновляем время последнего контакта
    n.lastSeen.Store(time.Now().UnixMilli())
    
    if n.logger != nil {
        n.logger.Debug(fmt.Sprintf("Node %s received request type %s from %s at %s", 
            n.ID, req.Type, req.FromNode, time.UnixMilli(req.Timestamp).Format("15:04:05.000")))
    }
    
    // Маршрутизация по типу запроса
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

// handleReplicateRequest обрабатывает запрос на репликацию документа.
// Сохраняет полученный документ в локальном хранилище.
//
// ПОРЯДОК ОБРАБОТКИ:
// 1. Декодирование данных репликации
// 2. Получение базы данных и коллекции
// 3. Создание документа с полями
// 4. Вставка документа в коллекцию
// 5. Логирование результата
func (n *Node) handleReplicateRequest(data []byte) {
    // Защита от паники
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Replicate request handler panicked: %v\n%s", r, debug.Stack()))
            }
        }
    }()
    
    startTime := time.Now().UnixMilli()
    
    // Структура данных репликации
    var repData struct {
        Database   string                 `json:"database"`    // Имя базы данных
        Collection string                 `json:"collection"`  // Имя коллекции
        Document   map[string]interface{} `json:"document"`    // Данные документа
        SourceNode string                 `json:"source_node"` // Узел-источник
        ReplicaID  string                 `json:"replica_id"`  // Уникальный ID репликации
    }
    
    if err := json.Unmarshal(data, &repData); err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to unmarshal replicate data: %v", n.ID, err))
        }
        return
    }
    
    // Получаем базу данных
    db, err := n.Storage.GetDatabase(repData.Database)
    if err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s database not found for replication: %s", n.ID, repData.Database))
        }
        return
    }
    
    // Получаем коллекцию
    coll, err := db.GetCollection(repData.Collection)
    if err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s collection not found for replication: %s", n.ID, repData.Collection))
        }
        return
    }
    
    // Извлекаем ID документа
    docID, ok := repData.Document["_id"].(string)
    if !ok {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s document missing _id field", n.ID))
        }
        return
    }
    
    // Создаём документ с полученными полями
    doc := storage.NewDocumentWithID(docID)
    for k, v := range repData.Document {
        doc.SetField(k, v)
    }
    
    // Вставляем документ в коллекцию
    if err := coll.Insert(doc); err != nil {
        if n.logger != nil {
            n.logger.Error(fmt.Sprintf("Node %s failed to replicate document: %v", n.ID, err))
        }
    } else {
        // Логируем успешную репликацию
        duration := time.Now().UnixMilli() - startTime
        if n.logger != nil {
            n.logger.Debug(fmt.Sprintf("Node %s replicated document %s from %s (took %d ms)", 
                n.ID, doc.ID, repData.SourceNode, duration))
        }
    }
}

// handleQueryRequest обрабатывает запрос на получение документа.
// Находит документ по ID и возвращает его в ответе.
//
// ПОРЯДОК ОБРАБОТКИ:
// 1. Декодирование данных запроса
// 2. Получение базы данных и коллекции
// 3. Поиск документа по ID
// 4. Формирование успешного или ошибочного ответа
func (n *Node) handleQueryRequest(data []byte, conn net.Conn) {
    // Защита от паники с отправкой ошибки
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Query request handler panicked: %v\n%s", r, debug.Stack()))
            }
            n.sendErrorResponse(conn, "Internal server error")
        }
    }()
    
    startTime := time.Now().UnixMilli()
    
    // Структура данных запроса
    var queryData struct {
        Database   string `json:"database"`    // Имя базы данных
        Collection string `json:"collection"`  // Имя коллекции
        DocumentID string `json:"document_id"` // ID документа
        RequestID  string `json:"request_id"`  // ID запроса для трекинга
    }
    
    if err := json.Unmarshal(data, &queryData); err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Получаем базу данных
    db, err := n.Storage.GetDatabase(queryData.Database)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Получаем коллекцию
    coll, err := db.GetCollection(queryData.Collection)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Ищем документ
    doc, err := coll.Find(queryData.DocumentID)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    duration := time.Now().UnixMilli() - startTime
    
    // Формируем успешный ответ
    response := map[string]interface{}{
        "status":      "success",
        "data":        doc,
        "node_id":     n.ID,
        "request_id":  queryData.RequestID,
        "duration_ms": duration,
        "timestamp":   time.Now().UnixMilli(),
    }
    
    // Отправляем ответ
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

// handleSyncRequest обрабатывает запрос на синхронизацию коллекции.
// Возвращает все документы коллекции (или только изменённые после указанного времени).
//
// ПОРЯДОК ОБРАБОТКИ:
// 1. Декодирование данных запроса
// 2. Получение базы данных и коллекции
// 3. Фильтрация документов по времени (если указано)
// 4. Формирование ответа со всеми документами
func (n *Node) handleSyncRequest(data []byte, conn net.Conn) {
    // Защита от паники с отправкой ошибки
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Sync request handler panicked: %v\n%s", r, debug.Stack()))
            }
            n.sendErrorResponse(conn, "Internal server error")
        }
    }()
    
    startTime := time.Now().UnixMilli()
    
    // Структура данных запроса синхронизации
    var syncData struct {
        Database   string `json:"database"`    // Имя базы данных
        Collection string `json:"collection"`  // Имя коллекции
        RequestID  string `json:"request_id"`  // ID запроса
        Since      int64  `json:"since"`       // Временная метка для инкрементальной синхронизации
    }
    
    if err := json.Unmarshal(data, &syncData); err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Получаем базу данных
    db, err := n.Storage.GetDatabase(syncData.Database)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Получаем коллекцию
    coll, err := db.GetCollection(syncData.Collection)
    if err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Получаем все документы
    docs := coll.GetAllDocuments()
    
    // Фильтруем по времени, если указано
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
    
    // Формируем ответ
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
    
    // Отправляем ответ
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

// handleHeartbeatRequest обрабатывает heartbeat-запрос.
// Подтверждает, что узел жив, и возвращает информацию о статусе.
//
// ОТВЕТ СОДЕРЖИТ:
//   - status: "alive" - подтверждение жизни
//   - node_id: ID текущего узла
//   - timestamp: время обработки
//   - uptime_ms: время работы узла в миллисекундах
func (n *Node) handleHeartbeatRequest(req NodeRequest, conn net.Conn) {
    // Защита от паники
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Heartbeat handler panicked: %v\n%s", r, debug.Stack()))
            }
        }
    }()
    
    // Обновляем время последнего контакта
    n.lastSeen.Store(time.Now().UnixMilli())
    
    // Формируем ответ
    response := map[string]interface{}{
        "status":     "alive",
        "node_id":    n.ID,
        "timestamp":  time.Now().UnixMilli(),
        "uptime_ms":  time.Now().UnixMilli() - n.startedAt,
    }
    
    // Отправляем ответ
    conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
    encoder := json.NewEncoder(conn)
    encoder.Encode(response)
}

// handleStatusSyncRequest обрабатывает запрос на синхронизацию статуса кластера.
// Обновляет информацию о лидере и термине через координатор.
//
// ПОРЯДОК ОБРАБОТКИ:
// 1. Декодирование данных статуса
// 2. Передача данных координатору
// 3. Формирование ответа с текущим статусом
func (n *Node) handleStatusSyncRequest(req NodeRequest, conn net.Conn) {
    // Защита от паники с отправкой ошибки
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Status sync handler panicked: %v\n%s", r, debug.Stack()))
            }
            n.sendErrorResponse(conn, "Internal server error")
        }
    }()
    
    // Структура данных статуса
    var syncStatus struct {
        LeaderID    string `json:"leader_id"`    // ID лидера
        Term        uint64 `json:"term"`         // Текущий термин Raft
        ClusterSize int    `json:"cluster_size"` // Размер кластера
    }
    
    if err := json.Unmarshal(req.Data, &syncStatus); err != nil {
        n.sendErrorResponse(conn, err.Error())
        return
    }
    
    // Передаём статус координатору
    if n.coordinator != nil {
        n.coordinator.HandleStatusSync(syncStatus.LeaderID, syncStatus.Term, syncStatus.ClusterSize)
    }
    
    // Формируем ответ
    response := map[string]interface{}{
        "status":    "synced",
        "node_id":   n.ID,
        "term":      n.coordinator.GetCurrentTerm(),
        "is_leader": n.coordinator.IsLeader(),
        "timestamp": time.Now().UnixMilli(),
    }
    
    // Отправляем ответ
    conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
    encoder := json.NewEncoder(conn)
    encoder.Encode(response)
}

// sendErrorResponse отправляет ответ об ошибке клиенту.
// Используется для единообразного форматирования ошибок.
//
// Параметры:
//   - conn: TCP-соединение
//   - errMsg: текст ошибки
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

// heartbeatLoop периодически отправляет heartbeat в кластер.
// Использует координатор для отправки heartbeat-сообщений.
//
// ИНТЕРВАЛ: 5 секунд
// УСЛОВИЕ: запускается только если координатор доступен
func (n *Node) heartbeatLoop() {
    // Защита от паники с автоматическим перезапуском
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
            // Отправляем heartbeat через координатор
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

// connectionHealthMonitor периодически проверяет здоровье активных соединений.
// Закрывает "мёртвые" соединения и удаляет их из пула.
//
// ИНТЕРВАЛ: 30 секунд
// МЕТОД ПРОВЕРКИ: чтение 1 байта с таймаутом 1 секунда
func (n *Node) connectionHealthMonitor() {
    // Защита от паники с перезапуском
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

// cleanupStaleConnections закрывает "зависшие" соединения.
// Проверяет каждое соединение в пуле и удаляет недоступные.
func (n *Node) cleanupStaleConnections() {
    // Защита от паники
    defer func() {
        if r := recover(); r != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Cleanup connections panicked: %v", r))
            }
        }
    }()
    
    n.connPool.Range(func(key, value interface{}) bool {
        if conn, ok := value.(net.Conn); ok {
            // Проверяем соединение с таймаутом 1 секунда
            conn.SetReadDeadline(time.Now().Add(1 * time.Second))
            buf := make([]byte, 1)
            _, err := conn.Read(buf)
            if err != nil {
                // Соединение недоступно - закрываем
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

// GetNodeStatus возвращает текущий статус узла.
// Потокобезопасный доступ через атомарную операцию.
func (n *Node) GetNodeStatus() NodeStatus {
    return NodeStatus(n.Status.Load())
}

// IsActive проверяет, активен ли узел.
// Возвращает true только для статуса StatusActive.
func (n *Node) IsActive() bool {
    return NodeStatus(n.Status.Load()) == StatusActive
}

// SetStatus устанавливает новый статус узла.
// При изменении статуса уведомляет координатор (если узел является лидером).
//
// ПОТОКОВАЯ БЕЗОПАСНОСТЬ:
// - Использует мьютекс для предотвращения гонок
// - Атомарно обновляет статус через Store
//
// Параметры:
//   - status: новый статус узла
//
// Возвращает: ошибку, если не удалось обновить статус через координатор
func (n *Node) SetStatus(status NodeStatus) error {
    n.mu.Lock()
    defer n.mu.Unlock()
    
    oldStatus := n.Status.Load()
    if oldStatus == int32(status) {
        return nil // Статус не изменился
    }
    
    // Если узел является лидером, обновляем статус через координатор
    if n.coordinator != nil && n.coordinator.IsLeader() {
        if err := n.coordinator.UpdateNodeStatus(n.ID, status); err != nil {
            if n.logger != nil {
                n.logger.Error(fmt.Sprintf("Failed to update node status via Raft: %v", err))
            }
            return err
        }
    }
    
    // Обновляем статус
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

// SetCoordinator устанавливает координатор для узла.
// Также обновляет время присоединения к кластеру.
//
// Параметры:
//   - coord: Raft-координатор
func (n *Node) SetCoordinator(coord *RaftCoordinator) {
    n.coordinator = coord
    now := time.Now().UnixMilli()
    n.joinedAt.Store(now)
    
    if n.logger != nil {
        n.logger.Info(fmt.Sprintf("Node %s joined cluster at %s", n.ID, n.GetJoinedAtStr()))
    }
}

// JoinCluster присоединяет узел к кластеру.
// Регистрирует узел в координаторе и устанавливает активный статус.
//
// ПОРЯДОК ДЕЙСТВИЙ:
// 1. Проверка, что узел ещё не присоединён
// 2. Установка координатора
// 3. Регистрация узла в координаторе
// 4. Установка статуса Active
//
// Параметры:
//   - coord: Raft-координатор
//
// Возвращает: ошибку, если присоединение не удалось
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

// LeaveCluster отключает узел от кластера.
// Устанавливает оффлайн-статус и удаляет узел из координатора.
//
// ПОРЯДОК ДЕЙСТВИЙ:
// 1. Установка статуса Offline
// 2. Удаление узла из координатора
// 3. Очистка координатора и времени присоединения
//
// Возвращает: ошибку, если отключение не удалось
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

// GetLastSeen возвращает время последнего контакта в миллисекундах.
func (n *Node) GetLastSeen() int64 {
    return n.lastSeen.Load()
}

// GetLastSeenStr возвращает строковое представление времени последнего контакта.
func (n *Node) GetLastSeenStr() string {
    lastSeen := n.lastSeen.Load()
    if lastSeen == 0 {
        return "never"
    }
    return time.UnixMilli(lastSeen).Format("2006-01-02 15:04:05.000")
}

// GetJoinedAt возвращает время присоединения к кластеру в миллисекундах.
func (n *Node) GetJoinedAt() int64 {
    return n.joinedAt.Load()
}

// GetJoinedAtStr возвращает строковое представление времени присоединения.
func (n *Node) GetJoinedAtStr() string {
    joinedAt := n.joinedAt.Load()
    if joinedAt == 0 {
        return "not joined"
    }
    return time.UnixMilli(joinedAt).Format("2006-01-02 15:04:05.000")
}

// GetStartedAt возвращает время запуска TCP-сервера в миллисекундах.
func (n *Node) GetStartedAt() int64 {
    return n.startedAt
}

// GetStartedAtStr возвращает строковое представление времени запуска.
func (n *Node) GetStartedAtStr() string {
    return time.UnixMilli(n.startedAt).Format("2006-01-02 15:04:05.000")
}

// GetCreatedAt возвращает время создания узла в миллисекундах.
func (n *Node) GetCreatedAt() int64 {
    return n.createdAt
}

// GetCreatedAtStr возвращает строковое представление времени создания.
func (n *Node) GetCreatedAtStr() string {
    return time.UnixMilli(n.createdAt).Format("2006-01-02 15:04:05.000")
}

// GetUptime возвращает продолжительность работы узла.
func (n *Node) GetUptime() time.Duration {
    if n.startedAt == 0 {
        return 0
    }
    return time.Duration(time.Now().UnixMilli()-n.startedAt) * time.Millisecond
}

// GetAddress возвращает адрес узла в формате "IP:PORT".
func (n *Node) GetAddress() string {
    return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

// =============================================================================
// СТАТИСТИКА
// =============================================================================

// GetStats возвращает полную статистику узла.
// Включает информацию о статусе, времени, нагрузке и компонентах.
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
    
    // Добавляем статистику пула воркеров, если доступен
    if n.workerPool != nil {
        stats["worker_pool"] = n.workerPool.GetStats()
    }
    
    // Добавляем статистику репликации, если доступна
    if n.replicator != nil {
        stats["replication"] = n.replicator.GetStats()
    }
    
    // Добавляем информацию о восстанавливаемых горутинах
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

// GetWorkerPoolStats возвращает статистику пула воркеров.
func (n *Node) GetWorkerPoolStats() map[string]interface{} {
    if n.workerPool == nil {
        return map[string]interface{}{"enabled": false}
    }
    return n.workerPool.GetStats()
}

// GetReplicationStats возвращает статистику репликации.
func (n *Node) GetReplicationStats() map[string]interface{} {
    if n.replicator == nil {
        return map[string]interface{}{"enabled": false}
    }
    return n.replicator.GetStats()
}

// GetPanicRecoveryStats возвращает статистику механизма восстановления.
func (n *Node) GetPanicRecoveryStats() map[string]interface{} {
    if n.panicRecoveryMgr == nil {
        return map[string]interface{}{"enabled": false}
    }
    return n.panicRecoveryMgr.GetStats()
}

// =============================================================================
// РЕПЛИКАЦИЯ ДОКУМЕНТОВ
// =============================================================================

// generateReplicationID генерирует уникальный ID для операции репликации.
// Используется для отслеживания и дедупликации репликаций.
func generateReplicationID() string {
    bytes := make([]byte, 16)
    rand.Read(bytes)
    return base64.URLEncoding.EncodeToString(bytes)
}

// ReplicateDocument реплицирует документ на все активные узлы кластера.
//
// ПОРЯДОК ДЕЙСТВИЙ:
// 1. Получение списка активных узлов от координатора
// 2. Формирование данных репликации
// 3. Асинхронная отправка на каждый узел (через пул воркеров)
// 4. Ожидание завершения всех репликаций с таймаутом
// 5. Возврат результата (успех/частичный успех/ошибка)
//
// Параметры:
//   - database: имя базы данных
//   - collection: имя коллекции
//   - doc: документ для репликации
//
// Возвращает: ошибку, если репликация не удалась
func (n *Node) ReplicateDocument(database, collection string, doc *storage.Document) error {
    if n.coordinator == nil {
        if n.logger != nil {
            n.logger.Warn("No coordinator set, skipping replication")
        }
        return fmt.Errorf("no coordinator set")
    }
    
    // Получаем список активных узлов
    nodes := n.coordinator.GetActiveNodes()
    if len(nodes) <= 1 {
        if n.logger != nil {
            n.logger.Debug("No other nodes for replication")
        }
        return nil // Только текущий узел - репликация не требуется
    }
    
    // Формируем данные для репликации
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
    
    // Отправляем репликацию на все узлы, кроме текущего
    var wg sync.WaitGroup
    var failedCount atomic.Int32
    var successCount atomic.Int32
    
    for _, nodeInfo := range nodes {
        if nodeInfo.ID == n.ID {
            continue // Пропускаем себя
        }
        
        wg.Add(1)
        targetNodeID := nodeInfo.ID
        targetAddress := fmt.Sprintf("%s:%d", nodeInfo.IP, nodeInfo.Port)
        
        // Создаём задачу для каждого узла
        taskID := fmt.Sprintf("replicate_%s_to_%s_%s", doc.ID, targetNodeID, repData.ReplicaID)
        err := n.workerPool.SubmitFunc(taskID, func() error {
            defer wg.Done()
            
            if n.replicator == nil {
                failedCount.Add(1)
                return fmt.Errorf("replicator not initialized")
            }
            
            // Выполняем репликацию
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
    
    // Ожидаем завершения всех репликаций с таймаутом 30 секунд
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
    
    // Проверяем результат
    if failedCount.Load() > 0 {
        return fmt.Errorf("replication partially failed: %d of %d nodes failed", failedCount.Load(), len(nodes)-1)
    }
    
    return nil
}

// =============================================================================
// УПРАВЛЕНИЕ PANIC RECOVERY
// =============================================================================

// SetPanicRecoveryManager устанавливает менеджер восстановления после паники.
// Позволяет включить расширенный механизм восстановления после создания узла.
//
// Параметры:
//   - mgr: менеджер восстановления
func (n *Node) SetPanicRecoveryManager(mgr *PanicRecoveryManager) {
    n.panicRecoveryMgr = mgr
    if n.logger != nil {
        n.logger.Debug("Panic recovery manager set for node")
    }
}

// =============================================================================
// МЕТОДЫ ДЛЯ GOSSIP И SELF-HEALING
// =============================================================================

// GetGossipManager возвращает gossip менеджер узла
func (n *Node) GetGossipManager() *GossipManager {
    if n.coordinator != nil {
        return n.coordinator.GetGossipManager()
    }
    return nil
}

// GetSelfHealingManager возвращает менеджер самоисцеления узла
func (n *Node) GetSelfHealingManager() *SelfHealingManager {
    if n.coordinator != nil {
        return n.coordinator.GetSelfHealingManager()
    }
    return nil
}

// GetDynamicConfigManager возвращает менеджер динамической конфигурации узла
func (n *Node) GetDynamicConfigManager() *config.DynamicConfigManager {
    if n.coordinator != nil {
        return n.coordinator.GetDynamicConfigManager()
    }
    return nil
}

// =============================================================================
// ОСТАНОВКА УЗЛА
// =============================================================================

// Stop останавливает узел и все его компоненты.
// Выполняет корректное завершение всех горутин и освобождение ресурсов.
//
// ПОРЯДОК ОСТАНОВКИ:
// 1. Обновление статуса на Offline
// 2. Остановка всех восстанавливаемых горутин
// 3. Закрытие канала stopChan (сигнал для всех горутин)
// 4. Остановка пула воркеров
// 5. Закрытие репликатора
// 6. Закрытие всех активных TCP-соединений
// 7. Логирование остановки
func (n *Node) Stop() {
    // Обновляем статус перед остановкой
    if n.coordinator != nil && n.coordinator.IsLeader() {
        n.SetStatus(StatusOffline)
    }
    n.Status.Store(int32(StatusOffline))
    n.stoppedAt = time.Now().UnixMilli()
    
    // Останавливаем все восстанавливаемые горутины
    n.routinesMu.RLock()
    for name, routine := range n.recoverableRoutines {
        routine.Stop()
        if n.logger != nil {
            n.logger.Debug(fmt.Sprintf("Stopped recoverable routine: %s", name))
        }
    }
    n.routinesMu.RUnlock()
    
    // Закрываем канал остановки
    close(n.stopChan)
    
    // Останавливаем пул воркеров
    if n.workerPool != nil {
        n.workerPool.Stop()
    }
    
    // Закрываем репликатор
    if n.replicator != nil {
        n.replicator.Close()
    }
    
    // Закрываем все активные соединения
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
