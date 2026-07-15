/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/transactions.go
// Назначение: Реализация транзакций с поддержкой MVCC (Multi-Version Concurrency Control) и WAL (Write-Ahead Log) без блокировок.
// Для распределённых транзакций используется протокол SAGA

package storage

import (
    "bufio"
    "encoding/binary"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sort"
    "sync"
    "sync/atomic"
    "time"

    "futriis/internal/config"
)

// =============================================================================
// БАЗОВЫЕ ТИПЫ
// =============================================================================

// TransactionID - уникальный идентификатор транзакции.
type TransactionID uint64

// TransactionState - состояние транзакции.
type TransactionState int32

const (
    TransactionActive TransactionState = iota
    TransactionCommitted
    TransactionAborted
    TransactionPrepared
)

// TransactionRecord - запись транзакции в WAL.
type TransactionRecord struct {
    ID            TransactionID    `json:"id"`
    State         TransactionState `json:"state"`
    Timestamp     int64            `json:"timestamp"`
    Operations    []Operation      `json:"operations"`
    IsDistributed bool             `json:"is_distributed"`
    Nodes         []string         `json:"nodes,omitempty"`
}

// WALRecord - запись в WAL (Write-Ahead Log).
type WALRecord struct {
    CRC        uint32 `json:"crc"`
    Length     uint32 `json:"length"`
    Type       byte   `json:"type"`
    Data       []byte `json:"data"`
    Timestamp  int64  `json:"timestamp"`
    LSN        uint64 `json:"lsn"`
}

// =============================================================================
// КОНСТАНТЫ
// =============================================================================

const (
    WALSegmentSize = 64 * 1024 * 1024
    WALSegmentPrefix = "wal_segment_"
    WALIndexPrefix = "wal_index_"

    VisibilityMapSize = 1024 * 1024
    VersionPruneInterval = 5 * time.Minute
    MaxVersionsPerDoc = 100
    VersionRetentionDays = 7

    DefaultTxTimeout = 30 * time.Second
    DeadlockCheckInterval = 1 * time.Second
    MaxSavepointsPerTx = 100

    AsyncRecoveryBufferSize = 10000
    AsyncRecoveryWorkers = 4
    AsyncRecoveryTimeout = 30 * time.Second
    FsyncMaxRetries = 3
    FsyncRetryDelay = 100 * time.Millisecond

    // Константы для Persistent SAGA
    SagaStateDir = "saga_states"
    SagaStateFilePrefix = "saga_state_"
    SagaStateFileSuffix = ".json"
    SagaCheckpointInterval = 30 * time.Second
    SagaMaxRetries = 5
    SagaRetryBackoff = 100 * time.Millisecond
)

// =============================================================================
// CRC32 - КОНТРОЛЬНАЯ СУММА
// =============================================================================

var crc32Table = [256]uint32{
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1a4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf8, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
}

func crc32(data []byte) uint32 {
    crc := uint32(0xFFFFFFFF)
    for _, b := range data {
        crc = (crc >> 8) ^ crc32Table[(crc^uint32(b))&0xFF]
    }
    return crc ^ 0xFFFFFFFF
}

// =============================================================================
// AUDIT LOGGER ДЛЯ ТРАНЗАКЦИЙ
// =============================================================================

// TransactionAuditEntry представляет запись аудита транзакции
type TransactionAuditEntry struct {
    TxID         TransactionID          `json:"tx_id"`
    Action       string                 `json:"action"`        // START, COMMIT, ABORT, PREPARE, SAVEPOINT, ROLLBACK
    State        TransactionState       `json:"state"`
    Timestamp    int64                  `json:"timestamp"`
    TimestampStr string                 `json:"timestamp_str"`
    Details      map[string]interface{} `json:"details"`
}

// TransactionAuditLogger управляет аудитом транзакций
type TransactionAuditLogger struct {
    entries   []TransactionAuditEntry
    mu        sync.RWMutex
    maxSize   int
    filePath  string
    fileMu    sync.Mutex
    enabled   bool
}

var globalTxAuditLogger = &TransactionAuditLogger{
    entries: make([]TransactionAuditEntry, 0),
    maxSize: 100000,
    enabled: true,
}

// InitTransactionAuditLogger инициализирует аудит логгер с записью в файл
func InitTransactionAuditLogger(filePath string) error {
    globalTxAuditLogger.filePath = filePath
    if filePath != "" {
        dir := filepath.Dir(filePath)
        if err := os.MkdirAll(dir, 0755); err != nil {
            return err
        }
        // Загружаем существующие записи
        globalTxAuditLogger.loadFromFile()
    }
    return nil
}

// loadFromFile загружает аудит из файла
func (tal *TransactionAuditLogger) loadFromFile() {
    if tal.filePath == "" {
        return
    }
    data, err := os.ReadFile(tal.filePath)
    if err != nil {
        return
    }
    var entries []TransactionAuditEntry
    if err := json.Unmarshal(data, &entries); err != nil {
        return
    }
    tal.mu.Lock()
    defer tal.mu.Unlock()
    tal.entries = entries
}

// saveToFile сохраняет аудит в файл
func (tal *TransactionAuditLogger) saveToFile() {
    if tal.filePath == "" {
        return
    }
    tal.fileMu.Lock()
    defer tal.fileMu.Unlock()

    tal.mu.RLock()
    data, err := json.MarshalIndent(tal.entries, "", "  ")
    tal.mu.RUnlock()

    if err != nil {
        return
    }
    os.WriteFile(tal.filePath, data, 0644)
}

// LogTransactionAudit записывает событие аудита транзакции
func LogTransactionAudit(txID TransactionID, action string, state TransactionState, details map[string]interface{}) {
    if !globalTxAuditLogger.enabled {
        return
    }

    now := time.Now()
    entry := TransactionAuditEntry{
        TxID:         txID,
        Action:       action,
        State:        state,
        Timestamp:    now.UnixMilli(),
        TimestampStr: now.Format("2006-01-02 15:04:05.000"),
        Details:      details,
    }

    globalTxAuditLogger.mu.Lock()
    defer globalTxAuditLogger.mu.Unlock()

    globalTxAuditLogger.entries = append(globalTxAuditLogger.entries, entry)
    if len(globalTxAuditLogger.entries) > globalTxAuditLogger.maxSize {
        globalTxAuditLogger.entries = globalTxAuditLogger.entries[len(globalTxAuditLogger.entries)-globalTxAuditLogger.maxSize:]
    }

    // Асинхронное сохранение в файл
    go globalTxAuditLogger.saveToFile()
}

// GetTransactionAuditLog возвращает лог аудита транзакций
func GetTransactionAuditLog() []TransactionAuditEntry {
    globalTxAuditLogger.mu.RLock()
    defer globalTxAuditLogger.mu.RUnlock()

    result := make([]TransactionAuditEntry, len(globalTxAuditLogger.entries))
    copy(result, globalTxAuditLogger.entries)
    return result
}

// GetTransactionAuditLogFiltered возвращает отфильтрованный лог аудита
func GetTransactionAuditLogFiltered(txID TransactionID, action string, fromTime, toTime int64) []TransactionAuditEntry {
    globalTxAuditLogger.mu.RLock()
    defer globalTxAuditLogger.mu.RUnlock()

    result := make([]TransactionAuditEntry, 0)
    for _, entry := range globalTxAuditLogger.entries {
        if txID > 0 && entry.TxID != txID {
            continue
        }
        if action != "" && entry.Action != action {
            continue
        }
        if fromTime > 0 && entry.Timestamp < fromTime {
            continue
        }
        if toTime > 0 && entry.Timestamp > toTime {
            continue
        }
        result = append(result, entry)
    }
    return result
}

// =============================================================================
// MVCC - MULTI-VERSION CONCURRENCY CONTROL (ПОЛНАЯ РЕАЛИЗАЦИЯ)
// =============================================================================

// MVCCManager - основной менеджер MVCC для production использования
type MVCCManager struct {
    // Версии документов: docID -> []*DocumentVersion
    versions sync.Map

    // Карта видимости для быстрой проверки
    visibilityMap *VisibilityMap

    // Кэш для чтения по временной метке
    readCache *ReadTimestampCache

    // Настройки
    maxVersionsPerDoc int
    retentionDuration time.Duration

    // Статистика
    stats MVCCStats

    // Защита для операций с версиями
    mu sync.RWMutex
}

// MVCCStats статистика MVCC
type MVCCStats struct {
    TotalVersionsCreated  atomic.Uint64
    TotalVersionsPruned   atomic.Uint64
    TotalCacheHits        atomic.Uint64
    TotalCacheMisses      atomic.Uint64
    TotalVisibilityHits   atomic.Uint64
    TotalVisibilityMisses atomic.Uint64
}

// NewMVCCManager создаёт новый MVCC менеджер
func NewMVCCManager(maxVersionsPerDoc int, retentionDays int) *MVCCManager {
    if maxVersionsPerDoc <= 0 {
        maxVersionsPerDoc = MaxVersionsPerDoc
    }
    if retentionDays <= 0 {
        retentionDays = VersionRetentionDays
    }

    m := &MVCCManager{
        maxVersionsPerDoc: maxVersionsPerDoc,
        retentionDuration: time.Duration(retentionDays) * 24 * time.Hour,
        visibilityMap:     NewVisibilityMap(VisibilityMapSize),
        readCache:         NewReadTimestampCache(10000, 5*time.Minute),
    }

    // Запускаем фоновую очистку старых версий
    go m.pruneOldVersionsLoop()

    return m
}

// CreateVersion создаёт новую версию документа
func (m *MVCCManager) CreateVersion(doc *Document, txID TransactionID) *DocumentVersion {
    version := &DocumentVersion{
        Document:  doc.Clone(),
        Timestamp: time.Now().UnixMilli(),
        TxID:      txID,
        VersionID: fmt.Sprintf("%s_%d_%d", doc.ID, txID, time.Now().UnixNano()),
    }

    // Сохраняем версию
    m.mu.Lock()
    defer m.mu.Unlock()

    val, _ := m.versions.LoadOrStore(doc.ID, make([]*DocumentVersion, 0))
    versions := val.([]*DocumentVersion)

    // Добавляем новую версию
    versions = append(versions, version)

    // Ограничиваем количество версий
    if len(versions) > m.maxVersionsPerDoc {
        // Удаляем самые старые версии, но сохраняем хотя бы одну
        versions = versions[len(versions)-m.maxVersionsPerDoc:]
        m.stats.TotalVersionsPruned.Add(uint64(len(versions)))
    }

    m.versions.Store(doc.ID, versions)

    // Отмечаем версию как видимую
    m.visibilityMap.MarkVisible(doc.ID, uint64(txID), true)

    m.stats.TotalVersionsCreated.Add(1)

    // Аудит создания версии
    LogTransactionAudit(txID, "VERSION_CREATE", TransactionActive, map[string]interface{}{
        "doc_id":   doc.ID,
        "version":  version.VersionID,
    })

    return version
}

// GetVersionAt возвращает версию документа на указанный момент времени
func (m *MVCCManager) GetVersionAt(docID string, timestamp int64) *Document {
    // Проверяем кэш
    if cached := m.readCache.Get(docID, timestamp); cached != nil {
        m.stats.TotalCacheHits.Add(1)
        return cached
    }
    m.stats.TotalCacheMisses.Add(1)

    m.mu.RLock()
    defer m.mu.RUnlock()

    val, ok := m.versions.Load(docID)
    if !ok {
        return nil
    }

    versions := val.([]*DocumentVersion)

    // Ищем версию с максимальной временной меткой <= запрошенной
    var result *DocumentVersion
    for i := len(versions) - 1; i >= 0; i-- {
        v := versions[i]
        if v.Timestamp <= timestamp && m.visibilityMap.IsVisible(docID, uint64(v.TxID)) {
            result = v
            break
        }
    }

    if result == nil {
        return nil
    }

    // Сохраняем в кэш
    doc := result.Document.Clone()
    m.readCache.Set(docID, timestamp, doc)

    return doc
}

// GetLatestVersion возвращает последнюю версию документа
func (m *MVCCManager) GetLatestVersion(docID string) *Document {
    m.mu.RLock()
    defer m.mu.RUnlock()

    val, ok := m.versions.Load(docID)
    if !ok {
        return nil
    }

    versions := val.([]*DocumentVersion)
    if len(versions) == 0 {
        return nil
    }

    return versions[len(versions)-1].Document.Clone()
}

// GetAllVersions возвращает все версии документа
func (m *MVCCManager) GetAllVersions(docID string) []*DocumentVersion {
    m.mu.RLock()
    defer m.mu.RUnlock()

    val, ok := m.versions.Load(docID)
    if !ok {
        return nil
    }

    versions := val.([]*DocumentVersion)
    result := make([]*DocumentVersion, len(versions))
    for i, v := range versions {
        result[i] = &DocumentVersion{
            Document:  v.Document.Clone(),
            Timestamp: v.Timestamp,
            TxID:      v.TxID,
            VersionID: v.VersionID,
        }
    }
    return result
}

// pruneOldVersionsLoop периодически удаляет старые версии
func (m *MVCCManager) pruneOldVersionsLoop() {
    ticker := time.NewTicker(VersionPruneInterval)
    defer ticker.Stop()

    for range ticker.C {
        m.PruneOldVersions()
    }
}

// PruneOldVersions удаляет старые версии документов
func (m *MVCCManager) PruneOldVersions() {
    cutoffTime := time.Now().Add(-m.retentionDuration).UnixMilli()
    pruned := int64(0)

    m.mu.Lock()
    defer m.mu.Unlock()

    m.versions.Range(func(key, value interface{}) bool {
        docID := key.(string)
        versions := value.([]*DocumentVersion)

        // Оставляем только версии новее cutoff
        newVersions := make([]*DocumentVersion, 0, len(versions))
        for _, v := range versions {
            if v.Timestamp >= cutoffTime {
                newVersions = append(newVersions, v)
            } else {
                pruned++
            }
        }

        // Всегда оставляем хотя бы одну версию
        if len(newVersions) == 0 && len(versions) > 0 {
            newVersions = append(newVersions, versions[len(versions)-1])
            pruned--
        }

        if len(newVersions) != len(versions) {
            m.versions.Store(docID, newVersions)
        }

        return true
    })

    if pruned > 0 {
        m.stats.TotalVersionsPruned.Add(uint64(pruned))
    }
}

// GetMVCCStats возвращает статистику MVCC
func (m *MVCCManager) GetMVCCStats() map[string]interface{} {
    return map[string]interface{}{
        "total_versions_created":    m.stats.TotalVersionsCreated.Load(),
        "total_versions_pruned":     m.stats.TotalVersionsPruned.Load(),
        "total_cache_hits":          m.stats.TotalCacheHits.Load(),
        "total_cache_misses":        m.stats.TotalCacheMisses.Load(),
        "total_visibility_hits":     m.stats.TotalVisibilityHits.Load(),
        "total_visibility_misses":   m.stats.TotalVisibilityMisses.Load(),
        "max_versions_per_doc":      m.maxVersionsPerDoc,
        "retention_days":            int(m.retentionDuration.Hours() / 24),
    }
}

// =============================================================================
// VISIBILITY MAP
// =============================================================================

// VisibilityMapEntry - запись в карте видимости.
type VisibilityMapEntry struct {
    DocID       string
    VisibleFrom uint64
    VisibleTo   uint64
    IsVisible   bool
    LastAccess  int64
}

// VisibilityMap - карта видимости версий.
type VisibilityMap struct {
    entries   sync.Map
    maxSize   int
    hitCount  atomic.Uint64
    missCount atomic.Uint64
    mu        sync.RWMutex
}

// NewVisibilityMap создаёт новую карту видимости.
func NewVisibilityMap(maxSize int) *VisibilityMap {
    if maxSize <= 0 {
        maxSize = VisibilityMapSize
    }
    return &VisibilityMap{maxSize: maxSize}
}

// MarkVisible - отмечает версию как видимую.
func (vm *VisibilityMap) MarkVisible(docID string, version uint64, visible bool) {
    key := fmt.Sprintf("%s@%d", docID, version)
    vm.entries.Store(key, &VisibilityMapEntry{
        DocID:       docID,
        VisibleFrom: version,
        VisibleTo:   version,
        IsVisible:   visible,
        LastAccess:  time.Now().Unix(),
    })
}

// IsVisible - проверяет, видима ли версия.
func (vm *VisibilityMap) IsVisible(docID string, version uint64) bool {
    key := fmt.Sprintf("%s@%d", docID, version)

    // Сначала проверяем точное совпадение
    if val, ok := vm.entries.Load(key); ok {
        entry := val.(*VisibilityMapEntry)
        entry.LastAccess = time.Now().Unix()
        vm.hitCount.Add(1)
        return entry.IsVisible
    }

    // Проверяем диапазон
    var found bool
    vm.entries.Range(func(k, v interface{}) bool {
        entry := v.(*VisibilityMapEntry)
        if entry.DocID == docID &&
            version >= entry.VisibleFrom &&
            version <= entry.VisibleTo &&
            entry.IsVisible {
            found = true
            return false
        }
        return true
    })

    if found {
        vm.hitCount.Add(1)
        return true
    }

    vm.missCount.Add(1)
    return false
}

// GetStats - возвращает статистику карты видимости.
func (vm *VisibilityMap) GetStats() map[string]interface{} {
    count := 0
    vm.entries.Range(func(_, _ interface{}) bool {
        count++
        return true
    })

    return map[string]interface{}{
        "hits":     vm.hitCount.Load(),
        "misses":   vm.missCount.Load(),
        "entries":  count,
        "max_size": vm.maxSize,
    }
}

// =============================================================================
// READ TIMESTAMP CACHE
// =============================================================================

// ReadTimestampCache - кэш для чтения по временной метке.
type ReadTimestampCache struct {
    cache   sync.Map
    maxSize int
    ttl     time.Duration
    hits    atomic.Uint64
    misses  atomic.Uint64
    size    atomic.Int64
    mu      sync.RWMutex
}

type cachedEntry struct {
    doc         *Document
    cachedAt    time.Time
    accessCount int64
}

// NewReadTimestampCache создаёт новый кэш чтения.
func NewReadTimestampCache(maxSize int, ttl time.Duration) *ReadTimestampCache {
    if maxSize <= 0 {
        maxSize = 10000
    }
    if ttl <= 0 {
        ttl = 5 * time.Minute
    }
    c := &ReadTimestampCache{maxSize: maxSize, ttl: ttl}

    // Запускаем фоновую очистку
    go c.cleanupLoop()

    return c
}

// Get - получает документ из кэша.
func (rtc *ReadTimestampCache) Get(docID string, timestamp int64) *Document {
    key := fmt.Sprintf("%s@%d", docID, timestamp)
    val, ok := rtc.cache.Load(key)
    if !ok {
        rtc.misses.Add(1)
        return nil
    }
    entry := val.(*cachedEntry)
    if time.Since(entry.cachedAt) > rtc.ttl {
        rtc.cache.Delete(key)
        rtc.size.Add(-1)
        rtc.misses.Add(1)
        return nil
    }
    entry.accessCount++
    rtc.hits.Add(1)
    return entry.doc
}

// Set - сохраняет документ в кэше.
func (rtc *ReadTimestampCache) Set(docID string, timestamp int64, doc *Document) {
    key := fmt.Sprintf("%s@%d", docID, timestamp)

    // Проверяем размер кэша
    if rtc.size.Load() >= int64(rtc.maxSize) {
        rtc.evictOldest()
    }

    rtc.cache.Store(key, &cachedEntry{
        doc:         doc,
        cachedAt:    time.Now(),
        accessCount: 0,
    })
    rtc.size.Add(1)
}

// evictOldest удаляет самый старый элемент кэша
func (rtc *ReadTimestampCache) evictOldest() {
    var oldestKey interface{}
    var oldestTime time.Time

    rtc.cache.Range(func(key, value interface{}) bool {
        entry := value.(*cachedEntry)
        if oldestKey == nil || entry.cachedAt.Before(oldestTime) {
            oldestKey = key
            oldestTime = entry.cachedAt
        }
        return true
    })

    if oldestKey != nil {
        rtc.cache.Delete(oldestKey)
        rtc.size.Add(-1)
    }
}

// cleanupLoop периодически очищает устаревшие записи
func (rtc *ReadTimestampCache) cleanupLoop() {
    ticker := time.NewTicker(rtc.ttl)
    defer ticker.Stop()

    for range ticker.C {
        rtc.cache.Range(func(key, value interface{}) bool {
            entry := value.(*cachedEntry)
            if time.Since(entry.cachedAt) > rtc.ttl {
                rtc.cache.Delete(key)
                rtc.size.Add(-1)
            }
            return true
        })
    }
}

// GetStats - возвращает статистику кэша.
func (rtc *ReadTimestampCache) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "hits":     rtc.hits.Load(),
        "misses":   rtc.misses.Load(),
        "size":     rtc.size.Load(),
        "max_size": rtc.maxSize,
        "ttl_secs": int(rtc.ttl.Seconds()),
    }
}

// =============================================================================
// PERSISTENT SAGA STATE - ОТКАЗОУСТОЙЧИВЫЙ ОРКЕСТРАТОР
// =============================================================================

// SagaState представляет состояние SAGA для сохранения на диск
type SagaState struct {
    ID                   string                 `json:"id"`
    Status               string                 `json:"status"`
    CurrentStep          int                    `json:"current_step"`
    Steps                []SagaStepState        `json:"steps"`
    Data                 map[string]interface{} `json:"data"`
    CreatedAt            int64                  `json:"created_at"`
    UpdatedAt            int64                  `json:"updated_at"`
    CompletedAt          int64                  `json:"completed_at,omitempty"`
    CompensationExecuted bool                   `json:"compensation_executed"`
    NodeID               string                 `json:"node_id"`           // ID узла, выполняющего SAGA
    CoordinatorID        string                 `json:"coordinator_id"`    // ID координатора
    Version              uint64                 `json:"version"`           // Версия для оптимистичной блокировки
    RetryCount           int                    `json:"retry_count"`
    LastError            string                 `json:"last_error,omitempty"`
    ExecutionID          string                 `json:"execution_id"`      // Глобальный ID выполнения
}

// SagaStepState представляет состояние шага SAGA
type SagaStepState struct {
    ID            string                 `json:"id"`
    Name          string                 `json:"name"`
    Status        string                 `json:"status"`
    Data          map[string]interface{} `json:"data"`
    StartedAt     int64                  `json:"started_at"`
    CompletedAt   int64                  `json:"completed_at"`
    ExecutionID   string                 `json:"execution_id"`
    RetryCount    int                    `json:"retry_count"`
    LastError     string                 `json:"last_error,omitempty"`
    CompensatedAt int64                  `json:"compensated_at,omitempty"`
}

// SagaPersistentStorage хранит состояния SAGA на диске
type SagaPersistentStorage struct {
    baseDir         string
    mu              sync.RWMutex
    cache           map[string]*SagaState
    cacheSize       int
    maxCache        int
    logger          LoggerInterface
    fsyncEnabled    bool
    fsyncMaxRetries int
    fsyncRetryDelay time.Duration
}

// NewSagaPersistentStorage создаёт новое хранилище состояний SAGA
func NewSagaPersistentStorage(baseDir string, logger LoggerInterface) (*SagaPersistentStorage, error) {
    return NewSagaPersistentStorageWithConfig(nil, logger)
}

// NewSagaPersistentStorageWithConfig создаёт хранилище с конфигурацией
func NewSagaPersistentStorageWithConfig(cfg *config.SagaConfig, logger LoggerInterface) (*SagaPersistentStorage, error) {
    baseDir := SagaStateDir
    maxCache := 10000
    fsyncEnabled := true
    fsyncMaxRetries := 3
    fsyncRetryDelay := 100 * time.Millisecond

    if cfg != nil {
        baseDir = cfg.GetStateDir()
        maxCache = cfg.GetMaxCacheSize()
        fsyncEnabled = cfg.IsFsyncEnabled()
        fsyncMaxRetries = cfg.GetFsyncMaxRetries()
        fsyncRetryDelay = cfg.GetFsyncRetryDelay()
    }

    fullPath := filepath.Join(baseDir)
    if err := os.MkdirAll(fullPath, 0755); err != nil {
        return nil, fmt.Errorf("failed to create saga state directory: %v", err)
    }

    return &SagaPersistentStorage{
        baseDir:         fullPath,
        cache:           make(map[string]*SagaState),
        maxCache:        maxCache,
        logger:          logger,
        fsyncEnabled:    fsyncEnabled,
        fsyncMaxRetries: fsyncMaxRetries,
        fsyncRetryDelay: fsyncRetryDelay,
    }, nil
}

// getStatePath возвращает путь к файлу состояния
func (sps *SagaPersistentStorage) getStatePath(sagaID string) string {
    return filepath.Join(sps.baseDir, fmt.Sprintf("%s%s%s", SagaStateFilePrefix, sagaID, SagaStateFileSuffix))
}

// Save сохраняет состояние SAGA на диск
func (sps *SagaPersistentStorage) Save(state *SagaState) error {
    sps.mu.Lock()
    defer sps.mu.Unlock()

    state.UpdatedAt = time.Now().UnixMilli()
    state.Version++

    // Сохраняем в кэш
    if len(sps.cache) >= sps.maxCache {
        // Удаляем самый старый элемент
        var oldestKey string
        var oldestTime int64 = time.Now().UnixMilli()
        for k, v := range sps.cache {
            if v.UpdatedAt < oldestTime {
                oldestTime = v.UpdatedAt
                oldestKey = k
            }
        }
        if oldestKey != "" {
            delete(sps.cache, oldestKey)
        }
    }
    sps.cache[state.ID] = state

    // Сохраняем на диск
    path := sps.getStatePath(state.ID)
    data, err := json.MarshalIndent(state, "", "  ")
    if err != nil {
        return fmt.Errorf("failed to marshal saga state: %v", err)
    }

    // Атомарная запись через временный файл
    tmpPath := path + ".tmp"
    if err := os.WriteFile(tmpPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write saga state: %v", err)
    }

    if sps.fsyncEnabled {
        // Синхронизируем временный файл
        f, err := os.OpenFile(tmpPath, os.O_RDWR, 0644)
        if err == nil {
            for i := 0; i < sps.fsyncMaxRetries; i++ {
                if err := f.Sync(); err == nil {
                    break
                }
                if i < sps.fsyncMaxRetries-1 {
                    time.Sleep(sps.fsyncRetryDelay)
                }
            }
            f.Close()
        }
    }

    if err := os.Rename(tmpPath, path); err != nil {
        return fmt.Errorf("failed to rename saga state: %v", err)
    }

    if sps.fsyncEnabled {
        // Синхронизируем директорию
        FsyncDir(sps.baseDir)
    }

    if sps.logger != nil {
        sps.logger.Debug(fmt.Sprintf("Saved saga state %s (version %d, status %s)", state.ID, state.Version, state.Status))
    }

    return nil
}

// Load загружает состояние SAGA с диска
func (sps *SagaPersistentStorage) Load(sagaID string) (*SagaState, error) {
    sps.mu.RLock()
    // Проверяем кэш
    if state, ok := sps.cache[sagaID]; ok {
        sps.mu.RUnlock()
        return state, nil
    }
    sps.mu.RUnlock()

    // Загружаем с диска
    path := sps.getStatePath(sagaID)
    data, err := os.ReadFile(path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, nil
        }
        return nil, fmt.Errorf("failed to read saga state: %v", err)
    }

    var state SagaState
    if err := json.Unmarshal(data, &state); err != nil {
        return nil, fmt.Errorf("failed to unmarshal saga state: %v", err)
    }

    // Сохраняем в кэш
    sps.mu.Lock()
    sps.cache[sagaID] = &state
    sps.mu.Unlock()

    return &state, nil
}

// Delete удаляет состояние SAGA с диска
func (sps *SagaPersistentStorage) Delete(sagaID string) error {
    sps.mu.Lock()
    delete(sps.cache, sagaID)
    sps.mu.Unlock()

    path := sps.getStatePath(sagaID)
    if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
        return fmt.Errorf("failed to delete saga state: %v", err)
    }
    return nil
}

// ListAll возвращает список всех сохранённых SAGA
func (sps *SagaPersistentStorage) ListAll() ([]*SagaState, error) {
    pattern := filepath.Join(sps.baseDir, fmt.Sprintf("%s*%s", SagaStateFilePrefix, SagaStateFileSuffix))
    files, err := filepath.Glob(pattern)
    if err != nil {
        return nil, err
    }

    states := make([]*SagaState, 0, len(files))
    for _, file := range files {
        data, err := os.ReadFile(file)
        if err != nil {
            continue
        }
        var state SagaState
        if err := json.Unmarshal(data, &state); err != nil {
            continue
        }
        states = append(states, &state)
    }
    return states, nil
}

// ListPending возвращает список незавершённых SAGA
func (sps *SagaPersistentStorage) ListPending() ([]*SagaState, error) {
    all, err := sps.ListAll()
    if err != nil {
        return nil, err
    }

    pending := make([]*SagaState, 0)
    for _, state := range all {
        if state.Status == "pending" || state.Status == "running" || state.Status == "compensating" {
            pending = append(pending, state)
        }
    }
    return pending, nil
}

// =============================================================================
// SAGA ORCHESTRATOR - ОТКАЗОУСТОЙЧИВЫЙ ОРКЕСТРАТОР БЕЗ SPOF
// =============================================================================

// SagaOrchestrator управляет выполнением SAGA транзакций
type SagaOrchestrator struct {
    storage      *SagaPersistentStorage
    coordinators []*SagaCoordinator
    mu           sync.RWMutex
    logger       LoggerInterface
    nodeID       string
    stopChan     chan struct{}
    wg           sync.WaitGroup
    isLeader     atomic.Bool
    leaderID     string
    electionMu   sync.Mutex
    config       *config.SagaConfig
    // Метрики
    metrics      *SagaMetrics
}

// SagaCoordinator представляет координатора SAGA
type SagaCoordinator struct {
    id             string
    orchestrator   *SagaOrchestrator
    activeSagas    sync.Map // map[string]*SagaTransaction
    stopChan       chan struct{}
    wg             sync.WaitGroup
    isActive       bool
    config         *config.SagaConfig
    mu             sync.RWMutex
}

// SagaMetrics собирает метрики по SAGA
type SagaMetrics struct {
    TotalStarted    atomic.Uint64
    TotalCompleted  atomic.Uint64
    TotalAborted    atomic.Uint64
    TotalFailed     atomic.Uint64
    TotalCompensated atomic.Uint64
    ActiveCount     atomic.Int64
    AvgDurationMs   atomic.Uint64
    TotalDurationMs atomic.Uint64
    RecoveryCount   atomic.Uint64
    mu              sync.RWMutex
    latencies       []int64
    maxLatency      int64
    minLatency      int64
}

// NewSagaOrchestrator создаёт новый оркестратор SAGA
func NewSagaOrchestrator(baseDir string, nodeID string, logger LoggerInterface) (*SagaOrchestrator, error) {
    return NewSagaOrchestratorWithConfig(nil, nodeID, logger)
}

// NewSagaOrchestratorWithConfig создаёт новый оркестратор SAGA с конфигурацией из config.toml
func NewSagaOrchestratorWithConfig(cfg *config.SagaConfig, nodeID string, logger LoggerInterface) (*SagaOrchestrator, error) {
    if cfg == nil {
        cfg = &config.SagaConfig{
            Enabled:                 true,
            CoordinatorCount:        3,
            StateDir:                "saga_states",
            MaxRetries:              5,
            RetryBackoffMs:          100,
            SagaTimeoutSec:          300,
            StuckCheckIntervalSec:   10,
            LeaderElectionIntervalSec: 5,
            RecoveryIntervalSec:     30,
            MetricsIntervalSec:      60,
            CleanupPeriodHours:      24,
            MaxCacheSize:            10000,
            OperationRetentionDays:  7,
            ChannelBufferSize:       10000,
            AsyncRecoveryWorkers:    4,
            AsyncRecoveryTimeoutSec: 30,
            FsyncEnabled:            true,
            FsyncMaxRetries:         3,
            FsyncRetryDelayMs:       100,
        }
    }

    // Создаём хранилище с настройками из конфига
    storage, err := NewSagaPersistentStorageWithConfig(cfg, logger)
    if err != nil {
        return nil, err
    }

    o := &SagaOrchestrator{
        storage:      storage,
        logger:       logger,
        nodeID:       nodeID,
        stopChan:     make(chan struct{}, cfg.GetChannelBufferSize()),
        metrics:      &SagaMetrics{minLatency: -1},
        coordinators: make([]*SagaCoordinator, 0),
        config:       cfg,
    }

    // Создаём координаторы с учётом конфигурации
    coordinatorCount := cfg.GetCoordinatorCount()
    for i := 0; i < coordinatorCount; i++ {
        coord := &SagaCoordinator{
            id:           fmt.Sprintf("%s-coord-%d", nodeID, i),
            orchestrator: o,
            stopChan:     make(chan struct{}, cfg.GetChannelBufferSize()),
            isActive:     true,
            config:       cfg,
        }
        o.coordinators = append(o.coordinators, coord)
        coord.wg.Add(1)
        go coord.run()
    }

    // Запускаем фоновые процессы с интервалами из конфига
    o.wg.Add(1)
    go o.leaderElectionLoop()

    o.wg.Add(1)
    go o.recoveryLoop()

    o.wg.Add(1)
    go o.metricsLoop()

    if cfg.IsSagaEnabled() && logger != nil {
        logger.Info(fmt.Sprintf("Saga orchestrator initialized on node %s with %d coordinators", nodeID, coordinatorCount))
    }

    return o, nil
}

// leaderElectionLoop выполняет выбор лидера
func (o *SagaOrchestrator) leaderElectionLoop() {
    defer o.wg.Done()

    interval := o.config.GetLeaderElectionInterval()
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            o.electLeader()
        case <-o.stopChan:
            return
        }
    }
}

// electLeader выбирает лидера среди координаторов
func (o *SagaOrchestrator) electLeader() {
    o.electionMu.Lock()
    defer o.electionMu.Unlock()

    // Простой алгоритм выбора лидера на основе nodeID
    // В production используется Raft или другой алгоритм консенсуса
    candidates := make([]string, 0)
    o.mu.RLock()
    for _, coord := range o.coordinators {
        if coord.isActive {
            candidates = append(candidates, coord.id)
        }
    }
    o.mu.RUnlock()

    if len(candidates) == 0 {
        o.isLeader.Store(false)
        o.leaderID = ""
        return
    }

    // Выбираем наименьший ID как лидера
    sort.Strings(candidates)
    leader := candidates[0]

    o.leaderID = leader
    isLeader := leader == o.coordinators[0].id // Первый координатор всегда лидер
    o.isLeader.Store(isLeader)

    if o.logger != nil {
        o.logger.Debug(fmt.Sprintf("Leader elected: %s (current node %s is leader: %v)", leader, o.nodeID, isLeader))
    }
}

// IsLeader возвращает true, если текущий узел является лидером
func (o *SagaOrchestrator) IsLeader() bool {
    return o.isLeader.Load()
}

// GetLeaderID возвращает ID текущего лидера
func (o *SagaOrchestrator) GetLeaderID() string {
    return o.leaderID
}

// recoveryLoop восстанавливает незавершённые SAGA
func (o *SagaOrchestrator) recoveryLoop() {
    defer o.wg.Done()

    interval := o.config.GetRecoveryInterval()
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            if o.IsLeader() {
                o.recoverPendingSagas()
            }
        case <-o.stopChan:
            return
        }
    }
}

// recoverPendingSagas восстанавливает незавершённые SAGA
func (o *SagaOrchestrator) recoverPendingSagas() {
    pending, err := o.storage.ListPending()
    if err != nil {
        if o.logger != nil {
            o.logger.Error(fmt.Sprintf("Failed to list pending sagas: %v", err))
        }
        return
    }

    if len(pending) == 0 {
        return
    }

    if o.logger != nil {
        o.logger.Info(fmt.Sprintf("Recovering %d pending sagas", len(pending)))
    }

    for _, state := range pending {
        // Проверяем, не выполняется ли SAGA на другом узле
        if state.NodeID != "" && state.NodeID != o.nodeID {
            // Проверяем, жив ли узел
            if !o.isNodeAlive(state.NodeID) {
                // Узел мёртв, перехватываем SAGA
                if o.logger != nil {
                    o.logger.Warn(fmt.Sprintf("Node %s is dead, taking over saga %s", state.NodeID, state.ID))
                }
                state.NodeID = o.nodeID
                o.storage.Save(state)
            } else {
                continue
            }
        }

        // Восстанавливаем SAGA
        if err := o.resumeSaga(state); err != nil {
            if o.logger != nil {
                o.logger.Error(fmt.Sprintf("Failed to resume saga %s: %v", state.ID, err))
            }
        } else {
            o.metrics.RecoveryCount.Add(1)
        }
    }
}

// isNodeAlive проверяет, жив ли узел
func (o *SagaOrchestrator) isNodeAlive(nodeID string) bool {
    // В production реализуется через heartbeat или service discovery
    // Для простоты считаем, что все узлы живы, кроме самого себя
    return nodeID == o.nodeID
}

// resumeSaga возобновляет выполнение SAGA
func (o *SagaOrchestrator) resumeSaga(state *SagaState) error {
    // Создаём SAGA транзакцию из состояния
    saga := &SagaTransaction{
        ID:                   state.ID,
        Status:               state.Status,
        CurrentStep:          state.CurrentStep,
        Data:                 state.Data,
        CreatedAt:            state.CreatedAt,
        UpdatedAt:            state.UpdatedAt,
        compensationExecuted: state.CompensationExecuted,
        executedOps:          make(map[string]bool),
    }

    // Восстанавливаем шаги
    saga.Steps = make([]*SagaStep, len(state.Steps))
    for i, stepState := range state.Steps {
        saga.Steps[i] = &SagaStep{
            ID:          stepState.ID,
            Name:        stepState.Name,
            Status:      stepState.Status,
            Data:        stepState.Data,
            StartedAt:   stepState.StartedAt,
            CompletedAt: stepState.CompletedAt,
            ExecutionID: stepState.ExecutionID,
            RetryCount:  stepState.RetryCount,
            LastError:   stepState.LastError,
        }
        if stepState.Status == "completed" {
            saga.executedOps[stepState.ExecutionID] = true
        }
    }

    // Сохраняем в активные SAGA
    o.mu.Lock()
    for _, coord := range o.coordinators {
        coord.activeSagas.Store(state.ID, saga)
    }
    o.mu.Unlock()

    // Продолжаем выполнение
    return o.executeSagaInternal(saga)
}

// metricsLoop собирает метрики
func (o *SagaOrchestrator) metricsLoop() {
    defer o.wg.Done()

    interval := o.config.GetMetricsInterval()
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            if o.logger != nil {
                metrics := o.GetMetrics()
                o.logger.Debug(fmt.Sprintf("Saga metrics: started=%d, completed=%d, aborted=%d, active=%d",
                    metrics["total_started"], metrics["total_completed"], metrics["total_aborted"], metrics["active_count"]))
            }
        case <-o.stopChan:
            return
        }
    }
}

// BeginSaga начинает новую SAGA транзакцию
func (o *SagaOrchestrator) BeginSaga(id string) (*SagaTransaction, error) {
    if !o.IsLeader() {
        return nil, fmt.Errorf("current node is not the leader")
    }

    // Проверяем, не существует ли уже SAGA
    existing, err := o.storage.Load(id)
    if err != nil {
        return nil, err
    }
    if existing != nil {
        return nil, fmt.Errorf("saga %s already exists", id)
    }

    now := time.Now().UnixMilli()
    state := &SagaState{
        ID:            id,
        Status:        "pending",
        CurrentStep:   0,
        Data:          make(map[string]interface{}),
        CreatedAt:     now,
        UpdatedAt:     now,
        NodeID:        o.nodeID,
        CoordinatorID: o.coordinators[0].id,
        Version:       1,
        ExecutionID:   fmt.Sprintf("%s_%d", id, now),
    }

    if err := o.storage.Save(state); err != nil {
        return nil, err
    }

    saga := &SagaTransaction{
        ID:           id,
        Steps:        make([]*SagaStep, 0),
        CurrentStep:  0,
        Status:       "pending",
        CreatedAt:    now,
        UpdatedAt:    now,
        Data:         make(map[string]interface{}),
        executedOps:  make(map[string]bool),
    }

    // Сохраняем в активные SAGA
    o.mu.RLock()
    for _, coord := range o.coordinators {
        coord.activeSagas.Store(id, saga)
    }
    o.mu.RUnlock()

    o.metrics.TotalStarted.Add(1)
    o.metrics.ActiveCount.Add(1)

    LogTransactionAudit(TransactionID(0), "SAGA_BEGIN", TransactionActive, map[string]interface{}{
        "saga_id": id,
        "node_id": o.nodeID,
    })

    if o.logger != nil {
        o.logger.Info(fmt.Sprintf("Saga %s started on node %s", id, o.nodeID))
    }

    return saga, nil
}

// AddStep добавляет шаг в SAGA транзакцию
func (s *SagaTransaction) AddStep(name string, execute, compensate func() error, data map[string]interface{}) *SagaTransaction {
    s.mu.Lock()
    defer s.mu.Unlock()

    stepID := fmt.Sprintf("%s_step_%d_%d", s.ID, len(s.Steps), time.Now().UnixNano())

    step := &SagaStep{
        ID:          stepID,
        Name:        name,
        Execute:     execute,
        Compensate:  compensate,
        Status:      "pending",
        Data:        data,
        StartedAt:   time.Now().UnixMilli(),
        ExecutionID: stepID,
        RetryCount:  0,
    }
    s.Steps = append(s.Steps, step)
    return s
}

// SetData устанавливает данные в SAGA транзакции
func (s *SagaTransaction) SetData(key string, value interface{}) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.Data[key] = value
}

// GetData получает данные из SAGA транзакции
func (s *SagaTransaction) GetData(key string) (interface{}, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    val, ok := s.Data[key]
    return val, ok
}

// HasExecuted проверяет, была ли выполнена операция (идемпотентность)
func (s *SagaTransaction) HasExecuted(operationID string) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    _, ok := s.executedOps[operationID]
    return ok
}

// MarkExecuted отмечает операцию как выполненную
func (s *SagaTransaction) MarkExecuted(operationID string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.executedOps[operationID] = true
}

// Execute выполняет SAGA транзакцию
func (o *SagaOrchestrator) Execute(saga *SagaTransaction) error {
    if !o.IsLeader() {
        return fmt.Errorf("current node is not the leader")
    }
    return o.executeSagaInternal(saga)
}

// executeSagaInternal внутренняя реализация выполнения SAGA
func (o *SagaOrchestrator) executeSagaInternal(saga *SagaTransaction) error {
    saga.mu.Lock()
    defer saga.mu.Unlock()

    if saga.Status != "pending" && saga.Status != "running" {
        return fmt.Errorf("saga %s is not in pending or running state", saga.ID)
    }

    saga.Status = "running"
    saga.UpdatedAt = time.Now().UnixMilli()

    // Обновляем состояние на диске
    if err := o.saveSagaState(saga); err != nil {
        return err
    }

    startTime := time.Now()

    for i := saga.CurrentStep; i < len(saga.Steps); i++ {
        step := saga.Steps[i]
        saga.CurrentStep = i

        if step.Status == "completed" {
            continue
        }

        step.Status = "running"
        step.StartedAt = time.Now().UnixMilli()

        if o.logger != nil {
            o.logger.Debug(fmt.Sprintf("Executing saga step %s: %s (execution_id: %s)",
                saga.ID, step.Name, step.ExecutionID))
        }

        // Проверка идемпотентности
        if saga.HasExecuted(step.ExecutionID) {
            if o.logger != nil {
                o.logger.Debug(fmt.Sprintf("Saga step %s already executed (idempotent), skipping", step.Name))
            }
            step.Status = "completed"
            step.CompletedAt = time.Now().UnixMilli()
            continue
        }

        var err error
        maxRetries := SagaMaxRetries
        if o.config != nil {
            maxRetries = o.config.GetMaxRetries()
        }
        for retry := 0; retry < maxRetries; retry++ {
            step.RetryCount = retry + 1
            if err = step.Execute(); err == nil {
                break
            }
            step.LastError = err.Error()
            if o.logger != nil {
                o.logger.Warn(fmt.Sprintf("Saga step %s failed (attempt %d/%d): %v",
                    step.Name, retry+1, maxRetries, err))
            }
            // Экспоненциальная задержка
            backoff := time.Duration(100*(1<<retry)) * time.Millisecond
            if o.config != nil {
                backoff = time.Duration(o.config.RetryBackoffMs*(1<<retry)) * time.Millisecond
            }
            time.Sleep(backoff)
        }

        step.CompletedAt = time.Now().UnixMilli()

        if err != nil {
            step.Status = "failed"
            saga.Status = "compensating"
            saga.UpdatedAt = time.Now().UnixMilli()

            // Сохраняем состояние перед компенсацией
            o.saveSagaState(saga)

            if o.logger != nil {
                o.logger.Error(fmt.Sprintf("Saga step %s failed, starting compensation", step.Name))
            }

            // Компенсация
            compensationErr := o.executeCompensation(saga, i)
            if compensationErr != nil {
                saga.Status = "compensation_failed"
                saga.UpdatedAt = time.Now().UnixMilli()
                o.saveSagaState(saga)

                o.metrics.TotalFailed.Add(1)
                o.metrics.ActiveCount.Add(-1)

                if o.logger != nil {
                    o.logger.Error(fmt.Sprintf("Compensation for saga %s failed: %v", saga.ID, compensationErr))
                }

                LogTransactionAudit(TransactionID(0), "SAGA_COMPENSATION_FAILED", TransactionAborted, map[string]interface{}{
                    "saga_id":     saga.ID,
                    "failed_step": step.Name,
                    "error":       compensationErr.Error(),
                })

                return fmt.Errorf("saga %s compensation failed: %v", saga.ID, compensationErr)
            }

            saga.Status = "aborted"
            saga.UpdatedAt = time.Now().UnixMilli()
            o.saveSagaState(saga)

            o.metrics.TotalAborted.Add(1)
            o.metrics.ActiveCount.Add(-1)

            duration := time.Since(startTime).Milliseconds()
            o.recordDuration(duration)

            LogTransactionAudit(TransactionID(0), "SAGA_ABORTED", TransactionAborted, map[string]interface{}{
                "saga_id":     saga.ID,
                "failed_step": step.Name,
                "error":       err.Error(),
                "duration_ms": duration,
            })

            return fmt.Errorf("saga %s aborted at step %s: %v", saga.ID, step.Name, err)
        }

        step.Status = "completed"
        saga.MarkExecuted(step.ExecutionID)
        saga.UpdatedAt = time.Now().UnixMilli()

        // Сохраняем состояние после каждого шага
        if err := o.saveSagaState(saga); err != nil {
            if o.logger != nil {
                o.logger.Error(fmt.Sprintf("Failed to save saga state after step %s: %v", step.Name, err))
            }
        }

        if o.logger != nil {
            o.logger.Debug(fmt.Sprintf("Saga step %s completed successfully", step.Name))
        }
    }

    saga.Status = "completed"
    saga.CompletedAt = time.Now().UnixMilli()
    saga.UpdatedAt = saga.CompletedAt

    // Сохраняем финальное состояние
    if err := o.saveSagaState(saga); err != nil {
        o.logger.Error(fmt.Sprintf("Failed to save final saga state: %v", err))
    }

    o.metrics.TotalCompleted.Add(1)
    o.metrics.ActiveCount.Add(-1)

    duration := time.Since(startTime).Milliseconds()
    o.recordDuration(duration)

    LogTransactionAudit(TransactionID(0), "SAGA_COMPLETED", TransactionCommitted, map[string]interface{}{
        "saga_id":     saga.ID,
        "steps":       len(saga.Steps),
        "duration_ms": duration,
    })

    if o.logger != nil {
        o.logger.Info(fmt.Sprintf("Saga %s completed successfully in %dms", saga.ID, duration))
    }

    return nil
}

// saveSagaState сохраняет состояние SAGA на диск
func (o *SagaOrchestrator) saveSagaState(saga *SagaTransaction) error {
    state := &SagaState{
        ID:          saga.ID,
        Status:      saga.Status,
        CurrentStep: saga.CurrentStep,
        Data:        saga.Data,
        CreatedAt:   saga.CreatedAt,
        UpdatedAt:   saga.UpdatedAt,
        CompletedAt: saga.CompletedAt,
        CompensationExecuted: saga.compensationExecuted,
        NodeID:      o.nodeID,
        CoordinatorID: o.coordinators[0].id,
        ExecutionID: fmt.Sprintf("%s_%d", saga.ID, saga.CreatedAt),
    }

    // Сохраняем шаги
    state.Steps = make([]SagaStepState, len(saga.Steps))
    for i, step := range saga.Steps {
        state.Steps[i] = SagaStepState{
            ID:          step.ID,
            Name:        step.Name,
            Status:      step.Status,
            Data:        step.Data,
            StartedAt:   step.StartedAt,
            CompletedAt: step.CompletedAt,
            ExecutionID: step.ExecutionID,
            RetryCount:  step.RetryCount,
            LastError:   step.LastError,
        }
    }

    return o.storage.Save(state)
}

// executeCompensation выполняет компенсацию
func (o *SagaOrchestrator) executeCompensation(saga *SagaTransaction, failedStep int) error {
    if saga.compensationExecuted {
        return nil
    }

    for j := failedStep; j >= 0; j-- {
        step := saga.Steps[j]
        if step.Status == "compensated" || step.Status == "pending" {
            continue
        }

        if step.Status == "completed" {
            if o.logger != nil {
                o.logger.Debug(fmt.Sprintf("Compensating step %s", step.Name))
            }

            var err error
            maxRetries := SagaMaxRetries
            if o.config != nil {
                maxRetries = o.config.GetMaxRetries()
            }
            for retry := 0; retry < maxRetries; retry++ {
                if err = step.Compensate(); err == nil {
                    break
                }
                if o.logger != nil {
                    o.logger.Warn(fmt.Sprintf("Compensation for step %s failed (attempt %d/%d): %v",
                        step.Name, retry+1, maxRetries, err))
                }
                backoff := time.Duration(100*(1<<retry)) * time.Millisecond
                if o.config != nil {
                    backoff = time.Duration(o.config.RetryBackoffMs*(1<<retry)) * time.Millisecond
                }
                time.Sleep(backoff)
            }

            if err != nil {
                step.Status = "compensation_failed"
                return fmt.Errorf("compensation for step %s failed: %v", step.Name, err)
            }

            step.Status = "compensated"
            step.CompletedAt = time.Now().UnixMilli()

            // Сохраняем состояние после компенсации
            o.saveSagaState(saga)

            if o.logger != nil {
                o.logger.Debug(fmt.Sprintf("Compensation for step %s completed", step.Name))
            }
        }
    }

    saga.compensationExecuted = true
    o.metrics.TotalCompensated.Add(1)

    return nil
}

// recordDuration записывает длительность выполнения
func (o *SagaOrchestrator) recordDuration(durationMs int64) {
    o.metrics.mu.Lock()
    defer o.metrics.mu.Unlock()

    o.metrics.TotalDurationMs.Add(uint64(durationMs))
    avg := o.metrics.TotalDurationMs.Load() / max(1, o.metrics.TotalCompleted.Load())
    o.metrics.AvgDurationMs.Store(avg)

    if o.metrics.minLatency == -1 || durationMs < o.metrics.minLatency {
        o.metrics.minLatency = durationMs
    }
    if durationMs > o.metrics.maxLatency {
        o.metrics.maxLatency = durationMs
    }

    // Сохраняем последние 1000 значений для перцентилей
    o.metrics.latencies = append(o.metrics.latencies, durationMs)
    if len(o.metrics.latencies) > 1000 {
        o.metrics.latencies = o.metrics.latencies[1:]
    }
}

// GetMetrics возвращает метрики SAGA
func (o *SagaOrchestrator) GetMetrics() map[string]interface{} {
    o.metrics.mu.RLock()
    defer o.metrics.mu.RUnlock()

    // Вычисляем перцентили
    latencies := make([]int64, len(o.metrics.latencies))
    copy(latencies, o.metrics.latencies)
    sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

    p50 := int64(0)
    p95 := int64(0)
    p99 := int64(0)
    if len(latencies) > 0 {
        p50 = latencies[int(float64(len(latencies))*0.5)]
        p95 = latencies[int(float64(len(latencies))*0.95)]
        p99 = latencies[int(float64(len(latencies))*0.99)]
    }

    return map[string]interface{}{
        "total_started":      o.metrics.TotalStarted.Load(),
        "total_completed":    o.metrics.TotalCompleted.Load(),
        "total_aborted":      o.metrics.TotalAborted.Load(),
        "total_failed":       o.metrics.TotalFailed.Load(),
        "total_compensated":  o.metrics.TotalCompensated.Load(),
        "active_count":       o.metrics.ActiveCount.Load(),
        "avg_duration_ms":    o.metrics.AvgDurationMs.Load(),
        "min_duration_ms":    o.metrics.minLatency,
        "max_duration_ms":    o.metrics.maxLatency,
        "p50_duration_ms":    p50,
        "p95_duration_ms":    p95,
        "p99_duration_ms":    p99,
        "recovery_count":     o.metrics.RecoveryCount.Load(),
        "is_leader":          o.IsLeader(),
        "leader_id":          o.GetLeaderID(),
        "node_id":            o.nodeID,
    }
}

// GetSaga возвращает SAGA по ID
func (o *SagaOrchestrator) GetSaga(id string) (*SagaTransaction, error) {
    // Проверяем активные SAGA
    o.mu.RLock()
    for _, coord := range o.coordinators {
        if val, ok := coord.activeSagas.Load(id); ok {
            o.mu.RUnlock()
            return val.(*SagaTransaction), nil
        }
    }
    o.mu.RUnlock()

    // Загружаем с диска
    state, err := o.storage.Load(id)
    if err != nil {
        return nil, err
    }
    if state == nil {
        return nil, fmt.Errorf("saga %s not found", id)
    }

    // Восстанавливаем транзакцию из состояния
    saga := &SagaTransaction{
        ID:          state.ID,
        Status:      state.Status,
        CurrentStep: state.CurrentStep,
        Data:        state.Data,
        CreatedAt:   state.CreatedAt,
        UpdatedAt:   state.UpdatedAt,
        CompletedAt: state.CompletedAt,
        compensationExecuted: state.CompensationExecuted,
        executedOps: make(map[string]bool),
    }

    for _, stepState := range state.Steps {
        step := &SagaStep{
            ID:          stepState.ID,
            Name:        stepState.Name,
            Status:      stepState.Status,
            Data:        stepState.Data,
            StartedAt:   stepState.StartedAt,
            CompletedAt: stepState.CompletedAt,
            ExecutionID: stepState.ExecutionID,
            RetryCount:  stepState.RetryCount,
            LastError:   stepState.LastError,
        }
        saga.Steps = append(saga.Steps, step)
        if stepState.Status == "completed" {
            saga.executedOps[stepState.ExecutionID] = true
        }
    }

    return saga, nil
}

// Stop останавливает оркестратор
func (o *SagaOrchestrator) Stop() {
    close(o.stopChan)
    o.wg.Wait()

    for _, coord := range o.coordinators {
        close(coord.stopChan)
        coord.wg.Wait()
    }

    if o.logger != nil {
        o.logger.Info("Saga orchestrator stopped")
    }
}

// =============================================================================
// SAGA COORDINATOR - ВЫПОЛНЕНИЕ SAGA
// =============================================================================

// run запускает координатор
func (c *SagaCoordinator) run() {
    defer c.wg.Done()

    interval := c.config.GetStuckCheckInterval()
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            // Периодическая проверка активных SAGA
            c.activeSagas.Range(func(key, value interface{}) bool {
                saga := value.(*SagaTransaction)
                if saga.Status == "running" {
                    // Проверяем, не зависла ли SAGA
                    timeout := c.config.GetSagaTimeout()
                    if time.Since(time.UnixMilli(saga.UpdatedAt)) > timeout {
                        if c.orchestrator.logger != nil {
                            c.orchestrator.logger.Warn(fmt.Sprintf("Saga %s appears to be stuck, attempting recovery", saga.ID))
                        }
                        // Пытаемся восстановить
                        if err := c.orchestrator.resumeSagaFromStorage(saga.ID); err != nil {
                            c.orchestrator.logger.Error(fmt.Sprintf("Failed to recover stuck saga %s: %v", saga.ID, err))
                        }
                    }
                }
                return true
            })
        case <-c.stopChan:
            return
        }
    }
}

// resumeSagaFromStorage восстанавливает SAGA из хранилища
func (o *SagaOrchestrator) resumeSagaFromStorage(sagaID string) error {
    state, err := o.storage.Load(sagaID)
    if err != nil {
        return err
    }
    if state == nil {
        return fmt.Errorf("saga %s not found in storage", sagaID)
    }

    // Проверяем, не выполняется ли SAGA на другом узле
    if state.NodeID != "" && state.NodeID != o.nodeID {
        if o.isNodeAlive(state.NodeID) {
            return fmt.Errorf("saga %s is being executed on node %s", sagaID, state.NodeID)
        }
    }

    return o.resumeSaga(state)
}

// =============================================================================
// SAGA TRANSACTION - ОПРЕДЕЛЕНИЕ СТРУКТУРЫ (ЕДИНСТВЕННОЕ МЕСТО)
// =============================================================================

// SagaStep представляет шаг в Saga транзакции.
type SagaStep struct {
    ID            string                 `json:"id"`
    Name          string                 `json:"name"`
    Execute       func() error           `json:"-"`
    Compensate    func() error           `json:"-"`
    Status        string                 `json:"status"`
    Data          map[string]interface{} `json:"data"`
    StartedAt     int64                  `json:"started_at"`
    CompletedAt   int64                  `json:"completed_at"`
    ExecutionID   string                 `json:"execution_id"`
    RetryCount    int                    `json:"retry_count"`
    LastError     string                 `json:"last_error,omitempty"`
}

// SagaTransaction представляет Saga транзакцию.
type SagaTransaction struct {
    ID          string                 `json:"id"`
    Steps       []*SagaStep            `json:"steps"`
    CurrentStep int                    `json:"current_step"`
    Status      string                 `json:"status"`
    CreatedAt   int64                  `json:"created_at"`
    UpdatedAt   int64                  `json:"updated_at"`
    CompletedAt int64                  `json:"completed_at,omitempty"`
    Data        map[string]interface{} `json:"data"`
    mu          sync.RWMutex
    // История выполненных операций для идемпотентности
    executedOps          map[string]bool `json:"-"`
    compensationExecuted bool            `json:"-"`
}

// =============================================================================
// ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ДЛЯ SAGA ORCHESTRATOR
// =============================================================================

var globalSagaOrchestrator *SagaOrchestrator
var sagaOrchestratorMu sync.RWMutex

// SetGlobalSagaOrchestrator устанавливает глобальный оркестратор SAGA
func SetGlobalSagaOrchestrator(o *SagaOrchestrator) {
    sagaOrchestratorMu.Lock()
    defer sagaOrchestratorMu.Unlock()
    globalSagaOrchestrator = o
}

// GetGlobalSagaOrchestrator возвращает глобальный оркестратор SAGA
func GetGlobalSagaOrchestrator() *SagaOrchestrator {
    sagaOrchestratorMu.RLock()
    defer sagaOrchestratorMu.RUnlock()
    return globalSagaOrchestrator
}

// =============================================================================
// SAGA MANAGER - ДЛЯ СОВМЕСТИМОСТИ С СУЩЕСТВУЮЩИМ КОДОМ
// =============================================================================

// SagaManager - для совместимости с существующим кодом
type SagaManager struct {
    orchestrator *SagaOrchestrator
    sagas        sync.Map
    logger       LoggerInterface
    stopChan     chan struct{}
    wg           sync.WaitGroup
    mu           sync.RWMutex
    maxRetries   int
    executedOps  sync.Map
}

// NewSagaManager создаёт новый менеджер Saga (обёртка над оркестратором)
func NewSagaManager(logger LoggerInterface) *SagaManager {
    orchestrator, err := NewSagaOrchestrator(SagaStateDir, "default-node", logger)
    if err != nil {
        if logger != nil {
            logger.Error(fmt.Sprintf("Failed to create saga orchestrator: %v", err))
        }
        return &SagaManager{
            logger:     logger,
            stopChan:   make(chan struct{}),
            maxRetries: 3,
        }
    }

    sm := &SagaManager{
        orchestrator: orchestrator,
        logger:       logger,
        stopChan:     make(chan struct{}),
        maxRetries:   3,
    }

    // Запускаем очистку выполненных операций
    sm.wg.Add(1)
    go sm.cleanupExecutedOpsLoop()

    return sm
}

// BeginSaga начинает новую Saga транзакцию
func (sm *SagaManager) BeginSaga(id string) *SagaTransaction {
    if sm.orchestrator != nil {
        saga, err := sm.orchestrator.BeginSaga(id)
        if err != nil {
            if sm.logger != nil {
                sm.logger.Error(fmt.Sprintf("Failed to begin saga: %v", err))
            }
            return &SagaTransaction{
                ID:          id,
                Status:      "pending",
                CreatedAt:   time.Now().UnixMilli(),
                UpdatedAt:   time.Now().UnixMilli(),
                Data:        make(map[string]interface{}),
                executedOps: make(map[string]bool),
            }
        }
        sm.sagas.Store(id, saga)
        return saga
    }

    // Fallback для совместимости
    saga := &SagaTransaction{
        ID:          id,
        Steps:       make([]*SagaStep, 0),
        CurrentStep: 0,
        Status:      "pending",
        CreatedAt:   time.Now().UnixMilli(),
        UpdatedAt:   time.Now().UnixMilli(),
        Data:        make(map[string]interface{}),
        executedOps: make(map[string]bool),
    }
    sm.sagas.Store(id, saga)
    LogTransactionAudit(TransactionID(0), "SAGA_BEGIN", TransactionActive, map[string]interface{}{
        "saga_id": id,
    })
    return saga
}

// Execute выполняет Saga транзакцию (оркестратор).
func (sm *SagaManager) Execute(saga *SagaTransaction) error {
    if sm.orchestrator != nil {
        return sm.orchestrator.Execute(saga)
    }

    // Fallback для совместимости
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
        step.StartedAt = time.Now().UnixMilli()

        if sm.logger != nil {
            sm.logger.Debug(fmt.Sprintf("Executing saga step %s: %s", saga.ID, step.Name))
        }

        var err error
        for retry := 0; retry < sm.maxRetries; retry++ {
            step.RetryCount = retry + 1
            if err = step.Execute(); err == nil {
                break
            }
            step.LastError = err.Error()
            if sm.logger != nil {
                sm.logger.Warn(fmt.Sprintf("Saga step %s failed (attempt %d/%d): %v",
                    step.Name, retry+1, sm.maxRetries, err))
            }
            time.Sleep(time.Duration(100*(retry+1)) * time.Millisecond)
        }

        step.CompletedAt = time.Now().UnixMilli()

        if err != nil {
            step.Status = "failed"
            saga.Status = "compensating"
            saga.UpdatedAt = time.Now().UnixMilli()

            if sm.logger != nil {
                sm.logger.Error(fmt.Sprintf("Saga step %s failed, starting compensation", step.Name))
            }

            compensationErr := sm.executeCompensation(saga, i)
            if compensationErr != nil {
                saga.Status = "compensation_failed"
                saga.UpdatedAt = time.Now().UnixMilli()
                if sm.logger != nil {
                    sm.logger.Error(fmt.Sprintf("Compensation for saga %s failed: %v", saga.ID, compensationErr))
                }
                LogTransactionAudit(TransactionID(0), "SAGA_COMPENSATION_FAILED", TransactionAborted, map[string]interface{}{
                    "saga_id":     saga.ID,
                    "failed_step": step.Name,
                    "error":       compensationErr.Error(),
                })
                return fmt.Errorf("saga %s compensation failed: %v", saga.ID, compensationErr)
            }

            saga.Status = "aborted"
            saga.UpdatedAt = time.Now().UnixMilli()
            LogTransactionAudit(TransactionID(0), "SAGA_ABORTED", TransactionAborted, map[string]interface{}{
                "saga_id":     saga.ID,
                "failed_step": step.Name,
                "error":       err.Error(),
            })
            return fmt.Errorf("saga %s aborted at step %s: %v", saga.ID, step.Name, err)
        }

        step.Status = "completed"
        saga.MarkExecuted(step.ExecutionID)
        saga.UpdatedAt = time.Now().UnixMilli()
    }

    saga.Status = "completed"
    saga.UpdatedAt = time.Now().UnixMilli()
    saga.CompletedAt = saga.UpdatedAt

    if sm.logger != nil {
        sm.logger.Info(fmt.Sprintf("Saga %s completed successfully", saga.ID))
    }

    LogTransactionAudit(TransactionID(0), "SAGA_COMPLETED", TransactionCommitted, map[string]interface{}{
        "saga_id": saga.ID,
        "steps":   len(saga.Steps),
    })

    return nil
}

// executeCompensation выполняет компенсацию с идемпотентностью
func (sm *SagaManager) executeCompensation(saga *SagaTransaction, failedStep int) error {
    if saga.compensationExecuted {
        if sm.logger != nil {
            sm.logger.Debug(fmt.Sprintf("Compensation for saga %s already executed (idempotent)", saga.ID))
        }
        return nil
    }

    // Компенсируем шаги в обратном порядке
    for j := failedStep; j >= 0; j-- {
        step := saga.Steps[j]
        if step.Status == "compensated" || step.Status == "pending" {
            continue
        }

        if sm.logger != nil {
            sm.logger.Debug(fmt.Sprintf("Compensating step %s", step.Name))
        }

        var err error
        for retry := 0; retry < sm.maxRetries; retry++ {
            if err = step.Compensate(); err == nil {
                break
            }
            if sm.logger != nil {
                sm.logger.Warn(fmt.Sprintf("Compensation for step %s failed (attempt %d/%d): %v",
                    step.Name, retry+1, sm.maxRetries, err))
            }
            time.Sleep(time.Duration(100*(retry+1)) * time.Millisecond)
        }

        if err != nil {
            step.Status = "compensation_failed"
            return fmt.Errorf("compensation for step %s failed: %v", step.Name, err)
        }

        step.Status = "compensated"
        step.CompletedAt = time.Now().UnixMilli()

        if sm.logger != nil {
            sm.logger.Debug(fmt.Sprintf("Compensation for step %s completed", step.Name))
        }
    }

    saga.compensationExecuted = true
    return nil
}

// cleanupExecutedOpsLoop очищает старые записи выполненных операций
func (sm *SagaManager) cleanupExecutedOpsLoop() {
    defer sm.wg.Done()

    ticker := time.NewTicker(24 * time.Hour)
    defer ticker.Stop()

    cutoff := int64(7 * 24 * 3600 * 1000) // 7 дней в миллисекундах

    for {
        select {
        case <-ticker.C:
            now := time.Now().UnixMilli()
            sm.executedOps.Range(func(key, value interface{}) bool {
                if ts, ok := value.(int64); ok {
                    if now-ts > cutoff {
                        sm.executedOps.Delete(key)
                    }
                }
                return true
            })
        case <-sm.stopChan:
            return
        }
    }
}

// GetSaga возвращает Saga по ID.
func (sm *SagaManager) GetSaga(id string) (*SagaTransaction, error) {
    if sm.orchestrator != nil {
        return sm.orchestrator.GetSaga(id)
    }

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

// GetActiveSagas возвращает все активные Saga транзакции.
func (sm *SagaManager) GetActiveSagas() []*SagaTransaction {
    result := make([]*SagaTransaction, 0)
    sm.sagas.Range(func(key, value interface{}) bool {
        saga := value.(*SagaTransaction)
        saga.mu.RLock()
        status := saga.Status
        saga.mu.RUnlock()
        if status == "pending" || status == "running" {
            result = append(result, saga)
        }
        return true
    })
    return result
}

// GetOrchestrator возвращает оркестратор
func (sm *SagaManager) GetOrchestrator() *SagaOrchestrator {
    return sm.orchestrator
}

// Stop останавливает менеджер Saga.
func (sm *SagaManager) Stop() {
    close(sm.stopChan)
    sm.wg.Wait()
    if sm.orchestrator != nil {
        sm.orchestrator.Stop()
    }
}

// max вспомогательная функция
func max(a, b uint64) uint64 {
    if a > b {
        return a
    }
    return b
}

// =============================================================================
// ОСТАЛЬНЫЕ ТИПЫ (сохранены для совместимости)
// =============================================================================

// Operation - операция транзакции.
type Operation struct {
    Type       string                 `json:"type"`
    Database   string                 `json:"database"`
    Collection string                 `json:"collection"`
    DocumentID string                 `json:"document_id"`
    Data       map[string]interface{} `json:"data"`
    Version    uint64                 `json:"version"`
    OldData    map[string]interface{} `json:"old_data"`
}

// DocumentVersion - версия документа.
type DocumentVersion struct {
    Document  *Document      `json:"document"`
    Timestamp int64          `json:"timestamp"`
    TxID      TransactionID  `json:"tx_id"`
    VersionID string         `json:"version_id"`
}

// =============================================================================
// WAL MANAGER - УПРАВЛЕНИЕ ЖУРНАЛОМ ПРЕДЗАПИСИ
// =============================================================================

// WALManager управляет WAL (Write-Ahead Log).
type WALManager struct {
    mu           sync.RWMutex
    file         *os.File
    writer       *bufio.Writer
    path         string
    currentLSN   uint64
    lastSync     time.Time
    syncInterval time.Duration
    bufferSize   int
    closed       bool
    writeChan    chan *WALRecord
    stopChan     chan struct{}
    wg           sync.WaitGroup
    batchSize    int
    fsyncEnabled bool
}

// NewWALManager создаёт новый WAL менеджер.
func NewWALManager(path string, fsyncEnabled bool) (*WALManager, error) {
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create WAL directory: %v", err)
    }

    file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
    if err != nil {
        return nil, fmt.Errorf("failed to open WAL file: %v", err)
    }

    var currentLSN uint64 = 1
    stat, err := file.Stat()
    if err == nil && stat.Size() > 0 {
        currentLSN = uint64(stat.Size()) / 100
        if currentLSN < 1 {
            currentLSN = 1
        }
    }

    wm := &WALManager{
        file:         file,
        writer:       bufio.NewWriterSize(file, 64*1024),
        path:         path,
        currentLSN:   currentLSN,
        lastSync:     time.Now(),
        syncInterval: 5 * time.Second,
        bufferSize:   64 * 1024,
        writeChan:    make(chan *WALRecord, 10000),
        stopChan:     make(chan struct{}),
        batchSize:    100,
        fsyncEnabled: fsyncEnabled,
    }

    wm.wg.Add(1)
    go wm.writerLoop()

    return wm, nil
}

// writerLoop - основной цикл асинхронной записи.
func (wm *WALManager) writerLoop() {
    defer wm.wg.Done()

    batch := make([]*WALRecord, 0, wm.batchSize)
    ticker := time.NewTicker(wm.syncInterval)
    defer ticker.Stop()

    for {
        select {
        case record, ok := <-wm.writeChan:
            if !ok {
                if len(batch) > 0 {
                    wm.flushBatch(batch)
                }
                return
            }
            batch = append(batch, record)
            if len(batch) >= wm.batchSize {
                wm.flushBatch(batch)
                batch = batch[:0]
            }

        case <-ticker.C:
            if len(batch) > 0 {
                wm.flushBatch(batch)
                batch = batch[:0]
            }
            if time.Since(wm.lastSync) >= wm.syncInterval {
                wm.sync()
            }

        case <-wm.stopChan:
            if len(batch) > 0 {
                wm.flushBatch(batch)
            }
            wm.sync()
            return
        }
    }
}

// flushBatch - записывает пакет записей в WAL.
func (wm *WALManager) flushBatch(batch []*WALRecord) {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    for _, record := range batch {
        data, err := json.Marshal(record)
        if err != nil {
            continue
        }

        lenBuf := make([]byte, 4)
        binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
        if _, err := wm.writer.Write(lenBuf); err != nil {
            continue
        }

        if _, err := wm.writer.Write(data); err != nil {
            continue
        }

        record.LSN = wm.currentLSN
        wm.currentLSN++
    }
}

// sync - синхронизирует данные с диском.
func (wm *WALManager) sync() {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    if err := wm.writer.Flush(); err == nil {
        if wm.fsyncEnabled {
            if err := RealFsyncWithRetry(wm.file, FsyncMaxRetries, FsyncRetryDelay); err == nil {
                wm.lastSync = time.Now()
            }
        } else {
            wm.lastSync = time.Now()
        }
    }
}

// Write - записывает запись в WAL (асинхронно).
func (wm *WALManager) Write(record *WALRecord) error {
    if wm.closed {
        return fmt.Errorf("WAL is closed")
    }

    record.Timestamp = time.Now().UnixMilli()

    data, err := json.Marshal(record.Data)
    if err != nil {
        return err
    }
    record.CRC = crc32(data)

    select {
    case wm.writeChan <- record:
        return nil
    case <-time.After(100 * time.Millisecond):
        return fmt.Errorf("WAL write timeout")
    }
}

// Sync - принудительная синхронизация WAL с диском
func (wm *WALManager) Sync() error {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    if err := wm.writer.Flush(); err != nil {
        return err
    }
    if wm.fsyncEnabled {
        return RealFsyncWithRetry(wm.file, FsyncMaxRetries, FsyncRetryDelay)
    }
    return nil
}

// ReadAll - читает все записи из WAL.
func (wm *WALManager) ReadAll() ([]*WALRecord, error) {
    wm.mu.RLock()
    defer wm.mu.RUnlock()

    wm.writer.Flush()

    file, err := os.Open(wm.path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    records := make([]*WALRecord, 0)
    reader := bufio.NewReader(file)
    lenBuf := make([]byte, 4)

    for {
        _, err := reader.Read(lenBuf)
        if err != nil {
            break
        }

        recordLen := binary.BigEndian.Uint32(lenBuf)
        recordData := make([]byte, recordLen)
        _, err = reader.Read(recordData)
        if err != nil {
            break
        }

        var record WALRecord
        if err := json.Unmarshal(recordData, &record); err != nil {
            continue
        }

        data, _ := json.Marshal(record.Data)
        if crc32(data) != record.CRC {
            continue
        }

        records = append(records, &record)
    }

    return records, nil
}

// Close - закрывает WAL менеджер.
func (wm *WALManager) Close() error {
    wm.mu.Lock()
    wm.closed = true
    wm.mu.Unlock()

    close(wm.stopChan)
    close(wm.writeChan)
    wm.wg.Wait()

    wm.mu.Lock()
    defer wm.mu.Unlock()

    if err := wm.writer.Flush(); err != nil {
        return err
    }
    if wm.fsyncEnabled {
        RealFsync(wm.file)
    }
    return wm.file.Close()
}

// =============================================================================
// SEGMENTED WAL MANAGER - СЕГМЕНТИРОВАННЫЙ ЖУРНАЛ
// =============================================================================

// WALSegment - сегмент WAL.
type WALSegment struct {
    ID       uint32
    File     *os.File
    Writer   *bufio.Writer
    Path     string
    StartLSN uint64
    EndLSN   uint64
    Size     int64
    mu       sync.Mutex
}

// WALIndexEntry - запись в индексе WAL.
type WALIndexEntry struct {
    LSN       uint64
    SegmentID uint32
    Offset    int64
    Length    uint32
    Checksum  uint32
}

// WALIndexManager - управление индексом WAL.
type WALIndexManager struct {
    index     map[uint64]*WALIndexEntry
    segments  map[uint32]*WALSegment
    mu        sync.RWMutex
    indexPath string
}

// SegmentedWALManager - менеджер сегментированного WAL.
type SegmentedWALManager struct {
    segmentsDir      string
    segments         map[uint32]*WALSegment
    currentSegment   *WALSegment
    currentSegmentID uint32
    index            *WALIndexManager
    mu               sync.RWMutex
    writeChan        chan *WALRecord
    stopChan         chan struct{}
    wg               sync.WaitGroup
    batchSize        int
    logger           LoggerInterface
    recoveryManager  *AsyncRecoveryManager
    recoveryComplete atomic.Bool
    backupLSN        atomic.Uint64
    fsyncEnabled     bool
}

// NewSegmentedWALManager создаёт новый сегментированный WAL менеджер.
func NewSegmentedWALManager(segmentsDir string, fsyncEnabled bool, logger LoggerInterface) (*SegmentedWALManager, error) {
    if err := os.MkdirAll(segmentsDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create segments dir: %v", err)
    }

    wm := &SegmentedWALManager{
        segmentsDir:  segmentsDir,
        segments:     make(map[uint32]*WALSegment),
        index: &WALIndexManager{
            index:     make(map[uint64]*WALIndexEntry),
            segments:  make(map[uint32]*WALSegment),
            indexPath: filepath.Join(segmentsDir, WALIndexPrefix+"index.json"),
        },
        writeChan:   make(chan *WALRecord, 10000),
        stopChan:    make(chan struct{}),
        batchSize:   100,
        logger:      logger,
        fsyncEnabled: fsyncEnabled,
    }

    if err := wm.loadExistingSegments(); err != nil {
        return nil, err
    }

    if err := wm.index.load(); err != nil {
        if logger != nil {
            logger.Warn(fmt.Sprintf("Failed to load WAL index: %v", err))
        }
    }

    if wm.currentSegment == nil {
        if err := wm.rotateSegment(); err != nil {
            return nil, err
        }
    }

    wm.wg.Add(1)
    go wm.writerLoop()

    return wm, nil
}

// GetBackupLSN - возвращает LSN для бэкапа.
func (wm *SegmentedWALManager) GetBackupLSN() uint64 {
    return wm.backupLSN.Load()
}

// SetBackupLSN - устанавливает LSN для бэкапа.
func (wm *SegmentedWALManager) SetBackupLSN(lsn uint64) {
    wm.backupLSN.Store(lsn)
}

// loadExistingSegments - загружает существующие сегменты из директории.
func (wm *SegmentedWALManager) loadExistingSegments() error {
    files, err := filepath.Glob(filepath.Join(wm.segmentsDir, WALSegmentPrefix+"*"))
    if err != nil {
        return err
    }

    for _, filePath := range files {
        var segmentID uint32
        if _, err := fmt.Sscanf(filepath.Base(filePath), WALSegmentPrefix+"%d.log", &segmentID); err != nil {
            continue
        }

        file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
        if err != nil {
            continue
        }

        stat, _ := file.Stat()
        segment := &WALSegment{
            ID:       segmentID,
            File:     file,
            Writer:   bufio.NewWriterSize(file, 64*1024),
            Path:     filePath,
            Size:     stat.Size(),
            StartLSN: uint64(segmentID) * WALSegmentSize / 100,
        }
        wm.segments[segmentID] = segment

        if segmentID > wm.currentSegmentID {
            wm.currentSegmentID = segmentID
            wm.currentSegment = segment
        }
    }
    return nil
}

// rotateSegment - создаёт новый сегмент.
func (wm *SegmentedWALManager) rotateSegment() error {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    newSegmentID := wm.currentSegmentID + 1
    segmentPath := filepath.Join(wm.segmentsDir, fmt.Sprintf(WALSegmentPrefix+"%d.log", newSegmentID))

    file, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
    if err != nil {
        return fmt.Errorf("failed to create segment: %v", err)
    }

    newSegment := &WALSegment{
        ID:       newSegmentID,
        File:     file,
        Writer:   bufio.NewWriterSize(file, 64*1024),
        Path:     segmentPath,
        StartLSN: wm.getCurrentLSN(),
    }

    if wm.currentSegment != nil {
        wm.currentSegment.Writer.Flush()
        if wm.fsyncEnabled {
            RealFsync(wm.currentSegment.File)
        }
        wm.currentSegment.File.Close()
    }

    wm.currentSegment = newSegment
    wm.currentSegmentID = newSegmentID
    wm.segments[newSegmentID] = newSegment

    if wm.logger != nil {
        wm.logger.Info(fmt.Sprintf("Created new WAL segment: %d", newSegmentID))
    }

    return nil
}

// getCurrentLSN - возвращает текущий LSN.
func (wm *SegmentedWALManager) getCurrentLSN() uint64 {
    wm.mu.RLock()
    defer wm.mu.RUnlock()
    if wm.currentSegment == nil {
        return 1
    }
    return wm.currentSegment.StartLSN + uint64(wm.currentSegment.Size/100)
}

// Write - записывает запись в WAL.
func (wm *SegmentedWALManager) Write(record *WALRecord) error {
    record.Timestamp = time.Now().UnixMilli()
    wm.writeChan <- record
    return nil
}

// writerLoop - основной цикл асинхронной записи.
func (wm *SegmentedWALManager) writerLoop() {
    defer wm.wg.Done()

    batch := make([]*WALRecord, 0, wm.batchSize)
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case record, ok := <-wm.writeChan:
            if !ok {
                wm.flushBatch(batch)
                return
            }
            batch = append(batch, record)
            if len(batch) >= wm.batchSize {
                wm.flushBatch(batch)
                batch = batch[:0]
            }

        case <-ticker.C:
            if len(batch) > 0 {
                wm.flushBatch(batch)
                batch = batch[:0]
            }

        case <-wm.stopChan:
            wm.flushBatch(batch)
            return
        }
    }
}

// flushBatch - записывает пакет записей в текущий сегмент.
func (wm *SegmentedWALManager) flushBatch(batch []*WALRecord) {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    for _, record := range batch {
        if wm.currentSegment.Size >= WALSegmentSize {
            wm.mu.Unlock()
            wm.rotateSegment()
            wm.mu.Lock()
        }

        data, err := json.Marshal(record)
        if err != nil {
            continue
        }

        lsnBytes := make([]byte, 8)
        binary.BigEndian.PutUint64(lsnBytes, record.LSN)
        crcData := append(lsnBytes, data...)
        record.CRC = crc32(crcData)

        lenBuf := make([]byte, 4)
        binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
        if _, err := wm.currentSegment.Writer.Write(lenBuf); err != nil {
            continue
        }

        if _, err := wm.currentSegment.Writer.Write(data); err != nil {
            continue
        }

        wm.index.addEntry(&WALIndexEntry{
            LSN:       record.LSN,
            SegmentID: wm.currentSegment.ID,
            Offset:    wm.currentSegment.Size,
            Length:    uint32(len(data)),
            Checksum:  record.CRC,
        })

        wm.currentSegment.Size += int64(4 + len(data))
        wm.currentSegment.EndLSN = record.LSN
    }

    wm.currentSegment.Writer.Flush()
    if wm.fsyncEnabled {
        RealFsyncWithRetry(wm.currentSegment.File, FsyncMaxRetries, FsyncRetryDelay)
    }
    wm.index.save()
}

// Sync - принудительная синхронизация WAL с диском
func (wm *SegmentedWALManager) Sync() error {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    if wm.currentSegment == nil {
        return nil
    }

    if err := wm.currentSegment.Writer.Flush(); err != nil {
        return err
    }

    if wm.fsyncEnabled {
        return RealFsyncWithRetry(wm.currentSegment.File, FsyncMaxRetries, FsyncRetryDelay)
    }
    return nil
}

// ReadAll - читает все записи из всех сегментов.
func (wm *SegmentedWALManager) ReadAll() ([]*WALRecord, error) {
    wm.mu.RLock()
    segments := make([]*WALSegment, 0, len(wm.segments))
    for _, seg := range wm.segments {
        segments = append(segments, seg)
    }
    wm.mu.RUnlock()

    sort.Slice(segments, func(i, j int) bool {
        return segments[i].ID < segments[j].ID
    })

    records := make([]*WALRecord, 0)

    for _, seg := range segments {
        segRecords, err := wm.readSegmentRecords(seg)
        if err != nil {
            return nil, err
        }
        records = append(records, segRecords...)
    }

    return records, nil
}

// ReadSince - читает записи начиная с указанного LSN.
func (wm *SegmentedWALManager) ReadSince(lsn uint64) ([]*WALRecord, error) {
    allRecords, err := wm.ReadAll()
    if err != nil {
        return nil, err
    }

    result := make([]*WALRecord, 0)
    for _, record := range allRecords {
        if record.LSN > lsn {
            result = append(result, record)
        }
    }
    return result, nil
}

// GetCurrentLSN - возвращает текущий LSN.
func (wm *SegmentedWALManager) GetCurrentLSN() uint64 {
    wm.mu.RLock()
    defer wm.mu.RUnlock()
    if wm.currentSegment == nil {
        return 1
    }
    return wm.currentSegment.EndLSN
}

// readSegmentRecords - читает записи из конкретного сегмента.
func (wm *SegmentedWALManager) readSegmentRecords(seg *WALSegment) ([]*WALRecord, error) {
    seg.mu.Lock()
    defer seg.mu.Unlock()

    if seg.File == nil {
        return nil, nil
    }

    seg.Writer.Flush()
    seg.File.Seek(0, 0)

    records := make([]*WALRecord, 0)
    reader := bufio.NewReader(seg.File)
    lenBuf := make([]byte, 4)

    for {
        _, err := reader.Read(lenBuf)
        if err != nil {
            break
        }

        recordLen := binary.BigEndian.Uint32(lenBuf)
        recordData := make([]byte, recordLen)
        _, err = reader.Read(recordData)
        if err != nil {
            break
        }

        var record WALRecord
        if err := json.Unmarshal(recordData, &record); err != nil {
            continue
        }

        lsnBytes := make([]byte, 8)
        binary.BigEndian.PutUint64(lsnBytes, record.LSN)
        crcData := append(lsnBytes, recordData...)
        if crc32(crcData) != record.CRC {
            continue
        }

        records = append(records, &record)
    }

    return records, nil
}

// Close - закрывает WAL менеджер.
func (wm *SegmentedWALManager) Close() error {
    close(wm.stopChan)
    close(wm.writeChan)
    wm.wg.Wait()

    wm.mu.Lock()
    defer wm.mu.Unlock()

    if wm.currentSegment != nil {
        wm.currentSegment.Writer.Flush()
        if wm.fsyncEnabled {
            RealFsyncWithRetry(wm.currentSegment.File, FsyncMaxRetries, FsyncRetryDelay)
        }
        wm.currentSegment.File.Close()
    }

    wm.index.save()
    return nil
}

// addEntry - добавляет запись в индекс.
func (im *WALIndexManager) addEntry(entry *WALIndexEntry) {
    im.mu.Lock()
    defer im.mu.Unlock()
    im.index[entry.LSN] = entry
}

// save - сохраняет индекс на диск.
func (im *WALIndexManager) save() error {
    im.mu.RLock()
    defer im.mu.RUnlock()

    data, err := json.Marshal(im.index)
    if err != nil {
        return err
    }
    return os.WriteFile(im.indexPath, data, 0644)
}

// load - загружает индекс с диска.
func (im *WALIndexManager) load() error {
    data, err := os.ReadFile(im.indexPath)
    if err != nil {
        if os.IsNotExist(err) {
            return nil
        }
        return err
    }

    if len(data) == 0 {
        return nil
    }

    return json.Unmarshal(data, &im.index)
}

// =============================================================================
// ASYNC RECOVERY MANAGER - АСИНХРОННОЕ ВОССТАНОВЛЕНИЕ
// =============================================================================

// AsyncRecoveryManager - управляет асинхронным восстановлением.
type AsyncRecoveryManager struct {
    recordChan   chan *WALRecord
    errChan      chan error
    doneChan     chan struct{}
    wg           sync.WaitGroup
    callback     func(*WALRecord) error
    mu           sync.RWMutex
    isRunning    bool
    recoveredCnt atomic.Uint64
    errorCnt     atomic.Uint64
    startTime    time.Time
}

// NewAsyncRecoveryManager создаёт новый менеджер асинхронного восстановления.
func NewAsyncRecoveryManager(callback func(*WALRecord) error, workers int) *AsyncRecoveryManager {
    arm := &AsyncRecoveryManager{
        recordChan: make(chan *WALRecord, AsyncRecoveryBufferSize),
        errChan:    make(chan error, workers),
        doneChan:   make(chan struct{}),
        callback:   callback,
        startTime:  time.Now(),
        isRunning:  true,
    }

    for i := 0; i < workers; i++ {
        arm.wg.Add(1)
        go arm.worker()
    }

    go arm.errorMonitor()

    return arm
}

// worker - воркер для обработки записей восстановления.
func (arm *AsyncRecoveryManager) worker() {
    defer arm.wg.Done()

    for record := range arm.recordChan {
        var err error
        if arm.callback != nil {
            err = arm.callback(record)
        }
        if err != nil {
            select {
            case arm.errChan <- err:
            default:
            }
            arm.errorCnt.Add(1)
        } else {
            arm.recoveredCnt.Add(1)
        }
    }
}

// errorMonitor - мониторинг ошибок восстановления.
func (arm *AsyncRecoveryManager) errorMonitor() {
    criticalErrors := 0
    for range arm.errChan {
        criticalErrors++
        if criticalErrors > 10 {
            arm.Stop()
            return
        }
    }
}

// Push - отправляет запись на восстановление.
func (arm *AsyncRecoveryManager) Push(record *WALRecord) bool {
    arm.mu.RLock()
    if !arm.isRunning {
        arm.mu.RUnlock()
        return false
    }
    arm.mu.RUnlock()

    select {
    case arm.recordChan <- record:
        return true
    case <-time.After(100 * time.Millisecond):
        return false
    }
}

// Wait - ожидает завершения восстановления.
func (arm *AsyncRecoveryManager) Wait() {
    close(arm.recordChan)
    arm.wg.Wait()
    close(arm.doneChan)
}

// Stop - останавливает восстановление.
func (arm *AsyncRecoveryManager) Stop() {
    arm.mu.Lock()
    if !arm.isRunning {
        arm.mu.Unlock()
        return
    }
    arm.isRunning = false
    arm.mu.Unlock()

    close(arm.recordChan)
}

// GetStats - возвращает статистику восстановления.
func (arm *AsyncRecoveryManager) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "recovered":  arm.recoveredCnt.Load(),
        "errors":     arm.errorCnt.Load(),
        "is_running": arm.isRunning,
        "elapsed_ms": time.Since(arm.startTime).Milliseconds(),
    }
}

// =============================================================================
// DEADLOCK DETECTOR - ОБНАРУЖЕНИЕ ВЗАИМНЫХ БЛОКИРОВОК
// =============================================================================

// DeadlockDetector - детектор взаимных блокировок.
type DeadlockDetector struct {
    waitForGraph  sync.Map
    checkInterval time.Duration
    timeout       time.Duration
    mu            sync.RWMutex
    stopChan      chan struct{}
    wg            sync.WaitGroup
    logger        LoggerInterface
}

// NewDeadlockDetector - создаёт новый детектор дедлоков.
func NewDeadlockDetector(checkInterval, timeout time.Duration) *DeadlockDetector {
    if checkInterval <= 0 {
        checkInterval = DeadlockCheckInterval
    }
    if timeout <= 0 {
        timeout = DefaultTxTimeout
    }
    d := &DeadlockDetector{
        checkInterval: checkInterval,
        timeout:       timeout,
        stopChan:      make(chan struct{}),
    }
    d.wg.Add(1)
    go d.detectLoop()
    return d
}

// SetLogger - устанавливает логгер.
func (dd *DeadlockDetector) SetLogger(logger LoggerInterface) {
    dd.logger = logger
}

// Stop - останавливает детектор.
func (dd *DeadlockDetector) Stop() {
    close(dd.stopChan)
    dd.wg.Wait()
}

// AddWaiting - добавляет зависимость ожидания.
func (dd *DeadlockDetector) AddWaiting(waiting, waitingFor TransactionID) {
    var list []TransactionID
    if val, ok := dd.waitForGraph.Load(waiting); ok {
        list = val.([]TransactionID)
    }
    list = append(list, waitingFor)
    dd.waitForGraph.Store(waiting, list)
}

// RemoveWaiting - удаляет зависимость ожидания.
func (dd *DeadlockDetector) RemoveWaiting(txID TransactionID) {
    dd.waitForGraph.Delete(txID)
}

// detectLoop - основной цикл обнаружения дедлоков.
func (dd *DeadlockDetector) detectLoop() {
    defer dd.wg.Done()
    ticker := time.NewTicker(dd.checkInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            dd.detect()
        case <-dd.stopChan:
            return
        }
    }
}

// detect - выполняет обнаружение дедлоков.
func (dd *DeadlockDetector) detect() {
    visited := make(map[TransactionID]bool)
    stack := make(map[TransactionID]bool)

    var dfs func(txID TransactionID) bool
    dfs = func(txID TransactionID) bool {
        visited[txID] = true
        stack[txID] = true

        val, ok := dd.waitForGraph.Load(txID)
        if ok {
            for _, next := range val.([]TransactionID) {
                if !visited[next] {
                    if dfs(next) {
                        return true
                    }
                } else if stack[next] {
                    dd.resolveDeadlock(txID, next)
                    return true
                }
            }
        }
        stack[txID] = false
        return false
    }

    dd.waitForGraph.Range(func(key, value interface{}) bool {
        txID := key.(TransactionID)
        if !visited[txID] {
            dfs(txID)
        }
        return true
    })
}

// resolveDeadlock - разрешает взаимную блокировку.
func (dd *DeadlockDetector) resolveDeadlock(txID1, txID2 TransactionID) {
    if globalTxManager != nil {
        if val, ok := globalTxManager.activeTransactions.Load(txID1); ok {
            tx := val.(*Transaction)
            if tx.IsDistributed {
                if dd.logger != nil {
                    dd.logger.Warn(fmt.Sprintf("Distributed transaction %d involved in deadlock with %d, aborting", txID1, txID2))
                }
                AbortDistributedTransaction(txID1)
            } else {
                tx.State.Store(int32(TransactionAborted))
                globalTxManager.activeTransactions.Delete(txID1)
                globalTxManager.stats.TotalDeadlocks.Add(1)
                globalTxManager.stats.ActiveCount.Add(^uint64(0))
                if dd.logger != nil {
                    dd.logger.Warn(fmt.Sprintf("Deadlock resolved: aborted transaction %d due to conflict with %d", txID1, txID2))
                }
            }
        }
    }
}

// =============================================================================
// DISTRIBUTED TRANSACTION COORDINATOR
// =============================================================================

// TxState - состояние распределённой транзакции.
type TxState int32

const (
    TxActive TxState = iota
    TxCommitted
    TxAborted
    TxTimeout
)

// DistributedTxInfo - информация о распределённой транзакции.
type DistributedTxInfo struct {
    TxID      TransactionID
    Nodes     []string
    Status    TxState
    StartTime int64
    Timeout   time.Duration
}

// DistributedTransactionCoordinator - координатор распределённых транзакций.
type DistributedTransactionCoordinator struct {
    pendingTxs sync.Map
    timeout    time.Duration
    mu         sync.RWMutex
}

// NewDistributedTransactionCoordinator - создаёт новый координатор.
func NewDistributedTransactionCoordinator(timeout time.Duration) *DistributedTransactionCoordinator {
    if timeout <= 0 {
        timeout = 30 * time.Second
    }
    return &DistributedTransactionCoordinator{timeout: timeout}
}

// Prepare - подготавливает распределённую транзакцию.
func (dtc *DistributedTransactionCoordinator) Prepare(txID TransactionID, nodes []string) error {
    info := &DistributedTxInfo{
        TxID:      txID,
        Nodes:     nodes,
        Status:    TxActive,
        StartTime: time.Now().UnixMilli(),
        Timeout:   dtc.timeout,
    }
    dtc.pendingTxs.Store(txID, info)
    return nil
}

// Commit - коммитит распределённую транзакцию.
func (dtc *DistributedTransactionCoordinator) Commit(txID TransactionID) error {
    val, ok := dtc.pendingTxs.Load(txID)
    if !ok {
        return fmt.Errorf("transaction not found: %d", txID)
    }
    info := val.(*DistributedTxInfo)
    info.Status = TxCommitted
    return nil
}

// Abort - отменяет распределённую транзакцию.
func (dtc *DistributedTransactionCoordinator) Abort(txID TransactionID) error {
    dtc.pendingTxs.Delete(txID)
    return nil
}

// =============================================================================
// Transaction - структура транзакции.
// =============================================================================

// Transaction - структура транзакции.
type Transaction struct {
    ID            TransactionID
    State         atomic.Int32
    Operations    []Operation
    StartTime     int64
    Version       uint64
    mu            sync.RWMutex
    IsDistributed bool
    Nodes         []string
    savepoints    []*Savepoint
    timeout       time.Duration
    timeoutTimer  *time.Timer
}

// Savepoint - точка сохранения в транзакции.
type Savepoint struct {
    Name      string
    Timestamp int64
    OpCount   int
    Snapshot  *Document
}

// TransactionOptions - опции создания транзакции.
type TransactionOptions struct {
    Timeout        time.Duration
    IsDistributed  bool
    Nodes          []string
    IsolationLevel string
}

// TransactionInfo - информация о транзакции.
type TransactionInfo struct {
    ID             string          `json:"id"`
    Status         string          `json:"status"`
    StartTime      int64           `json:"start_time"`
    OperationCount int             `json:"operation_count"`
    Operations     []OperationInfo `json:"operations,omitempty"`
    Savepoints     []string        `json:"savepoints,omitempty"`
    Nodes          []string        `json:"nodes,omitempty"`
}

// OperationInfo - информация об операции.
type OperationInfo struct {
    Type       string `json:"type"`
    Database   string `json:"database"`
    Collection string `json:"collection"`
    DocumentID string `json:"document_id"`
}

// TransactionStats - статистика по транзакциям.
type TransactionStats struct {
    TotalStarted    atomic.Uint64
    TotalCommitted  atomic.Uint64
    TotalAborted    atomic.Uint64
    TotalTimedOut   atomic.Uint64
    TotalDeadlocks  atomic.Uint64
    ActiveCount     atomic.Uint64
    PeakActiveCount atomic.Uint64
    MaxOpsPerTx     atomic.Uint64
    AvgOpsPerTx     atomic.Uint64
    TotalOps        atomic.Uint64
    StartTime       time.Time
}

// TransactionManager - основной менеджер транзакций.
type TransactionManager struct {
    activeTransactions sync.Map
    nextTxID           atomic.Uint64
    wal                *SegmentedWALManager
    logger             LoggerInterface
    mu                 sync.RWMutex
    walPath            string
    checkpointInterval int64
    lastCheckpoint     int64
    checkpointFile     *os.File
    documentVersions   sync.Map
    maxVersions        int
    visibilityMap      *VisibilityMap
    readCache          *ReadTimestampCache
    distCoord          *DistributedTransactionCoordinator
    deadlockDetector   *DeadlockDetector
    recoveryManager    *AsyncRecoveryManager
    recoveryComplete   atomic.Bool
    backupLock         sync.RWMutex
    backupInProgress   atomic.Bool
    stats              *TransactionStats
    // MVCC менеджер
    mvccManager        *MVCCManager
}

// =============================================================================
// ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ И ФУНКЦИИ ДОСТУПА
// =============================================================================

var (
    globalTxManager *TransactionManager
    txManagerOnce   sync.Once
    currentTx       atomic.Value
    globalStorage   *Storage
)

// InitTransactionManager - инициализирует менеджер транзакций.
func InitTransactionManager(walPath string) error {
    return InitTransactionManagerWithConfig(walPath, nil)
}

// InitTransactionManagerWithConfig - инициализирует менеджер транзакций с конфигурацией.
func InitTransactionManagerWithConfig(walPath string, config map[string]interface{}) error {
    var err error
    txManagerOnce.Do(func() {
        maxVersions := 10
        if config != nil {
            if v, ok := config["max_versions"].(int); ok && v > 0 {
                maxVersions = v
            }
        }

        globalTxManager = &TransactionManager{
            nextTxID:           atomic.Uint64{},
            walPath:            walPath,
            checkpointInterval: 300,
            lastCheckpoint:     time.Now().Unix(),
            maxVersions:        maxVersions,
            visibilityMap:      NewVisibilityMap(VisibilityMapSize),
            readCache:          NewReadTimestampCache(10000, 5*time.Minute),
            distCoord:          NewDistributedTransactionCoordinator(30 * time.Second),
            deadlockDetector:   NewDeadlockDetector(DeadlockCheckInterval, DefaultTxTimeout),
            stats:              &TransactionStats{StartTime: time.Now()},
            mvccManager:        NewMVCCManager(MaxVersionsPerDoc, VersionRetentionDays),
        }
        globalTxManager.nextTxID.Store(1)

        var walErr error
        // Используем fsync по умолчанию
        fsyncEnabled := true
        if config != nil {
            if v, ok := config["fsync_enabled"].(bool); ok {
                fsyncEnabled = v
            }
        }

        globalTxManager.wal, walErr = NewSegmentedWALManager(filepath.Dir(walPath), fsyncEnabled, nil)
        if walErr != nil {
            err = walErr
            return
        }

        globalTxManager.startAsyncRecovery()
        go globalTxManager.checkpointLoop()
        go globalTxManager.versionCleanupLoop()
        go globalTxManager.statsMonitor()
    })
    return err
}

// GetTransactionManager возвращает глобальный менеджер транзакций.
func GetTransactionManager() *TransactionManager {
    return globalTxManager
}

// SetTransactionLogger - устанавливает логгер для транзакций.
func SetTransactionLogger(logger LoggerInterface) {
    if globalTxManager != nil {
        globalTxManager.logger = logger
        if globalTxManager.wal != nil {
            globalTxManager.wal.logger = logger
        }
        if globalTxManager.deadlockDetector != nil {
            globalTxManager.deadlockDetector.SetLogger(logger)
        }
    }
}

// SetGlobalStorage - устанавливает глобальное хранилище.
func SetGlobalStorage(s *Storage) {
    globalStorage = s
}

// GetGlobalStorage - возвращает глобальное хранилище.
func GetGlobalStorage() *Storage {
    return globalStorage
}

// BeginTransaction - начинает новую транзакцию.
func BeginTransaction() *Transaction {
    if globalTxManager == nil {
        InitTransactionManager("futriis.wal")
    }
    return BeginTransactionWithOptions(&TransactionOptions{
        Timeout: DefaultTxTimeout,
    })
}

// BeginTransactionWithOptions - начинает транзакцию с опциями.
func BeginTransactionWithOptions(options *TransactionOptions) *Transaction {
    if globalTxManager == nil {
        InitTransactionManager("futriis.wal")
    }

    if options == nil {
        options = &TransactionOptions{
            Timeout: DefaultTxTimeout,
        }
    }

    tx := &Transaction{
        ID:            TransactionID(globalTxManager.nextTxID.Add(1) - 1),
        StartTime:     time.Now().UnixMilli(),
        Operations:    make([]Operation, 0, 100),
        Version:       1,
        savepoints:    make([]*Savepoint, 0),
        timeout:       options.Timeout,
        IsDistributed: options.IsDistributed,
        Nodes:         options.Nodes,
    }
    tx.State.Store(int32(TransactionActive))

    globalTxManager.activeTransactions.Store(tx.ID, tx)
    currentTx.Store(tx)

    globalTxManager.stats.TotalStarted.Add(1)
    globalTxManager.stats.ActiveCount.Add(1)

    // Аудит начала транзакции
    LogTransactionAudit(tx.ID, "START", TransactionActive, map[string]interface{}{
        "start_time":  tx.StartTime,
        "timeout_ms":  options.Timeout.Milliseconds(),
        "distributed": options.IsDistributed,
    })

    if options.Timeout > 0 {
        tx.timeoutTimer = time.AfterFunc(options.Timeout, func() {
            if TransactionState(tx.State.Load()) == TransactionActive {
                tx.State.Store(int32(TransactionAborted))
                globalTxManager.activeTransactions.Delete(tx.ID)
                globalTxManager.stats.TotalTimedOut.Add(1)
                globalTxManager.stats.ActiveCount.Add(^uint64(0))
                LogTransactionAudit(tx.ID, "TIMEOUT", TransactionAborted, map[string]interface{}{
                    "timeout_ms": options.Timeout.Milliseconds(),
                })
            }
        })
    }

    return tx
}

// BeginTransactionWithTimeout - начинает транзакцию с таймаутом.
func BeginTransactionWithTimeout(timeout time.Duration) *Transaction {
    return BeginTransactionWithOptions(&TransactionOptions{
        Timeout: timeout,
    })
}

// BeginDistributedTransaction - начинает распределённую транзакцию.
func BeginDistributedTransaction(nodes []string) (*Transaction, error) {
    if globalTxManager == nil {
        if err := InitTransactionManager("futriis.wal"); err != nil {
            return nil, err
        }
    }

    options := &TransactionOptions{
        Timeout:        30 * time.Second,
        IsDistributed:  true,
        Nodes:          nodes,
        IsolationLevel: "READ_COMMITTED",
    }

    tx := BeginTransactionWithOptions(options)
    if tx == nil {
        return nil, fmt.Errorf("failed to create transaction")
    }

    if err := globalTxManager.distCoord.Prepare(tx.ID, nodes); err != nil {
        return nil, err
    }

    if globalTxManager.logger != nil {
        globalTxManager.logger.Info(fmt.Sprintf("Distributed transaction %d started on nodes: %v", tx.ID, nodes))
    }

    return tx, nil
}

// CommitCurrentTransaction - коммитит текущую транзакцию.
func CommitCurrentTransaction() error {
    txVal := currentTx.Load()
    if txVal == nil {
        return fmt.Errorf("no active transaction")
    }

    tx := txVal.(*Transaction)
    if TransactionState(tx.State.Load()) != TransactionActive {
        return fmt.Errorf("transaction is not active")
    }

    if tx.IsDistributed {
        return fmt.Errorf("distributed transaction must use CommitDistributedTransaction")
    }

    if tx.timeout > 0 && time.Since(time.UnixMilli(tx.StartTime)) > tx.timeout {
        AbortCurrentTransaction()
        return fmt.Errorf("transaction timeout exceeded")
    }

    for _, op := range tx.Operations {
        if err := applyOperation(op); err != nil {
            AbortCurrentTransaction()
            return fmt.Errorf("transaction commit failed at operation %s: %v", op.Type, err)
        }
        if globalTxManager != nil && op.DocumentID != "" {
            if globalStorage != nil {
                db, _ := globalStorage.GetDatabase(op.Database)
                if db != nil {
                    coll, _ := db.GetCollection(op.Collection)
                    if coll != nil {
                        if doc, err := coll.Find(op.DocumentID); err == nil {
                            // Создаём MVCC версию при коммите
                            if globalTxManager.mvccManager != nil {
                                globalTxManager.mvccManager.CreateVersion(doc, tx.ID)
                            }
                            globalTxManager.AddDocumentVersion(op.DocumentID, &DocumentVersion{
                                Document:  doc.Clone(),
                                Timestamp: time.Now().UnixMilli(),
                                TxID:      tx.ID,
                            })
                        }
                    }
                }
            }
        }
    }

    tx.State.Store(int32(TransactionCommitted))

    txRecord := &TransactionRecord{
        ID:         tx.ID,
        State:      TransactionCommitted,
        Timestamp:  time.Now().UnixMilli(),
        Operations: tx.Operations,
    }

    data, err := json.Marshal(txRecord)
    if err == nil {
        walRecord := &WALRecord{
            Type: 1,
            Data: data,
        }
        globalTxManager.wal.Write(walRecord)
        // Принудительная синхронизация WAL для ACID
        globalTxManager.wal.Sync()
    }

    LogTransactionAudit(tx.ID, "COMMIT", TransactionCommitted, map[string]interface{}{
        "operations": len(tx.Operations),
    })

    globalTxManager.stats.TotalCommitted.Add(1)
    globalTxManager.stats.ActiveCount.Add(^uint64(0))
    globalTxManager.stats.TotalOps.Add(uint64(len(tx.Operations)))
    if uint64(len(tx.Operations)) > globalTxManager.stats.MaxOpsPerTx.Load() {
        globalTxManager.stats.MaxOpsPerTx.Store(uint64(len(tx.Operations)))
    }

    if tx.timeoutTimer != nil {
        tx.timeoutTimer.Stop()
    }

    currentTx.Store(nil)
    globalTxManager.activeTransactions.Delete(tx.ID)

    return nil
}

// AbortCurrentTransaction - отменяет текущую транзакцию.
func AbortCurrentTransaction() error {
    txVal := currentTx.Load()
    if txVal == nil {
        return fmt.Errorf("no active transaction")
    }

    tx := txVal.(*Transaction)
    tx.State.Store(int32(TransactionAborted))

    LogTransactionAudit(tx.ID, "ABORT", TransactionAborted, map[string]interface{}{
        "operations": len(tx.Operations),
    })

    globalTxManager.stats.TotalAborted.Add(1)
    globalTxManager.stats.ActiveCount.Add(^uint64(0))

    if tx.timeoutTimer != nil {
        tx.timeoutTimer.Stop()
    }

    currentTx.Store(nil)
    globalTxManager.activeTransactions.Delete(tx.ID)

    return nil
}

// CommitDistributedTransaction - коммитит распределённую транзакцию.
func CommitDistributedTransaction(txID TransactionID) error {
    if globalTxManager == nil {
        return fmt.Errorf("transaction manager not initialized")
    }

    val, ok := globalTxManager.activeTransactions.Load(txID)
    if !ok {
        return fmt.Errorf("transaction not found: %d", txID)
    }

    tx := val.(*Transaction)
    if TransactionState(tx.State.Load()) != TransactionActive {
        return fmt.Errorf("transaction is not active")
    }

    if err := globalTxManager.distCoord.Commit(txID); err != nil {
        return err
    }

    for _, op := range tx.Operations {
        if err := applyOperation(op); err != nil {
            return fmt.Errorf("failed to apply operation: %v", err)
        }
    }

    tx.State.Store(int32(TransactionCommitted))
    globalTxManager.activeTransactions.Delete(txID)
    globalTxManager.stats.TotalCommitted.Add(1)
    globalTxManager.stats.ActiveCount.Add(^uint64(0))

    if tx.timeoutTimer != nil {
        tx.timeoutTimer.Stop()
    }

    LogTransactionAudit(txID, "COMMIT_DISTRIBUTED", TransactionCommitted, map[string]interface{}{
        "nodes": tx.Nodes,
    })

    return nil
}

// AbortDistributedTransaction - отменяет распределённую транзакцию.
func AbortDistributedTransaction(txID TransactionID) error {
    if globalTxManager == nil {
        return fmt.Errorf("transaction manager not initialized")
    }

    val, ok := globalTxManager.activeTransactions.Load(txID)
    if !ok {
        return fmt.Errorf("transaction not found: %d", txID)
    }

    tx := val.(*Transaction)

    if err := globalTxManager.distCoord.Abort(txID); err != nil {
        return err
    }

    tx.State.Store(int32(TransactionAborted))
    globalTxManager.activeTransactions.Delete(txID)
    globalTxManager.stats.TotalAborted.Add(1)
    globalTxManager.stats.ActiveCount.Add(^uint64(0))

    if tx.timeoutTimer != nil {
        tx.timeoutTimer.Stop()
    }

    LogTransactionAudit(txID, "ABORT_DISTRIBUTED", TransactionAborted, map[string]interface{}{
        "nodes": tx.Nodes,
    })

    return nil
}

// applyOperation - применяет операцию к хранилищу.
func applyOperation(op Operation) error {
    if globalStorage == nil {
        return fmt.Errorf("storage not initialized")
    }

    db, err := globalStorage.GetDatabase(op.Database)
    if err != nil {
        return fmt.Errorf("database not found: %s", op.Database)
    }

    coll, err := db.GetCollection(op.Collection)
    if err != nil {
        return fmt.Errorf("collection not found: %s", op.Collection)
    }

    switch op.Type {
    case "insert":
        doc := NewDocumentWithID(op.DocumentID)
        for k, v := range op.Data {
            doc.SetField(k, v)
        }
        doc.Version = op.Version
        // Создаём MVCC версию при вставке
        if globalTxManager != nil && globalTxManager.mvccManager != nil {
            globalTxManager.mvccManager.CreateVersion(doc, 0)
        }
        return coll.Insert(doc)

    case "update":
        if err := coll.Update(op.DocumentID, op.Data); err != nil {
            return err
        }
        // Создаём MVCC версию при обновлении
        if globalTxManager != nil && globalTxManager.mvccManager != nil {
            if doc, err := coll.Find(op.DocumentID); err == nil {
                globalTxManager.mvccManager.CreateVersion(doc, 0)
            }
        }
        return nil

    case "delete":
        return coll.Delete(op.DocumentID)
    }

    return nil
}

// CreateSavepoint - создаёт точку сохранения в транзакции.
func (tx *Transaction) CreateSavepoint(name string) error {
    if TransactionState(tx.State.Load()) != TransactionActive {
        return fmt.Errorf("transaction is not active")
    }

    if len(tx.savepoints) >= MaxSavepointsPerTx {
        return fmt.Errorf("too many savepoints (max %d)", MaxSavepointsPerTx)
    }

    for _, sp := range tx.savepoints {
        if sp.Name == name {
            return fmt.Errorf("savepoint '%s' already exists", name)
        }
    }

    savepoint := &Savepoint{
        Name:      name,
        Timestamp: time.Now().UnixMilli(),
        OpCount:   len(tx.Operations),
    }

    if len(tx.Operations) > 0 {
        lastOp := tx.Operations[len(tx.Operations)-1]
        if lastOp.DocumentID != "" && globalStorage != nil {
            db, _ := globalStorage.GetDatabase(lastOp.Database)
            if db != nil {
                coll, _ := db.GetCollection(lastOp.Collection)
                if coll != nil {
                    if doc, err := coll.Find(lastOp.DocumentID); err == nil {
                        savepoint.Snapshot = doc.Clone()
                    }
                }
            }
        }
    }

    tx.mu.Lock()
    tx.savepoints = append(tx.savepoints, savepoint)
    tx.mu.Unlock()

    LogTransactionAudit(tx.ID, "SAVEPOINT", TransactionActive, map[string]interface{}{
        "savepoint": name,
        "op_count":  savepoint.OpCount,
    })

    return nil
}

// RollbackToSavepoint - откатывает транзакцию к точке сохранения.
func (tx *Transaction) RollbackToSavepoint(name string) error {
    if TransactionState(tx.State.Load()) != TransactionActive {
        return fmt.Errorf("transaction is not active")
    }

    tx.mu.Lock()
    defer tx.mu.Unlock()

    var targetIdx int = -1
    for i, sp := range tx.savepoints {
        if sp.Name == name {
            targetIdx = i
            break
        }
    }

    if targetIdx == -1 {
        return fmt.Errorf("savepoint '%s' not found", name)
    }

    if len(tx.Operations) > tx.savepoints[targetIdx].OpCount {
        tx.Operations = tx.Operations[:tx.savepoints[targetIdx].OpCount]
    }

    tx.savepoints = tx.savepoints[:targetIdx+1]

    LogTransactionAudit(tx.ID, "ROLLBACK_TO_SAVEPOINT", TransactionActive, map[string]interface{}{
        "savepoint": name,
    })

    return nil
}

// ReleaseSavepoint - освобождает точку сохранения.
func (tx *Transaction) ReleaseSavepoint(name string) error {
    tx.mu.Lock()
    defer tx.mu.Unlock()

    for i, sp := range tx.savepoints {
        if sp.Name == name {
            tx.savepoints = append(tx.savepoints[:i], tx.savepoints[i+1:]...)
            LogTransactionAudit(tx.ID, "RELEASE_SAVEPOINT", TransactionActive, map[string]interface{}{
                "savepoint": name,
            })
            return nil
        }
    }

    return fmt.Errorf("savepoint '%s' not found", name)
}

// GetSavepoints - возвращает список всех savepoints.
func (tx *Transaction) GetSavepoints() []string {
    tx.mu.RLock()
    defer tx.mu.RUnlock()

    names := make([]string, len(tx.savepoints))
    for i, sp := range tx.savepoints {
        names[i] = sp.Name
    }
    return names
}

// AddDocumentVersion - добавляет версию документа.
func (tm *TransactionManager) AddDocumentVersion(docID string, version *DocumentVersion) {
    val, _ := tm.documentVersions.LoadOrStore(docID, make([]*DocumentVersion, 0))
    versions := val.([]*DocumentVersion)
    versions = append(versions, version)

    if len(versions) > tm.maxVersions && tm.maxVersions > 0 {
        versions = versions[len(versions)-tm.maxVersions:]
    }
    tm.documentVersions.Store(docID, versions)

    if tm.visibilityMap != nil {
        tm.visibilityMap.MarkVisible(docID, uint64(version.TxID), true)
    }
}

// GetDocumentVersion - получает версию документа по временной метке.
func (tm *TransactionManager) GetDocumentVersion(docID string, timestamp int64) *Document {
    if tm.readCache != nil {
        if cached := tm.readCache.Get(docID, timestamp); cached != nil {
            return cached
        }
    }

    val, ok := tm.documentVersions.Load(docID)
    if !ok {
        return nil
    }

    versions := val.([]*DocumentVersion)
    for i := len(versions) - 1; i >= 0; i-- {
        if versions[i].Timestamp <= timestamp {
            doc := versions[i].Document.Clone()
            if tm.readCache != nil {
                tm.readCache.Set(docID, timestamp, doc)
            }
            return doc
        }
    }
    return nil
}

// checkpointLoop - периодическое создание чекпоинтов.
func (tm *TransactionManager) checkpointLoop() {
    ticker := time.NewTicker(time.Duration(tm.checkpointInterval) * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        tm.createCheckpoint()
    }
}

// createCheckpoint - создаёт чекпоинт.
func (tm *TransactionManager) createCheckpoint() {
    if tm.wal == nil {
        return
    }

    now := time.Now().Unix()
    if now-tm.lastCheckpoint < tm.checkpointInterval {
        return
    }

    checkpointPath := fmt.Sprintf("%s.checkpoint.%d", tm.walPath, now)

    checkpoint := make(map[string]interface{})
    checkpoint["timestamp"] = now
    checkpoint["backup_lsn"] = tm.wal.GetBackupLSN()

    data, err := json.Marshal(checkpoint)
    if err != nil {
        if tm.logger != nil {
            tm.logger.Error(fmt.Sprintf("Failed to marshal checkpoint: %v", err))
        }
        return
    }

    if err := os.WriteFile(checkpointPath, data, 0644); err != nil {
        if tm.logger != nil {
            tm.logger.Error(fmt.Sprintf("Failed to write checkpoint: %v", err))
        }
        return
    }

    tm.lastCheckpoint = now
    if tm.logger != nil {
        tm.logger.Info(fmt.Sprintf("Checkpoint created: %s", checkpointPath))
    }
}

// versionCleanupLoop - периодическая очистка старых версий.
func (tm *TransactionManager) versionCleanupLoop() {
    if tm.maxVersions <= 0 {
        return
    }

    ticker := time.NewTicker(VersionPruneInterval)
    defer ticker.Stop()

    for range ticker.C {
        cutoffTime := time.Now().AddDate(0, 0, -VersionRetentionDays).UnixMilli()

        tm.documentVersions.Range(func(key, value interface{}) bool {
            versions := value.([]*DocumentVersion)
            if len(versions) <= tm.maxVersions {
                return true
            }

            newVersions := make([]*DocumentVersion, 0, tm.maxVersions)
            for _, v := range versions {
                if v.Timestamp >= cutoffTime && len(newVersions) < tm.maxVersions {
                    newVersions = append(newVersions, v)
                }
            }

            if len(newVersions) < len(versions) {
                tm.documentVersions.Store(key, newVersions)
            }
            return true
        })
    }
}

// statsMonitor - мониторинг статистики.
func (tm *TransactionManager) statsMonitor() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        active := tm.stats.ActiveCount.Load()
        if active > tm.stats.PeakActiveCount.Load() {
            tm.stats.PeakActiveCount.Store(active)
        }
    }
}

// startAsyncRecovery - запускает асинхронное восстановление.
func (tm *TransactionManager) startAsyncRecovery() {
    if tm.wal == nil {
        tm.recoveryComplete.Store(true)
        return
    }

    if tm.logger != nil {
        tm.logger.Info("Starting asynchronous WAL recovery...")
    }

    records, err := tm.wal.ReadAll()
    if err != nil {
        if tm.logger != nil {
            tm.logger.Error(fmt.Sprintf("Failed to read WAL: %v", err))
        }
        tm.recoveryComplete.Store(true)
        return
    }

    if len(records) == 0 {
        if tm.logger != nil {
            tm.logger.Info("No records to recover")
        }
        tm.recoveryComplete.Store(true)
        return
    }

    tm.recoveryManager = NewAsyncRecoveryManager(func(record *WALRecord) error {
        if record.Type == 1 {
            var txRecord TransactionRecord
            if err := json.Unmarshal(record.Data, &txRecord); err != nil {
                return err
            }
            if txRecord.State == TransactionCommitted {
                for _, op := range txRecord.Operations {
                    if err := applyOperation(op); err != nil {
                        return err
                    }
                }
            }
        }
        return nil
    }, AsyncRecoveryWorkers)

    for _, record := range records {
        if !tm.recoveryManager.Push(record) {
            if tm.logger != nil {
                tm.logger.Warn("Recovery buffer full, some records may be delayed")
            }
        }
    }

    go func() {
        ticker := time.NewTicker(5 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                stats := tm.recoveryManager.GetStats()
                if tm.logger != nil {
                    tm.logger.Debug(fmt.Sprintf("Recovery progress: %d records recovered", stats["recovered"]))
                }
            case <-tm.recoveryManager.doneChan:
                stats := tm.recoveryManager.GetStats()
                if tm.logger != nil {
                    tm.logger.Info(fmt.Sprintf("WAL recovery completed: %d records recovered, %d errors",
                        stats["recovered"], stats["errors"]))
                }
                tm.recoveryComplete.Store(true)
                return
            }
        }
    }()
}

// IsRecoveryComplete - проверяет завершение восстановления.
func (tm *TransactionManager) IsRecoveryComplete() bool {
    return tm.recoveryComplete.Load()
}

// GetRecoveryProgress - возвращает прогресс восстановления.
func (tm *TransactionManager) GetRecoveryProgress() map[string]interface{} {
    if tm.recoveryManager == nil {
        return map[string]interface{}{
            "is_recovering": false,
            "recovered":     0,
            "complete":      true,
        }
    }

    stats := tm.recoveryManager.GetStats()
    return map[string]interface{}{
        "is_recovering": !tm.recoveryComplete.Load(),
        "recovered":     stats["recovered"],
        "complete":      tm.recoveryComplete.Load(),
        "elapsed_ms":    stats["elapsed_ms"],
    }
}

// LockForBackup - блокирует транзакции для бэкапа.
func (tm *TransactionManager) LockForBackup() {
    tm.backupLock.Lock()
    tm.backupInProgress.Store(true)
}

// UnlockForBackup - разблокирует транзакции после бэкапа.
func (tm *TransactionManager) UnlockForBackup() {
    tm.backupInProgress.Store(false)
    tm.backupLock.Unlock()
}

// IsBackupInProgress - проверяет выполнение бэкапа.
func (tm *TransactionManager) IsBackupInProgress() bool {
    return tm.backupInProgress.Load()
}

// GetTransactionStats - возвращает статистику транзакций.
func GetTransactionStats() map[string]interface{} {
    if globalTxManager == nil {
        return map[string]interface{}{
            "error": "transaction manager not initialized",
        }
    }

    stats := globalTxManager.stats
    active := stats.ActiveCount.Load()
    totalStarted := stats.TotalStarted.Load()
    totalCommitted := stats.TotalCommitted.Load()
    totalAborted := stats.TotalAborted.Load()
    totalTimedOut := stats.TotalTimedOut.Load()
    totalDeadlocks := stats.TotalDeadlocks.Load()

    return map[string]interface{}{
        "total_started":         totalStarted,
        "total_committed":       totalCommitted,
        "total_aborted":         totalAborted,
        "total_timed_out":       totalTimedOut,
        "total_deadlocks":       totalDeadlocks,
        "active_count":          active,
        "peak_active_count":     stats.PeakActiveCount.Load(),
        "max_ops_per_tx":        stats.MaxOpsPerTx.Load(),
        "avg_ops_per_tx":        stats.AvgOpsPerTx.Load(),
        "total_ops":             stats.TotalOps.Load(),
        "commit_rate":           float64(totalCommitted) / float64(totalStarted+1) * 100,
        "abort_rate":            float64(totalAborted) / float64(totalStarted+1) * 100,
        "uptime_seconds":        time.Since(stats.StartTime).Seconds(),
        "is_recovery_complete":  globalTxManager.IsRecoveryComplete(),
        "backup_in_progress":    globalTxManager.IsBackupInProgress(),
    }
}

// StopTransactionManager - останавливает менеджер транзакций.
func StopTransactionManager() error {
    if globalTxManager == nil {
        return nil
    }

    if globalTxManager.deadlockDetector != nil {
        globalTxManager.deadlockDetector.Stop()
    }

    if globalTxManager.wal != nil {
        // Принудительная синхронизация перед закрытием
        globalTxManager.wal.Sync()
        return globalTxManager.wal.Close()
    }

    return nil
}

// HasActiveTransaction - проверяет наличие активной транзакции.
func HasActiveTransaction() bool {
    return currentTx.Load() != nil
}

// GetCurrentTransactionID - возвращает ID текущей транзакции.
func GetCurrentTransactionID() string {
    txVal := currentTx.Load()
    if txVal == nil {
        return ""
    }
    tx := txVal.(*Transaction)
    return fmt.Sprintf("%d", tx.ID)
}

// GetActiveTransactions - возвращает список активных транзакций.
func GetActiveTransactions() []TransactionInfo {
    if globalTxManager == nil {
        return []TransactionInfo{}
    }

    transactions := make([]TransactionInfo, 0)

    globalTxManager.activeTransactions.Range(func(key, value interface{}) bool {
        tx := value.(*Transaction)
        status := "active"
        state := TransactionState(tx.State.Load())
        switch state {
        case TransactionCommitted:
            status = "committed"
        case TransactionAborted:
            status = "aborted"
        }

        tx.mu.RLock()
        opCount := len(tx.Operations)
        operations := make([]OperationInfo, 0, opCount)
        for _, op := range tx.Operations {
            operations = append(operations, OperationInfo{
                Type:       op.Type,
                Database:   op.Database,
                Collection: op.Collection,
                DocumentID: op.DocumentID,
            })
        }
        savepoints := tx.GetSavepoints()
        tx.mu.RUnlock()

        info := TransactionInfo{
            ID:             fmt.Sprintf("%d", tx.ID),
            Status:         status,
            StartTime:      tx.StartTime,
            OperationCount: opCount,
            Operations:     operations,
            Savepoints:     savepoints,
        }
        if tx.IsDistributed {
            info.Status = "distributed_" + status
            info.Nodes = tx.Nodes
        }
        transactions = append(transactions, info)
        return true
    })

    return transactions
}

// GetTransactionByID - возвращает транзакцию по ID.
func GetTransactionByID(id string) (*Transaction, error) {
    if globalTxManager == nil {
        return nil, fmt.Errorf("transaction manager not initialized")
    }

    var txID TransactionID
    fmt.Sscanf(id, "%d", &txID)

    if val, ok := globalTxManager.activeTransactions.Load(txID); ok {
        return val.(*Transaction), nil
    }

    return nil, fmt.Errorf("transaction not found")
}

// AddToTransaction - добавляет операцию в текущую транзакцию.
func AddToTransaction(coll *Collection, opType string, doc *Document) error {
    txVal := currentTx.Load()
    if txVal == nil {
        return fmt.Errorf("no active transaction")
    }

    tx := txVal.(*Transaction)
    if TransactionState(tx.State.Load()) != TransactionActive {
        return fmt.Errorf("transaction is not active")
    }

    op := Operation{
        Type:       opType,
        Database:   coll.dbName,
        Collection: coll.name,
        DocumentID: doc.ID,
        Data:       doc.GetFields(),
        Version:    doc.Version,
    }

    tx.mu.Lock()
    tx.Operations = append(tx.Operations, op)
    tx.mu.Unlock()

    LogTransactionAudit(tx.ID, "ADD_OPERATION", TransactionActive, map[string]interface{}{
        "operation": opType,
        "document":  doc.ID,
    })

    return nil
}

// FindInTransaction - находит документ в контексте транзакции.
func FindInTransaction(coll *Collection, id string) (*Document, error) {
    txVal := currentTx.Load()
    if txVal == nil {
        return coll.Find(id)
    }

    tx := txVal.(*Transaction)

    tx.mu.RLock()
    defer tx.mu.RUnlock()

    for i := len(tx.Operations) - 1; i >= 0; i-- {
        op := tx.Operations[i]
        if op.DocumentID == id {
            if op.Type == "delete" {
                return nil, fmt.Errorf("document deleted in transaction")
            }
            if op.Type == "insert" || op.Type == "update" {
                doc := NewDocumentWithID(op.DocumentID)
                for k, v := range op.Data {
                    doc.SetField(k, v)
                }
                doc.Version = op.Version
                return doc, nil
            }
        }
    }

    if globalTxManager != nil {
        if versionDoc := globalTxManager.GetDocumentVersion(id, tx.StartTime); versionDoc != nil {
            return versionDoc, nil
        }
    }

    return coll.Find(id)
}

// MVCCSnapshot - создаёт снапшот MVCC.
func MVCCSnapshot() uint64 {
    return uint64(time.Now().UnixNano())
}

// CreateDocumentVersion - создаёт версию документа.
func CreateDocumentVersion(doc *Document, txID TransactionID) *DocumentVersion {
    return &DocumentVersion{
        Document:  doc.Clone(),
        Timestamp: time.Now().UnixMilli(),
        TxID:      txID,
    }
}

// BeginTransactionOnCollection - начинает транзакцию на коллекции.
func BeginTransactionOnCollection(coll *Collection) error {
    if globalTxManager == nil {
        if err := InitTransactionManager("futriis.wal"); err != nil {
            return err
        }
    }

    tx := BeginTransaction()
    if tx == nil {
        return fmt.Errorf("failed to create transaction")
    }

    if globalTxManager.logger != nil {
        globalTxManager.logger.Debug(fmt.Sprintf("Transaction %d started on collection %s.%s", tx.ID, coll.dbName, coll.name))
    }

    return nil
}

// CheckTransactionTimeout - проверяет таймаут транзакции.
func CheckTransactionTimeout(txID TransactionID) error {
    if globalTxManager == nil {
        return fmt.Errorf("transaction manager not initialized")
    }

    val, ok := globalTxManager.activeTransactions.Load(txID)
    if !ok {
        return fmt.Errorf("transaction not found: %d", txID)
    }

    tx := val.(*Transaction)
    if TransactionState(tx.State.Load()) != TransactionActive {
        return nil
    }

    if tx.timeout > 0 && time.Since(time.UnixMilli(tx.StartTime)) > tx.timeout {
        tx.State.Store(int32(TransactionAborted))
        globalTxManager.activeTransactions.Delete(tx.ID)
        globalTxManager.stats.TotalTimedOut.Add(1)
        globalTxManager.stats.ActiveCount.Add(^uint64(0))

        if tx.timeoutTimer != nil {
            tx.timeoutTimer.Stop()
        }

        LogTransactionAudit(txID, "TIMEOUT_CHECK", TransactionAborted, map[string]interface{}{
            "timeout_ms": tx.timeout.Milliseconds(),
            "elapsed_ms": time.Since(time.UnixMilli(tx.StartTime)).Milliseconds(),
        })

        return fmt.Errorf("transaction %d timed out", txID)
    }

    return nil
}
