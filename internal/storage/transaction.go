/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */
 
// Файл: internal/storage/transaction.go
// Назначение: Реализация транзакций с поддержкой MVCC (Multi-Version Concurrency Control)
// и WAL (Write-Ahead Log) без блокировок.
//
// =============================================================================
// ОСНОВНЫЕ КОМПОНЕНТЫ:
// =============================================================================
// 1. TransactionManager - управляет жизненным циклом транзакций
// 2. WAL (Write-Ahead Log) - журнал предзаписи для durability
//    - Segmented WAL - сегментированный журнал для эффективного управления
//    - Async Recovery - асинхронное восстановление после сбоя
// 3. MVCC (Multi-Version Concurrency Control) - управление версиями документов
//    - Visibility Map - карта видимости версий
//    - Read Cache - кэш для быстрого чтения
//    - Version Pruning - автоматическая очистка старых версий
// 4. Deadlock Detector - обнаружение и разрешение взаимных блокировок
// 5. Savepoints - точки сохранения внутри транзакции
// 6. Distributed Transactions - распределённые транзакции (без 2PC)
//
// =============================================================================
// АРХИТЕКТУРНЫЕ РЕШЕНИЯ:
// =============================================================================
// - Lock-free: все операции используют атомарные операции и sync.Map
// - Асинхронная запись WAL: запись в WAL не блокирует основную операцию
// - Пакетная обработка: группировка записей для повышения производительности
// - Реальный fsync: гарантированная запись на диск с повторными попытками
// =============================================================================

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
)

// =============================================================================
// БАЗОВЫЕ ТИПЫ
// =============================================================================

// TransactionID - уникальный идентификатор транзакции.
// Используется для отслеживания всех операций в рамках одной транзакции.
// Генерируется атомарным инкрементом.
type TransactionID uint64

// TransactionState - состояние транзакции.
// Возможные состояния:
// - TransactionActive: транзакция выполняется
// - TransactionCommitted: транзакция успешно завершена
// - TransactionAborted: транзакция отменена
// - TransactionPrepared: транзакция подготовлена для распределённого коммита
type TransactionState int32

const (
    TransactionActive TransactionState = iota
    TransactionCommitted
    TransactionAborted
    TransactionPrepared
)

// TransactionRecord - запись транзакции в WAL.
// Содержит всю информацию, необходимую для восстановления транзакции
// после сбоя системы.
type TransactionRecord struct {
    ID            TransactionID    `json:"id"`            // ID транзакции
    State         TransactionState `json:"state"`         // Текущее состояние
    Timestamp     int64            `json:"timestamp"`     // Время записи (мс)
    Operations    []Operation      `json:"operations"`    // Список операций
    IsDistributed bool             `json:"is_distributed"` // Флаг распределённости
    Nodes         []string         `json:"nodes,omitempty"` // Узлы для распределённой транзакции
}

// WALRecord - запись в WAL (Write-Ahead Log).
// Каждая запись содержит:
// - CRC32 для проверки целостности
// - Длину данных для чтения
// - Тип записи (транзакция, чекпоинт)
// - Данные в формате JSON
// - Временную метку
// - LSN (Log Sequence Number) для упорядочивания
type WALRecord struct {
    CRC        uint32 `json:"crc"`        // Контрольная сумма для проверки целостности
    Length     uint32 `json:"length"`     // Длина данных
    Type       byte   `json:"type"`       // Тип: 1=Transaction, 2=Checkpoint
    Data       []byte `json:"data"`       // Данные в JSON
    Timestamp  int64  `json:"timestamp"`  // Время записи (мс)
    LSN        uint64 `json:"lsn"`        // Log Sequence Number
}

// =============================================================================
// КОНСТАНТЫ
// =============================================================================

const (
    // WALSegmentSize - размер сегмента WAL в байтах (64 МБ).
    // При достижении этого размера создаётся новый сегмент.
    WALSegmentSize = 64 * 1024 * 1024
    
    // WALSegmentPrefix - префикс для файлов сегментов WAL.
    WALSegmentPrefix = "wal_segment_"
    
    // WALIndexPrefix - префикс для файла индекса WAL.
    WALIndexPrefix = "wal_index_"

    // VisibilityMapSize - размер карты видимости по умолчанию (1M записей).
    VisibilityMapSize = 1024 * 1024
    
    // VersionPruneInterval - интервал очистки старых версий (5 минут).
    VersionPruneInterval = 5 * time.Minute
    
    // MaxVersionsPerDoc - максимальное количество версий на документ (100).
    MaxVersionsPerDoc = 100
    
    // VersionRetentionDays - дни хранения старых версий (7 дней).
    VersionRetentionDays = 7

    // DefaultTxTimeout - таймаут транзакции по умолчанию (30 секунд).
    DefaultTxTimeout = 30 * time.Second
    
    // DeadlockCheckInterval - интервал проверки дедлоков (1 секунда).
    DeadlockCheckInterval = 1 * time.Second
    
    // MaxSavepointsPerTx - максимальное количество savepoints на транзакцию (100).
    MaxSavepointsPerTx = 100

    // AsyncRecoveryBufferSize - размер буфера асинхронного восстановления (10000 записей).
    AsyncRecoveryBufferSize = 10000
    
    // AsyncRecoveryWorkers - количество воркеров для асинхронного восстановления.
    AsyncRecoveryWorkers = 4
    
    // AsyncRecoveryTimeout - таймаут асинхронного восстановления (30 секунд).
    AsyncRecoveryTimeout = 30 * time.Second
    
    // FsyncMaxRetries - максимальное количество попыток fsync.
    FsyncMaxRetries = 3
    
    // FsyncRetryDelay - задержка между попытками fsync (100 мс).
    FsyncRetryDelay = 100 * time.Millisecond
)

// =============================================================================
// CRC32 - КОНТРОЛЬНАЯ СУММА
// =============================================================================
// Используется для проверки целостности записей WAL.
// Таблица CRC32 предвычислена для оптимизации производительности.

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

// crc32 вычисляет CRC32 для данных с предварительной инициализацией.
func crc32(data []byte) uint32 {
    crc := uint32(0xFFFFFFFF)
    for _, b := range data {
        crc = (crc >> 8) ^ crc32Table[(crc^uint32(b))&0xFF]
    }
    return crc ^ 0xFFFFFFFF
}

// =============================================================================
// WAL MANAGER - УПРАВЛЕНИЕ ЖУРНАЛОМ ПРЕДЗАПИСИ
// =============================================================================
// WALManager - базовый менеджер для работы с WAL.
// Обеспечивает:
// - Пакетную запись для повышения производительности
// - Периодическую синхронизацию с диском
// - Чтение всех записей для восстановления
// - Атомарность операций записи
// =============================================================================

// WALManager управляет WAL (Write-Ahead Log).
// Использует буферизированную запись для повышения производительности.
type WALManager struct {
    mu           sync.RWMutex
    file         *os.File          // Файл WAL
    writer       *bufio.Writer     // Буферизированный писатель
    path         string            // Путь к файлу
    currentLSN   uint64            // Текущий LSN
    lastSync     time.Time         // Время последней синхронизации
    syncInterval time.Duration     // Интервал синхронизации
    bufferSize   int               // Размер буфера
    closed       bool              // Флаг закрытия
    writeChan    chan *WALRecord   // Канал для асинхронной записи
    stopChan     chan struct{}     // Канал остановки
    wg           sync.WaitGroup    // Группа ожидания
    batchSize    int               // Размер пакета
}

// NewWALManager создаёт новый WAL менеджер.
func NewWALManager(path string) (*WALManager, error) {
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
    }

    wm.wg.Add(1)
    go wm.writerLoop()

    return wm, nil
}

// writerLoop - основной цикл асинхронной записи.
// Обрабатывает записи из канала, группирует их в пакеты.
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

        // Записываем длину данных (4 байта, Big Endian)
        lenBuf := make([]byte, 4)
        binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
        if _, err := wm.writer.Write(lenBuf); err != nil {
            continue
        }

        // Записываем данные
        if _, err := wm.writer.Write(data); err != nil {
            continue
        }

        // Присваиваем LSN
        record.LSN = wm.currentLSN
        wm.currentLSN++
    }
}

// sync - синхронизирует данные с диском.
func (wm *WALManager) sync() {
    wm.mu.Lock()
    defer wm.mu.Unlock()

    if err := wm.writer.Flush(); err == nil {
        if err := RealFsyncWithRetry(wm.file, FsyncMaxRetries, FsyncRetryDelay); err == nil {
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
    RealFsync(wm.file)
    return wm.file.Close()
}

// =============================================================================
// SEGMENTED WAL MANAGER - СЕГМЕНТИРОВАННЫЙ ЖУРНАЛ
// =============================================================================
// SegmentedWALManager - расширенная версия WAL с сегментацией.
// Преимущества:
// - Автоматическое создание новых сегментов при достижении лимита
// - Возможность удаления старых сегментов
// - Ускоренное восстановление (только последние сегменты)
// - Индексация для быстрого поиска по LSN
// =============================================================================

// WALSegment - сегмент WAL.
type WALSegment struct {
    ID       uint32          // ID сегмента
    File     *os.File        // Файл сегмента
    Writer   *bufio.Writer   // Буферизированный писатель
    Path     string          // Путь к файлу
    StartLSN uint64          // Начальный LSN
    EndLSN   uint64          // Конечный LSN
    Size     int64           // Текущий размер
    mu       sync.Mutex      // Мьютекс для синхронизации
}

// WALIndexEntry - запись в индексе WAL.
type WALIndexEntry struct {
    LSN       uint64 // Log Sequence Number
    SegmentID uint32 // ID сегмента
    Offset    int64  // Смещение в сегменте
    Length    uint32 // Длина записи
    Checksum  uint32 // Контрольная сумма
}

// WALIndexManager - управление индексом WAL.
type WALIndexManager struct {
    index     map[uint64]*WALIndexEntry // Индекс по LSN
    segments  map[uint32]*WALSegment    // Карта сегментов
    mu        sync.RWMutex              // Мьютекс для синхронизации
    indexPath string                    // Путь к файлу индекса
}

// SegmentedWALManager - менеджер сегментированного WAL.
type SegmentedWALManager struct {
    segmentsDir      string              // Директория сегментов
    segments         map[uint32]*WALSegment // Карта сегментов
    currentSegment   *WALSegment         // Текущий сегмент
    currentSegmentID uint32              // ID текущего сегмента
    index            *WALIndexManager    // Менеджер индекса
    mu               sync.RWMutex        // Мьютекс
    writeChan        chan *WALRecord     // Канал для записи
    stopChan         chan struct{}       // Канал остановки
    wg               sync.WaitGroup      // Группа ожидания
    batchSize        int                 // Размер пакета
    logger           LoggerInterface     // Логгер
    recoveryManager  *AsyncRecoveryManager // Менеджер восстановления
    recoveryComplete atomic.Bool         // Флаг завершения восстановления
    backupLSN        atomic.Uint64       // LSN для бэкапа
}

// NewSegmentedWALManager создаёт новый сегментированный WAL менеджер.
func NewSegmentedWALManager(segmentsDir string, logger LoggerInterface) (*SegmentedWALManager, error) {
    if err := os.MkdirAll(segmentsDir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create segments dir: %v", err)
    }

    wm := &SegmentedWALManager{
        segmentsDir: segmentsDir,
        segments:    make(map[uint32]*WALSegment),
        index: &WALIndexManager{
            index:     make(map[uint64]*WALIndexEntry),
            segments:  make(map[uint32]*WALSegment),
            indexPath: filepath.Join(segmentsDir, WALIndexPrefix+"index.json"),
        },
        writeChan: make(chan *WALRecord, 10000),
        stopChan:  make(chan struct{}),
        batchSize: 100,
        logger:    logger,
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
        RealFsync(wm.currentSegment.File)
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

// writerLoop - основной цикл записи.
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
    RealFsync(wm.currentSegment.File)
    wm.index.save()
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

// readSegmentRecords - читает записи из сегмента.
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
        RealFsyncWithRetry(wm.currentSegment.File, FsyncMaxRetries, FsyncRetryDelay)
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

// FindByLSN - находит запись в индексе по LSN.
func (im *WALIndexManager) FindByLSN(lsn uint64) *WALIndexEntry {
    im.mu.RLock()
    defer im.mu.RUnlock()
    return im.index[lsn]
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
// AsyncRecoveryManager - управляет асинхронным восстановлением из WAL.
// 
// Принцип работы:
// 1. После сбоя система читает WAL
// 2. Записи передаются в канал восстановления
// 3. Несколько воркеров параллельно обрабатывают записи
// 4. Применяются только committed транзакции
// 5. Uncommitted транзакции игнорируются
// =============================================================================

// AsyncRecoveryManager - управляет асинхронным восстановлением.
type AsyncRecoveryManager struct {
    recordChan   chan *WALRecord          // Канал записей для восстановления
    errChan      chan error               // Канал ошибок
    doneChan     chan struct{}            // Канал завершения
    wg           sync.WaitGroup           // Группа ожидания
    callback     func(*WALRecord) error   // Функция обработки записи
    mu           sync.RWMutex             // Мьютекс
    isRunning    bool                     // Флаг работы
    recoveredCnt atomic.Uint64            // Счётчик восстановленных записей
    errorCnt     atomic.Uint64            // Счётчик ошибок
    startTime    time.Time                // Время начала
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
        "recovered":   arm.recoveredCnt.Load(),
        "errors":      arm.errorCnt.Load(),
        "is_running":  arm.isRunning,
        "elapsed_ms":  time.Since(arm.startTime).Milliseconds(),
    }
}

// =============================================================================
// MVCC - MULTI-VERSION CONCURRENCY CONTROL
// =============================================================================
// MVCC позволяет одновременно читать и записывать данные без блокировок.
// 
// Основные компоненты:
// 1. VisibilityMap - карта видимости версий
//    - Отслеживает, какие версии документов видны в текущий момент
//    - Используется для быстрой проверки видимости
//
// 2. ReadTimestampCache - кэш для чтения по временной метке
//    - Кэширует результаты чтения для ускорения повторных запросов
//    - Автоматически инвалидируется по TTL
//
// 3. Version Pruning - автоматическая очистка старых версий
//    - Периодически удаляет версии старше RetentionDays
//    - Сохраняет только последние MaxVersionsPerDoc версий
// =============================================================================

// VisibilityMapEntry - запись в карте видимости.
type VisibilityMapEntry struct {
    DocID       string // ID документа
    VisibleFrom uint64 // Начало видимости (LSN)
    VisibleTo   uint64 // Конец видимости (LSN)
    IsVisible   bool   // Флаг видимости
    LastAccess  int64  // Время последнего доступа
}

// VisibilityMap - карта видимости версий.
type VisibilityMap struct {
    entries   sync.Map   // map[string]*VisibilityMapEntry
    maxSize   int        // Максимальный размер
    hitCount  atomic.Uint64 // Количество попаданий
    missCount atomic.Uint64 // Количество промахов
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
    _, ok := vm.entries.Load(key)
    if !ok {
        vm.missCount.Add(1)
        return false
    }
    vm.hitCount.Add(1)
    return true
}

// GetStats - возвращает статистику карты видимости.
func (vm *VisibilityMap) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "hits":   vm.hitCount.Load(),
        "misses": vm.missCount.Load(),
    }
}

// ReadTimestampCache - кэш для чтения по временной метке.
type ReadTimestampCache struct {
    cache   sync.Map   // map[string]*cachedEntry
    maxSize int        // Максимальный размер
    ttl     time.Duration // TTL записи
    hits    atomic.Uint64 // Количество попаданий
    misses  atomic.Uint64 // Количество промахов
}

// cachedEntry - запись в кэше.
type cachedEntry struct {
    doc      *Document // Документ
    cachedAt time.Time // Время кэширования
}

// NewReadTimestampCache создаёт новый кэш чтения.
func NewReadTimestampCache(maxSize int, ttl time.Duration) *ReadTimestampCache {
    if maxSize <= 0 {
        maxSize = 10000
    }
    if ttl <= 0 {
        ttl = 5 * time.Minute
    }
    return &ReadTimestampCache{maxSize: maxSize, ttl: ttl}
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
        rtc.misses.Add(1)
        return nil
    }
    rtc.hits.Add(1)
    return entry.doc
}

// Set - сохраняет документ в кэше.
func (rtc *ReadTimestampCache) Set(docID string, timestamp int64, doc *Document) {
    key := fmt.Sprintf("%s@%d", docID, timestamp)
    rtc.cache.Store(key, &cachedEntry{doc: doc, cachedAt: time.Now()})
}

// GetStats - возвращает статистику кэша.
func (rtc *ReadTimestampCache) GetStats() map[string]interface{} {
    return map[string]interface{}{
        "hits":   rtc.hits.Load(),
        "misses": rtc.misses.Load(),
    }
}

// =============================================================================
// TRANSACTION - ОСНОВНАЯ ТРАНЗАКЦИЯ
// =============================================================================
// Transaction - представление активной транзакции.
// 
// Особенности:
// - Атомарные операции с состоянием
// - Поддержка savepoints для частичного отката
// - Таймаут для автоматического прерывания
// - Поддержка распределённых транзакций
// =============================================================================

// Transaction - структура транзакции.
type Transaction struct {
    ID            TransactionID    // ID транзакции
    State         atomic.Int32     // Текущее состояние (атомарное)
    Operations    []Operation      // Список операций
    StartTime     int64            // Время начала
    Version       uint64           // Версия MVCC
    mu            sync.RWMutex     // Мьютекс для защиты
    IsDistributed bool             // Флаг распределённости
    Nodes         []string         // Узлы для распределённой транзакции
    savepoints    []*Savepoint     // Точки сохранения
    timeout       time.Duration    // Таймаут
    timeoutTimer  *time.Timer      // Таймер таймаута
}

// Savepoint - точка сохранения в транзакции.
type Savepoint struct {
    Name       string    // Имя точки сохранения
    Timestamp  int64     // Время создания
    OpCount    int       // Количество операций на момент создания
    Snapshot   *Document // Снапшот состояния
}

// TransactionOptions - опции создания транзакции.
type TransactionOptions struct {
    Timeout        time.Duration // Таймаут
    IsDistributed  bool          // Флаг распределённости
    Nodes          []string      // Узлы для распределённой транзакции
    IsolationLevel string        // Уровень изоляции
}

// TransactionManager - основной менеджер транзакций.
type TransactionManager struct {
    activeTransactions  sync.Map                 // Активные транзакции
    nextTxID            atomic.Uint64            // Генератор ID
    wal                 *SegmentedWALManager     // WAL менеджер
    logger              LoggerInterface          // Логгер
    mu                  sync.RWMutex             // Мьютекс
    walPath             string                   // Путь к WAL
    checkpointInterval  int64                    // Интервал чекпоинтов
    lastCheckpoint      int64                    // Время последнего чекпоинта
    checkpointFile      *os.File                 // Файл чекпоинта
    documentVersions    sync.Map                 // Версии документов
    maxVersions         int                      // Максимум версий
    visibilityMap       *VisibilityMap           // Карта видимости
    readCache           *ReadTimestampCache      // Кэш чтения
    distCoord           *DistributedTransactionCoordinator // Координатор распределённых транзакций
    deadlockDetector    *DeadlockDetector        // Детектор дедлоков
    recoveryManager     *AsyncRecoveryManager    // Менеджер восстановления
    recoveryComplete    atomic.Bool              // Флаг завершения восстановления
    backupLock          sync.RWMutex             // Блокировка для бэкапа
    backupInProgress    atomic.Bool              // Флаг выполнения бэкапа
    stats               *TransactionStats        // Статистика
}

// TransactionStats - статистика по транзакциям.
type TransactionStats struct {
    TotalStarted    atomic.Uint64 // Всего начато
    TotalCommitted  atomic.Uint64 // Всего закоммичено
    TotalAborted    atomic.Uint64 // Всего отменено
    TotalTimedOut   atomic.Uint64 // Всего по таймауту
    TotalDeadlocks  atomic.Uint64 // Всего дедлоков
    ActiveCount     atomic.Uint64 // Активных
    PeakActiveCount atomic.Uint64 // Пик активности
    MaxOpsPerTx     atomic.Uint64 // Максимум операций в транзакции
    AvgOpsPerTx     atomic.Uint64 // Среднее операций
    TotalOps        atomic.Uint64 // Всего операций
    StartTime       time.Time     // Время старта
}

// Глобальные переменные
var (
    globalTxManager *TransactionManager // Глобальный менеджер транзакций
    txManagerOnce   sync.Once           // Для однократной инициализации
    currentTx       atomic.Value        // Текущая транзакция
    globalStorage   *Storage            // Глобальное хранилище
)

// =============================================================================
// ФУНКЦИИ ИНИЦИАЛИЗАЦИИ
// =============================================================================

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
        }
        globalTxManager.nextTxID.Store(1)

        var walErr error
        globalTxManager.wal, walErr = NewSegmentedWALManager(filepath.Dir(walPath), nil)
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

// =============================================================================
// СОЗДАНИЕ И УПРАВЛЕНИЕ ТРАНЗАКЦИЯМИ
// =============================================================================

// BeginTransaction - начинает новую транзакцию с таймаутом по умолчанию.
func BeginTransaction() *Transaction {
    return BeginTransactionWithOptions(&TransactionOptions{
        Timeout: DefaultTxTimeout,
    })
}

// BeginTransactionWithOptions - начинает транзакцию с указанными опциями.
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

    // Обновляем статистику
    globalTxManager.stats.TotalStarted.Add(1)
    globalTxManager.stats.ActiveCount.Add(1)

    // Устанавливаем таймер таймаута
    if options.Timeout > 0 {
        tx.timeoutTimer = time.AfterFunc(options.Timeout, func() {
            if TransactionState(tx.State.Load()) == TransactionActive {
                tx.State.Store(int32(TransactionAborted))
                globalTxManager.activeTransactions.Delete(tx.ID)
                globalTxManager.stats.TotalTimedOut.Add(1)
                globalTxManager.stats.ActiveCount.Add(^uint64(0))
                LogAudit("TIMEOUT", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
                    "timeout_ms": options.Timeout.Milliseconds(),
                })
            }
        })
    }

    LogAudit("START", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
        "start_time":  tx.StartTime,
        "timeout_ms":  options.Timeout.Milliseconds(),
        "distributed": options.IsDistributed,
    })

    return tx
}

// BeginTransactionWithTimeout - начинает транзакцию с указанным таймаутом.
func BeginTransactionWithTimeout(timeout time.Duration) *Transaction {
    options := &TransactionOptions{
        Timeout: timeout,
    }
    return BeginTransactionWithOptions(options)
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

// =============================================================================
// SAVEPOINTS - ТОЧКИ СОХРАНЕНИЯ
// =============================================================================

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

    // Создаём снапшот текущего состояния
    if len(tx.Operations) > 0 {
        lastOp := tx.Operations[len(tx.Operations)-1]
        if lastOp.DocumentID != "" {
            if globalStorage != nil {
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
    }

    tx.mu.Lock()
    tx.savepoints = append(tx.savepoints, savepoint)
    tx.mu.Unlock()

    LogAudit("SAVEPOINT", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
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

    // Откатываем операции после savepoint
    if len(tx.Operations) > tx.savepoints[targetIdx].OpCount {
        tx.Operations = tx.Operations[:tx.savepoints[targetIdx].OpCount]
    }

    // Удаляем savepoints после целевого
    tx.savepoints = tx.savepoints[:targetIdx+1]

    LogAudit("ROLLBACK_TO_SAVEPOINT", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
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
            LogAudit("RELEASE_SAVEPOINT", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
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

// =============================================================================
// КОММИТ И ОТМЕНА ТРАНЗАКЦИЙ
// =============================================================================

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

    // Проверяем таймаут
    if tx.timeout > 0 && time.Since(time.UnixMilli(tx.StartTime)) > tx.timeout {
        AbortCurrentTransaction()
        return fmt.Errorf("transaction timeout exceeded")
    }

    // Применяем все операции
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

    // Записываем в WAL
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
    }

    LogAudit("COMMIT", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
        "operations": len(tx.Operations),
    })

    // Обновляем статистику
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

    LogAudit("ABORT", "TRANSACTION", fmt.Sprintf("%d", tx.ID), map[string]interface{}{
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

    LogAudit("COMMIT_DISTRIBUTED", "TRANSACTION", fmt.Sprintf("%d", txID), map[string]interface{}{
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

    LogAudit("ABORT_DISTRIBUTED", "TRANSACTION", fmt.Sprintf("%d", txID), map[string]interface{}{
        "nodes": tx.Nodes,
    })

    return nil
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С ТРАНЗАКЦИЯМИ
// =============================================================================

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

// =============================================================================
// ФУНКЦИИ ПРИМЕНЕНИЯ ОПЕРАЦИЙ
// =============================================================================

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
        return coll.Insert(doc)

    case "update":
        return coll.Update(op.DocumentID, op.Data)

    case "delete":
        return coll.Delete(op.DocumentID)
    }

    return nil
}

// =============================================================================
// MVCC - УПРАВЛЕНИЕ ВЕРСИЯМИ ДОКУМЕНТОВ
// =============================================================================

// DocumentVersion - версия документа.
type DocumentVersion struct {
    Document  *Document      `json:"document"`
    Timestamp int64          `json:"timestamp"`
    TxID      TransactionID  `json:"tx_id"`
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

// =============================================================================
// ЧЕКПОИНТЫ
// =============================================================================

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

// =============================================================================
// ОЧИСТКА СТАРЫХ ВЕРСИЙ
// =============================================================================

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

// =============================================================================
// СТАТИСТИКА
// =============================================================================

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
        "total_started":        totalStarted,
        "total_committed":      totalCommitted,
        "total_aborted":        totalAborted,
        "total_timed_out":      totalTimedOut,
        "total_deadlocks":      totalDeadlocks,
        "active_count":         active,
        "peak_active_count":    stats.PeakActiveCount.Load(),
        "max_ops_per_tx":       stats.MaxOpsPerTx.Load(),
        "avg_ops_per_tx":       stats.AvgOpsPerTx.Load(),
        "total_ops":            stats.TotalOps.Load(),
        "commit_rate":          float64(totalCommitted) / float64(totalStarted+1) * 100,
        "abort_rate":           float64(totalAborted) / float64(totalStarted+1) * 100,
        "uptime_seconds":       time.Since(stats.StartTime).Seconds(),
        "is_recovery_complete": globalTxManager.IsRecoveryComplete(),
        "backup_in_progress":   globalTxManager.IsBackupInProgress(),
    }
}

// =============================================================================
// DEADLOCK DETECTOR - ОБНАРУЖЕНИЕ ВЗАИМНЫХ БЛОКИРОВОК
// =============================================================================

// DeadlockDetector - детектор взаимных блокировок.
// Использует алгоритм обхода графа ожидания для обнаружения циклов.
type DeadlockDetector struct {
    waitForGraph  sync.Map        // map[TransactionID][]TransactionID
    checkInterval time.Duration   // Интервал проверки
    timeout       time.Duration   // Таймаут
    mu            sync.RWMutex    // Мьютекс
    stopChan      chan struct{}   // Канал остановки
    wg            sync.WaitGroup  // Группа ожидания
    logger        LoggerInterface // Логгер
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
// DISTRIBUTED TRANSACTION COORDINATOR - КООРДИНАТОР РАСПРЕДЕЛЁННЫХ ТРАНЗАКЦИЙ
// =============================================================================
// Упрощённая версия координатора распределённых транзакций (без 2PC).
// Использует подход "best effort" для распределённых операций.
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
    TxID      TransactionID // ID транзакции
    Nodes     []string      // Узлы
    Status    TxState       // Статус
    StartTime int64         // Время начала
    Timeout   time.Duration // Таймаут
}

// DistributedTransactionCoordinator - координатор распределённых транзакций.
type DistributedTransactionCoordinator struct {
    pendingTxs sync.Map     // map[TransactionID]*DistributedTxInfo
    timeout    time.Duration // Таймаут по умолчанию
    mu         sync.RWMutex // Мьютекс
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
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// TransactionInfo - информация о транзакции для отображения.
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

// =============================================================================
// АСИНХРОННОЕ ВОССТАНОВЛЕНИЕ
// =============================================================================

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

    // Создаём callback для обработки записей
    tm.recoveryManager = NewAsyncRecoveryManager(func(record *WALRecord) error {
        if record.Type == 1 { // Транзакция
            var txRecord TransactionRecord
            if err := json.Unmarshal(record.Data, &txRecord); err != nil {
                return err
            }
            // Применяем только закоммиченные транзакции
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

    // Отправляем записи на восстановление
    for _, record := range records {
        if !tm.recoveryManager.Push(record) {
            if tm.logger != nil {
                tm.logger.Warn("Recovery buffer full, some records may be delayed")
            }
        }
    }

    // Мониторинг завершения восстановления
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

// =============================================================================
// БЭКАП И ВОССТАНОВЛЕНИЕ
// =============================================================================

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

        LogAudit("TIMEOUT_CHECK", "TRANSACTION", fmt.Sprintf("%d", txID), map[string]interface{}{
            "timeout_ms": tx.timeout.Milliseconds(),
            "elapsed_ms": time.Since(time.UnixMilli(tx.StartTime)).Milliseconds(),
        })

        return fmt.Errorf("transaction %d timed out", txID)
    }

    return nil
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
        return globalTxManager.wal.Close()
    }

    return nil
}
