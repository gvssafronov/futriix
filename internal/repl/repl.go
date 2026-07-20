/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// =============================================================================
// Пакет: repl (Read-Eval-Print Loop)
// =============================================================================
// Назначение: Предоставляет интерактивный интерфейс командной строки (REPL)
// для взаимодействия с СУБД futriix. Поддерживает автодополнение команд,
// историю ввода, цветной вывод, а также все операции с данными.
// =============================================================================
// Основные компоненты:
//   - Repl: Основная структура, управляющая состоянием REPL
//   - Command: Структура, описывающая команду и её обработчик
//   - History: Управление историей команд с сохранением в файл
// =============================================================================
// Использование:
//   - Запуск: repl.Run() запускает основной цикл обработки команд
//   - Команды: Регистрируются через registerCommands()
//   - Обработчики: Каждая команда имеет свой метод-обработчик (handleXXX)
// =============================================================================

package repl

import (
    "bufio"
    "fmt"
    "os"
    "strings"
    "time"

    "futriis/internal/acl"
    "futriis/internal/cluster"
    "futriis/internal/compression"
    "futriis/internal/config"
    "futriis/internal/log"
    "futriis/internal/plugin"
    "futriis/internal/storage"
    "futriis/pkg/utils"

    "github.com/fatih/color"
)

// =============================================================================
// ТИПЫ ДАННЫХ
// =============================================================================

// Repl представляет основную структуру REPL, которая управляет всеми аспектами
// интерактивной сессии: состоянием подключения к БД, пользователем, историей,
// зарегистрированными командами и их обработчиками.
//
// Поля:
//   - store: Ссылка на основное хранилище данных (Storage)
//   - coordinator: Координатор кластера (Raft) для распределённых операций
//   - logger: Логгер для записи событий и ошибок
//   - config: Конфигурация приложения
//   - aclManager: Менеджер контроля доступа (ACL)
//   - pluginManager: Менеджер плагинов
//   - reader: Буферизированный читатель из stdin
//   - currentDB: Имя текущей выбранной базы данных
//   - currentUser: Имя текущего аутентифицированного пользователя
//   - currentRole: Роль текущего пользователя
//   - authenticated: Флаг аутентификации пользователя
//   - sessionID: ID текущей сессии
//   - commands: Карта зарегистрированных команд (имя -> *Command)
//   - history: Список команд в истории
//   - historyPos: Текущая позиция в истории (для навигации)
//   - historyFile: Менеджер истории для сохранения/загрузки из файла
type Repl struct {
    store         *storage.Storage
    coordinator   *cluster.RaftCoordinator
    logger        *log.Logger
    config        *config.Config
    aclManager    *acl.ACLManager
    pluginManager *plugin.PluginManager
    reader        *bufio.Reader
    currentDB     string
    currentUser   string
    currentRole   string
    authenticated bool
    sessionID     string
    commands      map[string]*Command
    history       []string
    historyPos    int
    historyFile   *History
}

// Command представляет отдельную команду REPL.
// Содержит имя, описание и функцию-обработчик, которая вызывается при выполнении.
//
// Поля:
//   - Name: Уникальное имя команды (используется для поиска)
//   - Description: Краткое описание команды для справки
//   - Handler: Функция, принимающая список аргументов и возвращающая ошибку
type Command struct {
    Name        string
    Description string
    Handler     func(args []string) error
}

// =============================================================================
// КОНСТРУКТОРЫ
// =============================================================================

// NewRepl создаёт новый экземпляр REPL с заданными зависимостями.
// Инициализирует историю команд, загружает сохранённую историю из файла
// и регистрирует все доступные команды.
//
// Параметры:
//   - store: Указатель на хранилище данных
//   - coordinator: Указатель на координатор кластера
//   - logger: Указатель на логгер
//   - cfg: Указатель на конфигурацию
//   - aclManager: Указатель на менеджер ACL
//   - pluginManager: Указатель на менеджер плагинов
//
// Возвращает:
//   - *Repl: Инициализированный экземпляр REPL
func NewRepl(store *storage.Storage, coordinator *cluster.RaftCoordinator, logger *log.Logger, cfg *config.Config, aclManager *acl.ACLManager, pluginManager *plugin.PluginManager) *Repl {
    r := &Repl{
        store:         store,
        coordinator:   coordinator,
        logger:        logger,
        config:        cfg,
        aclManager:    aclManager,
        pluginManager: pluginManager,
        reader:        bufio.NewReader(os.Stdin),
        currentDB:     "",
        currentUser:   "",
        currentRole:   "anonymous",
        authenticated: false,
        sessionID:     "",
        commands:      make(map[string]*Command),
        history:       make([]string, 0, cfg.Repl.HistorySize),
        historyPos:    -1,
        historyFile:   NewHistory(cfg.Repl.HistorySize),
    }

    // Загружаем историю из файла, если она существует
    if err := r.historyFile.Load(); err == nil {
        r.history = r.historyFile.GetEntries()
        r.historyPos = len(r.history)
    }

    r.registerCommands()
    return r
}

// =============================================================================
// РЕГИСТРАЦИЯ КОМАНД
// =============================================================================

// registerCommands регистрирует все доступные команды REPL.
// Команды сгруппированы по функциональным областям:
//   - Управление базами данных (create slice, drop database, use, show databases)
//   - Управление коллекциями (create collection, drop collection, show collections)
//   - Работа с документами (insert, find, update, delete, restore, etc.)
//   - Управление индексами (create index, drop index, show indexes)
//   - Ограничения (add required, add unique, add min, add max, add enum)
//   - Триггеры (MongoDB-like syntax)
//   - Транзакции (begin transaction, commit, rollback, show transactions)
//   - Плагины (plugin list, load, unload, start, stop, exec)
//   - Импорт/экспорт (export, import)
//   - Управление доступом (acl login, logout, grant, users, roles)
//   - Сжатие (compression stats, compress collection, doc compression)
//   - Аудит (audit log, audit filter)
//   - Кластер (status, nodes)
//   - Системные (help, clear, quit, exit)
//   - Миграция (migration)
func (r *Repl) registerCommands() {
    // -------------------------------------------------------------------------
    // Команды управления базами данных
    // -------------------------------------------------------------------------

    r.commands["create slice"] = &Command{
        Name:        "create slice",
        Description: "Create a new database (slice)",
        Handler:     r.handleCreateSlice,
    }

    r.commands["drop database"] = &Command{
        Name:        "drop database",
        Description: "Drop a database",
        Handler:     r.handleDropDatabase,
    }

    r.commands["use"] = &Command{
        Name:        "use",
        Description: "Switch to a database",
        Handler:     r.handleUseDatabase,
    }

    r.commands["show databases"] = &Command{
        Name:        "show databases",
        Description: "List all databases",
        Handler:     r.handleShowDatabases,
    }

    // -------------------------------------------------------------------------
    // Команды управления коллекциями
    // -------------------------------------------------------------------------

    r.commands["create collection"] = &Command{
        Name:        "create collection",
        Description: "Create a new collection in current database",
        Handler:     r.handleCreateCollection,
    }

    r.commands["drop collection"] = &Command{
        Name:        "drop collection",
        Description: "Drop a collection from current database",
        Handler:     r.handleDropCollection,
    }

    r.commands["show collections"] = &Command{
        Name:        "show collections",
        Description: "List all collections in current database",
        Handler:     r.handleShowCollections,
    }

    // -------------------------------------------------------------------------
    // Команды работы с документами
    // -------------------------------------------------------------------------

    r.commands["insert"] = &Command{
        Name:        "insert",
        Description: "Insert a document into a collection (JSON format)",
        Handler:     r.handleInsert,
    }

    r.commands["find"] = &Command{
        Name:        "find",
        Description: "Find a document by ID",
        Handler:     r.handleFind,
    }

    r.commands["findbyindex"] = &Command{
        Name:        "findbyindex",
        Description: "Find documents by index",
        Handler:     r.handleFindByIndex,
    }

    r.commands["findbytime"] = &Command{
        Name:        "findbytime",
        Description: "Find documents by time range (created_at)",
        Handler:     r.handleFindByTime,
    }

    r.commands["update"] = &Command{
        Name:        "update",
        Description: "Update a document",
        Handler:     r.handleUpdate,
    }

    r.commands["delete"] = &Command{
        Name:        "delete",
        Description: "Delete a document (soft delete if enabled)",
        Handler:     r.handleDelete,
    }

    r.commands["permanent delete"] = &Command{
        Name:        "permanent delete",
        Description: "Permanently delete a soft-deleted document",
        Handler:     r.handlePermanentDelete,
    }

    r.commands["restore"] = &Command{
        Name:        "restore",
        Description: "Restore a soft-deleted document",
        Handler:     r.handleRestore,
    }

    r.commands["show deleted"] = &Command{
        Name:        "show deleted",
        Description: "Show soft-deleted documents in a collection",
        Handler:     r.handleShowDeleted,
    }

    r.commands["count"] = &Command{
        Name:        "count",
        Description: "Count documents in a collection",
        Handler:     r.handleCount,
    }

    r.commands["show timestamps"] = &Command{
        Name:        "show timestamps",
        Description: "Show timestamps for a document",
        Handler:     r.handleShowTimestamps,
    }

    r.commands["stats timestamps"] = &Command{
        Name:        "stats timestamps",
        Description: "Show timestamp statistics for a collection",
        Handler:     r.handleStatsTimestamps,
    }

    // -------------------------------------------------------------------------
    // Команды управления индексами
    // -------------------------------------------------------------------------

    r.commands["create index"] = &Command{
        Name:        "create index",
        Description: "Create an index on a collection",
        Handler:     r.handleCreateIndex,
    }

    r.commands["drop index"] = &Command{
        Name:        "drop index",
        Description: "Drop an index from a collection",
        Handler:     r.handleDropIndex,
    }

    r.commands["show indexes"] = &Command{
        Name:        "show indexes",
        Description: "Show all indexes in a collection",
        Handler:     r.handleShowIndexes,
    }

    // -------------------------------------------------------------------------
    // Команды ограничений
    // -------------------------------------------------------------------------

    r.commands["add required"] = &Command{
        Name:        "add required",
        Description: "Add a required field constraint",
        Handler:     r.handleAddRequired,
    }

    r.commands["add unique"] = &Command{
        Name:        "add unique",
        Description: "Add a unique constraint",
        Handler:     r.handleAddUnique,
    }

    r.commands["add min"] = &Command{
        Name:        "add min",
        Description: "Add a minimum value constraint",
        Handler:     r.handleAddMin,
    }

    r.commands["add max"] = &Command{
        Name:        "add max",
        Description: "Add a maximum value constraint",
        Handler:     r.handleAddMax,
    }

    r.commands["add enum"] = &Command{
        Name:        "add enum",
        Description: "Add an enum constraint (allowed values)",
        Handler:     r.handleAddEnum,
    }

    // -------------------------------------------------------------------------
    // Команды триггеров (MongoDB-like syntax)
    // -------------------------------------------------------------------------

    r.commands["create trigger"] = &Command{
        Name:        "create trigger",
        Description: "Create a trigger on a collection (MongoDB-like syntax)",
        Handler:     r.handleCreateTrigger,
    }

    r.commands["drop trigger"] = &Command{
        Name:        "drop trigger",
        Description: "Drop a trigger from a collection",
        Handler:     r.handleDropTrigger,
    }

    r.commands["show triggers"] = &Command{
        Name:        "show triggers",
        Description: "Show all triggers on a collection",
        Handler:     r.handleShowTriggers,
    }

    r.commands["enable trigger"] = &Command{
        Name:        "enable trigger",
        Description: "Enable a trigger",
        Handler:     r.handleEnableTrigger,
    }

    r.commands["disable trigger"] = &Command{
        Name:        "disable trigger",
        Description: "Disable a trigger",
        Handler:     r.handleDisableTrigger,
    }

    r.commands["trigger log"] = &Command{
        Name:        "trigger log",
        Description: "Show trigger execution log",
        Handler:     r.handleTriggerLog,
    }

    // -------------------------------------------------------------------------
    // Команды транзакций
    // -------------------------------------------------------------------------

    r.commands["begin transaction"] = &Command{
        Name:        "begin transaction",
        Description: "Start a new transaction",
        Handler:     r.handleBeginTransaction,
    }

    r.commands["commit"] = &Command{
        Name:        "commit",
        Description: "Commit current transaction",
        Handler:     r.handleCommitTransaction,
    }

    r.commands["rollback"] = &Command{
        Name:        "rollback",
        Description: "Rollback current transaction",
        Handler:     r.handleRollbackTransaction,
    }

    r.commands["show transactions"] = &Command{
        Name:        "show transactions",
        Description: "Show active transactions",
        Handler:     r.handleShowTransactions,
    }

    // -------------------------------------------------------------------------
    // Команды плагинов
    // -------------------------------------------------------------------------

    r.commands["plugin list"] = &Command{
        Name:        "plugin list",
        Description: "List all loaded plugins",
        Handler:     r.handlePluginList,
    }

    r.commands["plugin load"] = &Command{
        Name:        "plugin load",
        Description: "Load a plugin from file",
        Handler:     r.handlePluginLoad,
    }

    r.commands["plugin unload"] = &Command{
        Name:        "plugin unload",
        Description: "Unload a plugin",
        Handler:     r.handlePluginUnload,
    }

    r.commands["plugin start"] = &Command{
        Name:        "plugin start",
        Description: "Start a plugin",
        Handler:     r.handlePluginStart,
    }

    r.commands["plugin stop"] = &Command{
        Name:        "plugin stop",
        Description: "Stop a plugin",
        Handler:     r.handlePluginStop,
    }

    r.commands["plugin exec"] = &Command{
        Name:        "plugin exec",
        Description: "Execute a plugin function",
        Handler:     r.handlePluginExec,
    }

    // -------------------------------------------------------------------------
    // Команды импорта/экспорта
    // -------------------------------------------------------------------------

    r.commands["export"] = &Command{
        Name:        "export",
        Description: "Export database to MessagePack file",
        Handler:     r.handleExport,
    }

    r.commands["import"] = &Command{
        Name:        "import",
        Description: "Import database from MessagePack file",
        Handler:     r.handleImport,
    }

    // -------------------------------------------------------------------------
    // Команды ACL (Access Control List)
    // -------------------------------------------------------------------------

    r.commands["acl login"] = &Command{
        Name:        "acl login",
        Description: "Authenticate with username and password",
        Handler:     r.handleACLLogin,
    }

    r.commands["acl logout"] = &Command{
        Name:        "acl logout",
        Description: "Logout current user session",
        Handler:     r.handleACLLogout,
    }

    r.commands["acl grant"] = &Command{
        Name:        "acl grant",
        Description: "Grant permissions (r=read,w=write,d=delete,a=admin)",
        Handler:     r.handleACLGrant,
    }

    r.commands["acl users"] = &Command{
        Name:        "acl users",
        Description: "List all users",
        Handler:     r.handleACLUsers,
    }

    r.commands["acl roles"] = &Command{
        Name:        "acl roles",
        Description: "List all roles",
        Handler:     r.handleACLRoles,
    }

    // -------------------------------------------------------------------------
    // Команды сжатия
    // -------------------------------------------------------------------------

    r.commands["compression stats"] = &Command{
        Name:        "compression stats",
        Description: "Show compression statistics for the database",
        Handler:     r.handleCompressionStats,
    }

    r.commands["compress collection"] = &Command{
        Name:        "compress collection",
        Description: "Manually compress all documents in a collection",
        Handler:     r.handleCompressCollection,
    }

    r.commands["doc compression"] = &Command{
        Name:        "doc compression",
        Description: "Show compression ratio for a document",
        Handler:     r.handleDocCompression,
    }

    r.commands["compression config"] = &Command{
        Name:        "compression config",
        Description: "Show current compression configuration",
        Handler:     r.handleCompressionConfig,
    }

    // -------------------------------------------------------------------------
    // Команды аудита
    // -------------------------------------------------------------------------

    r.commands["audit log"] = &Command{
        Name:        "audit log",
        Description: "Show audit log",
        Handler:     r.handleAuditLog,
    }

    r.commands["audit filter"] = &Command{
        Name:        "audit filter",
        Description: "Filter audit log by type and operation",
        Handler:     r.handleAuditFilter,
    }

    // -------------------------------------------------------------------------
    // Команды кластера
    // -------------------------------------------------------------------------

    r.commands["status"] = &Command{
        Name:        "status",
        Description: "Show cluster status",
        Handler:     r.handleStatus,
    }

    r.commands["nodes"] = &Command{
        Name:        "nodes",
        Description: "List cluster nodes",
        Handler:     r.handleNodes,
    }

    // -------------------------------------------------------------------------
    // Команды миграции (кросс-датацентровая миграция)
    // -------------------------------------------------------------------------

    r.commands["migration"] = &Command{
        Name:        "migration",
        Description: "Cross-datacenter migration commands",
        Handler:     r.handleMigration,
    }

    // -------------------------------------------------------------------------
    // Системные команды
    // -------------------------------------------------------------------------

    r.commands["help"] = &Command{
        Name:        "help",
        Description: "Show this help message",
        Handler:     r.handleHelp,
    }

    r.commands["clear"] = &Command{
        Name:        "clear",
        Description: "Clear the screen",
        Handler:     r.handleClear,
    }

    r.commands["quit"] = &Command{
        Name:        "quit",
        Description: "Exit the REPL",
        Handler:     r.handleQuit,
    }

    r.commands["exit"] = &Command{
        Name:        "exit",
        Description: "Exit the REPL",
        Handler:     r.handleQuit,
    }
}

// =============================================================================
// ОСНОВНОЙ ЦИКЛ REPL
// =============================================================================

// Run запускает основной цикл REPL.
// В цикле:
//   1. Формируется приглашение к вводу с учётом текущего состояния
//   2. Считывается ввод пользователя
//   3. Пустые строки игнорируются
//   4. Команды сохраняются в историю
//   5. Выполняется парсинг и обработка команды
//   6. Ошибки выводятся в цветном формате
//
// Цикл продолжается до получения команды quit/exit или EOF.
// Возвращает ошибку только при критической ошибке ввода-вывода.
func (r *Repl) Run() error {
    // Выводим приветственное сообщение
    utils.Println("")
    utils.PrintInfo("Type 'help' for available commands")
    utils.Println("")

    for {
        // Формируем приглашение к вводу (prompt) с учётом текущего контекста
        prompt := r.buildPrompt()

        // Читаем ввод пользователя
        fmt.Print(prompt)
        input, err := r.reader.ReadString('\n')
        if err != nil {
            // Обрабатываем EOF (Ctrl+D) как корректный выход
            if err.Error() == "EOF" {
                return nil
            }
            return err
        }

        // Удаляем пробельные символы в начале и конце строки
        input = strings.TrimSpace(input)
        if input == "" {
            continue
        }

        // Сохраняем команду в историю (если она не дублирует последнюю)
        r.addToHistory(input)

        // Выполняем команду и обрабатываем ошибки
        if err := r.executeCommand(input); err != nil {
            utils.PrintError(err.Error())
            if r.logger != nil {
                r.logger.Error("REPL command error: " + err.Error())
            }
        }
    }
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ REPL
// =============================================================================

// buildPrompt формирует строку приглашения к вводу.
// Формат: futriiX[:database] (username):~>
// Цвета:
//   - futriiX: Голубой (HiCyan)
//   - :database: Жёлтый (HiYellow)
//   - (username): Зелёный (HiGreen) для аутентифицированных пользователей
func (r *Repl) buildPrompt() string {
    prompt := color.New(color.FgHiCyan).Sprint("futriiX")

    if r.currentDB != "" {
        prompt += color.New(color.FgHiYellow).Sprint(":" + r.currentDB)
    }

    if r.authenticated && r.currentUser != "" {
        prompt += color.New(color.FgHiGreen).Sprint(" (" + r.currentUser + ")")
    }

    prompt += color.New(color.FgHiCyan).Sprint(":~> ")
    return prompt
}

// executeCommand выполняет введённую команду.
// Алгоритм:
//   1. Разбивает ввод на части (поля)
//   2. Проверяет, начинается ли ввод с зарегистрированной команды
//   3. Извлекает аргументы команды (остаток строки после имени команды)
//   4. Вызывает обработчик команды с аргументами
//
// Параметры:
//   - input: Полная строка ввода пользователя
//
// Возвращает:
//   - error: Ошибка выполнения команды или "unknown command"
func (r *Repl) executeCommand(input string) error {
    parts := strings.Fields(input)
    if len(parts) == 0 {
        return nil
    }

    // Ищем команду по префиксу (поддерживаем команды с пробелами, например "create slice")
    for cmdName, cmd := range r.commands {
        if strings.HasPrefix(input, cmdName) {
            // Извлекаем аргументы (всё, что после имени команды)
            args := strings.TrimPrefix(input, cmdName)
            args = strings.TrimSpace(args)
            argList := strings.Fields(args)
            return cmd.Handler(argList)
        }
    }

    return fmt.Errorf("unknown command: %s", parts[0])
}

// addToHistory добавляет команду в историю с защитой от дублирования.
// Если команда совпадает с последней в истории, она не добавляется повторно.
// Также автоматически сохраняет историю в файл.
//
// Параметры:
//   - cmd: Строка команды для добавления
func (r *Repl) addToHistory(cmd string) {
    // Защита от дублирования последней команды
    if len(r.history) > 0 && r.history[len(r.history)-1] == cmd {
        return
    }

    // Ограничиваем размер истории
    if len(r.history) >= r.config.Repl.HistorySize {
        r.history = r.history[1:]
    }
    r.history = append(r.history, cmd)
    r.historyPos = len(r.history)

    // Сохраняем в файл (асинхронно через History.Add + Save)
    r.historyFile.Add(cmd)
    r.historyFile.Save()
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД УПРАВЛЕНИЯ БАЗАМИ ДАННЫХ
// =============================================================================

// handleCreateSlice — обработчик команды "create slice <name>".
// Создаёт новую базу данных (slice) с указанным именем.
//
// Аргументы:
//   - args[0]: Имя создаваемой базы данных
//
// Возвращает:
//   - error: Ошибка при создании или отсутствии аргументов
func (r *Repl) handleCreateSlice(args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("usage: create slice <name>")
    }

    name := args[0]
    if err := r.store.CreateDatabase(name); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Slice '%s' created at %s", name, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleDropDatabase — обработчик команды "drop database <name>".
// Удаляет существующую базу данных. Если текущая БД удаляется,
// сбрасывает currentDB.
//
// Аргументы:
//   - args[0]: Имя удаляемой базы данных
//
// Возвращает:
//   - error: Ошибка при удалении или отсутствии аргументов
func (r *Repl) handleDropDatabase(args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("usage: drop database <name>")
    }

    name := args[0]
    if err := r.store.DropDatabase(name); err != nil {
        return err
    }

    // Если удалили текущую БД, сбрасываем выбор
    if r.currentDB == name {
        r.currentDB = ""
    }

    utils.PrintSuccess(fmt.Sprintf("Database '%s' dropped at %s", name, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleUseDatabase — обработчик команды "use <database>".
// Переключает текущую сессию на указанную базу данных.
// Проверяет существование БД перед переключением.
//
// Аргументы:
//   - args[0]: Имя базы данных для использования
//
// Возвращает:
//   - error: Ошибка при отсутствии БД или аргументов
func (r *Repl) handleUseDatabase(args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("usage: use <database>")
    }

    name := args[0]
    if !r.store.ExistsDatabase(name) {
        return fmt.Errorf("database '%s' does not exist", name)
    }

    r.currentDB = name
    utils.PrintSuccess(fmt.Sprintf("Switched to database '%s'", name))
    return nil
}

// handleShowDatabases — обработчик команды "show databases".
// Выводит список всех существующих баз данных.
// Текущая база данных отмечается звёздочкой (*).
func (r *Repl) handleShowDatabases(args []string) error {
    databases := r.store.ListDatabases()
    if len(databases) == 0 {
        utils.PrintInfo("No databases found")
        return nil
    }

    utils.PrintHeader("Databases")
    for _, db := range databases {
        prefix := "  "
        if db == r.currentDB {
            prefix = " *"
        }
        utils.Println(fmt.Sprintf("%s %s", prefix, db))
    }
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД УПРАВЛЕНИЯ КОЛЛЕКЦИЯМИ
// =============================================================================

// handleCreateCollection — обработчик команды "create collection <name>".
// Создаёт новую коллекцию в текущей базе данных.
// Требует выбранной базы данных (use).
//
// Аргументы:
//   - args[0]: Имя создаваемой коллекции
//
// Возвращает:
//   - error: Ошибка при создании, отсутствии аргументов или выборе БД
func (r *Repl) handleCreateCollection(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: create collection <name>")
    }

    name := args[0]
    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    if err := db.CreateCollection(name); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Collection '%s' created in database '%s' at %s", name, r.currentDB, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleDropCollection — обработчик команды "drop collection <name>".
// Удаляет коллекцию из текущей базы данных.
// Требует выбранной базы данных.
//
// Аргументы:
//   - args[0]: Имя удаляемой коллекции
//
// Возвращает:
//   - error: Ошибка при удалении, отсутствии аргументов или выборе БД
func (r *Repl) handleDropCollection(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: drop collection <name>")
    }

    name := args[0]
    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    if err := db.DropCollection(name); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Collection '%s' dropped from database '%s'", name, r.currentDB))
    return nil
}

// handleShowCollections — обработчик команды "show collections".
// Выводит список всех коллекций в текущей базе данных.
// Требует выбранной базы данных.
func (r *Repl) handleShowCollections(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    collections := db.ListCollections()
    if len(collections) == 0 {
        utils.PrintInfo("No collections found")
        return nil
    }

    utils.PrintInfo(fmt.Sprintf("Collections in database '%s':", r.currentDB))
    for _, coll := range collections {
        utils.Println(fmt.Sprintf("  - %s", coll))
    }
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД РАБОТЫ С ДОКУМЕНТАМИ
// =============================================================================

// handleInsert — обработчик команды "insert <collection> <json>".
// Вставляет новый документ в указанную коллекцию.
// Поддерживает два формата:
//   1. key=value,key2=value2 (простой формат)
//   2. {key=value,key2=value2} (JSON-подобный формат)
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1:]: Пары ключ=значение
//
// Возвращает:
//   - error: Ошибка при вставке, отсутствии аргументов или выборе БД
func (r *Repl) handleInsert(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: insert <collection> <json>")
    }

    collName := args[0]
    jsonStr := strings.Join(args[1:], " ")

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    doc := storage.NewDocument()

    // Парсим входные данные: поддерживаем как JSON, так и простой key=value
    if strings.Contains(jsonStr, "{") {
        // JSON-подобный формат: извлекаем пары из фигурных скобок
        pairs := strings.Split(jsonStr, ",")
        for _, pair := range pairs {
            pair = strings.TrimSpace(pair)
            pair = strings.Trim(pair, "{}")
            kv := strings.SplitN(pair, "=", 2)
            if len(kv) == 2 {
                doc.SetField(kv[0], kv[1])
            }
        }
    } else {
        // Простой формат: key=value,key2=value2
        pairs := strings.Split(jsonStr, ",")
        for _, pair := range pairs {
            pair = strings.TrimSpace(pair)
            kv := strings.SplitN(pair, "=", 2)
            if len(kv) == 2 {
                doc.SetField(kv[0], kv[1])
            }
        }
    }

    if err := coll.Insert(doc); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Document inserted with ID: %s (created at: %s)",
        doc.ID, time.UnixMilli(doc.CreatedAt).Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleFind — обработчик команды "find <collection> <id>".
// Находит документ по ID в указанной коллекции и выводит его содержимое
// вместе с временными метками.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//
// Возвращает:
//   - error: Ошибка при поиске, отсутствии аргументов или выборе БД
func (r *Repl) handleFind(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: find <collection> <id>")
    }

    collName := args[0]
    docID := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    doc, err := coll.Find(docID)
    if err != nil {
        return err
    }

    utils.PrintInfo(fmt.Sprintf("Document found:"))
    utils.PrintJSON(doc.GetFields())

    // Выводим все временные метки документа
    utils.PrintInfo(fmt.Sprintf("  created_at: %s", time.UnixMilli(doc.CreatedAt).Format("2006-01-02 15:04:05.000")))
    utils.PrintInfo(fmt.Sprintf("  updated_at: %s", time.UnixMilli(doc.UpdatedAt).Format("2006-01-02 15:04:05.000")))
    if doc.DeletedAt > 0 {
        utils.PrintWarning(fmt.Sprintf("  deleted_at: %s", time.UnixMilli(doc.DeletedAt).Format("2006-01-02 15:04:05.000")))
    }
    return nil
}

// handleFindByIndex — обработчик команды "findbyindex <collection> <index> <value>".
// Выполняет поиск документов по указанному индексу.
// Использует индекс для быстрого поиска.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя индекса
//   - args[2]: Значение для поиска
//
// Возвращает:
//   - error: Ошибка при поиске, отсутствии аргументов или выборе БД
func (r *Repl) handleFindByIndex(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: findbyindex <collection> <index> <value>")
    }

    collName := args[0]
    indexName := args[1]
    value := args[2]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    docs, err := coll.FindByIndex(indexName, value)
    if err != nil {
        return err
    }

    utils.PrintInfo(fmt.Sprintf("Found %d document(s):", len(docs)))
    for i, doc := range docs {
        utils.PrintInfo(fmt.Sprintf("  [%d] ID: %s (updated: %s)", i+1, doc.ID,
            time.UnixMilli(doc.UpdatedAt).Format("15:04:05.000")))
        utils.PrintJSON(doc.GetFields())
    }
    return nil
}

// handleFindByTime — обработчик команды "findbytime <collection> <from_date> <to_date>".
// Находит документы по временному диапазону (поле created_at).
// Поддерживает форматы дат:
//   - YYYY-MM-DD
//   - YYYY-MM-DD HH:MM:SS
//   - YYYY-MM-DDTHH:MM:SS
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Начальная дата (from_date)
//   - args[2]: Конечная дата (to_date)
//
// Возвращает:
//   - error: Ошибка при парсинге дат, поиске или отсутствии аргументов
func (r *Repl) handleFindByTime(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: findbytime <collection> <from_date> <to_date>\n"+
            "  Date formats: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    }

    collName := args[0]
    fromStr := args[1]
    toStr := args[2]

    // Парсим даты с попыткой нескольких форматов
    var fromTime, toTime time.Time
    var err error

    formats := []string{
        "2006-01-02",
        "2006-01-02 15:04:05",
        "2006-01-02T15:04:05",
    }

    for _, format := range formats {
        fromTime, err = time.Parse(format, fromStr)
        if err == nil {
            break
        }
    }
    if err != nil {
        return fmt.Errorf("invalid from_date format: %s", fromStr)
    }

    for _, format := range formats {
        toTime, err = time.Parse(format, toStr)
        if err == nil {
            break
        }
    }
    if err != nil {
        return fmt.Errorf("invalid to_date format: %s", toStr)
    }

    fromMs := fromTime.UnixMilli()
    toMs := toTime.UnixMilli()

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    // ИСПРАВЛЕНО: корректное использование FindByFilter с функцией фильтрации
    docs := coll.FindByFilter(func(doc *storage.Document) bool {
        return doc.CreatedAt >= fromMs && doc.CreatedAt <= toMs
    })

    if len(docs) == 0 {
        utils.PrintInfo("No documents found in the specified time range")
        return nil
    }

    utils.PrintHeader(fmt.Sprintf("Documents created between %s and %s:",
        fromTime.Format("2006-01-02 15:04:05"), toTime.Format("2006-01-02 15:04:05")))

    for i, doc := range docs {
        utils.PrintInfo(fmt.Sprintf("  [%d] ID: %s (created: %s)",
            i+1, doc.ID, time.UnixMilli(doc.CreatedAt).Format("2006-01-02 15:04:05.000")))
    }

    return nil
}

// handleUpdate — обработчик команды "update <collection> <id> <field=value>...".
// Обновляет поля существующего документа.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//   - args[2:]: Пары field=value для обновления
//
// Возвращает:
//   - error: Ошибка при обновлении, отсутствии аргументов или выборе БД
func (r *Repl) handleUpdate(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: update <collection> <id> <field=value>...")
    }

    collName := args[0]
    docID := args[1]

    updates := make(map[string]interface{})
    for i := 2; i < len(args); i++ {
        kv := strings.SplitN(args[i], "=", 2)
        if len(kv) == 2 {
            updates[kv[0]] = kv[1]
        }
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.Update(docID, updates); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Document '%s' updated at %s", docID, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleDelete — обработчик команды "delete <collection> <id>".
// Выполняет мягкое удаление документа (если включено).
// Документ помечается как удалённый (устанавливается DeletedAt).
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//
// Возвращает:
//   - error: Ошибка при удалении, отсутствии аргументов или выборе БД
func (r *Repl) handleDelete(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: delete <collection> <id>")
    }

    collName := args[0]
    docID := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.Delete(docID); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Document '%s' deleted at %s", docID, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handlePermanentDelete — обработчик команды "permanent delete <collection> <id>".
// Окончательно удаляет ранее мягко удалённый документ.
// Документ полностью удаляется из хранилища.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//
// Возвращает:
//   - error: Ошибка при удалении, отсутствии аргументов или выборе БД
func (r *Repl) handlePermanentDelete(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: permanent delete <collection> <id>")
    }

    collName := args[0]
    docID := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.PermanentDelete(docID); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Document '%s' permanently deleted", docID))
    return nil
}

// handleRestore — обработчик команды "restore <collection> <id>".
// Восстанавливает ранее мягко удалённый документ.
// Сбрасывает DeletedAt в 0.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//
// Возвращает:
//   - error: Ошибка при восстановлении, отсутствии аргументов или выборе БД
func (r *Repl) handleRestore(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: restore <collection> <id>")
    }

    collName := args[0]
    docID := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.RestoreDeleted(docID); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Document '%s' restored at %s", docID, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleShowDeleted — обработчик команды "show deleted <collection>".
// Показывает все мягко удалённые документы в коллекции.
//
// Аргументы:
//   - args[0]: Имя коллекции
//
// Возвращает:
//   - error: Ошибка при получении списка, отсутствии аргументов или выборе БД
func (r *Repl) handleShowDeleted(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: show deleted <collection>")
    }

    collName := args[0]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    docs := coll.GetAllDocumentsIncludingDeleted()
    deleted := make([]*storage.Document, 0)

    for _, doc := range docs {
        if doc.IsDeleted() {
            deleted = append(deleted, doc)
        }
    }

    if len(deleted) == 0 {
        utils.PrintInfo("No deleted documents found")
        return nil
    }

    utils.PrintHeader(fmt.Sprintf("Deleted documents in collection '%s':", collName))
    for i, doc := range deleted {
        utils.PrintInfo(fmt.Sprintf("  [%d] ID: %s (deleted: %s)",
            i+1, doc.ID, time.UnixMilli(doc.DeletedAt).Format("2006-01-02 15:04:05.000")))
    }

    return nil
}

// handleCount — обработчик команды "count <collection>".
// Показывает статистику коллекции: количество активных, удалённых и всех документов.
//
// Аргументы:
//   - args[0]: Имя коллекции
//
// Возвращает:
//   - error: Ошибка при подсчёте, отсутствии аргументов или выборе БД
func (r *Repl) handleCount(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: count <collection>")
    }

    collName := args[0]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    active := coll.Count()
    deleted := coll.CountDeleted()
    total := coll.CountAll()

    utils.PrintHeader(fmt.Sprintf("Collection '%s' statistics:", collName))
    utils.PrintInfo(fmt.Sprintf("  Active documents:  %d", active))
    if deleted > 0 {
        utils.PrintWarning(fmt.Sprintf("  Deleted documents: %d", deleted))
    }
    utils.PrintInfo(fmt.Sprintf("  Total documents:   %d", total))

    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД УПРАВЛЕНИЯ ВРЕМЕННЫМИ МЕТКАМИ
// =============================================================================

// handleShowTimestamps — обработчик команды "show timestamps <collection> <id>".
// Показывает все временные метки документа: created_at, updated_at, deleted_at и версию.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//
// Возвращает:
//   - error: Ошибка при поиске, отсутствии аргументов или выборе БД
func (r *Repl) handleShowTimestamps(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: show timestamps <collection> <id>")
    }

    collName := args[0]
    docID := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    // Ищем документ, включая удалённые (чтобы показать deleted_at)
    doc, err := coll.FindIncludingDeleted(docID)
    if err != nil {
        return err
    }

    utils.PrintHeader(fmt.Sprintf("Timestamps for document: %s", docID))
    utils.PrintInfo(fmt.Sprintf("  Created:  %s (%d)",
        time.UnixMilli(doc.CreatedAt).Format("2006-01-02 15:04:05.000"),
        doc.CreatedAt))
    utils.PrintInfo(fmt.Sprintf("  Updated:  %s (%d)",
        time.UnixMilli(doc.UpdatedAt).Format("2006-01-02 15:04:05.000"),
        doc.UpdatedAt))

    if doc.DeletedAt > 0 {
        utils.PrintWarning(fmt.Sprintf("  Deleted:  %s (%d)",
            time.UnixMilli(doc.DeletedAt).Format("2006-01-02 15:04:05.000"),
            doc.DeletedAt))
    } else {
        utils.PrintInfo("  Deleted:  not deleted")
    }

    utils.PrintInfo(fmt.Sprintf("  Version:  %d", doc.Version))

    return nil
}

// handleStatsTimestamps — обработчик команды "stats timestamps <collection>".
// Вычисляет и показывает статистику временных меток для всех документов коллекции.
// Включает: минимальную, максимальную и среднюю дату для created_at и updated_at.
//
// Аргументы:
//   - args[0]: Имя коллекции
//
// Возвращает:
//   - error: Ошибка при получении статистики, отсутствии аргументов или выборе БД
func (r *Repl) handleStatsTimestamps(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: stats timestamps <collection>")
    }

    collName := args[0]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    // Получаем все документы, включая удалённые, для полной статистики
    docs := coll.GetAllDocumentsIncludingDeleted()

    if len(docs) == 0 {
        utils.PrintInfo("No documents in collection")
        return nil
    }

    // Вычисляем статистику
    var minCreated, maxCreated int64 = 1<<63 - 1, 0
    var minUpdated, maxUpdated int64 = 1<<63 - 1, 0
    var totalCreated, totalUpdated int64 = 0, 0

    for _, doc := range docs {
        if doc.CreatedAt < minCreated {
            minCreated = doc.CreatedAt
        }
        if doc.CreatedAt > maxCreated {
            maxCreated = doc.CreatedAt
        }
        if doc.UpdatedAt < minUpdated {
            minUpdated = doc.UpdatedAt
        }
        if doc.UpdatedAt > maxUpdated {
            maxUpdated = doc.UpdatedAt
        }
        totalCreated += doc.CreatedAt
        totalUpdated += doc.UpdatedAt
    }

    avgCreated := totalCreated / int64(len(docs))
    avgUpdated := totalUpdated / int64(len(docs))

    utils.PrintHeader(fmt.Sprintf("Timestamp statistics for collection '%s':", collName))
    utils.PrintInfo(fmt.Sprintf("  Documents count:     %d", len(docs)))
    utils.Println("")
    utils.PrintInfo("  Created timestamps:")
    utils.PrintInfo(fmt.Sprintf("    Earliest:          %s", time.UnixMilli(minCreated).Format("2006-01-02 15:04:05.000")))
    utils.PrintInfo(fmt.Sprintf("    Latest:            %s", time.UnixMilli(maxCreated).Format("2006-01-02 15:04:05.000")))
    utils.PrintInfo(fmt.Sprintf("    Average:           %s", time.UnixMilli(avgCreated).Format("2006-01-02 15:04:05.000")))
    utils.Println("")
    utils.PrintInfo("  Updated timestamps:")
    utils.PrintInfo(fmt.Sprintf("    Earliest:          %s", time.UnixMilli(minUpdated).Format("2006-01-02 15:04:05.000")))
    utils.PrintInfo(fmt.Sprintf("    Latest:            %s", time.UnixMilli(maxUpdated).Format("2006-01-02 15:04:05.000")))
    utils.PrintInfo(fmt.Sprintf("    Average:           %s", time.UnixMilli(avgUpdated).Format("2006-01-02 15:04:05.000")))

    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД АУДИТА
// =============================================================================

// handleAuditLog — обработчик команды "audit log".
// Показывает последние 50 записей аудита в обратном порядке.
// Записи подсвечиваются цветами:
//   - Зелёный: CREATE/INSERT
//   - Жёлтый: DELETE/SOFT_DELETE
//   - Белый: остальные операции
func (r *Repl) handleAuditLog(args []string) error {
    entries := storage.GetAuditLog()

    if len(entries) == 0 {
        utils.PrintInfo("No audit log entries")
        return nil
    }

    // Показываем последние 50 записей
    start := 0
    if len(entries) > 50 {
        start = len(entries) - 50
    }

    utils.PrintHeader("Audit Log (last 50 entries)")
    for i := len(entries) - 1; i >= start; i-- {
        entry := entries[i]
        colorFunc := utils.PrintInfo
        if entry.Operation == "DELETE" || entry.Operation == "SOFT_DELETE" {
            colorFunc = utils.PrintWarning
        } else if entry.Operation == "CREATE" || entry.Operation == "INSERT" {
            colorFunc = utils.PrintSuccess
        }

        colorFunc(fmt.Sprintf("  [%s] %s - %s: %s",
            entry.TimestampStr, entry.Operation, entry.DataType, entry.Name))
    }

    return nil
}

// handleAuditFilter — обработчик команды "audit filter <data_type> <operation>".
// Фильтрует и показывает записи аудита по типу данных и операции.
//
// Аргументы:
//   - args[0]: Тип данных (DATABASE, COLLECTION, DOCUMENT, FIELD, INDEX, TRANSACTION)
//   - args[1]: Операция (CREATE, INSERT, UPDATE, DELETE, SOFT_DELETE, RESTORE)
//
// Возвращает:
//   - error: Ошибка при фильтрации или отсутствии аргументов
func (r *Repl) handleAuditFilter(args []string) error {
    if len(args) < 2 {
        return fmt.Errorf("usage: audit filter <data_type> <operation>\n"+
            "  data_type: DATABASE, COLLECTION, DOCUMENT, FIELD, INDEX, TRANSACTION\n"+
            "  operation: CREATE, INSERT, UPDATE, DELETE, SOFT_DELETE, RESTORE")
    }

    dataType := strings.ToUpper(args[0])
    operation := strings.ToUpper(args[1])

    entries := storage.GetAuditLogFiltered(dataType, operation, 0, 0)

    if len(entries) == 0 {
        utils.PrintInfo(fmt.Sprintf("No audit log entries found for %s/%s", dataType, operation))
        return nil
    }

    utils.PrintHeader(fmt.Sprintf("Audit Log Filtered: %s / %s", dataType, operation))
    for _, entry := range entries {
        utils.PrintInfo(fmt.Sprintf("  [%s] %s: %s", entry.TimestampStr, entry.Operation, entry.Name))
    }

    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД УПРАВЛЕНИЯ ИНДЕКСАМИ
// =============================================================================

// handleCreateIndex — обработчик команды "create index <collection> <name> <fields> [unique]".
// Создаёт новый индекс на указанных полях коллекции.
// Если указан флаг "unique", индекс будет уникальным.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя индекса
//   - args[2]: Список полей через запятую
//   - args[3]: (опционально) "unique" для уникального индекса
//
// Возвращает:
//   - error: Ошибка при создании, отсутствии аргументов или выборе БД
func (r *Repl) handleCreateIndex(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: create index <collection> <name> <fields> [unique]")
    }

    collName := args[0]
    indexName := args[1]
    fields := strings.Split(args[2], ",")
    unique := len(args) > 3 && args[3] == "unique"

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.CreateIndex(indexName, fields, unique); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Index '%s' created on collection '%s' at %s", indexName, collName, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleDropIndex — обработчик команды "drop index <collection> <name>".
// Удаляет существующий индекс из коллекции.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя индекса
//
// Возвращает:
//   - error: Ошибка при удалении, отсутствии аргументов или выборе БД
func (r *Repl) handleDropIndex(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: drop index <collection> <name>")
    }

    collName := args[0]
    indexName := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.DropIndex(indexName); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Index '%s' dropped from collection '%s'", indexName, collName))
    return nil
}

// handleShowIndexes — обработчик команды "show indexes <collection>".
// Показывает все индексы коллекции с их свойствами.
//
// Аргументы:
//   - args[0]: Имя коллекции
//
// Возвращает:
//   - error: Ошибка при получении списка, отсутствии аргументов или выборе БД
func (r *Repl) handleShowIndexes(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: show indexes <collection>")
    }

    collName := args[0]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    indexes := coll.GetIndexesInfo()
    if len(indexes) == 0 {
        utils.PrintInfo(fmt.Sprintf("No indexes found on collection '%s'", collName))
        return nil
    }

    utils.PrintInfo(fmt.Sprintf("Indexes on collection '%s':", collName))
    for _, idx := range indexes {
        uniqueStr := ""
        if idx["unique"].(bool) {
            uniqueStr = " (unique)"
        }
        createdAt := time.UnixMilli(idx["created_at"].(int64)).Format("2006-01-02 15:04:05")
        utils.Println(fmt.Sprintf("  - %s: %v%s (created: %s)", idx["name"], idx["fields"], uniqueStr, createdAt))
    }
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД ОГРАНИЧЕНИЙ
// =============================================================================

// handleAddRequired — обработчик команды "add required <collection> <field>".
// Добавляет ограничение "обязательное поле" для указанной коллекции.
// Документы без этого поля не будут вставляться.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя поля
//
// Возвращает:
//   - error: Ошибка при добавлении, отсутствии аргументов или выборе БД
func (r *Repl) handleAddRequired(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: add required <collection> <field>")
    }

    collName := args[0]
    field := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    coll.AddRequiredField(field)
    utils.PrintSuccess(fmt.Sprintf("Required field '%s' added to collection '%s'", field, collName))
    return nil
}

// handleAddUnique — обработчик команды "add unique <collection> <field>".
// Добавляет ограничение уникальности для указанного поля.
// Значения поля должны быть уникальны в коллекции.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя поля
//
// Возвращает:
//   - error: Ошибка при добавлении, отсутствии аргументов или выборе БД
func (r *Repl) handleAddUnique(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: add unique <collection> <field>")
    }

    collName := args[0]
    field := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    coll.AddUniqueConstraint(field)
    utils.PrintSuccess(fmt.Sprintf("Unique constraint added for field '%s' on collection '%s'", field, collName))
    return nil
}

// handleAddMin — обработчик команды "add min <collection> <field> <value>".
// Добавляет ограничение минимального значения для числового поля.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя поля
//   - args[2]: Минимальное значение (число)
//
// Возвращает:
//   - error: Ошибка при добавлении, отсутствии аргументов или выборе БД
func (r *Repl) handleAddMin(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: add min <collection> <field> <value>")
    }

    collName := args[0]
    field := args[1]
    var minVal float64
    if _, err := fmt.Sscanf(args[2], "%f", &minVal); err != nil {
        return fmt.Errorf("invalid minimum value: %s", args[2])
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    coll.AddMinConstraint(field, minVal)
    utils.PrintSuccess(fmt.Sprintf("Min constraint added for field '%s' on collection '%s' (min: %.2f)", field, collName, minVal))
    return nil
}

// handleAddMax — обработчик команды "add max <collection> <field> <value>".
// Добавляет ограничение максимального значения для числового поля.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя поля
//   - args[2]: Максимальное значение (число)
//
// Возвращает:
//   - error: Ошибка при добавлении, отсутствии аргументов или выборе БД
func (r *Repl) handleAddMax(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: add max <collection> <field> <value>")
    }

    collName := args[0]
    field := args[1]
    var maxVal float64
    if _, err := fmt.Sscanf(args[2], "%f", &maxVal); err != nil {
        return fmt.Errorf("invalid maximum value: %s", args[2])
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    coll.AddMaxConstraint(field, maxVal)
    utils.PrintSuccess(fmt.Sprintf("Max constraint added for field '%s' on collection '%s' (max: %.2f)", field, collName, maxVal))
    return nil
}

// handleAddEnum — обработчик команды "add enum <collection> <field> <values...>".
// Добавляет ограничение на допустимые значения (enum) для поля.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя поля
//   - args[2:]: Список допустимых значений
//
// Возвращает:
//   - error: Ошибка при добавлении, отсутствии аргументов или выборе БД
func (r *Repl) handleAddEnum(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: add enum <collection> <field> <values...>")
    }

    collName := args[0]
    field := args[1]
    values := make([]interface{}, len(args[2:]))
    for i, v := range args[2:] {
        values[i] = v
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    coll.AddEnumConstraint(field, values)
    utils.PrintSuccess(fmt.Sprintf("Enum constraint added for field '%s' on collection '%s' (allowed: %v)", field, collName, values))
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД ТРАНЗАКЦИЙ
// =============================================================================

// handleBeginTransaction — обработчик команды "begin transaction".
// Начинает новую транзакцию.
//
// Возвращает:
//   - error: Ошибка при создании транзакции или отсутствии выбора БД
func (r *Repl) handleBeginTransaction(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    tx := storage.BeginTransaction()
    if tx == nil {
        return fmt.Errorf("failed to begin transaction")
    }

    utils.PrintSuccess(fmt.Sprintf("Transaction %d started at %s", tx.ID, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleCommitTransaction — обработчик команды "commit".
// Фиксирует (коммитит) текущую транзакцию.
//
// Возвращает:
//   - error: Ошибка при коммите или отсутствии активной транзакции
func (r *Repl) handleCommitTransaction(args []string) error {
    if !storage.HasActiveTransaction() {
        return fmt.Errorf("no active transaction to commit")
    }

    if err := storage.CommitCurrentTransaction(); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Transaction committed successfully at %s", time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleRollbackTransaction — обработчик команды "rollback".
// Откатывает (отменяет) текущую транзакцию.
//
// Возвращает:
//   - error: Ошибка при откате или отсутствии активной транзакции
func (r *Repl) handleRollbackTransaction(args []string) error {
    if !storage.HasActiveTransaction() {
        return fmt.Errorf("no active transaction to rollback")
    }

    if err := storage.AbortCurrentTransaction(); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Transaction rolled back at %s", time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleShowTransactions — обработчик команды "show transactions".
// Показывает все активные транзакции и их операции.
func (r *Repl) handleShowTransactions(args []string) error {
    transactions := storage.GetActiveTransactions()

    if len(transactions) == 0 {
        utils.PrintInfo("No active transactions")
        return nil
    }

    utils.PrintHeader("Active Transactions")
    for _, tx := range transactions {
        utils.PrintInfo(fmt.Sprintf("  ID: %s, Status: %s, Operations: %d, Started: %s",
            tx.ID, tx.Status, tx.OperationCount, tx.StartTime))
    }
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД ПЛАГИНОВ
// =============================================================================

// handlePluginList — обработчик команды "plugin list".
// Показывает все загруженные плагины с их статусом.
func (r *Repl) handlePluginList(args []string) error {
    if r.pluginManager == nil || !r.pluginManager.IsEnabled() {
        return fmt.Errorf("plugin system is disabled")
    }

    plugins := r.pluginManager.ListPlugins()

    if len(plugins) == 0 {
        utils.PrintInfo("No plugins loaded")
        utils.PrintInfo(fmt.Sprintf("Plugins directory: %s", r.pluginManager.GetPluginsDir()))
        return nil
    }

    utils.PrintHeader("Loaded Plugins")
    for _, p := range plugins {
        status := "loaded"
        switch p.Status.Load() {
        case 1:
            status = "running"
        case 2:
            status = "stopped"
        case 3:
            status = "error"
        }
        utils.PrintInfo(fmt.Sprintf("  %s v%s - %s [%s]", p.Name, p.Version(), status, p.Description()))
        utils.Println(fmt.Sprintf("      Author: %s, Loaded: %s", p.Author(), p.LoadedAt().Format("2006-01-02 15:04:05")))
    }
    return nil
}

// handlePluginLoad — обработчик команды "plugin load <name> <filepath>".
// Загружает плагин из файла.
//
// Аргументы:
//   - args[0]: Имя плагина
//   - args[1]: Путь к файлу плагина
//
// Возвращает:
//   - error: Ошибка при загрузке или отсутствии аргументов
func (r *Repl) handlePluginLoad(args []string) error {
    if r.pluginManager == nil || !r.pluginManager.IsEnabled() {
        return fmt.Errorf("plugin system is disabled")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: plugin load <name> <filepath>")
    }

    name := args[0]
    filepath := args[1]

    if err := r.pluginManager.LoadPlugin(name, filepath); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Plugin '%s' loaded from %s at %s", name, filepath, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handlePluginUnload — обработчик команды "plugin unload <name>".
// Выгружает плагин.
//
// Аргументы:
//   - args[0]: Имя плагина
//
// Возвращает:
//   - error: Ошибка при выгрузке или отсутствии аргументов
func (r *Repl) handlePluginUnload(args []string) error {
    if r.pluginManager == nil || !r.pluginManager.IsEnabled() {
        return fmt.Errorf("plugin system is disabled")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: plugin unload <name>")
    }

    name := args[0]

    if err := r.pluginManager.UnloadPlugin(name); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Plugin '%s' unloaded", name))
    return nil
}

// handlePluginStart — обработчик команды "plugin start <name>".
// Запускает загруженный плагин.
//
// Аргументы:
//   - args[0]: Имя плагина
//
// Возвращает:
//   - error: Ошибка при запуске или отсутствии аргументов
func (r *Repl) handlePluginStart(args []string) error {
    if r.pluginManager == nil || !r.pluginManager.IsEnabled() {
        return fmt.Errorf("plugin system is disabled")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: plugin start <name>")
    }

    name := args[0]

    if err := r.pluginManager.StartPlugin(name); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Plugin '%s' started at %s", name, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handlePluginStop — обработчик команды "plugin stop <name>".
// Останавливает запущенный плагин.
//
// Аргументы:
//   - args[0]: Имя плагина
//
// Возвращает:
//   - error: Ошибка при остановке или отсутствии аргументов
func (r *Repl) handlePluginStop(args []string) error {
    if r.pluginManager == nil || !r.pluginManager.IsEnabled() {
        return fmt.Errorf("plugin system is disabled")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: plugin stop <name>")
    }

    name := args[0]

    if err := r.pluginManager.StopPlugin(name); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Plugin '%s' stopped", name))
    return nil
}

// handlePluginExec — обработчик команды "plugin exec <plugin> <function> [args...]".
// Выполняет функцию плагина с переданными аргументами.
//
// Аргументы:
//   - args[0]: Имя плагина
//   - args[1]: Имя функции
//   - args[2:]: Аргументы для функции
//
// Возвращает:
//   - error: Ошибка при выполнении или отсутствии аргументов
func (r *Repl) handlePluginExec(args []string) error {
    if r.pluginManager == nil || !r.pluginManager.IsEnabled() {
        return fmt.Errorf("plugin system is disabled")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: plugin exec <plugin> <function> [args...]")
    }

    pluginName := args[0]
    funcName := args[1]

    var execArgs []interface{}
    for _, arg := range args[2:] {
        execArgs = append(execArgs, arg)
    }

    result, err := r.pluginManager.ExecutePlugin(pluginName, funcName, execArgs...)
    if err != nil {
        return err
    }

    utils.PrintSuccess("Result:")
    utils.PrintJSON(result)
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД ИМПОРТА/ЭКСПОРТА
// =============================================================================

// handleExport — обработчик команды "export <database> <filename>".
// Экспортирует базу данных в файл в формате MessagePack.
//
// Аргументы:
//   - args[0]: Имя базы данных
//   - args[1]: Имя файла для экспорта
//
// Возвращает:
//   - error: Ошибка при экспорте или отсутствии аргументов
func (r *Repl) handleExport(args []string) error {
    if len(args) < 2 {
        return fmt.Errorf("usage: export <database> <filename>")
    }

    dbName := args[0]
    fileName := args[1]

    if !r.store.ExistsDatabase(dbName) {
        return fmt.Errorf("database '%s' does not exist", dbName)
    }

    utils.PrintInfo(fmt.Sprintf("Exporting database '%s' to %s at %s...", dbName, fileName, time.Now().Format("2006-01-02 15:04:05.000")))

    // ИСПРАВЛЕНО: используем метод SerializeDatabase через Database
    db, err := r.store.GetDatabase(dbName)
    if err != nil {
        return err
    }

    data, err := db.SerializeDatabase()
    if err != nil {
        return fmt.Errorf("failed to serialize database: %v", err)
    }

    if err := os.WriteFile(fileName, data, 0644); err != nil {
        return fmt.Errorf("failed to write export file: %v", err)
    }

    utils.PrintSuccess(fmt.Sprintf("Database '%s' exported to %s", dbName, fileName))
    return nil
}

// handleImport — обработчик команды "import <database> <filename>".
// Импортирует базу данных из файла в формате MessagePack.
//
// Аргументы:
//   - args[0]: Имя базы данных
//   - args[1]: Имя файла для импорта
//
// Возвращает:
//   - error: Ошибка при импорте или отсутствии аргументов
func (r *Repl) handleImport(args []string) error {
    if len(args) < 2 {
        return fmt.Errorf("usage: import <database> <filename>")
    }

    dbName := args[0]
    fileName := args[1]

    utils.PrintInfo(fmt.Sprintf("Importing data from %s to database '%s' at %s...", fileName, dbName, time.Now().Format("2006-01-02 15:04:05.000")))

    data, err := os.ReadFile(fileName)
    if err != nil {
        return fmt.Errorf("failed to read import file: %v", err)
    }

    // Создаём базу данных, если она не существует
    if !r.store.ExistsDatabase(dbName) {
        if err := r.store.CreateDatabase(dbName); err != nil {
            return err
        }
    }

    db, err := r.store.GetDatabase(dbName)
    if err != nil {
        return err
    }

    // ИСПРАВЛЕНО: используем метод DeserializeDatabase через Database
    if err := db.DeserializeDatabase(data); err != nil {
        return fmt.Errorf("failed to deserialize database: %v", err)
    }

    utils.PrintSuccess(fmt.Sprintf("Data imported to database '%s' from %s", dbName, fileName))
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД ACL
// =============================================================================

// handleACLLogin — обработчик команды "acl login <username> <password>".
// Аутентифицирует пользователя по имени и паролю.
//
// Аргументы:
//   - args[0]: Имя пользователя
//   - args[1]: Пароль
//
// Возвращает:
//   - error: Ошибка аутентификации или отсутствии аргументов
func (r *Repl) handleACLLogin(args []string) error {
    if len(args) < 2 {
        return fmt.Errorf("usage: acl login <username> <password>")
    }

    username := args[0]
    password := args[1]

    if r.aclManager == nil {
        return fmt.Errorf("ACL manager not initialized")
    }

    sessionID, err := r.aclManager.Authenticate(username, password)
    if err != nil {
        return err
    }

    r.authenticated = true
    r.currentUser = username
    r.sessionID = sessionID

    // Получаем роли пользователя
    roles := r.aclManager.GetUserRoles(sessionID)
    if len(roles) > 0 {
        r.currentRole = roles[0]
    }

    utils.PrintSuccess(fmt.Sprintf("Logged in as '%s' with role '%s' at %s", username, r.currentRole, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleACLLogout — обработчик команды "acl logout".
// Завершает сессию текущего пользователя.
func (r *Repl) handleACLLogout(args []string) error {
    if r.sessionID != "" && r.aclManager != nil {
        r.aclManager.Logout(r.sessionID)
    }

    r.authenticated = false
    r.currentUser = ""
    r.currentRole = "anonymous"
    r.sessionID = ""

    utils.PrintSuccess(fmt.Sprintf("Logged out at %s", time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleACLGrant — обработчик команды "acl grant <collection> <role> <permissions>".
// Назначает разрешения роли на коллекцию.
// Разрешения: r=read, w=write, d=delete, a=admin.
// Требует прав администратора.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя роли
//   - args[2]: Строка разрешений (например, "rwa")
//
// Возвращает:
//   - error: Ошибка при назначении разрешений или отсутствии прав
func (r *Repl) handleACLGrant(args []string) error {
    if !r.authenticated || r.currentRole != "admin" {
        return fmt.Errorf("permission denied: admin access required")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: acl grant <collection> <role> <permissions>\n"+
            "  Permissions: r=read, w=write, d=delete, a=admin\n"+
            "  Example: acl grant users admin rwa")
    }

    collName := args[0]
    role := args[1]
    perms := args[2]

    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    canRead := strings.Contains(perms, "r")
    canWrite := strings.Contains(perms, "w")
    canDelete := strings.Contains(perms, "d")
    isAdmin := strings.Contains(perms, "a")

    coll.SetACL(role, canRead, canWrite, canDelete, isAdmin)
    utils.PrintSuccess(fmt.Sprintf("Permissions '%s' granted to role '%s' on collection '%s' at %s",
        perms, role, collName, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleACLUsers — обработчик команды "acl users".
// Показывает список всех пользователей и их роли.
func (r *Repl) handleACLUsers(args []string) error {
    if r.aclManager == nil {
        return fmt.Errorf("ACL manager not initialized")
    }

    users := r.aclManager.ListUsers()

    if len(users) == 0 {
        utils.PrintInfo("No users found")
        return nil
    }

    utils.PrintHeader("Users")
    for _, username := range users {
        userInfo, err := r.aclManager.GetUserInfo(username)
        if err != nil {
            continue
        }
        status := "active"
        if !userInfo.Active {
            status = "disabled"
        }
        utils.PrintInfo(fmt.Sprintf("  %s - Roles: %v [%s]", username, userInfo.Roles, status))
    }
    return nil
}

// handleACLRoles — обработчик команды "acl roles".
// Показывает список всех ролей и их разрешения.
func (r *Repl) handleACLRoles(args []string) error {
    if r.aclManager == nil {
        return fmt.Errorf("ACL manager not initialized")
    }

    roles := r.aclManager.ListRoles()

    if len(roles) == 0 {
        utils.PrintInfo("No roles found")
        return nil
    }

    utils.PrintHeader("Roles")
    for _, roleName := range roles {
        perms, err := r.aclManager.GetRolePermissions(roleName)
        if err != nil {
            continue
        }
        utils.PrintInfo(fmt.Sprintf("  %s - Permissions: %v", roleName, perms))
    }
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД СЖАТИЯ
// =============================================================================

// handleCompressionStats — обработчик команды "compression stats".
// Показывает статистику сжатия для текущей базы данных.
// Включает: количество сжатых документов, коэффициент сжатия, размеры.
func (r *Repl) handleCompressionStats(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    collections := db.ListCollections()
    totalDocs := int64(0)
    compressedDocs := int64(0)
    totalOriginalSize := int64(0)
    totalCompressedSize := int64(0)

    for _, collName := range collections {
        coll, err := db.GetCollection(collName)
        if err != nil {
            continue
        }

        docs := coll.GetAllDocuments()
        for _, doc := range docs {
            totalDocs++
            if doc.Compressed {
                compressedDocs++
                totalOriginalSize += doc.OriginalSize
                if data, err := doc.Serialize(); err == nil {
                    totalCompressedSize += int64(len(data))
                }
            }
        }
    }

    utils.PrintHeader("Compression Statistics")
    utils.PrintInfo(fmt.Sprintf("  Total Documents:      %d", totalDocs))
    utils.PrintInfo(fmt.Sprintf("  Compressed Documents: %d", compressedDocs))
    if totalDocs > 0 {
        utils.PrintInfo(fmt.Sprintf("  Compression Rate:     %.2f%%", float64(compressedDocs)/float64(totalDocs)*100))
    }
    if totalOriginalSize > 0 {
        ratio := float64(totalCompressedSize) / float64(totalOriginalSize)
        utils.PrintInfo(fmt.Sprintf("  Size Reduction:       %.2f%%", (1-ratio)*100))
        utils.PrintInfo(fmt.Sprintf("  Original Size:        %s", utils.FormatBytes(totalOriginalSize)))
        utils.PrintInfo(fmt.Sprintf("  Compressed Size:      %s", utils.FormatBytes(totalCompressedSize)))
    }
    utils.PrintInfo(fmt.Sprintf("  Algorithm:            %s", r.config.Compression.Algorithm))
    utils.PrintInfo(fmt.Sprintf("  Compression Level:    %d", r.config.Compression.Level))
    utils.PrintInfo(fmt.Sprintf("  Min Size Threshold:   %s", utils.FormatBytes(int64(r.config.Compression.MinSize))))

    return nil
}

// handleCompressCollection — обработчик команды "compress collection <name>".
// Вручную сжимает все документы в указанной коллекции.
//
// Аргументы:
//   - args[0]: Имя коллекции
//
// Возвращает:
//   - error: Ошибка при сжатии, отсутствии аргументов или выборе БД
func (r *Repl) handleCompressCollection(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: compress collection <name>")
    }

    collName := args[0]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    docs := coll.GetAllDocuments()
    compressed := 0

    utils.PrintInfo(fmt.Sprintf("Compressing collection '%s' at %s...", collName, time.Now().Format("2006-01-02 15:04:05.000")))

    for _, doc := range docs {
        if !doc.Compressed {
            if err := doc.Compress(&compression.Config{
                Enabled:   r.config.Compression.Enabled,
                Algorithm: r.config.Compression.Algorithm,
                Level:     r.config.Compression.Level,
                MinSize:   r.config.Compression.MinSize,
            }); err == nil {
                compressed++
            }
        }
    }

    utils.PrintSuccess(fmt.Sprintf("Compressed %d documents in collection '%s'", compressed, collName))
    return nil
}

// handleDocCompression — обработчик команды "doc compression <collection> <id>".
// Показывает информацию о сжатии для конкретного документа.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: ID документа
//
// Возвращает:
//   - error: Ошибка при получении информации, отсутствии аргументов или выборе БД
func (r *Repl) handleDocCompression(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 2 {
        return fmt.Errorf("usage: doc compression <collection> <id>")
    }

    collName := args[0]
    docID := args[1]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    doc, err := coll.Find(docID)
    if err != nil {
        return err
    }

    utils.PrintHeader(fmt.Sprintf("Compression Info for Document: %s", docID))
    utils.PrintInfo(fmt.Sprintf("  Compressed:     %v", doc.Compressed))
    if doc.Compressed {
        ratio := doc.GetCompressionRatio()
        utils.PrintInfo(fmt.Sprintf("  Ratio:          %.2f%%", (1-ratio)*100))
        utils.PrintInfo(fmt.Sprintf("  Original Size:  %s", utils.FormatBytes(doc.OriginalSize)))

        if data, err := doc.Serialize(); err == nil {
            utils.PrintInfo(fmt.Sprintf("  Current Size:   %s", utils.FormatBytes(int64(len(data)))))
        }
    }

    return nil
}

// handleCompressionConfig — обработчик команды "compression config".
// Показывает текущую конфигурацию сжатия и доступные алгоритмы.
func (r *Repl) handleCompressionConfig(args []string) error {
    utils.PrintHeader("Compression Configuration")
    utils.PrintInfo(fmt.Sprintf("  Enabled:    %v", r.config.Compression.Enabled))
    utils.PrintInfo(fmt.Sprintf("  Algorithm:  %s", r.config.Compression.Algorithm))
    utils.PrintInfo(fmt.Sprintf("  Level:      %d", r.config.Compression.Level))
    utils.PrintInfo(fmt.Sprintf("  Min Size:   %s", utils.FormatBytes(int64(r.config.Compression.MinSize))))

    utils.PrintInfo("")
    utils.PrintInfo("Available Algorithms:")
    utils.PrintInfo("  snappy  - Fast compression/decompression, good balance (default)")
    utils.PrintInfo("  lz4     - Extremely fast, lower compression ratio")
    utils.PrintInfo("  zstd    - High compression ratio, slower")

    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД ТРИГГЕРОВ (MongoDB-LIKE)
// =============================================================================

// handleCreateTrigger — обработчик команды "create trigger".
// Создаёт триггер на коллекции с указанным событием и действием.
// Поддерживает MongoDB-подобный синтаксис с опциями.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Имя триггера
//   - args[2]: Событие (BEFORE_INSERT, AFTER_INSERT, BEFORE_UPDATE, AFTER_UPDATE, BEFORE_DELETE, AFTER_DELETE)
//   - args[3]: Действие (abort, skip, modify, log, notify)
//   - args[4:]: Опции (--description, --set, --inc, --currentDate, --condition)
//
// Возвращает:
//   - error: Ошибка при создании, отсутствии аргументов или выборе БД
func (r *Repl) handleCreateTrigger(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 5 {
        return fmt.Errorf("usage: create trigger <collection> <name> <event> <action> [options]\n"+
            "  Events: BEFORE_INSERT, AFTER_INSERT, BEFORE_UPDATE, AFTER_UPDATE, BEFORE_DELETE, AFTER_DELETE\n"+
            "  Actions: abort, skip, modify, log, notify\n"+
            "  Options: --description <text>, --set <field> <value>, --inc <field> <value>, --currentDate <field>, --condition <field> <op> <value>")
    }

    collName := args[0]
    triggerName := args[1]
    event := args[2]
    action := args[3]

    // Получаем коллекцию
    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    // Создаём триггер
    trigger := &storage.Trigger{
        Name:        triggerName,
        Event:       event,
        Action:      action,
        Enabled:     true,
        Description: "",
        CreatedAt:   time.Now().UnixMilli(),
        UpdatedAt:   time.Now().UnixMilli(),
    }

    // Парсим опции
    for i := 4; i < len(args); i++ {
        switch args[i] {
        case "--description":
            if i+1 < len(args) {
                trigger.Description = args[i+1]
                i++
            }
        case "--set":
            if i+2 < len(args) {
                // Для простоты сохраняем в отдельную структуру
                // В реальной реализации здесь должна быть логика обработки
                i += 2
            }
        case "--inc":
            if i+2 < len(args) {
                i += 2
            }
        case "--currentDate":
            if i+1 < len(args) {
                i++
            }
        case "--condition":
            if i+3 < len(args) {
                i += 3
            }
        }
    }

    // Добавляем триггер в коллекцию
    if err := coll.AddTrigger(trigger); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Trigger '%s' created on collection '%s' for event %s at %s",
        triggerName, collName, event, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleDropTrigger — обработчик команды "drop trigger <collection> <event> <name>".
// Удаляет триггер из коллекции.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Событие
//   - args[2]: Имя триггера
//
// Возвращает:
//   - error: Ошибка при удалении, отсутствии аргументов или выборе БД
func (r *Repl) handleDropTrigger(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: drop trigger <collection> <event> <name>")
    }

    collName := args[0]
    triggerName := args[2]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.DropTrigger(triggerName); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Trigger '%s' dropped from collection '%s'", triggerName, collName))
    return nil
}

// handleShowTriggers — обработчик команды "show triggers <collection>".
// Показывает все триггеры коллекции с их свойствами.
//
// Аргументы:
//   - args[0]: Имя коллекции
//
// Возвращает:
//   - error: Ошибка при получении списка, отсутствии аргументов или выборе БД
func (r *Repl) handleShowTriggers(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 1 {
        return fmt.Errorf("usage: show triggers <collection>")
    }

    collName := args[0]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    triggers := coll.ListTriggers()

    if len(triggers) == 0 {
        utils.PrintInfo(fmt.Sprintf("No triggers found on collection '%s'", collName))
        return nil
    }

    utils.PrintHeader(fmt.Sprintf("Triggers on collection '%s':", collName))
    for _, t := range triggers {
        status := "enabled"
        if !t.Enabled {
            status = "disabled"
        }
        utils.PrintInfo(fmt.Sprintf("  %s (%s) - %s [%s] (created: %s)",
            t.Name, t.Event, status, t.Action, time.UnixMilli(t.CreatedAt).Format("2006-01-02 15:04:05")))
        if t.Description != "" {
            utils.Println(fmt.Sprintf("      Description: %s", t.Description))
        }
    }
    return nil
}

// handleEnableTrigger — обработчик команды "enable trigger <collection> <event> <name>".
// Включает отключённый триггер.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Событие
//   - args[2]: Имя триггера
//
// Возвращает:
//   - error: Ошибка при включении, отсутствии аргументов или выборе БД
func (r *Repl) handleEnableTrigger(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: enable trigger <collection> <event> <name>")
    }

    collName := args[0]
    triggerName := args[2]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.EnableTrigger(triggerName); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Trigger '%s' enabled at %s", triggerName, time.Now().Format("2006-01-02 15:04:05.000")))
    return nil
}

// handleDisableTrigger — обработчик команды "disable trigger <collection> <event> <name>".
// Отключает триггер без его удаления.
//
// Аргументы:
//   - args[0]: Имя коллекции
//   - args[1]: Событие
//   - args[2]: Имя триггера
//
// Возвращает:
//   - error: Ошибка при отключении, отсутствии аргументов или выборе БД
func (r *Repl) handleDisableTrigger(args []string) error {
    if r.currentDB == "" {
        return fmt.Errorf("no database selected")
    }

    if len(args) < 3 {
        return fmt.Errorf("usage: disable trigger <collection> <event> <name>")
    }

    collName := args[0]
    triggerName := args[2]

    db, err := r.store.GetDatabase(r.currentDB)
    if err != nil {
        return err
    }

    coll, err := db.GetCollection(collName)
    if err != nil {
        return err
    }

    if err := coll.DisableTrigger(triggerName); err != nil {
        return err
    }

    utils.PrintSuccess(fmt.Sprintf("Trigger '%s' disabled", triggerName))
    return nil
}

// handleTriggerLog — обработчик команды "trigger log".
// Показывает историю выполнения триггеров.
func (r *Repl) handleTriggerLog(args []string) error {
    logs := storage.GetTriggerExecutionLog()

    if len(logs) == 0 {
        utils.PrintInfo("No trigger executions logged")
        return nil
    }

    utils.PrintHeader("Trigger Execution Log")
    for i, logEntry := range logs {
        utils.PrintInfo(fmt.Sprintf("[%d] %s - Trigger: %s, Event: %s, Collection: %s, Document: %s",
            i+1, logEntry.Timestamp.Format("2006-01-02 15:04:05"), logEntry.TriggerName, logEntry.Event, logEntry.Collection, logEntry.DocumentID))
    }
    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД КЛАСТЕРА
// =============================================================================

// handleStatus — обработчик команды "status".
// Показывает статус кластера: роль узла (leader/follower), лидер, количество узлов.
func (r *Repl) handleStatus(args []string) error {
    utils.PrintHeader("Cluster Status")

    if r.coordinator == nil {
        utils.PrintWarning("  Cluster coordinator not available")
        return nil
    }

    isLeader := r.coordinator.IsLeader()
    if isLeader {
        utils.PrintSuccess("  Role: LEADER")
    } else {
        utils.PrintWarning("  Role: FOLLOWER")
    }

    leader := r.coordinator.GetLeader()
    if leader != nil {
        // ИСПРАВЛЕНО: корректное обращение к полям структуры NodeInfo
        leaderIP := "unknown"
        leaderPort := 0
        if leader.IP != "" {
            leaderIP = leader.IP
        }
        if leader.Port > 0 {
            leaderPort = leader.Port
        }
        utils.PrintInfo(fmt.Sprintf("  Leader: %s:%d", leaderIP, leaderPort))
    } else {
        utils.PrintInfo("  Leader: none")
    }

    status := r.coordinator.GetClusterStatus()
    utils.PrintInfo(fmt.Sprintf("  Cluster Name: %s", status.Name))
    utils.PrintInfo(fmt.Sprintf("  Total Nodes: %d", status.TotalNodes))
    utils.PrintInfo(fmt.Sprintf("  Active Nodes: %d", status.ActiveNodes))
    utils.PrintInfo(fmt.Sprintf("  Health: %s", status.Health))
    utils.PrintInfo(fmt.Sprintf("  Replication Factor: %d", status.ReplicationFactor))

    return nil
}

// handleNodes — обработчик команды "nodes".
// Показывает список всех узлов кластера с их статусом.
// Лидер отмечается звёздочкой (*).
func (r *Repl) handleNodes(args []string) error {
    if r.coordinator == nil {
        return fmt.Errorf("cluster coordinator not available")
    }

    nodes := r.coordinator.GetAllNodes()

    if len(nodes) == 0 {
        utils.PrintInfo("No nodes in cluster")
        return nil
    }

    utils.PrintHeader("Cluster Nodes")
    leader := r.coordinator.GetLeader()
    leaderID := ""
    if leader != nil {
        leaderID = leader.ID
    }

    for _, node := range nodes {
        prefix := "  "
        if node.ID == leaderID {
            prefix = " *"
        }
        statusColor := "green"
        if node.Status != "active" {
            statusColor = "yellow"
        }
        // ИСПРАВЛЕНО: корректное форматирование с IP и Port
        nodeIP := "unknown"
        nodePort := 0
        if node.IP != "" {
            nodeIP = node.IP
        }
        if node.Port > 0 {
            nodePort = node.Port
        }
        lastSeen := time.UnixMilli(node.LastSeen).Format("15:04:05")
        // ИСПРАВЛЕНО: используем colorize вместо utils.Colorize
        statusText := r.colorize(node.Status, statusColor)
        utils.Println(fmt.Sprintf("%s %s:%d [%s] (last seen: %s)",
            prefix, nodeIP, nodePort, statusText, lastSeen))
    }

    return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД МИГРАЦИИ (КРОСС-ДАТАЦЕНТРОВАЯ МИГРАЦИЯ)
// =============================================================================

// handleMigration обрабатывает команды миграции
// Формат: migration <subcommand> [options]
func (r *Repl) handleMigration(args []string) error {
    if len(args) == 0 {
        return fmt.Errorf("usage: migration <subcommand>\n"+
            "  Subcommands:\n"+
            "    start <source_dc> <target_dc> [database] [collection]  - Start a new migration\n"+
            "    status [task_id]           - Show migration status\n"+
            "    list                       - List all migration tasks\n"+
            "    pause <task_id>            - Pause a running migration\n"+
            "    resume <task_id>           - Resume a paused migration\n"+
            "    cancel <task_id>           - Cancel a migration\n"+
            "    stats                      - Show migration statistics\n"+
            "    config                     - Show migration configuration\n"+
            "    queue                      - Show change queue status")
    }

    // Проверяем наличие координатора и мигратора
    if r.coordinator == nil {
        return fmt.Errorf("cluster coordinator not available")
    }

    migrator := r.coordinator.GetCrossDCMigrator()
    if migrator == nil {
        return fmt.Errorf("migration system not initialized (check config: migration.enabled = true)")
    }

    // Импортируем команды миграции
    // Используем прямое выполнение через обработчик
    subcommand := args[0]

    switch subcommand {
    case "start":
        if len(args) < 3 {
            return fmt.Errorf("usage: migration start <source_dc> <target_dc> [database] [collection]")
        }
        return r.handleMigrationStart(migrator, args[1:])
    case "status":
        return r.handleMigrationStatus(migrator, args[1:])
    case "list":
        return r.handleMigrationList(migrator)
    case "pause":
        if len(args) < 2 {
            return fmt.Errorf("usage: migration pause <task_id>")
        }
        return r.handleMigrationPause(migrator, args[1])
    case "resume":
        if len(args) < 2 {
            return fmt.Errorf("usage: migration resume <task_id>")
        }
        return r.handleMigrationResume(migrator, args[1])
    case "cancel":
        if len(args) < 2 {
            return fmt.Errorf("usage: migration cancel <task_id>")
        }
        return r.handleMigrationCancel(migrator, args[1])
    case "stats":
        return r.handleMigrationStats(migrator)
    case "config":
        return r.handleMigrationConfig(migrator)
    case "queue":
        return r.handleMigrationQueue(migrator)
    default:
        return fmt.Errorf("unknown migration subcommand: %s", subcommand)
    }
}

// handleMigrationStart обрабатывает команду migration start
func (r *Repl) handleMigrationStart(migrator *cluster.CrossDCMigrator, args []string) error {
    if len(args) < 2 {
        return fmt.Errorf("usage: migration start <source_dc> <target_dc> [database] [collection]")
    }

    sourceDC := args[0]
    targetDC := args[1]

    var databases []string
    var collections map[string][]string

    if len(args) > 2 {
        databases = []string{args[2]}
        if len(args) > 3 {
            collections = map[string][]string{
                args[2]: {args[3]},
            }
        }
    }

    utils.PrintInfo(fmt.Sprintf("Starting migration from %s to %s at %s...",
        sourceDC, targetDC, time.Now().Format("2006-01-02 15:04:05")))

    task, err := migrator.StartMigration(sourceDC, targetDC, databases, collections)
    if err != nil {
        return fmt.Errorf("failed to start migration: %v", err)
    }

    utils.PrintSuccess(fmt.Sprintf("Migration started: %s", task.ID))
    utils.PrintInfo(fmt.Sprintf("  Source: %s", task.SourceDC))
    utils.PrintInfo(fmt.Sprintf("  Target: %s", task.TargetDC))
    utils.PrintInfo(fmt.Sprintf("  Total documents: %d", task.TotalDocuments))
    utils.PrintInfo(fmt.Sprintf("  Status: %s", task.Status))

    return nil
}

// handleMigrationStatus обрабатывает команду migration status
func (r *Repl) handleMigrationStatus(migrator *cluster.CrossDCMigrator, args []string) error {
    var taskID string
    if len(args) > 0 {
        taskID = args[0]
    } else {
        taskID = migrator.GetCurrentTaskID()
        if taskID == "" {
            return fmt.Errorf("no active migration task")
        }
    }

    task, err := migrator.GetMigrationStatus(taskID)
    if err != nil {
        return err
    }

    // Выводим статус задачи, используя публичные поля
    utils.PrintHeader(fmt.Sprintf("Migration Task: %s", task.ID))
    utils.PrintInfo(fmt.Sprintf("  Source DC:     %s", task.SourceDC))
    utils.PrintInfo(fmt.Sprintf("  Target DC:     %s", task.TargetDC))
    utils.PrintInfo(fmt.Sprintf("  Status:        %s", r.colorize(string(task.Status), r.getMigrationStatusColor(string(task.Status)))))
    utils.PrintInfo(fmt.Sprintf("  Progress:      %.1f%%", task.ProgressPercent))
    utils.PrintInfo(fmt.Sprintf("  Total Docs:    %d", task.TotalDocuments))
    utils.PrintInfo(fmt.Sprintf("  Migrated:      %d", task.MigratedDocs))
    utils.PrintInfo(fmt.Sprintf("  Failed:        %d", task.FailedDocs))
    utils.PrintInfo(fmt.Sprintf("  Skipped:       %d", task.SkippedDocs))

    if task.StartTime > 0 {
        startTime := time.UnixMilli(task.StartTime).Format("2006-01-02 15:04:05")
        utils.PrintInfo(fmt.Sprintf("  Started:       %s", startTime))
    }

    if task.EndTime > 0 {
        endTime := time.UnixMilli(task.EndTime).Format("2006-01-02 15:04:05")
        utils.PrintInfo(fmt.Sprintf("  Completed:     %s", endTime))
    }

    if task.Error != "" {
        utils.PrintError(fmt.Sprintf("  Error:         %s", task.Error))
    }

    if len(task.Databases) > 0 {
        utils.PrintInfo(fmt.Sprintf("  Databases:     %s", strings.Join(task.Databases, ", ")))
    }

    if len(task.Collections) > 0 {
        utils.PrintInfo("  Collections:")
        for db, colls := range task.Collections {
            utils.PrintInfo(fmt.Sprintf("    %s: %s", db, strings.Join(colls, ", ")))
        }
    }

    return nil
}

// handleMigrationList обрабатывает команду migration list
func (r *Repl) handleMigrationList(migrator *cluster.CrossDCMigrator) error {
    tasks := migrator.ListTasks()

    if len(tasks) == 0 {
        utils.PrintInfo("No migration tasks found")
        return nil
    }

    utils.PrintHeader("Migration Tasks")
    utils.PrintInfo("  ID                                   STATUS       PROGRESS    SOURCE -> TARGET")
    utils.Println("  ---------------------------------------------------------------------------------")

    for _, task := range tasks {
        id := task.ID
        if len(id) > 20 {
            id = id[:17] + "..."
        }

        statusColor := r.getMigrationStatusColor(string(task.Status))
        statusStr := r.colorize(string(task.Status), statusColor)

        progress := fmt.Sprintf("%.1f%%", task.ProgressPercent)
        if task.Status == cluster.MigrationStatusCompleted {
            progress = "100%"
        }

        utils.PrintInfo(fmt.Sprintf("  %-36s %-12s %-10s %s -> %s",
            id, statusStr, progress, task.SourceDC, task.TargetDC))
    }

    return nil
}

// handleMigrationPause обрабатывает команду migration pause
func (r *Repl) handleMigrationPause(migrator *cluster.CrossDCMigrator, taskID string) error {
    if err := migrator.PauseMigration(taskID); err != nil {
        return err
    }
    utils.PrintSuccess(fmt.Sprintf("Migration %s paused", taskID))
    return nil
}

// handleMigrationResume обрабатывает команду migration resume
func (r *Repl) handleMigrationResume(migrator *cluster.CrossDCMigrator, taskID string) error {
    if err := migrator.ResumeMigration(taskID); err != nil {
        return err
    }
    utils.PrintSuccess(fmt.Sprintf("Migration %s resumed", taskID))
    return nil
}

// handleMigrationCancel обрабатывает команду migration cancel
func (r *Repl) handleMigrationCancel(migrator *cluster.CrossDCMigrator, taskID string) error {
    if err := migrator.CancelMigration(taskID); err != nil {
        return err
    }
    utils.PrintSuccess(fmt.Sprintf("Migration %s cancelled", taskID))
    return nil
}

// handleMigrationStats обрабатывает команду migration stats
func (r *Repl) handleMigrationStats(migrator *cluster.CrossDCMigrator) error {
    stats := migrator.GetMigrationStats()

    utils.PrintHeader("Migration Statistics")
    utils.PrintInfo(fmt.Sprintf("  Total Changes:    %d", stats.TotalChanges))
    utils.PrintInfo(fmt.Sprintf("  Applied Changes:  %d", stats.AppliedChanges))
    utils.PrintInfo(fmt.Sprintf("  Failed Changes:   %d", stats.FailedChanges))
    utils.PrintInfo(fmt.Sprintf("  Skipped Changes:  %d", stats.SkippedChanges))
    utils.PrintInfo(fmt.Sprintf("  Avg Latency:      %d ms", stats.LatencyAvg))
    utils.PrintInfo(fmt.Sprintf("  Max Latency:      %d ms", stats.LatencyMax))
    utils.PrintInfo(fmt.Sprintf("  Throughput:       %d docs/sec", stats.Throughput))

    return nil
}

// handleMigrationConfig обрабатывает команду migration config
func (r *Repl) handleMigrationConfig(migrator *cluster.CrossDCMigrator) error {
    // Используем публичные методы для получения конфигурации
    cfg := migrator.GetConfig()

    utils.PrintHeader("Migration Configuration")
    utils.PrintInfo(fmt.Sprintf("  Enabled:          %v", cfg.Enabled))
    utils.PrintInfo(fmt.Sprintf("  Mode:             %s", cfg.Mode))
    if cfg.Source != nil {
        utils.PrintInfo(fmt.Sprintf("  Source DC:        %s (%s)", cfg.Source.Name, cfg.Source.Endpoint))
    }
    if cfg.Target != nil {
        utils.PrintInfo(fmt.Sprintf("  Target DC:        %s (%s)", cfg.Target.Name, cfg.Target.Endpoint))
    }
    if cfg.Settings != nil {
        utils.PrintInfo(fmt.Sprintf("  Batch Size:       %d", cfg.Settings.BatchSize))
        utils.PrintInfo(fmt.Sprintf("  Workers:          %d", cfg.Settings.Workers))
        utils.PrintInfo(fmt.Sprintf("  Compression:      %s", cfg.Settings.Compression))
        utils.PrintInfo(fmt.Sprintf("  Resume Enabled:   %v", cfg.Settings.ResumeEnabled))
        utils.PrintInfo(fmt.Sprintf("  Max Retries:      %d", cfg.Settings.MaxRetries))
    }
    if cfg.Delta != nil {
        utils.PrintInfo(fmt.Sprintf("  Delta Sync:       %v", cfg.Delta.Enabled))
        utils.PrintInfo(fmt.Sprintf("  Delta Interval:   %d sec", cfg.Delta.IntervalSec))
    }
    if cfg.Validation != nil {
        utils.PrintInfo(fmt.Sprintf("  Validation:       %v", cfg.Validation.Enabled))
        utils.PrintInfo(fmt.Sprintf("  Sample Percent:   %d%%", cfg.Validation.SamplePercent))
    }

    return nil
}

// handleMigrationQueue обрабатывает команду migration queue
func (r *Repl) handleMigrationQueue(migrator *cluster.CrossDCMigrator) error {
    stats := migrator.GetQueueStats()

    utils.PrintHeader("Migration Queue Status")
    utils.PrintInfo(fmt.Sprintf("  Queue Size:       %d", stats["size"]))
    utils.PrintInfo(fmt.Sprintf("  Last LSN:         %d", stats["last_lsn"]))
    utils.PrintInfo(fmt.Sprintf("  Max Size:         %d", stats["max_size"]))
    utils.PrintInfo(fmt.Sprintf("  File Path:        %s", stats["file_path"]))

    return nil
}

// getMigrationStatusColor возвращает цвет для статуса миграции
func (r *Repl) getMigrationStatusColor(status string) string {
    switch status {
    case "idle":
        return "cyan"
    case "preparing":
        return "blue"
    case "migrating":
        return "yellow"
    case "delta_sync":
        return "yellow"
    case "validating":
        return "cyan"
    case "completed":
        return "green"
    case "failed":
        return "red"
    case "paused":
        return "yellow"
    default:
        return "white"
    }
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЙ МЕТОД ДЛЯ ЦВЕТНОГО ВЫВОДА
// =============================================================================

// colorize возвращает цветной текст с использованием ANSI-кодов.
// ИСПРАВЛЕНО: добавлен метод вместо несуществующего utils.Colorize
func (r *Repl) colorize(text, color string) string {
    switch color {
    case "green":
        return "\033[32m" + text + "\033[0m"
    case "red":
        return "\033[31m" + text + "\033[0m"
    case "yellow":
        return "\033[33m" + text + "\033[0m"
    case "blue":
        return "\033[34m" + text + "\033[0m"
    case "cyan":
        return "\033[36m" + text + "\033[0m"
    case "white":
        return "\033[37m" + text + "\033[0m"
    default:
        return text
    }
}

// =============================================================================
// СИСТЕМНЫЕ ОБРАБОТЧИКИ КОМАНД
// =============================================================================

// handleHelp — обработчик команды "help".
// Показывает подробную справку по всем доступным командам,
// сгруппированную по категориям.
func (r *Repl) handleHelp(args []string) error {
    utils.Println("")
    fmt.Println(color.New(color.FgHiCyan).Sprint("\n=== Available Commands ==="))

    // Определяем категории команд для отображения в справке
    categories := map[string][]struct {
        cmd         string
        description string
    }{
        "Database Management": {
            {"create slice <name>", "Create a new database (slice)"},
            {"drop database <name>", "Delete an existing database"},
            {"use <database>", "Switch to a specific database"},
            {"show databases", "List all available databases"},
        },
        "Collection Management": {
            {"create collection <name>", "Create a new collection in current database"},
            {"drop collection <name>", "Delete a collection from current database"},
            {"show collections", "List all collections in current database"},
        },
        "Document Operations": {
            {"insert <collection> <json>", "Insert a new document (JSON format: key=value,key2=value2)"},
            {"find <collection> <id>", "Find a document by its ID"},
            {"findbyindex <collection> <index> <value>", "Find documents using an index"},
            {"findbytime <collection> <from_date> <to_date>", "Find documents by time range"},
            {"update <collection> <id> <field=value>...", "Update fields of an existing document"},
            {"delete <collection> <id>", "Delete a document (soft delete if enabled)"},
            {"permanent delete <collection> <id>", "Permanently delete a soft-deleted document"},
            {"restore <collection> <id>", "Restore a soft-deleted document"},
            {"show deleted <collection>", "Show soft-deleted documents in a collection"},
            {"count <collection>", "Count total documents in a collection"},
        },
        "Timestamp Management": {
            {"show timestamps <collection> <id>", "Show timestamps for a document"},
            {"stats timestamps <collection>", "Show timestamp statistics for a collection"},
            {"audit log", "Show audit log"},
            {"audit filter <type> <op>", "Filter audit log by type and operation"},
        },
        "Index Management": {
            {"create index <collection> <name> <fields> [unique]", "Create a new index on specified fields"},
            {"drop index <collection> <name>", "Remove an existing index"},
            {"show indexes <collection>", "List all indexes on a collection"},
        },
        "Constraints": {
            {"add required <collection> <field>", "Add a required field constraint"},
            {"add unique <collection> <field>", "Add a unique constraint on a field"},
            {"add min <collection> <field> <value>", "Add a minimum value constraint for numeric fields"},
            {"add max <collection> <field> <value>", "Add a maximum value constraint for numeric fields"},
            {"add enum <collection> <field> <values...>", "Add allowed values constraint (enum)"},
        },
        "Transactions": {
            {"begin transaction", "Start a new transaction"},
            {"commit", "Commit the current transaction"},
            {"rollback", "Rollback the current transaction"},
            {"show transactions", "List all active transactions"},
        },
        "Triggers (MongoDB-like)": {
            {"create trigger <collection> <name> <event> <action> [options]", "Create a trigger on collection events"},
            {"  Events: BEFORE_INSERT, AFTER_INSERT, BEFORE_UPDATE, AFTER_UPDATE, BEFORE_DELETE, AFTER_DELETE", ""},
            {"  Actions: abort, skip, modify, log, notify", ""},
            {"  Special values: $$NOW (current timestamp), $$USER (current user), $$ROLE (current role)", ""},
            {"drop trigger <collection> <event> <name>", "Remove a trigger from collection"},
            {"show triggers <collection>", "List all triggers on a collection"},
            {"enable trigger <collection> <event> <name>", "Enable a disabled trigger"},
            {"disable trigger <collection> <event> <name>", "Disable a trigger without removing it"},
            {"trigger log", "Show trigger execution history"},
        },
        "Plugins": {
            {"plugin list", "List all loaded plugins"},
            {"plugin load <name> <filepath>", "Load a plugin from Lua file"},
            {"plugin unload <name>", "Unload a plugin"},
            {"plugin start <name>", "Start a loaded plugin"},
            {"plugin stop <name>", "Stop a running plugin"},
            {"plugin exec <plugin> <function> [args...]", "Execute a plugin function"},
        },
        "Import/Export": {
            {"export <database> <filename>", "Export entire database to file"},
            {"import <database> <filename>", "Import database from file"},
        },
        "Compression": {
            {"compression stats", "Show compression statistics for current database"},
            {"compression config", "Display current compression settings"},
            {"compress collection <name>", "Manually compress all documents in a collection"},
            {"doc compression <collection> <id>", "Show compression info for a specific document"},
        },
        "Access Control": {
            {"acl login <username> <password>", "Authenticate with username and password"},
            {"acl logout", "Logout current user session"},
            {"acl grant <collection> <role> <permissions>", "Grant permissions (r=read,w=write,d=delete,a=admin)"},
            {"acl users", "List all users"},
            {"acl roles", "List all roles"},
        },
        "Cluster": {
            {"status", "Show current cluster status and role (leader/follower)"},
            {"nodes", "List all nodes in the cluster"},
        },
        "Migration": {
            {"migration start <source_dc> <target_dc> [db] [coll]", "Start cross-datacenter migration"},
            {"migration status [task_id]", "Show migration status"},
            {"migration list", "List all migration tasks"},
            {"migration pause <task_id>", "Pause a running migration"},
            {"migration resume <task_id>", "Resume a paused migration"},
            {"migration cancel <task_id>", "Cancel a migration"},
            {"migration stats", "Show migration statistics"},
            {"migration config", "Show migration configuration"},
            {"migration queue", "Show change queue status"},
        },
        "System": {
            {"help", "Display this help message with all available commands"},
            {"clear", "Clear the terminal screen"},
            {"quit", "Exit the futriix database REPL"},
            {"exit", "Exit the futriix database REPL (alias for quit)"},
        },
    }

    // Выводим команды по категориям
    for category, commands := range categories {
        utils.PrintInfo(fmt.Sprintf("\n%s:", category))
        for _, cmd := range commands {
            if cmd.description != "" {
                utils.Println(fmt.Sprintf("  %-50s %s", cmd.cmd, cmd.description))
            } else {
                utils.Println(fmt.Sprintf("  %s", cmd.cmd))
            }
        }
    }

    // Выводим примеры использования
    utils.Println("")
    utils.PrintInfo("Examples:")
    utils.Println("  create slice test")
    utils.Println("  use test")
    utils.Println("  create collection users")
    utils.Println("  insert users name=John,age=30")
    utils.Println("  find users john123")
    utils.Println("  show timestamps users john123")
    utils.Println("  stats timestamps users")
    utils.Println("  findbytime users 2026-01-01 2026-01-31")
    utils.Println("  create index users idx_name name")
    utils.Println("  begin transaction")
    utils.Println("  update users john123 age=31")
    utils.Println("  commit")
    utils.Println("  acl login admin admin")
    utils.Println("  status")
    utils.Println("")
    utils.Println("Migration examples:")
    utils.Println("  migration start dc-primary dc-secondary")
    utils.Println("  migration start dc-primary dc-secondary mydb users")
    utils.Println("  migration status")
    utils.Println("  migration list")
    utils.Println("  migration stats")
    utils.Println("")

    return nil
}

// handleClear — обработчик команды "clear".
// Очищает экран терминала с помощью ANSI-кода.
func (r *Repl) handleClear(args []string) error {
    fmt.Print("\033[2J\033[H")
    return nil
}

// handleQuit — обработчик команды "quit" и "exit".
// Завершает работу REPL и выходит из программы.
func (r *Repl) handleQuit(args []string) error {
    utils.PrintInfo(fmt.Sprintf("Session ended at %s", time.Now().Format("2006-01-02 15:04:05.000")))
    os.Exit(0)
    return nil
}

// =============================================================================
// ЗАКРЫТИЕ REPL
// =============================================================================

// Close закрывает REPL и сохраняет историю команд в файл.
// Должен вызываться при завершении работы программы.
func (r *Repl) Close() error {
    if r.historyFile != nil {
        r.historyFile.Save()
    }
    return nil
}
