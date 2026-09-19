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
// Назначение: Интерактивный интерфейс командной строки (REPL) для СУБД futriix.
// Поддерживает:
//   - автодополнение (Tab) команд, коллекций, полей, операторов;
//   - историю ввода с навигацией PgUp/PgDown и стрелками вверх/вниз;
//   - многострочный ввод (обратный слэш в конце строки, незакрытые скобки);
//   - пейджер для длинных результатов;
//   - табличный вывод по умолчанию.
// Совместимо с Linux и OpenIndiana.
// =============================================================================

package repl

import (
	"errors"
	"fmt"
	"io"
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

	"github.com/chzyer/readline"
	"github.com/fatih/color"
)

// =============================================================================
// ТИПЫ ДАННЫХ
// =============================================================================

// Repl представляет основную структуру REPL.
type Repl struct {
	store         *storage.Storage
	coordinator   *cluster.RaftCoordinator
	logger        *log.Logger
	config        *config.Config
	aclManager    *acl.ACLManager
	pluginManager *plugin.PluginManager

	// Терминальный ввод/вывод.
	rl        *readline.Instance
	interrupt bool

	currentDB     string
	currentUser   string
	currentRole   string
	authenticated bool
	sessionID     string

	commands    map[string]*Command
	history     []string
	historyPos  int
	historyFile *History

	pager     *Pager
	completer *Completer
}

// Command представляет отдельную команду REPL.
type Command struct {
	Name        string
	Description string
	Handler     func(args []string) error
}

// =============================================================================
// КОНСТРУКТОРЫ
// =============================================================================

// NewRepl создаёт новый экземпляр REPL с использованием chzyer/readline.
func NewRepl(
	store *storage.Storage,
	coordinator *cluster.RaftCoordinator,
	logger *log.Logger,
	cfg *config.Config,
	aclManager *acl.ACLManager,
	pluginManager *plugin.PluginManager,
) (*Repl, error) {

	r := &Repl{
		store:         store,
		coordinator:   coordinator,
		logger:        logger,
		config:        cfg,
		aclManager:    aclManager,
		pluginManager: pluginManager,
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

	// Пейджер.
	r.pager = NewPager(true, 24)

	// Загружаем историю из файла.
	if err := r.historyFile.Load(); err == nil {
		r.history = r.historyFile.GetEntries()
		r.historyPos = len(r.history)
	}

	// Регистрируем команды и автодополнение.
	r.registerCommands()
	r.completer = NewCompleter(r)

	// Настраиваем readline.
	// ВАЖНО: AutoComplete задаётся один раз здесь, при создании Instance.
	// Метода SetAutoCompleter у *readline.Instance не существует.
	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 r.buildPrompt(),
		HistoryFile:            r.historyFile.FilePath(),
		HistoryLimit:           cfg.Repl.HistorySize,
		HistorySearchFold:      true,
		AutoComplete:           r.completer.ReadlineCompleter(),
		InterruptPrompt:        "^C",
		EOFPrompt:              "exit",
		DisableAutoSaveHistory: true, // сохраняем сами через historyFile
		VimMode:                false, // emacs-режим как в bash
		FuncFilterInputRune: func(rn rune) (rune, bool) {
			// Блокируем Ctrl+Z, чтобы случайно не свернуть процесс.
			if rn == readline.CharCtrlZ {
				return rn, false
			}
			return rn, true
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize readline: %w", err)
	}

	r.rl = rl
	return r, nil
}

// =============================================================================
// РЕГИСТРАЦИЯ КОМАНД
// =============================================================================

func (r *Repl) registerCommands() {
	// -------------------------------------------------------------------------
	// Базы данных
	// -------------------------------------------------------------------------
	r.commands["create slice"] = &Command{Name: "create slice", Description: "Create a new database (slice)", Handler: r.handleCreateSlice}
	r.commands["drop database"] = &Command{Name: "drop database", Description: "Drop a database", Handler: r.handleDropDatabase}
	r.commands["use"] = &Command{Name: "use", Description: "Switch to a database", Handler: r.handleUseDatabase}
	r.commands["show databases"] = &Command{Name: "show databases", Description: "List all databases", Handler: r.handleShowDatabases}

	// -------------------------------------------------------------------------
	// Коллекции
	// -------------------------------------------------------------------------
	r.commands["create collection"] = &Command{Name: "create collection", Description: "Create a new collection in current database", Handler: r.handleCreateCollection}
	r.commands["drop collection"] = &Command{Name: "drop collection", Description: "Drop a collection from current database", Handler: r.handleDropCollection}
	r.commands["show collections"] = &Command{Name: "show collections", Description: "List all collections in current database", Handler: r.handleShowCollections}

	// -------------------------------------------------------------------------
	// Документы
	// -------------------------------------------------------------------------
	r.commands["insert"] = &Command{Name: "insert", Description: "Insert a document into a collection (JSON format)", Handler: r.handleInsert}
	r.commands["find"] = &Command{Name: "find", Description: "Find a document by ID", Handler: r.handleFind}
	r.commands["findbyindex"] = &Command{Name: "findbyindex", Description: "Find documents by index", Handler: r.handleFindByIndex}
	r.commands["findbytime"] = &Command{Name: "findbytime", Description: "Find documents by time range (created_at)", Handler: r.handleFindByTime}
	r.commands["update"] = &Command{Name: "update", Description: "Update a document", Handler: r.handleUpdate}
	r.commands["delete"] = &Command{Name: "delete", Description: "Delete a document (soft delete if enabled)", Handler: r.handleDelete}
	r.commands["permanent delete"] = &Command{Name: "permanent delete", Description: "Permanently delete a soft-deleted document", Handler: r.handlePermanentDelete}
	r.commands["restore"] = &Command{Name: "restore", Description: "Restore a soft-deleted document", Handler: r.handleRestore}
	r.commands["show deleted"] = &Command{Name: "show deleted", Description: "Show soft-deleted documents in a collection", Handler: r.handleShowDeleted}
	r.commands["count"] = &Command{Name: "count", Description: "Count documents in a collection", Handler: r.handleCount}
	r.commands["show timestamps"] = &Command{Name: "show timestamps", Description: "Show timestamps for a document", Handler: r.handleShowTimestamps}
	r.commands["stats timestamps"] = &Command{Name: "stats timestamps", Description: "Show timestamp statistics for a collection", Handler: r.handleStatsTimestamps}

	// -------------------------------------------------------------------------
	// Индексы
	// -------------------------------------------------------------------------
	r.commands["create index"] = &Command{Name: "create index", Description: "Create an index on a collection", Handler: r.handleCreateIndex}
	r.commands["drop index"] = &Command{Name: "drop index", Description: "Drop an index from a collection", Handler: r.handleDropIndex}
	r.commands["show indexes"] = &Command{Name: "show indexes", Description: "Show all indexes in a collection", Handler: r.handleShowIndexes}

	// -------------------------------------------------------------------------
	// Ограничения
	// -------------------------------------------------------------------------
	r.commands["add required"] = &Command{Name: "add required", Description: "Add a required field constraint", Handler: r.handleAddRequired}
	r.commands["add unique"] = &Command{Name: "add unique", Description: "Add a unique constraint", Handler: r.handleAddUnique}
	r.commands["add min"] = &Command{Name: "add min", Description: "Add a minimum value constraint", Handler: r.handleAddMin}
	r.commands["add max"] = &Command{Name: "add max", Description: "Add a maximum value constraint", Handler: r.handleAddMax}
	r.commands["add enum"] = &Command{Name: "add enum", Description: "Add an enum constraint (allowed values)", Handler: r.handleAddEnum}

	// -------------------------------------------------------------------------
	// Триггеры
	// -------------------------------------------------------------------------
	r.commands["create trigger"] = &Command{Name: "create trigger", Description: "Create a trigger on a collection (MongoDB-like syntax)", Handler: r.handleCreateTrigger}
	r.commands["drop trigger"] = &Command{Name: "drop trigger", Description: "Drop a trigger from a collection", Handler: r.handleDropTrigger}
	r.commands["show triggers"] = &Command{Name: "show triggers", Description: "Show all triggers on a collection", Handler: r.handleShowTriggers}
	r.commands["enable trigger"] = &Command{Name: "enable trigger", Description: "Enable a trigger", Handler: r.handleEnableTrigger}
	r.commands["disable trigger"] = &Command{Name: "disable trigger", Description: "Disable a trigger", Handler: r.handleDisableTrigger}
	r.commands["trigger log"] = &Command{Name: "trigger log", Description: "Show trigger execution log", Handler: r.handleTriggerLog}

	// -------------------------------------------------------------------------
	// Транзакции
	// -------------------------------------------------------------------------
	r.commands["begin transaction"] = &Command{Name: "begin transaction", Description: "Start a new transaction", Handler: r.handleBeginTransaction}
	r.commands["commit"] = &Command{Name: "commit", Description: "Commit current transaction", Handler: r.handleCommitTransaction}
	r.commands["rollback"] = &Command{Name: "rollback", Description: "Rollback current transaction", Handler: r.handleRollbackTransaction}
	r.commands["show transactions"] = &Command{Name: "show transactions", Description: "Show active transactions", Handler: r.handleShowTransactions}

	// -------------------------------------------------------------------------
	// Плагины
	// -------------------------------------------------------------------------
	r.commands["plugin list"] = &Command{Name: "plugin list", Description: "List all loaded plugins", Handler: r.handlePluginList}
	r.commands["plugin load"] = &Command{Name: "plugin load", Description: "Load a plugin from file", Handler: r.handlePluginLoad}
	r.commands["plugin unload"] = &Command{Name: "plugin unload", Description: "Unload a plugin", Handler: r.handlePluginUnload}
	r.commands["plugin start"] = &Command{Name: "plugin start", Description: "Start a plugin", Handler: r.handlePluginStart}
	r.commands["plugin stop"] = &Command{Name: "plugin stop", Description: "Stop a plugin", Handler: r.handlePluginStop}
	r.commands["plugin exec"] = &Command{Name: "plugin exec", Description: "Execute a plugin function", Handler: r.handlePluginExec}

	// -------------------------------------------------------------------------
	// Импорт/экспорт
	// -------------------------------------------------------------------------
	r.commands["export"] = &Command{Name: "export", Description: "Export database to MessagePack file", Handler: r.handleExport}
	r.commands["import"] = &Command{Name: "import", Description: "Import database from MessagePack file", Handler: r.handleImport}

	// -------------------------------------------------------------------------
	// ACL
	// -------------------------------------------------------------------------
	r.commands["acl login"] = &Command{Name: "acl login", Description: "Authenticate with username and password", Handler: r.handleACLLogin}
	r.commands["acl logout"] = &Command{Name: "acl logout", Description: "Logout current user session", Handler: r.handleACLLogout}
	r.commands["acl grant"] = &Command{Name: "acl grant", Description: "Grant permissions (r=read,w=write,d=delete,a=admin)", Handler: r.handleACLGrant}
	r.commands["acl users"] = &Command{Name: "acl users", Description: "List all users", Handler: r.handleACLUsers}
	r.commands["acl roles"] = &Command{Name: "acl roles", Description: "List all roles", Handler: r.handleACLRoles}

	// -------------------------------------------------------------------------
	// Сжатие
	// -------------------------------------------------------------------------
	r.commands["compression stats"] = &Command{Name: "compression stats", Description: "Show compression statistics for the database", Handler: r.handleCompressionStats}
	r.commands["compress collection"] = &Command{Name: "compress collection", Description: "Manually compress all documents in a collection", Handler: r.handleCompressCollection}
	r.commands["doc compression"] = &Command{Name: "doc compression", Description: "Show compression ratio for a document", Handler: r.handleDocCompression}
	r.commands["compression config"] = &Command{Name: "compression config", Description: "Show current compression configuration", Handler: r.handleCompressionConfig}

	// -------------------------------------------------------------------------
	// Аудит
	// -------------------------------------------------------------------------
	r.commands["audit log"] = &Command{Name: "audit log", Description: "Show audit log", Handler: r.handleAuditLog}
	r.commands["audit filter"] = &Command{Name: "audit filter", Description: "Filter audit log by type and operation", Handler: r.handleAuditFilter}

	// -------------------------------------------------------------------------
	// Кластер
	// -------------------------------------------------------------------------
	r.commands["status"] = &Command{Name: "status", Description: "Show cluster status", Handler: r.handleStatus}
	r.commands["nodes"] = &Command{Name: "nodes", Description: "List cluster nodes", Handler: r.handleNodes}

	// -------------------------------------------------------------------------
	// Миграция
	// -------------------------------------------------------------------------
	r.commands["migration"] = &Command{Name: "migration", Description: "Cross-datacenter migration commands", Handler: r.handleMigration}

	// -------------------------------------------------------------------------
	// Системные
	// -------------------------------------------------------------------------
	r.commands["help"] = &Command{Name: "help", Description: "Show this help message", Handler: r.handleHelp}
	r.commands["clear"] = &Command{Name: "clear", Description: "Clear the screen", Handler: r.handleClear}
	r.commands["quit"] = &Command{Name: "quit", Description: "Exit the REPL", Handler: r.handleQuit}
	r.commands["exit"] = &Command{Name: "exit", Description: "Exit the REPL", Handler: r.handleQuit}
}

// =============================================================================
// ОСНОВНОЙ ЦИКЛ REPL
// =============================================================================

// Run запускает основной цикл REPL с использованием readline.
func (r *Repl) Run() error {
	defer r.rl.Close()

	utils.Println("")
	utils.PrintInfo("Type 'help' for available commands")
	utils.PrintInfo("Multiline: end line with '\\' to continue, or use '{' ... '}'")
	utils.PrintInfo("PgUp/PgDown/↑/↓ — history, Tab — autocomplete, Ctrl+D — exit")
	utils.Println("")

	for {
		// Обновляем prompt (может измениться currentDB/currentUser).
		r.rl.SetPrompt(r.buildPrompt())

		line, err := r.rl.Readline()
		if err != nil {
			switch {
			case errors.Is(err, readline.ErrInterrupt):
				// Ctrl+C — прерываем текущий ввод, не выходим.
				utils.Println("^C")
				continue
			case errors.Is(err, io.EOF):
				// Ctrl+D — выход.
				return nil
			default:
				return err
			}
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Многострочный ввод.
		full, err := r.collectMultiline(line)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, readline.ErrInterrupt) {
				utils.Println("^C")
				continue
			}
			return err
		}
		if full == "" {
			continue
		}

		// Сохраняем в историю (readline + наш файл).
		r.addToHistory(full)

		if err := r.executeCommand(full); err != nil {
			utils.PrintError(err.Error())
			if r.logger != nil {
				r.logger.Error("REPL command error: " + err.Error())
			}
		}
	}
}

// collectMultiline продолжает ввод, если строка не закончена.
// Признаки продолжения:
//   - обратный слэш в конце;
//   - незакрытые {}, [], ().
//
// Пустая строка завершает многострочный ввод.
func (r *Repl) collectMultiline(first string) (string, error) {
	acc := first
	depthCurly, depthSquare, depthParen := countBrackets(acc)

	for {
		cont := strings.HasSuffix(acc, "\\") ||
			depthCurly > 0 || depthSquare > 0 || depthParen > 0

		if !cont {
			break
		}

		r.rl.SetPrompt("... ")
		next, err := r.rl.Readline()
		if err != nil {
			return "", err
		}
		r.rl.SetPrompt(r.buildPrompt())

		if strings.HasSuffix(acc, "\\") {
			acc = strings.TrimSuffix(acc, "\\")
		}

		next = strings.TrimSpace(next)
		if next == "" && depthCurly <= 0 && depthSquare <= 0 && depthParen <= 0 {
			break
		}

		acc += " " + next
		depthCurly, depthSquare, depthParen = countBrackets(acc)
	}

	return strings.TrimSpace(acc), nil
}

// countBrackets считает баланс {}, [], () с учётом строк.
func countBrackets(s string) (curly, square, paren int) {
	inString := false
	var quote rune
	escaped := false

	for _, ch := range s {
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && inString {
			escaped = true
			continue
		}
		if inString {
			if ch == quote {
				inString = false
			}
			continue
		}
		switch ch {
		case '"', '\'':
			inString = true
			quote = ch
		case '{':
			curly++
		case '}':
			curly--
		case '[':
			square++
		case ']':
			square--
		case '(':
			paren++
		case ')':
			paren--
		}
	}
	return
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
// =============================================================================

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

func (r *Repl) executeCommand(input string) error {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil
	}

	switch parts[0] {
	case "pager":
		if len(parts) > 1 {
			switch parts[1] {
			case "on":
				r.pager.SetEnabled(true)
				utils.PrintSuccess("Pager enabled")
			case "off":
				r.pager.SetEnabled(false)
				utils.PrintSuccess("Pager disabled")
			default:
				utils.PrintInfo(fmt.Sprintf("Pager: %v", r.pager.Enabled()))
			}
		} else {
			utils.PrintInfo(fmt.Sprintf("Pager: %v", r.pager.Enabled()))
		}
		return nil
	}

	for cmdName, cmd := range r.commands {
		if strings.HasPrefix(input, cmdName) {
			args := strings.TrimPrefix(input, cmdName)
			args = strings.TrimSpace(args)
			argList := strings.Fields(args)
			return cmd.Handler(argList)
		}
	}

	return fmt.Errorf("unknown command: %s", parts[0])
}

func (r *Repl) addToHistory(cmd string) {
	if len(r.history) > 0 && r.history[len(r.history)-1] == cmd {
		return
	}

	if len(r.history) >= r.config.Repl.HistorySize {
		r.history = r.history[1:]
	}
	r.history = append(r.history, cmd)
	r.historyPos = len(r.history)

	_ = r.historyFile.Add(cmd)
	_ = r.historyFile.Save()
}

// =============================================================================
// PgUp/PgDown — программная навигация
// =============================================================================
// Эти методы сохранены для случаев, когда readline недоступен (например,
// в неинтерактивном режиме или при юнит-тестах). В интерактивном режиме
// PgUp/PgDown обрабатываются самим readline.

// HandlePgUp возвращает предыдущую команду из истории.
func (r *Repl) HandlePgUp() string {
	if len(r.history) == 0 {
		return ""
	}
	if r.historyPos > 0 {
		r.historyPos--
	}
	if r.historyPos < 0 {
		r.historyPos = 0
	}
	return r.history[r.historyPos]
}

// HandlePgDown возвращает следующую команду из истории.
func (r *Repl) HandlePgDown() string {
	if len(r.history) == 0 {
		return ""
	}
	if r.historyPos < len(r.history)-1 {
		r.historyPos++
	} else {
		r.historyPos = len(r.history)
		return ""
	}
	return r.history[r.historyPos]
}

// =============================================================================
// ВЫВОД ЧЕРЕЗ ПЕЙДЖЕР
// =============================================================================

func (r *Repl) output(text string) {
	if r.pager != nil && r.pager.Enabled() {
		_ = r.pager.Page(text)
		return
	}
	fmt.Print(text)
}

// =============================================================================
// ЗАКРЫТИЕ
// =============================================================================

func (r *Repl) Close() error {
	if r.historyFile != nil {
		_ = r.historyFile.Save()
	}
	if r.rl != nil {
		return r.rl.Close()
	}
	return nil
}

// =============================================================================
// ОБРАБОТЧИКИ КОМАНД
// =============================================================================

// -----------------------------------------------------------------------------
// Базы данных
// -----------------------------------------------------------------------------

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

func (r *Repl) handleDropDatabase(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: drop database <name>")
	}
	name := args[0]
	if err := r.store.DropDatabase(name); err != nil {
		return err
	}
	if r.currentDB == name {
		r.currentDB = ""
	}
	utils.PrintSuccess(fmt.Sprintf("Database '%s' dropped at %s", name, time.Now().Format("2006-01-02 15:04:05.000")))
	return nil
}

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

func (r *Repl) handleShowDatabases(args []string) error {
	databases := r.store.ListDatabases()
	if len(databases) == 0 {
		utils.PrintInfo("No databases found")
		return nil
	}

	t := NewTable("DATABASE", "CURRENT")
	for _, db := range databases {
		cur := ""
		if db == r.currentDB {
			cur = "*"
		}
		t.AddRow(db, cur)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Коллекции
// -----------------------------------------------------------------------------

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

	t := NewTable("COLLECTION")
	for _, coll := range collections {
		t.AddRow(coll)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Документы
// -----------------------------------------------------------------------------

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

	if strings.Contains(jsonStr, "{") {
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

	fields := doc.GetFields()
	row := map[string]interface{}{"_id": doc.ID}
	for k, v := range fields {
		row[k] = v
	}
	rows := []map[string]interface{}{row}

	t := MapToTable(rows)
	r.output(t.String())

	utils.PrintInfo(fmt.Sprintf("created_at: %s", time.UnixMilli(doc.CreatedAt).Format("2006-01-02 15:04:05.000")))
	utils.PrintInfo(fmt.Sprintf("updated_at: %s", time.UnixMilli(doc.UpdatedAt).Format("2006-01-02 15:04:05.000")))
	if doc.DeletedAt > 0 {
		utils.PrintWarning(fmt.Sprintf("deleted_at: %s", time.UnixMilli(doc.DeletedAt).Format("2006-01-02 15:04:05.000")))
	}
	return nil
}

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

	if len(docs) == 0 {
		utils.PrintInfo("No documents found")
		return nil
	}

	rows := make([]map[string]interface{}, 0, len(docs))
	for _, doc := range docs {
		row := map[string]interface{}{"_id": doc.ID}
		for k, v := range doc.GetFields() {
			row[k] = v
		}
		rows = append(rows, row)
	}
	t := MapToTable(rows)
	r.output(t.String())
	return nil
}

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
	docs := coll.FindByFilter(func(doc *storage.Document) bool {
		return doc.CreatedAt >= fromMs && doc.CreatedAt <= toMs
	})

	if len(docs) == 0 {
		utils.PrintInfo("No documents found in the specified time range")
		return nil
	}

	rows := make([]map[string]interface{}, 0, len(docs))
	for _, doc := range docs {
		row := map[string]interface{}{"_id": doc.ID}
		for k, v := range doc.GetFields() {
			row[k] = v
		}
		rows = append(rows, row)
	}
	t := MapToTable(rows)
	r.output(t.String())
	return nil
}

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

	t := NewTable("ID", "DELETED_AT", "FIELDS")
	for _, doc := range deleted {
		t.AddRow(
			doc.ID,
			time.UnixMilli(doc.DeletedAt).Format("2006-01-02 15:04:05.000"),
			utils.ColorizeTextAny(doc.GetFields()),
		)
	}
	r.output(t.String())
	return nil
}

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

	t := NewTable("METRIC", "VALUE")
	t.AddRow("Active documents", fmt.Sprintf("%d", active))
	t.AddRow("Deleted documents", fmt.Sprintf("%d", deleted))
	t.AddRow("Total documents", fmt.Sprintf("%d", total))
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Временные метки
// -----------------------------------------------------------------------------

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
	doc, err := coll.FindIncludingDeleted(docID)
	if err != nil {
		return err
	}

	t := NewTable("FIELD", "VALUE")
	t.AddRow("Created", time.UnixMilli(doc.CreatedAt).Format("2006-01-02 15:04:05.000"))
	t.AddRow("Updated", time.UnixMilli(doc.UpdatedAt).Format("2006-01-02 15:04:05.000"))
	if doc.DeletedAt > 0 {
		t.AddRow("Deleted", time.UnixMilli(doc.DeletedAt).Format("2006-01-02 15:04:05.000"))
	} else {
		t.AddRow("Deleted", "not deleted")
	}
	t.AddRow("Version", fmt.Sprintf("%d", doc.Version))
	r.output(t.String())
	return nil
}

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

	docs := coll.GetAllDocumentsIncludingDeleted()
	if len(docs) == 0 {
		utils.PrintInfo("No documents in collection")
		return nil
	}

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

	t := NewTable("METRIC", "CREATED", "UPDATED")
	t.AddRow("Documents", fmt.Sprintf("%d", len(docs)), "")
	t.AddRow("Earliest", time.UnixMilli(minCreated).Format("2006-01-02 15:04:05.000"), time.UnixMilli(minUpdated).Format("2006-01-02 15:04:05.000"))
	t.AddRow("Latest", time.UnixMilli(maxCreated).Format("2006-01-02 15:04:05.000"), time.UnixMilli(maxUpdated).Format("2006-01-02 15:04:05.000"))
	t.AddRow("Average", time.UnixMilli(avgCreated).Format("2006-01-02 15:04:05.000"), time.UnixMilli(avgUpdated).Format("2006-01-02 15:04:05.000"))
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Аудит
// -----------------------------------------------------------------------------

func (r *Repl) handleAuditLog(args []string) error {
	entries := storage.GetAuditLog()
	if len(entries) == 0 {
		utils.PrintInfo("No audit log entries")
		return nil
	}

	start := 0
	if len(entries) > 50 {
		start = len(entries) - 50
	}

	t := NewTable("TIMESTAMP", "OPERATION", "TYPE", "NAME")
	for i := len(entries) - 1; i >= start; i-- {
		entry := entries[i]
		t.AddRow(entry.TimestampStr, entry.Operation, entry.DataType, entry.Name)
	}
	r.output(t.String())
	return nil
}

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

	t := NewTable("TIMESTAMP", "OPERATION", "TYPE", "NAME")
	for _, entry := range entries {
		t.AddRow(entry.TimestampStr, entry.Operation, entry.DataType, entry.Name)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Индексы
// -----------------------------------------------------------------------------

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

	t := NewTable("NAME", "FIELDS", "UNIQUE", "CREATED_AT")
	for _, idx := range indexes {
		uniqueStr := "false"
		if idx["unique"].(bool) {
			uniqueStr = "true"
		}
		createdAt := time.UnixMilli(idx["created_at"].(int64)).Format("2006-01-02 15:04:05")
		t.AddRow(idx["name"].(string), fmt.Sprintf("%v", idx["fields"]), uniqueStr, createdAt)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Ограничения
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Транзакции
// -----------------------------------------------------------------------------

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

// handleShowTransactions выводит список активных транзакций.
// ИСПРАВЛЕНО: tx.StartTime имеет тип int64 (Unix-миллисекунды),
// поэтому преобразуем его в строку через time.UnixMilli(...).Format(...).
func (r *Repl) handleShowTransactions(args []string) error {
	transactions := storage.GetActiveTransactions()
	if len(transactions) == 0 {
		utils.PrintInfo("No active transactions")
		return nil
	}

	t := NewTable("ID", "STATUS", "OPERATIONS", "STARTED")
	for _, tx := range transactions {
		started := ""
		if tx.StartTime > 0 {
			started = time.UnixMilli(tx.StartTime).Format("2006-01-02 15:04:05.000")
		}
		t.AddRow(
			fmt.Sprintf("%v", tx.ID),
			tx.Status,
			fmt.Sprintf("%d", tx.OperationCount),
			started,
		)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Плагины
// -----------------------------------------------------------------------------

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

	t := NewTable("NAME", "VERSION", "STATUS", "AUTHOR", "LOADED_AT")
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
		t.AddRow(p.Name, p.Version(), status, p.Author(), p.LoadedAt().Format("2006-01-02 15:04:05"))
	}
	r.output(t.String())
	return nil
}

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
	r.output(FormatCell(result))
	return nil
}

// -----------------------------------------------------------------------------
// Экспорт/импорт
// -----------------------------------------------------------------------------

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

	if !r.store.ExistsDatabase(dbName) {
		if err := r.store.CreateDatabase(dbName); err != nil {
			return err
		}
	}

	db, err := r.store.GetDatabase(dbName)
	if err != nil {
		return err
	}
	if err := db.DeserializeDatabase(data); err != nil {
		return fmt.Errorf("failed to deserialize database: %v", err)
	}
	utils.PrintSuccess(fmt.Sprintf("Data imported to database '%s' from %s", dbName, fileName))
	return nil
}

// -----------------------------------------------------------------------------
// ACL
// -----------------------------------------------------------------------------

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

	roles := r.aclManager.GetUserRoles(sessionID)
	if len(roles) > 0 {
		r.currentRole = roles[0]
	}

	utils.PrintSuccess(fmt.Sprintf("Logged in as '%s' with role '%s' at %s", username, r.currentRole, time.Now().Format("2006-01-02 15:04:05.000")))
	return nil
}

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

func (r *Repl) handleACLUsers(args []string) error {
	if r.aclManager == nil {
		return fmt.Errorf("ACL manager not initialized")
	}
	users := r.aclManager.ListUsers()
	if len(users) == 0 {
		utils.PrintInfo("No users found")
		return nil
	}

	t := NewTable("USERNAME", "ROLES", "STATUS")
	for _, username := range users {
		userInfo, err := r.aclManager.GetUserInfo(username)
		if err != nil {
			continue
		}
		status := "active"
		if !userInfo.Active {
			status = "disabled"
		}
		t.AddRow(username, fmt.Sprintf("%v", userInfo.Roles), status)
	}
	r.output(t.String())
	return nil
}

func (r *Repl) handleACLRoles(args []string) error {
	if r.aclManager == nil {
		return fmt.Errorf("ACL manager not initialized")
	}
	roles := r.aclManager.ListRoles()
	if len(roles) == 0 {
		utils.PrintInfo("No roles found")
		return nil
	}

	t := NewTable("ROLE", "PERMISSIONS")
	for _, roleName := range roles {
		perms, err := r.aclManager.GetRolePermissions(roleName)
		if err != nil {
			continue
		}
		t.AddRow(roleName, fmt.Sprintf("%v", perms))
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Сжатие
// -----------------------------------------------------------------------------

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

	t := NewTable("METRIC", "VALUE")
	t.AddRow("Total Documents", fmt.Sprintf("%d", totalDocs))
	t.AddRow("Compressed Documents", fmt.Sprintf("%d", compressedDocs))
	if totalDocs > 0 {
		t.AddRow("Compression Rate", fmt.Sprintf("%.2f%%", float64(compressedDocs)/float64(totalDocs)*100))
	}
	if totalOriginalSize > 0 {
		ratio := float64(totalCompressedSize) / float64(totalOriginalSize)
		t.AddRow("Size Reduction", fmt.Sprintf("%.2f%%", (1-ratio)*100))
		t.AddRow("Original Size", utils.FormatBytes(totalOriginalSize))
		t.AddRow("Compressed Size", utils.FormatBytes(totalCompressedSize))
	}
	t.AddRow("Algorithm", r.config.Compression.Algorithm)
	t.AddRow("Compression Level", fmt.Sprintf("%d", r.config.Compression.Level))
	t.AddRow("Min Size Threshold", utils.FormatBytes(int64(r.config.Compression.MinSize)))
	r.output(t.String())
	return nil
}

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

	t := NewTable("METRIC", "VALUE")
	t.AddRow("Compressed", fmt.Sprintf("%v", doc.Compressed))
	if doc.Compressed {
		ratio := doc.GetCompressionRatio()
		t.AddRow("Ratio", fmt.Sprintf("%.2f%%", (1-ratio)*100))
		t.AddRow("Original Size", utils.FormatBytes(doc.OriginalSize))
		if data, err := doc.Serialize(); err == nil {
			t.AddRow("Current Size", utils.FormatBytes(int64(len(data))))
		}
	}
	r.output(t.String())
	return nil
}

func (r *Repl) handleCompressionConfig(args []string) error {
	t := NewTable("SETTING", "VALUE")
	t.AddRow("Enabled", fmt.Sprintf("%v", r.config.Compression.Enabled))
	t.AddRow("Algorithm", r.config.Compression.Algorithm)
	t.AddRow("Level", fmt.Sprintf("%d", r.config.Compression.Level))
	t.AddRow("Min Size", utils.FormatBytes(int64(r.config.Compression.MinSize)))
	t.AddRow("snappy", "Fast compression/decompression, good balance (default)")
	t.AddRow("lz4", "Extremely fast, lower compression ratio")
	t.AddRow("zstd", "High compression ratio, slower")
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Триггеры
// -----------------------------------------------------------------------------

func (r *Repl) handleCreateTrigger(args []string) error {
	if r.currentDB == "" {
		return fmt.Errorf("no database selected")
	}
	if len(args) < 4 {
		return fmt.Errorf("usage: create trigger <collection> <name> <event> <action> [options]\n"+
			"  Events: BEFORE_INSERT, AFTER_INSERT, BEFORE_UPDATE, AFTER_UPDATE, BEFORE_DELETE, AFTER_DELETE\n"+
			"  Actions: abort, skip, modify, log, notify\n"+
			"  Options: --description <text>, --set <field> <value>, --inc <field> <value>, --currentDate <field>, --condition <field> <op> <value>")
	}

	collName := args[0]
	triggerName := args[1]
	event := args[2]
	action := args[3]

	db, err := r.store.GetDatabase(r.currentDB)
	if err != nil {
		return err
	}
	coll, err := db.GetCollection(collName)
	if err != nil {
		return err
	}

	trigger := &storage.Trigger{
		Name:        triggerName,
		Event:       event,
		Action:      action,
		Enabled:     true,
		Description: "",
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	}

	for i := 4; i < len(args); i++ {
		switch args[i] {
		case "--description":
			if i+1 < len(args) {
				trigger.Description = args[i+1]
				i++
			}
		}
	}

	if err := coll.AddTrigger(trigger); err != nil {
		return err
	}
	utils.PrintSuccess(fmt.Sprintf("Trigger '%s' created on collection '%s' for event %s at %s",
		triggerName, collName, event, time.Now().Format("2006-01-02 15:04:05.000")))
	return nil
}

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

	t := NewTable("NAME", "EVENT", "ACTION", "STATUS", "CREATED_AT", "DESCRIPTION")
	for _, tr := range triggers {
		status := "enabled"
		if !tr.Enabled {
			status = "disabled"
		}
		t.AddRow(tr.Name, tr.Event, tr.Action, status,
			time.UnixMilli(tr.CreatedAt).Format("2006-01-02 15:04:05"), tr.Description)
	}
	r.output(t.String())
	return nil
}

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

func (r *Repl) handleTriggerLog(args []string) error {
	logs := storage.GetTriggerExecutionLog()
	if len(logs) == 0 {
		utils.PrintInfo("No trigger executions logged")
		return nil
	}

	t := NewTable("TIMESTAMP", "TRIGGER", "EVENT", "COLLECTION", "DOCUMENT")
	for _, entry := range logs {
		t.AddRow(
			entry.Timestamp.Format("2006-01-02 15:04:05"),
			entry.TriggerName,
			entry.Event,
			entry.Collection,
			entry.DocumentID,
		)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Кластер
// -----------------------------------------------------------------------------

func (r *Repl) handleStatus(args []string) error {
	if r.coordinator == nil {
		utils.PrintWarning("Cluster coordinator not available")
		return nil
	}

	t := NewTable("METRIC", "VALUE")
	isLeader := r.coordinator.IsLeader()
	if isLeader {
		t.AddRow("Role", "LEADER")
	} else {
		t.AddRow("Role", "FOLLOWER")
	}

	leader := r.coordinator.GetLeader()
	if leader != nil {
		leaderIP := "unknown"
		leaderPort := 0
		if leader.IP != "" {
			leaderIP = leader.IP
		}
		if leader.Port > 0 {
			leaderPort = leader.Port
		}
		t.AddRow("Leader", fmt.Sprintf("%s:%d", leaderIP, leaderPort))
	} else {
		t.AddRow("Leader", "none")
	}

	status := r.coordinator.GetClusterStatus()
	t.AddRow("Cluster Name", status.Name)
	t.AddRow("Total Nodes", fmt.Sprintf("%d", status.TotalNodes))
	t.AddRow("Active Nodes", fmt.Sprintf("%d", status.ActiveNodes))
	t.AddRow("Health", status.Health)
	t.AddRow("Replication Factor", fmt.Sprintf("%d", status.ReplicationFactor))
	r.output(t.String())
	return nil
}

func (r *Repl) handleNodes(args []string) error {
	if r.coordinator == nil {
		return fmt.Errorf("cluster coordinator not available")
	}

	nodes := r.coordinator.GetAllNodes()
	if len(nodes) == 0 {
		utils.PrintInfo("No nodes in cluster")
		return nil
	}

	leader := r.coordinator.GetLeader()
	leaderID := ""
	if leader != nil {
		leaderID = leader.ID
	}

	t := NewTable("", "ID", "ADDRESS", "STATUS", "LAST_SEEN")
	for _, node := range nodes {
		prefix := "  "
		if node.ID == leaderID {
			prefix = " *"
		}
		nodeIP := "unknown"
		nodePort := 0
		if node.IP != "" {
			nodeIP = node.IP
		}
		if node.Port > 0 {
			nodePort = node.Port
		}
		lastSeen := time.UnixMilli(node.LastSeen).Format("15:04:05")
		t.AddRow(prefix, node.ID, fmt.Sprintf("%s:%d", nodeIP, nodePort), node.Status, lastSeen)
	}
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Миграция
// -----------------------------------------------------------------------------

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

	if r.coordinator == nil {
		return fmt.Errorf("cluster coordinator not available")
	}

	migrator := r.coordinator.GetCrossDCMigrator()
	if migrator == nil {
		return fmt.Errorf("migration system not initialized (check config: migration.enabled = true)")
	}

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

	t := NewTable("FIELD", "VALUE")
	t.AddRow("Task ID", task.ID)
	t.AddRow("Source", task.SourceDC)
	t.AddRow("Target", task.TargetDC)
	t.AddRow("Total documents", fmt.Sprintf("%d", task.TotalDocuments))
	t.AddRow("Status", string(task.Status))
	r.output(t.String())
	return nil
}

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

	t := NewTable("FIELD", "VALUE")
	t.AddRow("Task ID", task.ID)
	t.AddRow("Source DC", task.SourceDC)
	t.AddRow("Target DC", task.TargetDC)
	t.AddRow("Status", string(task.Status))
	t.AddRow("Progress", fmt.Sprintf("%.1f%%", task.ProgressPercent))
	t.AddRow("Total Docs", fmt.Sprintf("%d", task.TotalDocuments))
	t.AddRow("Migrated", fmt.Sprintf("%d", task.MigratedDocs))
	t.AddRow("Failed", fmt.Sprintf("%d", task.FailedDocs))
	t.AddRow("Skipped", fmt.Sprintf("%d", task.SkippedDocs))

	if task.StartTime > 0 {
		t.AddRow("Started", time.UnixMilli(task.StartTime).Format("2006-01-02 15:04:05"))
	}
	if task.EndTime > 0 {
		t.AddRow("Completed", time.UnixMilli(task.EndTime).Format("2006-01-02 15:04:05"))
	}
	if task.Error != "" {
		t.AddRow("Error", task.Error)
	}
	if len(task.Databases) > 0 {
		t.AddRow("Databases", strings.Join(task.Databases, ", "))
	}
	if len(task.Collections) > 0 {
		for db, colls := range task.Collections {
			t.AddRow("Collections", fmt.Sprintf("%s: %s", db, strings.Join(colls, ", ")))
		}
	}
	r.output(t.String())
	return nil
}

func (r *Repl) handleMigrationList(migrator *cluster.CrossDCMigrator) error {
	tasks := migrator.ListTasks()
	if len(tasks) == 0 {
		utils.PrintInfo("No migration tasks found")
		return nil
	}

	t := NewTable("ID", "STATUS", "PROGRESS", "SOURCE", "TARGET")
	for _, task := range tasks {
		id := task.ID
		if len(id) > 36 {
			id = id[:33] + "..."
		}
		progress := fmt.Sprintf("%.1f%%", task.ProgressPercent)
		if task.Status == cluster.MigrationStatusCompleted {
			progress = "100%"
		}
		t.AddRow(id, string(task.Status), progress, task.SourceDC, task.TargetDC)
	}
	r.output(t.String())
	return nil
}

func (r *Repl) handleMigrationPause(migrator *cluster.CrossDCMigrator, taskID string) error {
	if err := migrator.PauseMigration(taskID); err != nil {
		return err
	}
	utils.PrintSuccess(fmt.Sprintf("Migration %s paused", taskID))
	return nil
}

func (r *Repl) handleMigrationResume(migrator *cluster.CrossDCMigrator, taskID string) error {
	if err := migrator.ResumeMigration(taskID); err != nil {
		return err
	}
	utils.PrintSuccess(fmt.Sprintf("Migration %s resumed", taskID))
	return nil
}

func (r *Repl) handleMigrationCancel(migrator *cluster.CrossDCMigrator, taskID string) error {
	if err := migrator.CancelMigration(taskID); err != nil {
		return err
	}
	utils.PrintSuccess(fmt.Sprintf("Migration %s cancelled", taskID))
	return nil
}

func (r *Repl) handleMigrationStats(migrator *cluster.CrossDCMigrator) error {
	stats := migrator.GetMigrationStats()
	t := NewTable("METRIC", "VALUE")
	t.AddRow("Total Changes", fmt.Sprintf("%d", stats.TotalChanges))
	t.AddRow("Applied Changes", fmt.Sprintf("%d", stats.AppliedChanges))
	t.AddRow("Failed Changes", fmt.Sprintf("%d", stats.FailedChanges))
	t.AddRow("Skipped Changes", fmt.Sprintf("%d", stats.SkippedChanges))
	t.AddRow("Avg Latency", fmt.Sprintf("%d ms", stats.LatencyAvg))
	t.AddRow("Max Latency", fmt.Sprintf("%d ms", stats.LatencyMax))
	t.AddRow("Throughput", fmt.Sprintf("%d docs/sec", stats.Throughput))
	r.output(t.String())
	return nil
}

func (r *Repl) handleMigrationConfig(migrator *cluster.CrossDCMigrator) error {
	cfg := migrator.GetConfig()
	t := NewTable("SETTING", "VALUE")
	t.AddRow("Enabled", fmt.Sprintf("%v", cfg.Enabled))
	t.AddRow("Mode", cfg.Mode)
	if cfg.Source != nil {
		t.AddRow("Source DC", fmt.Sprintf("%s (%s)", cfg.Source.Name, cfg.Source.Endpoint))
	}
	if cfg.Target != nil {
		t.AddRow("Target DC", fmt.Sprintf("%s (%s)", cfg.Target.Name, cfg.Target.Endpoint))
	}
	if cfg.Settings != nil {
		t.AddRow("Batch Size", fmt.Sprintf("%d", cfg.Settings.BatchSize))
		t.AddRow("Workers", fmt.Sprintf("%d", cfg.Settings.Workers))
		t.AddRow("Compression", cfg.Settings.Compression)
		t.AddRow("Resume Enabled", fmt.Sprintf("%v", cfg.Settings.ResumeEnabled))
		t.AddRow("Max Retries", fmt.Sprintf("%d", cfg.Settings.MaxRetries))
	}
	if cfg.Delta != nil {
		t.AddRow("Delta Sync", fmt.Sprintf("%v", cfg.Delta.Enabled))
		t.AddRow("Delta Interval", fmt.Sprintf("%d sec", cfg.Delta.IntervalSec))
	}
	if cfg.Validation != nil {
		t.AddRow("Validation", fmt.Sprintf("%v", cfg.Validation.Enabled))
		t.AddRow("Sample Percent", fmt.Sprintf("%d%%", cfg.Validation.SamplePercent))
	}
	r.output(t.String())
	return nil
}

func (r *Repl) handleMigrationQueue(migrator *cluster.CrossDCMigrator) error {
	stats := migrator.GetQueueStats()
	t := NewTable("METRIC", "VALUE")
	t.AddRow("Queue Size", fmt.Sprintf("%v", stats["size"]))
	t.AddRow("Last LSN", fmt.Sprintf("%v", stats["last_lsn"]))
	t.AddRow("Max Size", fmt.Sprintf("%v", stats["max_size"]))
	t.AddRow("File Path", fmt.Sprintf("%v", stats["file_path"]))
	r.output(t.String())
	return nil
}

// -----------------------------------------------------------------------------
// Системные команды
// -----------------------------------------------------------------------------

func (r *Repl) handleHelp(args []string) error {
	var sb strings.Builder
	sb.WriteString("\n=== Available Commands ===\n")

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
		"Pager & Input": {
			{"pager on|off", "Enable/disable pager"},
			{"pager", "Show pager status"},
			{"PgUp/PgDown", "Navigate command history (like in bash)"},
			{"Tab", "Autocomplete command / collection / field"},
			{"Ctrl+D", "Exit REPL"},
			{"Ctrl+C", "Interrupt current input"},
		},
		"System": {
			{"help", "Display this help message with all available commands"},
			{"clear", "Clear the terminal screen"},
			{"quit", "Exit the futriix database REPL"},
			{"exit", "Exit the futriix database REPL (alias for quit)"},
		},
	}

	for category, commands := range categories {
		sb.WriteString(fmt.Sprintf("\n%s:\n", category))
		for _, cmd := range commands {
			if cmd.description != "" {
				sb.WriteString(fmt.Sprintf("  %-50s %s\n", cmd.cmd, cmd.description))
			} else {
				sb.WriteString(fmt.Sprintf("  %s\n", cmd.cmd))
			}
		}
	}

	sb.WriteString("\nMulti-line input:\n")
	sb.WriteString("  End line with '\\' to continue\n")
	sb.WriteString("  Or use '{' ... '}' — input continues until brackets are balanced\n")
	sb.WriteString("  Empty line finishes multi-line input\n")

	r.output(sb.String())
	return nil
}

func (r *Repl) handleClear(args []string) error {
	fmt.Print("\033[2J\033[H")
	return nil
}

func (r *Repl) handleQuit(args []string) error {
	utils.PrintInfo(fmt.Sprintf("Session ended at %s", time.Now().Format("2006-01-02 15:04:05.000")))
	if r.rl != nil {
		_ = r.rl.Close()
	}
	os.Exit(0)
	return nil
}
