/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/repl/completer.go
// Назначение: Автодополнение команд, имён коллекций, полей и операторов.
// Поддерживает как собственный API, так и интеграцию с chzyer/readline.

package repl

import (
	"sort"
	"strings"

	"github.com/chzyer/readline"
)

// Completer предоставляет автодополнение.
type Completer struct {
	repl *Repl
}

// NewCompleter создаёт автодополнение.
func NewCompleter(r *Repl) *Completer {
	return &Completer{repl: r}
}

// Complete возвращает список подсказок для заданного префикса.
func (c *Completer) Complete(prefix string) []string {
	if c == nil || c.repl == nil {
		return nil
	}

	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil
	}

	parts := strings.Fields(prefix)
	trailingSpace := strings.HasSuffix(prefix, " ")

	if len(parts) == 0 || (len(parts) == 1 && !trailingSpace) {
		return c.completeCommand(prefix)
	}

	cmdName := ""
	for name := range c.repl.commands {
		if strings.HasPrefix(prefix, name) {
			if len(name) > len(cmdName) {
				cmdName = name
			}
		}
	}

	if cmdName == "" {
		if len(parts) > 0 {
			cmdName = parts[0]
		}
	}

	argsPrefix := strings.TrimPrefix(prefix, cmdName)
	argsPrefix = strings.TrimSpace(argsPrefix)

	argParts := strings.Fields(argsPrefix)
	lastArg := ""
	if len(argParts) > 0 && !trailingSpace {
		lastArg = argParts[len(argParts)-1]
	}

	return c.completeArgs(cmdName, lastArg)
}

func (c *Completer) completeCommand(prefix string) []string {
	var out []string
	seen := map[string]struct{}{}
	for name := range c.repl.commands {
		if strings.HasPrefix(name, prefix) {
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				out = append(out, name)
			}
		}
	}

	extra := []string{
		"help", "clear", "quit", "exit", "pager",
	}
	for _, e := range extra {
		if strings.HasPrefix(e, prefix) {
			if _, ok := seen[e]; !ok {
				seen[e] = struct{}{}
				out = append(out, e)
			}
		}
	}

	sort.Strings(out)
	return out
}

func (c *Completer) completeArgs(cmdName, lastArg string) []string {
	switch cmdName {
	case "use", "drop database", "create slice", "export", "import":
		return c.completeDatabases(lastArg)
	case "create collection", "drop collection", "show collections",
		"show indexes", "show triggers", "show deleted", "count",
		"stats timestamps", "compress collection":
		return c.completeCollections(lastArg)
	case "insert", "find", "update", "delete", "permanent delete", "restore",
		"findbyindex", "findbytime", "create index", "drop index",
		"add required", "add unique", "add min", "add max", "add enum",
		"create trigger", "drop trigger", "enable trigger", "disable trigger",
		"show timestamps", "doc compression":
		return c.completeCollectionThenField(cmdName, lastArg)
	case "migration":
		return c.completeMigration(lastArg)
	default:
		return c.completeOperators(lastArg)
	}
}

func (c *Completer) completeDatabases(prefix string) []string {
	if c.repl.store == nil {
		return nil
	}
	var out []string
	for _, db := range c.repl.store.ListDatabases() {
		if strings.HasPrefix(db, prefix) {
			out = append(out, db)
		}
	}
	sort.Strings(out)
	return out
}

func (c *Completer) completeCollections(prefix string) []string {
	if c.repl.store == nil || c.repl.currentDB == "" {
		return nil
	}
	db, err := c.repl.store.GetDatabase(c.repl.currentDB)
	if err != nil {
		return nil
	}
	var out []string
	for _, coll := range db.ListCollections() {
		if strings.HasPrefix(coll, prefix) {
			out = append(out, coll)
		}
	}
	sort.Strings(out)
	return out
}

func (c *Completer) completeCollectionThenField(cmdName, lastArg string) []string {
	if c.repl.store == nil || c.repl.currentDB == "" {
		return c.completeCollections(lastArg)
	}

	db, err := c.repl.store.GetDatabase(c.repl.currentDB)
	if err != nil {
		return nil
	}
	colls := db.ListCollections()
	for _, coll := range colls {
		if coll == lastArg {
			return c.completeFields(coll, "")
		}
	}

	if cols := c.completeCollections(lastArg); len(cols) > 0 {
		return cols
	}

	if len(colls) > 0 {
		return c.completeFields(colls[0], lastArg)
	}
	return nil
}

func (c *Completer) completeFields(collName, prefix string) []string {
	if c.repl.store == nil || c.repl.currentDB == "" {
		return nil
	}
	db, err := c.repl.store.GetDatabase(c.repl.currentDB)
	if err != nil {
		return nil
	}
	coll, err := db.GetCollection(collName)
	if err != nil {
		return nil
	}

	fieldSet := map[string]struct{}{}
	for _, doc := range coll.GetAllDocuments() {
		for k := range doc.GetFields() {
			fieldSet[k] = struct{}{}
		}
	}

	var out []string
	for k := range fieldSet {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func (c *Completer) completeOperators(prefix string) []string {
	ops := []string{
		"BEFORE_INSERT", "AFTER_INSERT",
		"BEFORE_UPDATE", "AFTER_UPDATE",
		"BEFORE_DELETE", "AFTER_DELETE",
		"abort", "skip", "modify", "log", "notify",
		"unique", "required",
		"start", "status", "list", "pause", "resume", "cancel", "stats", "config", "queue", "validate",
		"--description", "--set", "--inc", "--currentDate", "--condition",
	}
	var out []string
	for _, op := range ops {
		if strings.HasPrefix(op, prefix) {
			out = append(out, op)
		}
	}
	sort.Strings(out)
	return out
}

func (c *Completer) completeMigration(prefix string) []string {
	subs := []string{"start", "status", "list", "pause", "resume", "cancel", "stats", "config", "queue", "validate"}
	var out []string
	for _, s := range subs {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// CommonPrefix возвращает общий префикс для набора строк.
func CommonPrefix(items []string) string {
	if len(items) == 0 {
		return ""
	}
	prefix := items[0]
	for _, s := range items[1:] {
		for !strings.HasPrefix(s, prefix) {
			if len(prefix) == 0 {
				return ""
			}
			prefix = prefix[:len(prefix)-1]
		}
	}
	return prefix
}

// ReadlineCompleter строит статическое дерево команд для readline.
// Динамические имена коллекций/полей дополняются через DynamicCompleter
// (см. ReadlineDynamicFunc ниже), чтобы не пересобирать дерево на каждый ввод.
func (c *Completer) ReadlineCompleter() *readline.PrefixCompleter {
	items := []readline.PrefixCompleterInterface{}

	// Сортируем имена команд для стабильного вывода.
	names := make([]string, 0, len(c.repl.commands))
	for name := range c.repl.commands {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		items = append(items, readline.PcItem(name))
	}

	// Системные команды.
	items = append(items, readline.PcItem("help"))
	items = append(items, readline.PcItem("clear"))
	items = append(items, readline.PcItem("quit"))
	items = append(items, readline.PcItem("exit"))
	items = append(items, readline.PcItem("pager",
		readline.PcItem("on"),
		readline.PcItem("off"),
	))

	return readline.NewPrefixCompleter(items...)
}

// ReadlineDynamicFunc возвращает функцию для readline.Config.DynamicComplete.
// Она вызывается на каждый Tab и позволяет дополнять коллекции и поля.
func (c *Completer) ReadlineDynamicFunc() func(string, int) []string {
	return func(line string, pos int) []string {
		prefix := ""
		if pos <= len(line) {
			prefix = line[:pos]
		}
		return c.Complete(prefix)
	}
}
