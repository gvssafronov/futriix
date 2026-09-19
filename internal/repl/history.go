/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/repl/history.go
// Назначение: Управление историей команд REPL с поддержкой навигации
// (PgUp/PgDown через readline), загрузки и сохранения в файл.
// Совместимо с Linux и OpenIndiana.

package repl

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// History управляет историей команд.
type History struct {
	entries  []string
	maxSize  int
	filePath string
	mu       sync.Mutex
}

// NewHistory создаёт новый объект истории.
func NewHistory(maxSize int) *History {
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		homeDir = os.TempDir()
	}
	filePath := filepath.Join(homeDir, ".futriis_history")

	return &History{
		entries:  make([]string, 0, maxSize),
		maxSize:  maxSize,
		filePath: filePath,
	}
}

// FilePath возвращает путь к файлу истории (для readline).
func (h *History) FilePath() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.filePath
}

// Add добавляет команду в историю.
func (h *History) Add(cmd string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return nil
	}

	if len(h.entries) > 0 && h.entries[len(h.entries)-1] == cmd {
		return nil
	}

	h.entries = append(h.entries, cmd)

	if len(h.entries) > h.maxSize {
		h.entries = h.entries[len(h.entries)-h.maxSize:]
	}

	return nil
}

// Load загружает историю из файла.
func (h *History) Load() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	file, err := os.Open(h.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		cmd := scanner.Text()
		if cmd != "" {
			h.entries = append(h.entries, cmd)
		}
	}

	if len(h.entries) > h.maxSize {
		h.entries = h.entries[len(h.entries)-h.maxSize:]
	}

	return scanner.Err()
}

// Save сохраняет историю в файл.
func (h *History) Save() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	file, err := os.Create(h.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, cmd := range h.entries {
		if _, err := writer.WriteString(cmd + "\n"); err != nil {
			return err
		}
	}

	return writer.Flush()
}

// GetEntries возвращает копию всех записей истории.
func (h *History) GetEntries() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, len(h.entries))
	copy(out, h.entries)
	return out
}

// Len возвращает количество записей.
func (h *History) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.entries)
}

// At возвращает запись по индексу.
func (h *History) At(i int) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if i < 0 || i >= len(h.entries) {
		return ""
	}
	return h.entries[i]
}

// Clear очищает историю.
func (h *History) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = h.entries[:0]
}
