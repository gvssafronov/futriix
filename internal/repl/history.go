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
// Назначение: Управление историей команд REPL

package repl

import (
	"bufio"
	"os"
	"path/filepath"
)

// History управляет историей команд
type History struct {
	entries  []string
	maxSize  int
	filePath string
}

// NewHistory создаёт новый объект истории
func NewHistory(maxSize int) *History {
	homeDir, _ := os.UserHomeDir()
	filePath := filepath.Join(homeDir, ".futriis_history")
	
	return &History{
		entries:  make([]string, 0, maxSize),
		maxSize:  maxSize,
		filePath: filePath,
	}
}

// Add добавляет команду в историю
func (h *History) Add(cmd string) error {
	// Не добавляем дубликаты подряд
	if len(h.entries) > 0 && h.entries[len(h.entries)-1] == cmd {
		return nil
	}
	
	h.entries = append(h.entries, cmd)
	
	// Ограничиваем размер истории
	if len(h.entries) > h.maxSize {
		h.entries = h.entries[len(h.entries)-h.maxSize:]
	}
	
	return nil
}

// Load загружает историю из файла
func (h *History) Load() error {
	file, err := os.Open(h.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cmd := scanner.Text()
		if cmd != "" {
			h.entries = append(h.entries, cmd)
		}
	}
	
	// Ограничиваем размер
	if len(h.entries) > h.maxSize {
		h.entries = h.entries[len(h.entries)-h.maxSize:]
	}
	
	return scanner.Err()
}

// Save сохраняет историю в файл
func (h *History) Save() error {
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

// GetEntries возвращает все записи истории
func (h *History) GetEntries() []string {
	return h.entries
}
