/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/repl/table.go
// Назначение: Табличный вывод результатов (по умолчанию для find/count/stats и т.п.).
// Работает без внешних зависимостей, совместимо с Linux и OpenIndiana.

package repl

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// Table — простая ASCII-таблица.
type Table struct {
	headers []string
	rows    [][]string
}

// NewTable создаёт таблицу с заголовками.
func NewTable(headers ...string) *Table {
	return &Table{
		headers: append([]string(nil), headers...),
		rows:    make([][]string, 0),
	}
}

// AddRow добавляет строку.
func (t *Table) AddRow(cells ...string) {
	row := make([]string, len(cells))
	copy(row, cells)
	t.rows = append(t.rows, row)
}

// Len возвращает число строк.
func (t *Table) Len() int {
	return len(t.rows)
}

// String рендерит таблицу.
func (t *Table) String() string {
	if len(t.headers) == 0 && len(t.rows) == 0 {
		return ""
	}

	cols := len(t.headers)
	for _, r := range t.rows {
		if len(r) > cols {
			cols = len(r)
		}
	}

	// Нормализуем
	headers := make([]string, cols)
	copy(headers, t.headers)

	rows := make([][]string, len(t.rows))
	for i, r := range t.rows {
		rr := make([]string, cols)
		copy(rr, r)
		rows[i] = rr
	}

	// Ширины
	widths := make([]int, cols)
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if w := displayWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}

	var sb strings.Builder

	// Разделитель
	sep := buildSeparator(widths)

	// Заголовок
	sb.WriteString(sep)
	sb.WriteString("\n")
	sb.WriteString(renderRow(headers, widths))
	sb.WriteString("\n")
	sb.WriteString(sep)
	sb.WriteString("\n")

	// Строки
	for _, r := range rows {
		sb.WriteString(renderRow(r, widths))
		sb.WriteString("\n")
	}

	sb.WriteString(sep)
	sb.WriteString("\n")

	return sb.String()
}

func buildSeparator(widths []int) string {
	var sb strings.Builder
	sb.WriteString("+")
	for _, w := range widths {
		sb.WriteString(strings.Repeat("-", w+2))
		sb.WriteString("+")
	}
	return sb.String()
}

func renderRow(cells []string, widths []int) string {
	var sb strings.Builder
	sb.WriteString("|")
	for i, w := range widths {
		cell := ""
		if i < len(cells) {
			cell = cells[i]
		}
		sb.WriteString(" ")
		sb.WriteString(padRight(cell, w))
		sb.WriteString(" |")
	}
	return sb.String()
}

func padRight(s string, w int) string {
	dw := displayWidth(s)
	if dw >= w {
		return s
	}
	return s + strings.Repeat(" ", w-dw)
}

// displayWidth учитывает широкие руны (грубо).
func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		if r == '\t' {
			w += 4
			continue
		}
		if r < 32 {
			continue
		}
		if isWide(r) {
			w += 2
		} else {
			w += 1
		}
	}
	_ = utf8.RuneCountInString(s)
	return w
}

// isWide — грубая эвристика для широких символов CJK.
func isWide(r rune) bool {
	return (r >= 0x1100 && r <= 0x115F) ||
		(r >= 0x2E80 && r <= 0xA4CF) ||
		(r >= 0xAC00 && r <= 0xD7A3) ||
		(r >= 0xF900 && r <= 0xFAFF) ||
		(r >= 0xFE30 && r <= 0xFE4F) ||
		(r >= 0xFF00 && r <= 0xFF60) ||
		(r >= 0xFFE0 && r <= 0xFFE6)
}

// MapToTable преобразует список map в таблицу с сортировкой ключей.
func MapToTable(rows []map[string]interface{}) *Table {
	if len(rows) == 0 {
		return NewTable()
	}

	// Собираем все ключи
	keySet := map[string]struct{}{}
	for _, r := range rows {
		for k := range r {
			keySet[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// _id всегда первым, если есть
	if _, ok := keySet["_id"]; ok {
		sort.SliceStable(keys, func(i, j int) bool {
			if keys[i] == "_id" {
				return true
			}
			if keys[j] == "_id" {
				return false
			}
			return keys[i] < keys[j]
		})
	}

	t := NewTable(keys...)
	for _, r := range rows {
		cells := make([]string, len(keys))
		for i, k := range keys {
			v, ok := r[k]
			if !ok {
				cells[i] = ""
				continue
			}
			cells[i] = FormatCell(v)
		}
		t.AddRow(cells...)
	}

	return t
}

// FormatCell форматирует значение для ячейки.
func FormatCell(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", x)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
