/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/repl/pager.go
// Назначение: Пейджер для постраничного вывода длинных результатов.
// Использует внешний pager (less/more) если доступен, иначе встроенный.
// Совместимо с Linux и OpenIndiana.

package repl

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Pager предоставляет постраничный вывод.
type Pager struct {
	enabled    bool
	threshold  int
	external   string
	lines      int
}

// NewPager создаёт пейджер.
func NewPager(enabled bool, threshold int) *Pager {
	p := &Pager{
		enabled:   enabled,
		threshold: threshold,
		lines:     24,
	}
	p.external = detectPager()
	return p
}

// detectPager ищет доступный внешний pager.
// Порядок: $PAGER -> less -> more -> "" (встроенный).
func detectPager() string {
	if env := os.Getenv("PAGER"); env != "" {
		return env
	}
	if path, err := exec.LookPath("less"); err == nil {
		return path
	}
	if path, err := exec.LookPath("more"); err == nil {
		return path
	}
	return ""
}

// Enabled возвращает, включён ли пейджер.
func (p *Pager) Enabled() bool {
	return p != nil && p.enabled
}

// SetEnabled включает/выключает пейджер.
func (p *Pager) SetEnabled(v bool) {
	if p != nil {
		p.enabled = v
	}
}

// SetThreshold устанавливает порог строк.
func (p *Pager) SetThreshold(n int) {
	if p != nil && n > 0 {
		p.threshold = n
	}
}

// Page выводит текст через пейджер, если он длиннее порога.
func (p *Pager) Page(text string) error {
	if p == nil || !p.enabled {
		fmt.Print(text)
		return nil
	}

	lineCount := strings.Count(text, "\n")
	if lineCount < p.threshold {
		fmt.Print(text)
		return nil
	}

	if p.external != "" {
		if err := p.pageExternal(text); err == nil {
			return nil
		}
		// fallback на встроенный
	}

	return p.pageBuiltin(text)
}

// pageExternal запускает внешний pager.
func (p *Pager) pageExternal(text string) error {
	args := []string{}
	base := strings.ToLower(p.external)

	if strings.Contains(base, "less") {
		// -R: сохранять ANSI-цвета, -F: выйти если всё влезает, -X: не чистить экран
		args = append(args, "-RFX")
	}

	cmd := exec.Command(p.external, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	if _, err := io.WriteString(stdin, text); err != nil {
		stdin.Close()
		_ = cmd.Wait()
		return err
	}
	stdin.Close()

	return cmd.Wait()
}

// pageBuiltin — встроенный пейджер (fallback).
// Управление: Enter — следующая строка, Space — страница, q — выход.
func (p *Pager) pageBuiltin(text string) error {
	scanner := bufio.NewScanner(strings.NewReader(text))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lines := []string{}
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	pageSize := p.lines - 1
	if pageSize < 5 {
		pageSize = 5
	}

	reader := bufio.NewReader(os.Stdin)
	pos := 0

	for pos < len(lines) {
		end := pos + pageSize
		if end > len(lines) {
			end = len(lines)
		}
		for i := pos; i < end; i++ {
			fmt.Println(lines[i])
		}
		pos = end

		if pos >= len(lines) {
			break
		}

		fmt.Printf("\033[7m--More--\033[0m (Enter/Space/q) ")
		ch, err := reader.ReadByte()
		fmt.Print("\r\033[K")
		if err != nil {
			break
		}
		switch ch {
		case 'q', 'Q':
			return nil
		case '\n', '\r':
			// одна строка
			if pos < len(lines) {
				fmt.Println(lines[pos])
				pos++
			}
		case ' ':
			// страница
		default:
			// игнорируем
		}
	}
	return nil
}
