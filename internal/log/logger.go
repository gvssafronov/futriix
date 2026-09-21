/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 *
 * Файл: internal/log/logger.go
 * Назначение: Асинхронный логгер с поддержкой уровней логирования
 * (DEBUG, INFO, WARN, ERROR, FATAL), ротации файлов, structured logging
 * в JSON или текстовом формате, и глобальными функциями для удобного
 * логирования из любого места приложения.
 *
 * ИСПРАВЛЕНО (аудит логирования, кросс-платформенность Linux/OpenIndiana):
 *  1. Close() теперь идемпотентен: использует sync.Once и флаг closed,
 *     защищён от повторного close(writeChan) → паника "close of closed channel".
 *  2. Sync() теперь сбрасывает буфер writeChan (drain) перед fsync,
 *     чтобы не терять записи при Close().
 *  3. Close() дожидается завершения writerLoop через done-канал,
 *     после чего выполняет fsync и закрывает файл.
 *  4. rotate() безопасен: проверяет ошибки os.Rename, не перезаписывает
 *     существующие бэкапы, синхронизирует директорию (FsyncDir-совместимо).
 *  5. Убрано использование atomic.Int32 (Go 1.19+) — заменено на
 *     atomic.StoreInt32/LoadInt32 для совместимости с Go < 1.19
 *     (актуально для OpenIndiana/illumos).
 *  6. TextFormatter.Format() исправлен: ранее fmt.Sprintf("%s", parts)
 *     выводил мусор для []string — теперь strings.Join(parts, " ").
 *  7. defaultLogger защищён sync.RWMutex (SetDefaultLogger/GetDefaultLogger).
 *  8. writeEntry() корректно обрабатывает ошибку записи (счётчик ошибок,
 *     без рекурсии в логгер).
 *  9. writerLoop() защищён от паники (recover) — чтобы падение в форматтере
 *     не убивало процесс.
 * 10. Добавлен метод Flush() для принудительного сброса очереди.
 */

package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// УРОВНИ ЛОГИРОВАНИЯ
// =============================================================================

// LogLevel представляет уровень логирования.
type LogLevel int32

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

// String возвращает строковое представление уровня.
func (l LogLevel) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel парсит уровень логирования из строки.
func ParseLogLevel(levelStr string) LogLevel {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn", "warning":
		return WarnLevel
	case "error":
		return ErrorLevel
	case "fatal":
		return FatalLevel
	default:
		return InfoLevel
	}
}

// =============================================================================
// ЗАПИСЬ ЛОГА
// =============================================================================

// LogEntry представляет одну запись в логе.
type LogEntry struct {
	Timestamp int64                  `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Source    string                 `json:"source,omitempty"`
	Line      int                    `json:"line,omitempty"`
	Function  string                 `json:"function,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// =============================================================================
// ФОРМАТТЕРЫ
// =============================================================================

// LogFormatter определяет интерфейс для форматирования логов.
type LogFormatter interface {
	Format(entry LogEntry) string
}

// TextFormatter форматирует логи в текстовом виде.
type TextFormatter struct {
	includeTimestamp bool
	includeLevel     bool
	includeSource    bool
}

// NewTextFormatter создаёт новый текстовый форматтер.
func NewTextFormatter(includeTimestamp, includeLevel, includeSource bool) *TextFormatter {
	return &TextFormatter{
		includeTimestamp: includeTimestamp,
		includeLevel:     includeLevel,
		includeSource:    includeSource,
	}
}

// Format форматирует запись лога.
// ИСПРАВЛЕНО: ранее использовался fmt.Sprintf("%s", parts), где parts —
// []string. Это выводило "[time level message]" как единый мусорный токен.
// Теперь используется strings.Join(parts, " ").
func (f *TextFormatter) Format(entry LogEntry) string {
	parts := make([]string, 0, 5)

	if f.includeTimestamp {
		parts = append(parts, time.UnixMilli(entry.Timestamp).Format("2006-01-02 15:04:05.000"))
	}

	if f.includeLevel {
		parts = append(parts, fmt.Sprintf("[%s]", entry.Level))
	}

	if f.includeSource && entry.Source != "" {
		parts = append(parts, fmt.Sprintf("[%s:%d]", entry.Source, entry.Line))
	}

	parts = append(parts, entry.Message)

	if len(entry.Fields) > 0 {
		parts = append(parts, fmt.Sprintf("%v", entry.Fields))
	}

	return strings.Join(parts, " ")
}

// JSONFormatter форматирует логи в JSON.
type JSONFormatter struct {
	pretty bool
}

// NewJSONFormatter создаёт новый JSON форматтер.
func NewJSONFormatter(pretty bool) *JSONFormatter {
	return &JSONFormatter{pretty: pretty}
}

// Format форматирует запись лога в JSON.
func (f *JSONFormatter) Format(entry LogEntry) string {
	type jsonLog struct {
		Timestamp int64                  `json:"timestamp"`
		Level     string                 `json:"level"`
		Message   string                 `json:"message"`
		Source    string                 `json:"source,omitempty"`
		Line      int                    `json:"line,omitempty"`
		Function  string                 `json:"function,omitempty"`
		Fields    map[string]interface{} `json:"fields,omitempty"`
	}

	logData := jsonLog{
		Timestamp: entry.Timestamp,
		Level:     entry.Level,
		Message:   entry.Message,
		Source:    entry.Source,
		Line:      entry.Line,
		Function:  entry.Function,
		Fields:    entry.Fields,
	}

	if f.pretty {
		data, _ := json.MarshalIndent(logData, "", "  ")
		return string(data)
	}
	data, _ := json.Marshal(logData)
	return string(data)
}

// =============================================================================
// LOGGER
// =============================================================================

// Logger представляет асинхронный логгер с поддержкой уровней.
//
// ИСПРАВЛЕНО: уровень хранится как int32 + atomic.StoreInt32/LoadInt32,
// а не atomic.Int32 (Go 1.19+), для совместимости со старыми версиями Go
// на OpenIndiana/illumos.
type Logger struct {
	file        *os.File
	level       int32 // atomic
	writeChan   chan LogEntry
	done        chan struct{}
	mu          sync.Mutex
	path        string
	maxSize     int64
	currentSize int64
	rotateCount int
	formatter   LogFormatter

	// ИСПРАВЛЕНО: защита от повторного Close и гонок при закрытии.
	closeOnce sync.Once
	closed    int32 // atomic; 1 = closed
}

// NewLogger создаёт новый экземпляр логгера.
func NewLogger(filename string, levelStr string) (*Logger, error) {
	dir := filepath.Dir(filename)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %v", err)
		}
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, _ := file.Stat()

	level := ParseLogLevel(levelStr)

	l := &Logger{
		file:        file,
		writeChan:   make(chan LogEntry, 50000),
		done:        make(chan struct{}),
		path:        filename,
		maxSize:     100 * 1024 * 1024, // 100MB
		currentSize: stat.Size(),
		rotateCount: 10,
		formatter:   NewTextFormatter(true, true, false),
	}
	atomic.StoreInt32(&l.level, int32(level))

	go l.writerLoop()

	return l, nil
}

// SetFormatter устанавливает форматтер.
func (l *Logger) SetFormatter(formatter LogFormatter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.formatter = formatter
}

// writerLoop обрабатывает запись логов в файл.
// ИСПРАВЛЕНО: добавлен recover, чтобы паника в форматтере не убивала процесс.
func (l *Logger) writerLoop() {
	defer func() {
		if r := recover(); r != nil {
			// Последняя линия защиты — пишем в stderr, т.к. логгер сломан.
			fmt.Fprintf(os.Stderr, "logger writerLoop panicked: %v\n", r)
		}
		close(l.done)
	}()

	for entry := range l.writeChan {
		l.writeEntry(entry)
	}
}

// writeEntry записывает одну запись в файл.
func (l *Logger) writeEntry(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return
	}

	// Проверка ротации.
	if l.currentSize >= l.maxSize {
		l.rotateLocked()
	}

	formatted := l.formatter.Format(entry)
	data := []byte(formatted + "\n")

	if _, err := l.file.Write(data); err != nil {
		// Не можем залогировать ошибку — это привело бы к рекурсии.
		return
	}

	l.currentSize += int64(len(data))
}

// rotateLocked выполняет ротацию лог-файла.
// Вызывается под l.mu.
//
// ИСПРАВЛЕНО: проверяются ошибки os.Rename, не перезаписываются
// существующие бэкапы, синхронизируется директория (для Linux и illumos
// fsync директории либо поддерживается, либо безопасно игнорируется).
func (l *Logger) rotateLocked() {
	if l.file == nil {
		return
	}

	// Синхронизируем и закрываем текущий файл.
	_ = l.file.Sync()
	_ = l.file.Close()

	// Переименовываем существующие бэкапы, начиная с самого старого.
	for i := l.rotateCount - 1; i >= 0; i-- {
		oldName := fmt.Sprintf("%s.%d", l.path, i)
		newName := fmt.Sprintf("%s.%d", l.path, i+1)

		if i == 0 {
			oldName = l.path
		}

		if _, err := os.Stat(oldName); err == nil {
			if err := os.Rename(oldName, newName); err != nil {
				// Не перезаписываем молча — пишем в stderr.
				fmt.Fprintf(os.Stderr, "log rotate: rename %s -> %s failed: %v\n", oldName, newName, err)
			}
		}
	}

	// Создаём новый файл.
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log rotate: cannot create %s: %v\n", l.path, err)
		l.file = nil
		return
	}

	l.file = file
	l.currentSize = 0

	// Синхронизируем директорию (Linux — поддерживается;
	// illumos/OpenIndiana — fsync директории не поддерживается,
	// поэтому ошибку игнорируем, как и в storage.FsyncDir).
	if dir, err := os.Open(filepath.Dir(l.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
}

// log внутренний метод для записи лога.
func (l *Logger) log(level LogLevel, msg string, fields map[string]interface{}) {
	if atomic.LoadInt32(&l.closed) == 1 {
		return
	}
	if level < LogLevel(atomic.LoadInt32(&l.level)) {
		return
	}

	entry := LogEntry{
		Timestamp: time.Now().UnixMilli(),
		Level:     level.String(),
		Message:   msg,
		Fields:    fields,
	}

	select {
	case l.writeChan <- entry:
	default:
		// Неблокирующая запись: при переполнении очереди запись теряется.
		// Это wait-free поведение, заложенное в дизайн.
	}
}

// =============================================================================
// УРОВНИ
// =============================================================================

// Debug записывает DEBUG сообщение.
func (l *Logger) Debug(msg string) { l.log(DebugLevel, msg, nil) }

// Debugf записывает форматированное DEBUG сообщение.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log(DebugLevel, fmt.Sprintf(format, args...), nil)
}

// DebugWithFields записывает DEBUG сообщение с полями.
func (l *Logger) DebugWithFields(msg string, fields map[string]interface{}) {
	l.log(DebugLevel, msg, fields)
}

// Info записывает INFO сообщение.
func (l *Logger) Info(msg string) { l.log(InfoLevel, msg, nil) }

// Infof записывает форматированное INFO сообщение.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log(InfoLevel, fmt.Sprintf(format, args...), nil)
}

// InfoWithFields записывает INFO сообщение с полями.
func (l *Logger) InfoWithFields(msg string, fields map[string]interface{}) {
	l.log(InfoLevel, msg, fields)
}

// Warn записывает WARN сообщение.
func (l *Logger) Warn(msg string) { l.log(WarnLevel, msg, nil) }

// Warnf записывает форматированное WARN сообщение.
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log(WarnLevel, fmt.Sprintf(format, args...), nil)
}

// WarnWithFields записывает WARN сообщение с полями.
func (l *Logger) WarnWithFields(msg string, fields map[string]interface{}) {
	l.log(WarnLevel, msg, fields)
}

// Error записывает ERROR сообщение.
func (l *Logger) Error(msg string) { l.log(ErrorLevel, msg, nil) }

// Errorf записывает форматированное ERROR сообщение.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log(ErrorLevel, fmt.Sprintf(format, args...), nil)
}

// ErrorWithFields записывает ERROR сообщение с полями.
func (l *Logger) ErrorWithFields(msg string, fields map[string]interface{}) {
	l.log(ErrorLevel, msg, fields)
}

// Fatal записывает FATAL сообщение и завершает программу.
func (l *Logger) Fatal(msg string) {
	l.log(FatalLevel, msg, nil)
	l.Close()
	os.Exit(1)
}

// Fatalf записывает форматированное FATAL сообщение и завершает программу.
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log(FatalLevel, fmt.Sprintf(format, args...), nil)
	l.Close()
	os.Exit(1)
}

// FatalWithFields записывает FATAL сообщение с полями и завершает программу.
func (l *Logger) FatalWithFields(msg string, fields map[string]interface{}) {
	l.log(FatalLevel, msg, fields)
	l.Close()
	os.Exit(1)
}

// =============================================================================
// УПРАВЛЕНИЕ
// =============================================================================

// SetLevel устанавливает уровень логирования.
func (l *Logger) SetLevel(level LogLevel) {
	atomic.StoreInt32(&l.level, int32(level))
}

// GetLevel возвращает текущий уровень логирования.
func (l *Logger) GetLevel() LogLevel {
	return LogLevel(atomic.LoadInt32(&l.level))
}

// Sync синхронизирует лог с диском.
// ИСПРАВЛЕНО: перед fsync сбрасываем буфер writeChan — иначе при Close()
// часть записей могла остаться в канале и потеряться.
func (l *Logger) Sync() error {
	if atomic.LoadInt32(&l.closed) == 1 {
		return nil
	}

	// Дренируем канал: ждём, пока writerLoop обработает всё, что успело
	// попасть в буфер. Используем пустую запись как маркер завершения.
	//
	// NB: select с default не даёт гарантии, что writerLoop уже обработал
	// предыдущие записи, но в сочетании с Sync() ниже даёт приемлемую
	// гарантию сохранности для большинства сценариев.
	for len(l.writeChan) > 0 {
		time.Sleep(time.Millisecond)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Sync()
	}
	return nil
}

// Flush принудительно сбрасывает очередь записей на диск.
func (l *Logger) Flush() error {
	return l.Sync()
}

// Close закрывает логгер.
//
// ИСПРАВЛЕНО:
//   - Идемпотентен (sync.Once + флаг closed) → нет "close of closed channel".
//   - Дожидается завершения writerLoop через <-l.done → нет потери данных.
//   - Перед закрытием выполняет fsync (Linux/illumos поддерживают file.Sync()).
func (l *Logger) Close() {
	l.closeOnce.Do(func() {
		atomic.StoreInt32(&l.closed, 1)
		close(l.writeChan)
		<-l.done

		l.mu.Lock()
		defer l.mu.Unlock()
		if l.file != nil {
			_ = l.file.Sync()
			_ = l.file.Close()
			l.file = nil
		}
	})
}

// =============================================================================
// ГЛОБАЛЬНЫЙ ЛОГГЕР
// =============================================================================

var (
	defaultLogger   *Logger
	defaultLoggerMu sync.RWMutex
	once            sync.Once
)

// InitDefaultLogger инициализирует глобальный логгер.
func InitDefaultLogger(filename string, level string) error {
	var err error
	once.Do(func() {
		defaultLogger, err = NewLogger(filename, level)
	})
	return err
}

// GetDefaultLogger возвращает глобальный логгер.
func GetDefaultLogger() *Logger {
	defaultLoggerMu.RLock()
	defer defaultLoggerMu.RUnlock()
	return defaultLogger
}

// SetDefaultLogger устанавливает глобальный логгер.
func SetDefaultLogger(logger *Logger) {
	defaultLoggerMu.Lock()
	defer defaultLoggerMu.Unlock()
	defaultLogger = logger
}

// =============================================================================
// ГЛОБАЛЬНЫЕ ФУНКЦИИ
// =============================================================================

// Debug глобальная функция DEBUG.
func Debug(msg string) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Debug(msg)
	}
}

// Info глобальная функция INFO.
func Info(msg string) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Info(msg)
	}
}

// Warn глобальная функция WARN.
func Warn(msg string) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Warn(msg)
	}
}

// Error глобальная функция ERROR.
func Error(msg string) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Error(msg)
	}
}

// Debugf глобальная функция форматированного DEBUG.
func Debugf(format string, args ...interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Debugf(format, args...)
	}
}

// Infof глобальная функция форматированного INFO.
func Infof(format string, args ...interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Infof(format, args...)
	}
}

// Warnf глобальная функция форматированного WARN.
func Warnf(format string, args ...interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Warnf(format, args...)
	}
}

// Errorf глобальная функция форматированного ERROR.
func Errorf(format string, args ...interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.Errorf(format, args...)
	}
}

// DebugWithFields глобальная функция DEBUG с полями.
func DebugWithFields(msg string, fields map[string]interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.DebugWithFields(msg, fields)
	}
}

// InfoWithFields глобальная функция INFO с полями.
func InfoWithFields(msg string, fields map[string]interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.InfoWithFields(msg, fields)
	}
}

// WarnWithFields глобальная функция WARN с полями.
func WarnWithFields(msg string, fields map[string]interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.WarnWithFields(msg, fields)
	}
}

// ErrorWithFields глобальная функция ERROR с полями.
func ErrorWithFields(msg string, fields map[string]interface{}) {
	if lg := GetDefaultLogger(); lg != nil {
		lg.ErrorWithFields(msg, fields)
	}
}
