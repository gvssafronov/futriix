/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 
 // Файл: internal/log/logger.go
// Назначение: Асинхронный логгер с поддержкой уровней логирования (DEBUG, INFO, WARN, ERROR, FATAL),
// ротации файлов, structured logging в JSON или текстовом формате, и глобальными функциями
// для удобного логирования из любого места приложения без создания экземпляра логгера.
 */

package log

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "sync/atomic"
    "time"
)

// LogLevel представляет уровень логирования
type LogLevel int32

const (
    DebugLevel LogLevel = iota
    InfoLevel
    WarnLevel
    ErrorLevel
    FatalLevel
)

// String возвращает строковое представление уровня
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

// ParseLogLevel парсит уровень логирования из строки
func ParseLogLevel(levelStr string) LogLevel {
    switch levelStr {
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

// LogEntry представляет одну запись в логе
type LogEntry struct {
    Timestamp   int64                  `json:"timestamp"`
    Level       string                 `json:"level"`
    Message     string                 `json:"message"`
    Source      string                 `json:"source,omitempty"`
    Line        int                    `json:"line,omitempty"`
    Function    string                 `json:"function,omitempty"`
    Fields      map[string]interface{} `json:"fields,omitempty"`
}

// Logger представляет асинхронный логгер с поддержкой уровней
type Logger struct {
    file        *os.File
    level       atomic.Int32
    writeChan   chan LogEntry
    done        chan struct{}
    mu          sync.Mutex
    path        string
    maxSize     int64
    currentSize int64
    rotateCount int
    formatter   LogFormatter
}

// LogFormatter определяет интерфейс для форматирования логов
type LogFormatter interface {
    Format(entry LogEntry) string
}

// TextFormatter форматирует логи в текстовом виде
type TextFormatter struct {
    includeTimestamp bool
    includeLevel     bool
    includeSource    bool
}

// NewTextFormatter создаёт новый текстовый форматтер
func NewTextFormatter(includeTimestamp, includeLevel, includeSource bool) *TextFormatter {
    return &TextFormatter{
        includeTimestamp: includeTimestamp,
        includeLevel:     includeLevel,
        includeSource:    includeSource,
    }
}

// Format форматирует запись лога
func (f *TextFormatter) Format(entry LogEntry) string {
    var parts []string
    
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
    
    return fmt.Sprintf("%s", parts)
}

// JSONFormatter форматирует логи в JSON
type JSONFormatter struct {
    pretty bool
}

// NewJSONFormatter создаёт новый JSON форматтер
func NewJSONFormatter(pretty bool) *JSONFormatter {
    return &JSONFormatter{pretty: pretty}
}

// Format форматирует запись лога в JSON
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

// NewLogger создаёт новый экземпляр логгера
func NewLogger(filename string, levelStr string) (*Logger, error) {
    dir := filepath.Dir(filename)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, fmt.Errorf("failed to create log directory: %v", err)
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
    l.level.Store(int32(level))
    
    go l.writerLoop()
    
    return l, nil
}

// SetFormatter устанавливает форматтер
func (l *Logger) SetFormatter(formatter LogFormatter) {
    l.mu.Lock()
    defer l.mu.Unlock()
    l.formatter = formatter
}

// writerLoop обрабатывает запись логов в файл
func (l *Logger) writerLoop() {
    for entry := range l.writeChan {
        l.writeEntry(entry)
    }
    close(l.done)
}

// writeEntry записывает одну запись в файл
func (l *Logger) writeEntry(entry LogEntry) {
    l.mu.Lock()
    defer l.mu.Unlock()
    
    // Проверка ротации
    if l.currentSize >= l.maxSize {
        l.rotate()
    }
    
    formatted := l.formatter.Format(entry)
    data := []byte(formatted + "\n")
    
    if _, err := l.file.Write(data); err != nil {
        // Не можем залогировать ошибку, так как это приведёт к рекурсии
        return
    }
    
    l.currentSize += int64(len(data))
}

// rotate выполняет ротацию лог-файла
func (l *Logger) rotate() {
    l.file.Sync()
    l.file.Close()
    
    // Переименовываем существующий файл
    for i := l.rotateCount - 1; i >= 0; i-- {
        oldName := fmt.Sprintf("%s.%d", l.path, i)
        newName := fmt.Sprintf("%s.%d", l.path, i+1)
        
        if i == 0 {
            oldName = l.path
        }
        
        if _, err := os.Stat(oldName); err == nil {
            os.Rename(oldName, newName)
        }
    }
    
    // Создаём новый файл
    file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return
    }
    
    l.file = file
    l.currentSize = 0
}

// log внутренний метод для записи лога
func (l *Logger) log(level LogLevel, msg string, fields map[string]interface{}) {
    if level < LogLevel(l.level.Load()) {
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
        // Неблокирующая запись, старый лог теряется - wait-free
    }
}

// Debug записывает DEBUG сообщение
func (l *Logger) Debug(msg string) {
    l.log(DebugLevel, msg, nil)
}

// Debugf записывает форматированное DEBUG сообщение
func (l *Logger) Debugf(format string, args ...interface{}) {
    l.log(DebugLevel, fmt.Sprintf(format, args...), nil)
}

// DebugWithFields записывает DEBUG сообщение с полями
func (l *Logger) DebugWithFields(msg string, fields map[string]interface{}) {
    l.log(DebugLevel, msg, fields)
}

// Info записывает INFO сообщение
func (l *Logger) Info(msg string) {
    l.log(InfoLevel, msg, nil)
}

// Infof записывает форматированное INFO сообщение
func (l *Logger) Infof(format string, args ...interface{}) {
    l.log(InfoLevel, fmt.Sprintf(format, args...), nil)
}

// InfoWithFields записывает INFO сообщение с полями
func (l *Logger) InfoWithFields(msg string, fields map[string]interface{}) {
    l.log(InfoLevel, msg, fields)
}

// Warn записывает WARN сообщение
func (l *Logger) Warn(msg string) {
    l.log(WarnLevel, msg, nil)
}

// Warnf записывает форматированное WARN сообщение
func (l *Logger) Warnf(format string, args ...interface{}) {
    l.log(WarnLevel, fmt.Sprintf(format, args...), nil)
}

// WarnWithFields записывает WARN сообщение с полями
func (l *Logger) WarnWithFields(msg string, fields map[string]interface{}) {
    l.log(WarnLevel, msg, fields)
}

// Error записывает ERROR сообщение
func (l *Logger) Error(msg string) {
    l.log(ErrorLevel, msg, nil)
}

// Errorf записывает форматированное ERROR сообщение
func (l *Logger) Errorf(format string, args ...interface{}) {
    l.log(ErrorLevel, fmt.Sprintf(format, args...), nil)
}

// ErrorWithFields записывает ERROR сообщение с полями
func (l *Logger) ErrorWithFields(msg string, fields map[string]interface{}) {
    l.log(ErrorLevel, msg, fields)
}

// Fatal записывает FATAL сообщение и завершает программу
func (l *Logger) Fatal(msg string) {
    l.log(FatalLevel, msg, nil)
    l.Close()
    os.Exit(1)
}

// Fatalf записывает форматированное FATAL сообщение и завершает программу
func (l *Logger) Fatalf(format string, args ...interface{}) {
    l.log(FatalLevel, fmt.Sprintf(format, args...), nil)
    l.Close()
    os.Exit(1)
}

// FatalWithFields записывает FATAL сообщение с полями и завершает программу
func (l *Logger) FatalWithFields(msg string, fields map[string]interface{}) {
    l.log(FatalLevel, msg, fields)
    l.Close()
    os.Exit(1)
}

// SetLevel устанавливает уровень логирования
func (l *Logger) SetLevel(level LogLevel) {
    l.level.Store(int32(level))
}

// GetLevel возвращает текущий уровень логирования
func (l *Logger) GetLevel() LogLevel {
    return LogLevel(l.level.Load())
}

// Sync синхронизирует лог с диском
func (l *Logger) Sync() error {
    l.mu.Lock()
    defer l.mu.Unlock()
    if l.file != nil {
        return l.file.Sync()
    }
    return nil
}

// Close закрывает логгер
func (l *Logger) Close() {
    close(l.writeChan)
    <-l.done
    l.mu.Lock()
    defer l.mu.Unlock()
    if l.file != nil {
        l.file.Sync()
        l.file.Close()
    }
}

// ========== Глобальные функции для удобства ==========

var defaultLogger *Logger
var once sync.Once

// InitDefaultLogger инициализирует глобальный логгер
func InitDefaultLogger(filename string, level string) error {
    var err error
    once.Do(func() {
        defaultLogger, err = NewLogger(filename, level)
    })
    return err
}

// GetDefaultLogger возвращает глобальный логгер
func GetDefaultLogger() *Logger {
    return defaultLogger
}

// SetDefaultLogger устанавливает глобальный логгер
func SetDefaultLogger(logger *Logger) {
    defaultLogger = logger
}

// Debug глобальная функция DEBUG
func Debug(msg string) {
    if defaultLogger != nil {
        defaultLogger.Debug(msg)
    }
}

// Info глобальная функция INFO
func Info(msg string) {
    if defaultLogger != nil {
        defaultLogger.Info(msg)
    }
}

// Warn глобальная функция WARN
func Warn(msg string) {
    if defaultLogger != nil {
        defaultLogger.Warn(msg)
    }
}

// Error глобальная функция ERROR
func Error(msg string) {
    if defaultLogger != nil {
        defaultLogger.Error(msg)
    }
}

// Debugf глобальная функция форматированного DEBUG
func Debugf(format string, args ...interface{}) {
    if defaultLogger != nil {
        defaultLogger.Debugf(format, args...)
    }
}

// Infof глобальная функция форматированного INFO
func Infof(format string, args ...interface{}) {
    if defaultLogger != nil {
        defaultLogger.Infof(format, args...)
    }
}

// Warnf глобальная функция форматированного WARN
func Warnf(format string, args ...interface{}) {
    if defaultLogger != nil {
        defaultLogger.Warnf(format, args...)
    }
}

// Errorf глобальная функция форматированного ERROR
func Errorf(format string, args ...interface{}) {
    if defaultLogger != nil {
        defaultLogger.Errorf(format, args...)
    }
}

// DebugWithFields глобальная функция DEBUG с полями
func DebugWithFields(msg string, fields map[string]interface{}) {
    if defaultLogger != nil {
        defaultLogger.DebugWithFields(msg, fields)
    }
}

// InfoWithFields глобальная функция INFO с полями
func InfoWithFields(msg string, fields map[string]interface{}) {
    if defaultLogger != nil {
        defaultLogger.InfoWithFields(msg, fields)
    }
}

// WarnWithFields глобальная функция WARN с полями
func WarnWithFields(msg string, fields map[string]interface{}) {
    if defaultLogger != nil {
        defaultLogger.WarnWithFields(msg, fields)
    }
}

// ErrorWithFields глобальная функция ERROR с полями
func ErrorWithFields(msg string, fields map[string]interface{}) {
    if defaultLogger != nil {
        defaultLogger.ErrorWithFields(msg, fields)
    }
}
