/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: pkg/utils/ansi.go
// Назначение: Кроссплатформенная работа с ANSI-последовательностями для управления
// цветом, стилем текста, позиционированием курсора и очисткой экрана.
// Обеспечивает совместимость с Linux (Debian/Fedora) и Illumos (OpenIndiana/OmniOS),
// автоматически отключая ANSI-коды в неподдерживаемых средах.

package utils

import (
    "encoding/json"
    "fmt"
    "io"
    "os"
    "runtime"
    "strconv"
    "strings"
    "sync/atomic"
)

// ANSICode представляет ANSI escape код
type ANSICode string

// Базовые ANSI коды форматирования
const (
    ANSIReset      ANSICode = "\033[0m"
    ANSIBold       ANSICode = "\033[1m"
    ANSIDim        ANSICode = "\033[2m"
    ANSIItalic     ANSICode = "\033[3m"
    ANSIUnderline  ANSICode = "\033[4m"
    ANSIBlink      ANSICode = "\033[5m"
    ANSIReverse    ANSICode = "\033[7m"
    ANSIHidden     ANSICode = "\033[8m"
    ANSIStrike     ANSICode = "\033[9m"
)

// ANSI коды цветов текста (foreground)
const (
    ANSIFgBlack   ANSICode = "\033[30m"
    ANSIFgRed     ANSICode = "\033[31m"
    ANSIFgGreen   ANSICode = "\033[32m"
    ANSIFgYellow  ANSICode = "\033[33m"
    ANSIFgBlue    ANSICode = "\033[34m"
    ANSIFgMagenta ANSICode = "\033[35m"
    ANSIFgCyan    ANSICode = "\033[36m"
    ANSIFgWhite   ANSICode = "\033[37m"
    
    ANSIFgBrightBlack   ANSICode = "\033[90m"
    ANSIFgBrightRed     ANSICode = "\033[91m"
    ANSIFgBrightGreen   ANSICode = "\033[92m"
    ANSIFgBrightYellow  ANSICode = "\033[93m"
    ANSIFgBrightBlue    ANSICode = "\033[94m"
    ANSIFgBrightMagenta ANSICode = "\033[95m"
    ANSIFgBrightCyan    ANSICode = "\033[96m"
    ANSIFgBrightWhite   ANSICode = "\033[97m"
    
    // Специальный цвет #00bfff (Deep Sky Blue)
    ANSIFgDeepSkyBlue ANSICode = "\033[38;2;0;191;255m"
)

// ANSI коды цветов фона (background)
const (
    ANSIBgBlack   ANSICode = "\033[40m"
    ANSIBgRed     ANSICode = "\033[41m"
    ANSIBgGreen   ANSICode = "\033[42m"
    ANSIBgYellow  ANSICode = "\033[43m"
    ANSIBgBlue    ANSICode = "\033[44m"
    ANSIBgMagenta ANSICode = "\033[45m"
    ANSIBgCyan    ANSICode = "\033[46m"
    ANSIBgWhite   ANSICode = "\033[47m"
    
    ANSIBgBrightBlack   ANSICode = "\033[100m"
    ANSIBgBrightRed     ANSICode = "\033[101m"
    ANSIBgBrightGreen   ANSICode = "\033[102m"
    ANSIBgBrightYellow  ANSICode = "\033[103m"
    ANSIBgBrightBlue    ANSICode = "\033[104m"
    ANSIBgBrightMagenta ANSICode = "\033[105m"
    ANSIBgBrightCyan    ANSICode = "\033[106m"
    ANSIBgBrightWhite   ANSICode = "\033[107m"
)

// ANSI коды управления курсором и экраном
const (
    ANSICursorHome      ANSICode = "\033[H"
    ANSICursorUp        ANSICode = "\033[A"
    ANSICursorDown      ANSICode = "\033[B"
    ANSICursorForward   ANSICode = "\033[C"
    ANSICursorBack      ANSICode = "\033[D"
    ANSIClearScreen     ANSICode = "\033[2J"
    ANSIClearLine       ANSICode = "\033[2K"
    ANSISaveCursor      ANSICode = "\033[s"
    ANSIRestoreCursor   ANSICode = "\033[u"
    ANSIHideCursor      ANSICode = "\033[?25l"
    ANSIShowCursor      ANSICode = "\033[?25h"
)

// ANSI коды для работы с табуляцией и скроллингом
const (
    ANSIScrollUp    ANSICode = "\033[D"
    ANSIScrollDown  ANSICode = "\033[M"
    ANSISetTab      ANSICode = "\033H"
    ANSIClearTab    ANSICode = "\033[g"
    ANSIClearAllTabs ANSICode = "\033[3g"
)

// ANSI коды для 256-цветной и RGB палитры
const (
    ANSI256FgPrefix = "\033[38;5;"
    ANSI256BgPrefix = "\033[48;5;"
    ANSIRGBFgPrefix = "\033[38;2;"
    ANSIRGBBgPrefix = "\033[48;2;"
    ANSISuffix      = "m"
)

// ANSISupportLevel определяет уровень поддержки ANSI в терминале
type ANSISupportLevel int32

const (
    SupportNone    ANSISupportLevel = iota // Нет поддержки ANSI
    SupportBasic                           // Только базовые 16 цветов
    Support256                             // Поддержка 256 цветов
    SupportTrueColor                       // Поддержка True Color (RGB)
)

// ANSIEnableState хранит состояние включения/выключения ANSI
var (
    ansiEnabled   atomic.Bool
    supportLevel  atomic.Int32
    termEnv       string
    colorTermEnv  string
)

func init() {
    // Определяем поддержку ANSI при загрузке пакета
    detectANSISupport()
    
    // По умолчанию ANSI включен, если есть поддержка
    if getSupportLevel() != SupportNone {
        ansiEnabled.Store(true)
    } else {
        ansiEnabled.Store(false)
    }
}

// detectANSISupport определяет уровень поддержки ANSI в текущем окружении
func detectANSISupport() {
    // Проверяем операционную систему
    switch runtime.GOOS {
    case "linux", "illumos", "solaris", "darwin":
        // Unix-like системы обычно поддерживают ANSI
    default:
        supportLevel.Store(int32(SupportNone))
        return
    }
    
    // Проверяем переменные окружения
    termEnv = strings.ToLower(os.Getenv("TERM"))
    colorTermEnv = strings.ToLower(os.Getenv("COLORTERM"))
    
    // Проверка на True Color поддержку
    if colorTermEnv == "truecolor" || colorTermEnv == "24bit" {
        supportLevel.Store(int32(SupportTrueColor))
        return
    }
    
    // Проверка на 256 цветов
    if strings.Contains(termEnv, "256") || strings.Contains(colorTermEnv, "256") {
        supportLevel.Store(int32(Support256))
        return
    }
    
    // Проверка на базовую поддержку цвета
    if strings.Contains(termEnv, "color") || strings.Contains(termEnv, "xterm") ||
       strings.Contains(termEnv, "screen") || strings.Contains(termEnv, "vt100") {
        supportLevel.Store(int32(SupportBasic))
        return
    }
    
    // Поддержка IllumOS (OpenIndiana, OmniOS)
    if runtime.GOOS == "illumos" || runtime.GOOS == "solaris" {
        // В IllumOS терминалы обычно поддерживают ANSI
        supportLevel.Store(int32(SupportBasic))
        return
    }
    
    supportLevel.Store(int32(SupportNone))
}

// getSupportLevel возвращает текущий уровень поддержки ANSI
func getSupportLevel() ANSISupportLevel {
    return ANSISupportLevel(supportLevel.Load())
}

// EnableANSI включает вывод ANSI кодов
func EnableANSI() {
    ansiEnabled.Store(true)
}

// DisableANSI отключает вывод ANSI кодов
func DisableANSI() {
    ansiEnabled.Store(false)
}

// IsANSIEnabled возвращает текущий статус ANSI
func IsANSIEnabled() bool {
    return ansiEnabled.Load() && getSupportLevel() != SupportNone
}

// ApplyCode применяет ANSI код к строке (если ANSI включен)
func ApplyCode(text string, code ANSICode) string {
    if !IsANSIEnabled() {
        return text
    }
    return string(code) + text + string(ANSIReset)
}

// ColorizeText раскрашивает текст указанным цветом с поддержкой стилей
func ColorizeText(text string, color ANSICode, styles ...ANSICode) string {
    if !IsANSIEnabled() {
        return text
    }
    
    var builder strings.Builder
    builder.WriteString(string(color))
    
    for _, style := range styles {
        builder.WriteString(string(style))
    }
    
    builder.WriteString(text)
    builder.WriteString(string(ANSIReset))
    
    return builder.String()
}

// SetColorEnabled включает или отключает цветной вывод (глобальная настройка)
func SetColorEnabled(enabled bool) {
    if enabled {
        EnableANSI()
    } else {
        DisableANSI()
    }
}

// DisableColorMode отключает цветной режим
func DisableColorMode() {
    DisableANSI()
}

// Print выводит текст без форматирования
func Print(a ...interface{}) {
    fmt.Print(a...)
}

// Println выводит строку с переводом
func Println(a ...interface{}) {
    fmt.Println(a...)
}

// PrintInfo выводит информационное сообщение (синим цветом)
func PrintInfo(msg string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgBrightCyan) + msg + string(ANSIReset))
    } else {
        fmt.Println(msg)
    }
}

// PrintSuccess выводит сообщение об успехе (зелёным цветом)
func PrintSuccess(msg string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgBrightGreen) + msg + string(ANSIReset))
    } else {
        fmt.Println("✓ " + msg)
    }
}

// PrintError выводит сообщение об ошибке (красным цветом)
func PrintError(msg string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgBrightRed) + "Error: " + msg + string(ANSIReset))
    } else {
        fmt.Println("Error: " + msg)
    }
}

// PrintWarning выводит предупреждение (жёлтым цветом)
func PrintWarning(msg string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgBrightYellow) + "Warning: " + msg + string(ANSIReset))
    } else {
        fmt.Println("Warning: " + msg)
    }
}

// PrintHeader выводит заголовок (фиолетовым, жирным шрифтом)
func PrintHeader(msg string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgBrightMagenta) + string(ANSIBold) + msg + string(ANSIReset))
    } else {
        fmt.Println("=== " + msg + " ===")
    }
}

// PrintJSON выводит данные в формате JSON с отступами
func PrintJSON(data interface{}) {
    jsonData, err := json.MarshalIndent(data, "", "  ")
    if err != nil {
        PrintError("Failed to marshal JSON: " + err.Error())
        return
    }
    fmt.Println(string(jsonData))
}

// FormatBytes форматирует байты в человекочитаемый вид (B, KB, MB, GB, TB)
func FormatBytes(bytes int64) string {
    const unit = 1024
    if bytes < unit {
        return fmt.Sprintf("%d B", bytes)
    }
    div, exp := int64(unit), 0
    for n := bytes / unit; n >= unit; n /= unit {
        div *= unit
        exp++
    }
    switch exp {
    case 0:
        return fmt.Sprintf("%.1f KB", float64(bytes)/float64(div))
    case 1:
        return fmt.Sprintf("%.1f MB", float64(bytes)/float64(div))
    case 2:
        return fmt.Sprintf("%.1f GB", float64(bytes)/float64(div))
    case 3:
        return fmt.Sprintf("%.1f TB", float64(bytes)/float64(div))
    default:
        return fmt.Sprintf("%d B", bytes)
    }
}

// FormatDuration форматирует длительность в человекочитаемый вид
func FormatDuration(milliseconds int64) string {
    seconds := milliseconds / 1000
    minutes := seconds / 60
    hours := minutes / 60
    days := hours / 24
    
    if days > 0 {
        return fmt.Sprintf("%d days", days)
    }
    if hours > 0 {
        return fmt.Sprintf("%d hours", hours)
    }
    if minutes > 0 {
        return fmt.Sprintf("%d minutes", minutes)
    }
    if seconds > 0 {
        return fmt.Sprintf("%d seconds", seconds)
    }
    return fmt.Sprintf("%d ms", milliseconds)
}

// TruncateString обрезает строку до указанной длины и добавляет "..."
func TruncateString(s string, maxLen int) string {
    if len(s) <= maxLen {
        return s
    }
    return s[:maxLen-3] + "..."
}

// Indent добавляет отступ к каждой строке
func Indent(text string, indent string) string {
    lines := strings.Split(text, "\n")
    for i, line := range lines {
        lines[i] = indent + line
    }
    return strings.Join(lines, "\n")
}

// Confirm запрашивает подтверждение у пользователя
func Confirm(prompt string) bool {
    fmt.Print(prompt + " [y/N]: ")
    var response string
    fmt.Scanln(&response)
    response = strings.ToLower(strings.TrimSpace(response))
    return response == "y" || response == "yes"
}

// PrintDataTable выводит данные в виде таблицы (обёртка над PrintTable)
func PrintDataTable(headers []string, rows [][]string) {
    if len(rows) == 0 {
        PrintInfo("No data to display")
        return
    }
    
    // Вычисляем ширину колонок
    colWidths := make([]int, len(headers))
    for i, header := range headers {
        colWidths[i] = len(header)
    }
    
    for _, row := range rows {
        for i, cell := range row {
            if len(cell) > colWidths[i] {
                colWidths[i] = len(cell)
            }
        }
    }
    
    // Выводим заголовки
    separator := "+"
    for _, width := range colWidths {
        separator += strings.Repeat("-", width+2) + "+"
    }
    
    fmt.Println(separator)
    headerLine := "|"
    for i, header := range headers {
        headerLine += fmt.Sprintf(" %-*s |", colWidths[i], header)
    }
    fmt.Println(headerLine)
    fmt.Println(separator)
    
    // Выводим строки
    for _, row := range rows {
        line := "|"
        for i, cell := range row {
            line += fmt.Sprintf(" %-*s |", colWidths[i], cell)
        }
        fmt.Println(line)
    }
    fmt.Println(separator)
}

// GetEnv возвращает переменную окружения или значение по умолчанию
func GetEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

// Contains проверяет, содержит ли слайс указанный элемент
func Contains(slice []string, item string) bool {
    for _, s := range slice {
        if s == item {
            return true
        }
    }
    return false
}

// Unique возвращает уникальные элементы слайса
func Unique(slice []string) []string {
    keys := make(map[string]bool)
    result := make([]string, 0, len(slice))
    for _, item := range slice {
        if !keys[item] {
            keys[item] = true
            result = append(result, item)
        }
    }
    return result
}

// PrintDeepSkyBlue выводит текст цветом #00bfff без перевода строки
func PrintDeepSkyBlue(text string) {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIFgDeepSkyBlue) + text)
    } else {
        fmt.Print(text)
    }
}

// PrintlnDeepSkyBlue выводит строку цветом #00bfff с переводом строки
func PrintlnDeepSkyBlue(text string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgDeepSkyBlue) + text)
    } else {
        fmt.Println(text)
    }
}

// PrintDeepSkyBlueReset выводит текст цветом #00bfff и сбрасывает цвет после
func PrintDeepSkyBlueReset(text string) {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIFgDeepSkyBlue) + text + string(ANSIReset))
    } else {
        fmt.Print(text)
    }
}

// PrintlnDeepSkyBlueReset выводит строку цветом #00bfff с переводом строки и сбрасывает цвет
func PrintlnDeepSkyBlueReset(text string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgDeepSkyBlue) + text + string(ANSIReset))
    } else {
        fmt.Println(text)
    }
}

// ResetColor сбрасывает цвет вывода на стандартный
func ResetColor() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIReset))
    }
}

// SetGlobalColor устанавливает цвет для всего последующего вывода
func SetGlobalColor(color ANSICode) {
    if IsANSIEnabled() {
        fmt.Print(string(color))
    }
}

// ResetGlobalColor сбрасывает глобальный цвет вывода
func ResetGlobalColor() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIReset))
    }
}

// Colorize256 раскрашивает текст 256-цветной палитрой
func Colorize256(text string, colorCode int, isBackground bool) string {
    if !IsANSIEnabled() || getSupportLevel() < Support256 {
        return text
    }
    
    if colorCode < 0 || colorCode > 255 {
        return text
    }
    
    prefix := ANSI256FgPrefix
    if isBackground {
        prefix = ANSI256BgPrefix
    }
    
    code := string(prefix) + strconv.Itoa(colorCode) + ANSISuffix
    return code + text + string(ANSIReset)
}

// ColorizeRGB раскрашивает текст RGB цветом
func ColorizeRGB(text string, r, g, b int, isBackground bool) string {
    if !IsANSIEnabled() || getSupportLevel() < SupportTrueColor {
        return text
    }
    
    if r < 0 || r > 255 || g < 0 || g > 255 || b < 0 || b > 255 {
        return text
    }
    
    prefix := ANSIRGBFgPrefix
    if isBackground {
        prefix = ANSIRGBBgPrefix
    }
    
    code := string(prefix) + strconv.Itoa(r) + ";" + strconv.Itoa(g) + ";" + strconv.Itoa(b) + ANSISuffix
    return code + text + string(ANSIReset)
}

// ClearScreen очищает экран и перемещает курсор в домашнюю позицию
func ClearScreen() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIClearScreen) + string(ANSICursorHome))
    } else {
        // Если ANSI не поддерживается, просто выводим несколько пустых строк
        fmt.Print("\n\n\n\n\n\n\n\n\n\n")
    }
}

// ClearLine очищает текущую строку
func ClearLine() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIClearLine))
    }
}

// MoveCursor перемещает курсор в указанную позицию
func MoveCursor(row, col int) {
    if IsANSIEnabled() {
        fmt.Printf("\033[%d;%dH", row, col)
    }
}

// MoveCursorUp перемещает курсор вверх на n позиций
func MoveCursorUp(n int) {
    if IsANSIEnabled() && n > 0 {
        fmt.Printf("\033[%dA", n)
    }
}

// MoveCursorDown перемещает курсор вниз на n позиций
func MoveCursorDown(n int) {
    if IsANSIEnabled() && n > 0 {
        fmt.Printf("\033[%dB", n)
    }
}

// MoveCursorForward перемещает курсор вперёд на n позиций
func MoveCursorForward(n int) {
    if IsANSIEnabled() && n > 0 {
        fmt.Printf("\033[%dC", n)
    }
}

// MoveCursorBack перемещает курсор назад на n позиций
func MoveCursorBack(n int) {
    if IsANSIEnabled() && n > 0 {
        fmt.Printf("\033[%dD", n)
    }
}

// SaveCursorPosition сохраняет текущую позицию курсора
func SaveCursorPosition() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSISaveCursor))
    }
}

// RestoreCursorPosition восстанавливает сохранённую позицию курсора
func RestoreCursorPosition() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIRestoreCursor))
    }
}

// HideCursor скрывает курсор
func HideCursor() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIHideCursor))
    }
}

// ShowCursor показывает курсор
func ShowCursor() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIShowCursor))
    }
}

// SetTitle устанавливает заголовок терминала
func SetTitle(title string) {
    if IsANSIEnabled() {
        fmt.Printf("\033]0;%s\007", title)
    }
}

// GetCursorPosition возвращает текущую позицию курсора
// Возвращает (row, col, error)
func GetCursorPosition() (int, int, error) {
    if !IsANSIEnabled() {
        return 0, 0, fmt.Errorf("ANSI not supported")
    }
    
    fmt.Print("\033[6n")
    var row, col int
    _, err := fmt.Scanf("\033[%d;%dR", &row, &col)
    return row, col, err
}

// PrintProgressBar выводит прогресс-бар с указанным процентом
func PrintProgressBar(percentage float64, width int, color ANSICode) {
    if percentage < 0 {
        percentage = 0
    }
    if percentage > 100 {
        percentage = 100
    }
    
    filled := int(float64(width) * percentage / 100.0)
    empty := width - filled
    
    bar := "["
    if filled > 0 {
        bar += strings.Repeat("=", filled-1)
        if percentage < 100 {
            bar += ">"
        } else {
            bar += "="
        }
    }
    if empty > 0 {
        bar += strings.Repeat(" ", empty)
    }
    bar += "]"
    
    coloredBar := ColorizeText(bar, color)
    fmt.Printf("\r%s %.1f%%", coloredBar, percentage)
}

// PrintTable выводит данные в виде таблицы с ANSI-форматированием
func PrintTable(headers []string, rows [][]string, borderColor ANSICode) {
    if len(rows) == 0 {
        return
    }
    
    // Вычисляем ширину колонок
    colWidths := make([]int, len(headers))
    for i, header := range headers {
        colWidths[i] = len(header)
    }
    
    for _, row := range rows {
        for i, cell := range row {
            if i < len(colWidths) && len(cell) > colWidths[i] {
                colWidths[i] = len(cell)
            }
        }
    }
    
    // Выводим разделитель
    printTableBorder(colWidths, borderColor)
    
    // Выводим заголовки
    fmt.Print(ColorizeText("|", borderColor))
    for i, header := range headers {
        padded := fmt.Sprintf(" %-*s ", colWidths[i], header)
        fmt.Print(ColorizeText(padded, ANSIFgBrightCyan, ANSIBold))
        fmt.Print(ColorizeText("|", borderColor))
    }
    fmt.Println()
    
    // Выводим разделитель
    printTableBorder(colWidths, borderColor)
    
    // Выводим строки данных
    for _, row := range rows {
        fmt.Print(ColorizeText("|", borderColor))
        for i, cell := range row {
            if i >= len(colWidths) {
                break
            }
            padded := fmt.Sprintf(" %-*s ", colWidths[i], cell)
            fmt.Print(padded)
            fmt.Print(ColorizeText("|", borderColor))
        }
        fmt.Println()
    }
    
    // Выводим нижнюю границу
    printTableBorder(colWidths, borderColor)
}

// printTableBorder выводит границу таблицы
func printTableBorder(colWidths []int, borderColor ANSICode) {
    fmt.Print(ColorizeText("+", borderColor))
    for _, width := range colWidths {
        fmt.Print(ColorizeText(strings.Repeat("-", width+2), borderColor))
        fmt.Print(ColorizeText("+", borderColor))
    }
    fmt.Println()
}

// FadeText создаёт эффект затухания текста (градиент)
func FadeText(text string, startColor, endColor ANSICode) string {
    if !IsANSIEnabled() || len(text) == 0 {
        return text
    }
    
    runes := []rune(text)
    result := strings.Builder{}
    
    for i, r := range runes {
        // Рассчитываем прогресс для градиента
        progress := float64(i) / float64(len(runes)-1)
        _ = progress // Используем progress для будущего расширения функционала
        
        // Для простоты используем начальный цвет на всём протяжении
        // В будущем здесь можно реализовать плавный переход между цветами
        if i < len(runes)/2 {
            result.WriteString(string(startColor))
        } else {
            result.WriteString(string(endColor))
        }
        result.WriteRune(r)
    }
    
    result.WriteString(string(ANSIReset))
    return result.String()
}

// BlinkText создаёт мигающий текст
func BlinkText(text string) string {
    return ApplyCode(text, ANSIBlink)
}

// ReverseText инвертирует цвета текста
func ReverseText(text string) string {
    return ApplyCode(text, ANSIReverse)
}

// UnderlineText подчёркивает текст
func UnderlineText(text string) string {
    return ApplyCode(text, ANSIUnderline)
}

// BoldText делает текст жирным
func BoldText(text string) string {
    return ApplyCode(text, ANSIBold)
}

// ItalicText делает текст курсивом
func ItalicText(text string) string {
    return ApplyCode(text, ANSIItalic)
}

// StrikeText зачёркивает текст
func StrikeText(text string) string {
    return ApplyCode(text, ANSIStrike)
}

// GetANSIColorByName возвращает ANSI код по имени цвета
func GetANSIColorByName(colorName string) ANSICode {
    colorMap := map[string]ANSICode{
        "black":   ANSIFgBlack,
        "red":     ANSIFgRed,
        "green":   ANSIFgGreen,
        "yellow":  ANSIFgYellow,
        "blue":    ANSIFgBlue,
        "magenta": ANSIFgMagenta,
        "cyan":    ANSIFgCyan,
        "white":   ANSIFgWhite,
        
        "bright_black":   ANSIFgBrightBlack,
        "bright_red":     ANSIFgBrightRed,
        "bright_green":   ANSIFgBrightGreen,
        "bright_yellow":  ANSIFgBrightYellow,
        "bright_blue":    ANSIFgBrightBlue,
        "bright_magenta": ANSIFgBrightMagenta,
        "bright_cyan":    ANSIFgBrightCyan,
        "bright_white":   ANSIFgBrightWhite,
        
        "deepskyblue": ANSIFgDeepSkyBlue,
        "#00bfff":     ANSIFgDeepSkyBlue,
    }
    
    if color, ok := colorMap[strings.ToLower(colorName)]; ok {
        return color
    }
    return ANSIFgWhite
}

// GetANSIBgColorByName возвращает ANSI код фона по имени цвета
func GetANSIBgColorByName(colorName string) ANSICode {
    colorMap := map[string]ANSICode{
        "black":   ANSIBgBlack,
        "red":     ANSIBgRed,
        "green":   ANSIBgGreen,
        "yellow":  ANSIBgYellow,
        "blue":    ANSIBgBlue,
        "magenta": ANSIBgMagenta,
        "cyan":    ANSIBgCyan,
        "white":   ANSIBgWhite,
        
        "bright_black":   ANSIBgBrightBlack,
        "bright_red":     ANSIBgBrightRed,
        "bright_green":   ANSIBgBrightGreen,
        "bright_yellow":  ANSIBgBrightYellow,
        "bright_blue":    ANSIBgBrightBlue,
        "bright_magenta": ANSIBgBrightMagenta,
        "bright_cyan":    ANSIBgBrightCyan,
        "bright_white":   ANSIBgBrightWhite,
    }
    
    if color, ok := colorMap[strings.ToLower(colorName)]; ok {
        return color
    }
    return ANSIBgBlack
}

// StripANSI удаляет все ANSI коды из строки
func StripANSI(text string) string {
    result := strings.Builder{}
    inEscape := false
    
    for i := 0; i < len(text); i++ {
        if text[i] == '\033' {
            inEscape = true
            continue
        }
        
        if inEscape {
            if (text[i] >= 'A' && text[i] <= 'Z') || (text[i] >= 'a' && text[i] <= 'z') {
                inEscape = false
            }
            continue
        }
        
        result.WriteByte(text[i])
    }
    
    return result.String()
}

// MeasureString возвращает видимую длину строки без ANSI кодов
func MeasureString(text string) int {
    return len(StripANSI(text))
}

// CenterText центрирует текст с учётом ANSI кодов
func CenterText(text string, width int) string {
    strippedLen := MeasureString(text)
    if strippedLen >= width {
        return text
    }
    
    padding := (width - strippedLen) / 2
    return strings.Repeat(" ", padding) + text + strings.Repeat(" ", width-padding-strippedLen)
}

// RightAlign выравнивает текст по правому краю
func RightAlign(text string, width int) string {
    strippedLen := MeasureString(text)
    if strippedLen >= width {
        return text
    }
    
    padding := width - strippedLen
    return strings.Repeat(" ", padding) + text
}

// LeftAlign выравнивает текст по левому краю
func LeftAlign(text string, width int) string {
    strippedLen := MeasureString(text)
    if strippedLen >= width {
        return text
    }
    
    padding := width - strippedLen
    return text + strings.Repeat(" ", padding)
}

// PrintColoredBox выводит цветной прямоугольник
func PrintColoredBox(width, height int, borderColor, fillColor ANSICode) {
    if !IsANSIEnabled() {
        return
    }
    
    // Верхняя граница
    fmt.Print(ColorizeText("+"+strings.Repeat("-", width-2)+"+", borderColor))
    fmt.Println()
    
    // Заполнение
    for i := 0; i < height-2; i++ {
        fmt.Print(ColorizeText("|", borderColor))
        fmt.Print(ColorizeText(strings.Repeat(" ", width-2), fillColor))
        fmt.Print(ColorizeText("|", borderColor))
        fmt.Println()
    }
    
    // Нижняя граница
    fmt.Print(ColorizeText("+"+strings.Repeat("-", width-2)+"+", borderColor))
    fmt.Println()
}

// GetSupportInfo возвращает информацию о поддержке ANSI
func GetSupportInfo() map[string]interface{} {
    info := make(map[string]interface{})
    info["enabled"] = IsANSIEnabled()
    info["support_level"] = getSupportLevel().String()
    info["os"] = runtime.GOOS
    info["term"] = termEnv
    info["colorterm"] = colorTermEnv
    
    return info
}

// String возвращает строковое представление уровня поддержки ANSI
func (l ANSISupportLevel) String() string {
    switch l {
    case SupportNone:
        return "none"
    case SupportBasic:
        return "basic"
    case Support256:
        return "256"
    case SupportTrueColor:
        return "truecolor"
    default:
        return "unknown"
    }
}

// WrapWithColor оборачивает текст в цвет с возможностью вложенности
type ColorWrapper struct {
    codes []ANSICode
}

// NewColorWrapper создаёт новый обёртку для цвета
func NewColorWrapper(codes ...ANSICode) *ColorWrapper {
    return &ColorWrapper{codes: codes}
}

// Wrap оборачивает текст в цвет
func (cw *ColorWrapper) Wrap(text string) string {
    if !IsANSIEnabled() {
        return text
    }
    
    var builder strings.Builder
    for _, code := range cw.codes {
        builder.WriteString(string(code))
    }
    builder.WriteString(text)
    builder.WriteString(string(ANSIReset))
    
    return builder.String()
}

// AddCode добавляет ANSI код в обёртку
func (cw *ColorWrapper) AddCode(code ANSICode) {
    cw.codes = append(cw.codes, code)
}

// Clear очищает все ANSI коды
func (cw *ColorWrapper) Clear() {
    cw.codes = nil
}

// GradientColor создаёт градиент между двумя цветами
type GradientColor struct {
    startColor ANSICode
    endColor   ANSICode
}

// NewGradient создаёт новый градиент
func NewGradient(start, end ANSICode) *GradientColor {
    return &GradientColor{
        startColor: start,
        endColor:   end,
    }
}

// Apply применяет градиент к тексту
func (g *GradientColor) Apply(text string) string {
    if !IsANSIEnabled() || len(text) == 0 {
        return text
    }
    
    runes := []rune(text)
    result := strings.Builder{}
    
    for i, r := range runes {
        // Рассчитываем прогресс для плавного перехода
        ratio := float64(i) / float64(len(runes)-1)
        
        // Простая интерполяция - используем разные цвета для разных частей
        if ratio < 0.33 {
            result.WriteString(string(g.startColor))
        } else if ratio < 0.66 {
            result.WriteString(string(ANSIReset))
            result.WriteString(string(ANSIFgGreen))
        } else {
            result.WriteString(string(g.endColor))
        }
        result.WriteRune(r)
    }
    
    result.WriteString(string(ANSIReset))
    return result.String()
}

// PrintDeepSkyBlueAll выводит текст цветом #00bfff с автоматическим сбросом после каждой строки
func PrintDeepSkyBlueAll(text string) {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIFgDeepSkyBlue) + text + string(ANSIReset))
    } else {
        fmt.Print(text)
    }
}

// PrintlnDeepSkyBlueAll выводит строку цветом #00bfff с переводом строки и автоматическим сбросом
func PrintlnDeepSkyBlueAll(text string) {
    if IsANSIEnabled() {
        fmt.Println(string(ANSIFgDeepSkyBlue) + text + string(ANSIReset))
    } else {
        fmt.Println(text)
    }
}

// FprintfDeepSkyBlue форматированный вывод в io.Writer цветом #00bfff
func FprintfDeepSkyBlue(w io.Writer, format string, args ...interface{}) {
    msg := fmt.Sprintf(format, args...)
    if IsANSIEnabled() {
        fmt.Fprint(w, string(ANSIFgDeepSkyBlue)+msg+string(ANSIReset))
    } else {
        fmt.Fprint(w, msg)
    }
}

// InitDeepSkyBlueMode включает режим постоянного цвета #00bfff
func InitDeepSkyBlueMode() {
    if IsANSIEnabled() {
        fmt.Print(string(ANSIFgDeepSkyBlue))
    }
}
