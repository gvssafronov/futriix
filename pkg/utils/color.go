/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: pkg/utils/color.go
// Назначение: Кроссплатформенная цветная печать с ANSI-кодами,
// работающая в Linux (Debian/Fedora) и Illumos (OpenIndiana/OmniOS).

package utils

import (
    "time"
    
    "github.com/fatih/color"
)

// Глобальный объект цвета Deep Sky Blue (#00bfff)
var (
    deepSkyBlueColor = color.New(color.FgHiCyan) // Ярко-голубой, близкий к #00bfff
    // Альтернатива с точным RGB-кодом (работает в современных терминалах):
    // deepSkyBlueColor = color.New(color.FgRGB(0, 191, 255))
)

// EnableDeepSkyBlueMode включает режим постоянного цвета.
// В библиотеке fatih/color для этого используется функция color.Set().
func EnableDeepSkyBlueMode() {
    deepSkyBlueColor.Set()
}

// DisableDeepSkyBlueMode отключает режим постоянного цвета и сбрасывает настройки.
func DisableDeepSkyBlueMode() {
    color.Unset()
}

// SetDeepSkyBlueEnabled — удобная функция для включения/выключения цвета.
func SetDeepSkyBlueEnabled(enabled bool) {
    if enabled {
        EnableDeepSkyBlueMode()
    } else {
        DisableDeepSkyBlueMode()
    }
}

// PrintDeepSkyBlueColored выводит текст цветом Deep Sky Blue (без перевода строки)
func PrintDeepSkyBlueColored(a ...interface{}) {
    deepSkyBlueColor.Print(a...)
}

// PrintlnDeepSkyBlueColored выводит строку с цветом Deep Sky Blue и добавляет перевод строки
func PrintlnDeepSkyBlueColored(a ...interface{}) {
    deepSkyBlueColor.Println(a...)
}

// PrintfDeepSkyBlue форматирует и выводит текст цветом Deep Sky Blue.
func PrintfDeepSkyBlue(format string, a ...interface{}) {
    deepSkyBlueColor.Printf(format, a...)
}

// PrintErrorRed выводит сообщение об ошибке красным цветом.
func PrintErrorRed(msg string) {
    errorColor := color.New(color.FgRed)
    errorColor.Println("Error: " + msg)
}

// ColorizeTextAny преобразует любой тип в цветную строку.
func ColorizeTextAny(v interface{}) string {
    return deepSkyBlueColor.Sprint(v)
}

// ColorizeTextInt преобразует int в цветную строку.
func ColorizeTextInt(n int) string {
    return deepSkyBlueColor.Sprint(n)
}

// ColorizeTextByColor возвращает строку, окрашенную в указанный цвет.
// Поддерживаемые цвета: red, green, yellow, blue, magenta, cyan, white, black
func ColorizeTextByColor(text string, colorName string) string {
    switch colorName {
    case "red":
        return color.New(color.FgRed).Sprint(text)
    case "green":
        return color.New(color.FgGreen).Sprint(text)
    case "yellow":
        return color.New(color.FgYellow).Sprint(text)
    case "blue":
        return color.New(color.FgBlue).Sprint(text)
    case "magenta":
        return color.New(color.FgMagenta).Sprint(text)
    case "cyan":
        return color.New(color.FgCyan).Sprint(text)
    case "white":
        return color.New(color.FgWhite).Sprint(text)
    case "black":
        return color.New(color.FgBlack).Sprint(text)
    default:
        return text
    }
}

// GetCurrentTimestamp возвращает текущий timestamp в миллисекундах
func GetCurrentTimestamp() int64 {
    return time.Now().UnixMilli()
}
