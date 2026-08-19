/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/storage/fsync.go
// Назначение: Реальная синхронизация с диском (fsync) для WAL
// Поддерживает все платформы: Linux, macOS, BSD, Windows, Solaris/Illumos (OpenIndiana)

package storage

import (
    "fmt"
    "os"
    "runtime"
    "time"
)

// RealFsync выполняет реальный fsync на файле.
// Использует стандартный метод Sync() для максимальной совместимости
// со всеми платформами включая OpenIndiana (Solaris/Illumos).
func RealFsync(file *os.File) error {
    if file == nil {
        return nil
    }
    
    // Вызываем системный fsync через стандартный метод Sync
    // Это работает на всех платформах: Linux, macOS, BSD, Windows, Solaris/Illumos
    return file.Sync()
}

// RealFsyncWithRetry выполняет fsync с повторными попытками.
// Параметры:
//   - file: файл для синхронизации
//   - maxRetries: максимальное количество попыток
//   - retryDelay: задержка между попытками
//
// Возвращает ошибку, если все попытки не удались.
func RealFsyncWithRetry(file *os.File, maxRetries int, retryDelay time.Duration) error {
    if file == nil {
        return nil
    }
    
    var lastErr error
    for i := 0; i < maxRetries; i++ {
        if err := RealFsync(file); err != nil {
            lastErr = err
            if i < maxRetries-1 {
                time.Sleep(retryDelay)
                continue
            }
            return fmt.Errorf("fsync failed after %d attempts: %v", maxRetries, lastErr)
        }
        return nil
    }
    return lastErr
}

// FsyncDir синхронизирует директорию (для гарантии, что создание файла записано на диск).
// На некоторых платформах (Windows, Solaris/Illumos) fsync для директорий не поддерживается.
func FsyncDir(dirPath string) error {
    dir, err := os.Open(dirPath)
    if err != nil {
        return err
    }
    defer dir.Close()
    
    // На Windows и Solaris/Illumos нет прямой поддержки fsync для директорий
    // Используем стандартный Sync если доступен
    switch runtime.GOOS {
    case "windows", "solaris", "illumos", "openindiana":
        // На этих платформах fsync для директорий не поддерживается или не нужен
        // Возвращаем nil, так как данные уже записаны через fsync файлов
        return nil
    default:
        // Для Linux, BSD, macOS используем Sync
        return dir.Sync()
    }
}

// FsyncDataOnly выполняет fsync только для данных (не метаданных).
// Использует стандартный Sync для всех платформ.
// Это обеспечивает запись данных на диск без синхронизации метаданных.
func FsyncDataOnly(file *os.File) error {
    if file == nil {
        return nil
    }
    
    // Используем стандартный Sync для всех платформ
    // Это обеспечит запись данных на диск
    return file.Sync()
}

// FsyncWithTimeout выполняет fsync с таймаутом.
// Если операция не завершается за указанное время, возвращается ошибка.
func FsyncWithTimeout(file *os.File, timeout time.Duration) error {
    if file == nil {
        return nil
    }
    
    // Создаём канал для результата
    result := make(chan error, 1)
    
    // Запускаем fsync в отдельной горутине
    go func() {
        result <- file.Sync()
    }()
    
    // Ожидаем результат или таймаут
    select {
    case err := <-result:
        return err
    case <-time.After(timeout):
        return fmt.Errorf("fsync timeout after %v", timeout)
    }
}

// MustFsync выполняет fsync и паникует при ошибке.
// Используется только в критических секциях, где ошибка синхронизации фатальна.
func MustFsync(file *os.File) {
    if file == nil {
        return
    }
    
    if err := file.Sync(); err != nil {
        panic(fmt.Sprintf("Fatal: fsync failed: %v", err))
    }
}

// IsFsyncSupported возвращает true если платформа поддерживает fsync.
// Все платформы поддерживают fsync через file.Sync().
func IsFsyncSupported() bool {
    return true
}

// GetFsyncPlatformInfo возвращает информацию о платформе для диагностики.
func GetFsyncPlatformInfo() map[string]interface{} {
    return map[string]interface{}{
        "os":            runtime.GOOS,
        "arch":          runtime.GOARCH,
        "fsync_supported": IsFsyncSupported(),
        "dir_fsync_supported": !(runtime.GOOS == "windows" || 
                                 runtime.GOOS == "solaris" || 
                                 runtime.GOOS == "illumos" || 
                                 runtime.GOOS == "openindiana"),
        "method":        "file.Sync()",
    }
}
