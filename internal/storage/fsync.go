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

package storage

import (
    "fmt"
    "os"
    "runtime"
    "syscall"
    "time"
)

// RealFsync выполняет реальный fsync на файле
func RealFsync(file *os.File) error {
    if file == nil {
        return nil
    }
    
    // Вызываем системный fsync
    return file.Sync()
}

// RealFsyncWithRetry выполняет fsync с повторными попытками
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

// FsyncDir синхронизирует директорию (для гарантии, что создание файла записано на диск)
func FsyncDir(dirPath string) error {
    dir, err := os.Open(dirPath)
    if err != nil {
        return err
    }
    defer dir.Close()
    
    if runtime.GOOS == "windows" {
        // На Windows нет прямой поддержки fsync для директорий
        return nil
    }
    
    return dir.Sync()
}

// FsyncDataOnly выполняет fsync только для данных (не метаданных)
func FsyncDataOnly(file *os.File) error {
    if file == nil {
        return nil
    }
    
    fd := int(file.Fd())
    _, _, err := syscall.Syscall(syscall.SYS_FSYNC, uintptr(fd), 0, 0)
    if err != 0 {
        return fmt.Errorf("fsync failed: %v", err)
    }
    return nil
}
