/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */
 
// Файл: internal/compression/compression.go
// Назначение: Реализация сжатия данных с использованием различных алгоритмов.
// Поддерживаемый алгоритм: Brotli.
// Обеспечивает прозрачное сжатие/распаковку для документов.
// LZ4 был удалён в пользу Brotli для лучшего версионирования.

package compression

import (
    "bytes"
    "encoding/binary"
    "fmt"
    
    "github.com/golang/snappy"
    "github.com/klauspost/compress/zstd"
    "github.com/andybalholm/brotli"
)

// Config представляет конфигурацию сжатия
type Config struct {
    Enabled   bool   // Включено ли сжатие
    Algorithm string // Алгоритм сжатия: snappy, brotli, zstd
    Level     int    // Уровень сжатия (1-9)
    MinSize   int    // Минимальный размер для сжатия (байт)
}

// MagicNumber используется для идентификации сжатых данных
var MagicNumber = []byte{0x46, 0x54, 0x52, 0x53} // "FTRS" - Futriis

// CompressionType определяет тип сжатия
type CompressionType byte

const (
    CompressionNone     CompressionType = 0x00
    CompressionSnappy   CompressionType = 0x01
    CompressionBrotli   CompressionType = 0x02
    CompressionZstd     CompressionType = 0x03
)

// Compress сжимает данные с использованием указанного алгоритма
func Compress(data []byte, config *Config) ([]byte, error) {
    if !config.Enabled {
        return data, nil
    }
    
    if len(data) < config.MinSize {
        return data, nil
    }
    
    var compressed []byte
    var err error
    var compType CompressionType
    
    switch config.Algorithm {
    case "snappy":
        compressed = snappy.Encode(nil, data)
        compType = CompressionSnappy
        
    case "brotli":
        buf := bytes.NewBuffer(nil)
        writer := brotli.NewWriter(buf)
        
        // Устанавливаем уровень сжатия для Brotli
        // Brotli использует качество от 0 до 11, где 11 - максимальное сжатие
        quality := config.Level
        if quality < 0 {
            quality = 4 // стандартное качество по умолчанию
        }
        if quality > 11 {
            quality = 11
        }
        
        // Brotli Writer не имеет прямой установки качества в этой библиотеке,
        // используем стандартные настройки
        if _, err := writer.Write(data); err != nil {
            return nil, fmt.Errorf("brotli write failed: %v", err)
        }
        if err := writer.Close(); err != nil {
            return nil, fmt.Errorf("brotli close failed: %v", err)
        }
        compressed = buf.Bytes()
        compType = CompressionBrotli
        
    case "zstd":
        // Для Zstandard используем предустановленные уровни скорости
        var encoder *zstd.Encoder
        var encoderLevel zstd.EncoderLevel
        
        // Выбираем уровень сжатия на основе config.Level
        switch {
        case config.Level <= 1:
            encoderLevel = zstd.SpeedFastest
        case config.Level <= 3:
            encoderLevel = zstd.SpeedDefault
        case config.Level <= 6:
            encoderLevel = zstd.SpeedBetterCompression
        default:
            encoderLevel = zstd.SpeedBestCompression
        }
        
        // Создаём энкодер с выбранным уровнем
        encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(encoderLevel))
        if err != nil {
            return nil, fmt.Errorf("failed to create zstd encoder: %v", err)
        }
        defer encoder.Close()
        
        compressed = encoder.EncodeAll(data, nil)
        compType = CompressionZstd
        
    default:
        return nil, fmt.Errorf("unsupported compression algorithm: %s", config.Algorithm)
    }
    
    // Проверяем, что сжатие действительно уменьшило размер
    if len(compressed) >= len(data) {
        return data, nil
    }
    
    // Добавляем заголовок: магическое число (4 байта) + тип сжатия (1 байт) + оригинальный размер (8 байт)
    header := make([]byte, 4+1+8)
    copy(header[0:4], MagicNumber)
    header[4] = byte(compType)
    binary.LittleEndian.PutUint64(header[5:], uint64(len(data)))
    
    result := make([]byte, 0, len(header)+len(compressed))
    result = append(result, header...)
    result = append(result, compressed...)
    
    return result, nil
}

// Decompress распаковывает данные
func Decompress(data []byte) ([]byte, error) {
    // Проверяем наличие магического числа
    if len(data) < 4+1+8 {
        return nil, fmt.Errorf("data too short for compressed format")
    }
    
    // Проверяем магическое число
    if !bytes.Equal(data[0:4], MagicNumber) {
        return nil, fmt.Errorf("invalid magic number")
    }
    
    compType := CompressionType(data[4])
    originalSize := binary.LittleEndian.Uint64(data[5:13])
    compressedData := data[13:]
    
    if originalSize == 0 {
        return nil, fmt.Errorf("invalid original size")
    }
    
    var decompressed []byte
    var err error
    
    switch compType {
    case CompressionSnappy:
        decompressed, err = snappy.Decode(nil, compressedData)
        if err != nil {
            return nil, fmt.Errorf("snappy decode failed: %v", err)
        }
        
    case CompressionBrotli:
        reader := brotli.NewReader(bytes.NewReader(compressedData))
        buf := bytes.NewBuffer(nil)
        _, err = buf.ReadFrom(reader)
        if err != nil {
            return nil, fmt.Errorf("brotli decode failed: %v", err)
        }
        decompressed = buf.Bytes()
        
    case CompressionZstd:
        decoder, err := zstd.NewReader(nil)
        if err != nil {
            return nil, fmt.Errorf("failed to create zstd decoder: %v", err)
        }
        defer decoder.Close()
        
        decompressed, err = decoder.DecodeAll(compressedData, nil)
        if err != nil {
            return nil, fmt.Errorf("zstd decode failed: %v", err)
        }
        
    case CompressionNone:
        return compressedData, nil
        
    default:
        return nil, fmt.Errorf("unsupported compression type: %d", compType)
    }
    
    return decompressed, nil
}

// DecompressAuto автоматически определяет, сжаты ли данные, и распаковывает при необходимости
func DecompressAuto(data []byte) ([]byte, error) {
    // Проверяем, есть ли магическое число (признак сжатых данных)
    if len(data) >= 4 && bytes.Equal(data[0:4], MagicNumber) {
        return Decompress(data)
    }
    return data, nil
}

// IsCompressed проверяет, сжаты ли данные
func IsCompressed(data []byte) bool {
    if len(data) < 4 {
        return false
    }
    return bytes.Equal(data[0:4], MagicNumber)
}

// GetCompressionType возвращает тип сжатия данных
func GetCompressionType(data []byte) CompressionType {
    if !IsCompressed(data) || len(data) < 5 {
        return CompressionNone
    }
    return CompressionType(data[4])
}

// GetCompressionRatio возвращает коэффициент сжатия
func GetCompressionRatio(original, compressed []byte) float64 {
    if len(original) == 0 {
        return 1.0
    }
    return float64(len(compressed)) / float64(len(original))
}
