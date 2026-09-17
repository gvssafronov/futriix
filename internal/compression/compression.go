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
// Защита от collision magic bytes через усиленную проверку заголовка

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

// Расширенный magic с версией для предотвращения collision
var MagicNumberV2 = []byte{0x46, 0x54, 0x52, 0x53, 0x02} // "FTRS\x02"

// MinCompressedSize — минимальный размер валидного сжатого блока
// Для защиты от collision с пользовательскими данными
const MinCompressedSize = 14 // 5 (magic v2) + 1 (type) + 8 (size)

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
        
        if _, err := writer.Write(data); err != nil {
            return nil, fmt.Errorf("brotli write failed: %v", err)
        }
        if err := writer.Close(); err != nil {
            return nil, fmt.Errorf("brotli close failed: %v", err)
        }
        compressed = buf.Bytes()
        compType = CompressionBrotli
        
    case "zstd":
        var encoder *zstd.Encoder
        var encoderLevel zstd.EncoderLevel
        
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
    
    // Используем MagicNumberV2 (5 байт) + type (1) + size (8) = 14 байт
    header := make([]byte, 5+1+8)
    copy(header[0:5], MagicNumberV2)
    header[5] = byte(compType)
    binary.LittleEndian.PutUint64(header[6:], uint64(len(data)))
    
    result := make([]byte, 0, len(header)+len(compressed))
    result = append(result, header...)
    result = append(result, compressed...)
    
    return result, nil
}

// Decompress распаковывает данные
// Поддержка обоих форматов (V1 и V2), усиленная валидация
func Decompress(data []byte) ([]byte, error) {
    // Проверяем V2 формат (5-байтовый magic)
    if len(data) >= MinCompressedSize && bytes.Equal(data[0:5], MagicNumberV2) {
        return decompressV2(data)
    }
    
    // Проверяем V1 формат (4-байтовый magic) для обратной совместимости
    if len(data) >= 4+1+8 && bytes.Equal(data[0:4], MagicNumber) {
        return decompressV1(data)
    }
    
    return nil, fmt.Errorf("invalid magic number")
}

// decompressV1 распаковывает данные в старом формате V1
func decompressV1(data []byte) ([]byte, error) {
    if len(data) < 4+1+8 {
        return nil, fmt.Errorf("data too short for compressed format V1")
    }
    
    compType := CompressionType(data[4])
    originalSize := binary.LittleEndian.Uint64(data[5:13])
    compressedData := data[13:]
    
    if originalSize == 0 {
        return nil, fmt.Errorf("invalid original size")
    }
    
    return doDecompress(compType, compressedData, originalSize)
}

// decompressV2 распаковывает данные в новом формате V2
//  Добавлена проверка согласованности размера
func decompressV2(data []byte) ([]byte, error) {
    if len(data) < MinCompressedSize {
        return nil, fmt.Errorf("data too short for compressed format V2")
    }
    
    compType := CompressionType(data[5])
    originalSize := binary.LittleEndian.Uint64(data[6:14])
    compressedData := data[14:]
    
    // Проверка разумности размера
    if originalSize == 0 || originalSize > 100*1024*1024*1024 { // Max 100GB
        return nil, fmt.Errorf("invalid original size: %d", originalSize)
    }
    
    if len(compressedData) == 0 {
        return nil, fmt.Errorf("no compressed data")
    }
    
    return doDecompress(compType, compressedData, originalSize)
}

// doDecompress выполняет распаковку по типу
func doDecompress(compType CompressionType, compressedData []byte, originalSize uint64) ([]byte, error) {
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
    
    // Проверяем, что распакованный размер совпадает с заявленным
    if uint64(len(decompressed)) != originalSize {
        return nil, fmt.Errorf("size mismatch: expected %d, got %d", originalSize, len(decompressed))
    }
    
    return decompressed, nil
}

// DecompressAuto автоматически определяет, сжаты ли данные, и распаковывает при необходимости
// Усиленная проверка для предотвращения collision
func DecompressAuto(data []byte) ([]byte, error) {
    // Проверяем V2 magic (более надёжный)
    if len(data) >= MinCompressedSize && bytes.Equal(data[0:5], MagicNumberV2) {
        return Decompress(data)
    }
    
    // Проверяем V1 magic (для обратной совместимости)
    if len(data) >= 4+1+8 && bytes.Equal(data[0:4], MagicNumber) {
        // Дополнительная проверка: тип сжатия должен быть валидным
        compType := CompressionType(data[4])
        if compType <= CompressionZstd {
            return Decompress(data)
        }
    }
    
    // Данные не сжаты
    return data, nil
}

// IsCompressed проверяет, сжаты ли данные
// Проверка обоих форматов
func IsCompressed(data []byte) bool {
    if len(data) >= MinCompressedSize && bytes.Equal(data[0:5], MagicNumberV2) {
        return true
    }
    if len(data) >= 4+1+8 && bytes.Equal(data[0:4], MagicNumber) {
        compType := CompressionType(data[4])
        return compType <= CompressionZstd
    }
    return false
}

// GetCompressionType возвращает тип сжатия данных
func GetCompressionType(data []byte) CompressionType {
    if len(data) >= MinCompressedSize && bytes.Equal(data[0:5], MagicNumberV2) {
        return CompressionType(data[5])
    }
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
