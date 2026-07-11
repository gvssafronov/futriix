/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/commands/export_import.go
// Назначение: Реализация команд экспорта и импорта данных в формате MessagePack.
// Синтаксис: export "Имя_слайса" "название_экспортируемого_файла".msgpack
//  import "Имя_слайса" "название_импортируемого_файла".msgpack

package commands

import (
    "fmt"
    "os"
    "strings"
    
    "futriis/internal/storage"
    "futriis/pkg/utils"
    "futriis/internal/serializer"
)

// ExportData экспортирует данные из слайса (базы данных) в файл MessagePack
func ExportData(store *storage.Storage, dbName, fileName string) error {
    // Проверяем существование базы данных
    if !store.ExistsDatabase(dbName) {
        return fmt.Errorf("database '%s' not found", dbName)
    }
    
    // Получаем базу данных
    db, err := store.GetDatabase(dbName)
    if err != nil {
        return fmt.Errorf("failed to get database: %v", err)
    }
    
    // Собираем все данные из всех коллекций
    exportData := make(map[string]interface{})
    
    collections := db.ListCollections()
    for _, collName := range collections {
        coll, err := db.GetCollection(collName)
        if err != nil {
            continue
        }
        
        // Получаем все документы коллекции
        docs := coll.GetAllDocuments()
        
        // Сериализуем документы в формат для экспорта
        collData := make([]map[string]interface{}, 0, len(docs))
        for _, doc := range docs {
            docData := map[string]interface{}{
                "_id":        doc.ID,
                "fields":     doc.GetFields(),
                "created_at": doc.CreatedAt,
                "updated_at": doc.UpdatedAt,
                "version":    doc.Version,
            }
            collData = append(collData, docData)
        }
        
        exportData[collName] = collData
    }
    
    // Добавляем метаданные
    exportData["_metadata"] = map[string]interface{}{
        "database":    dbName,
        "export_time": fmt.Sprintf("%d", utils.GetCurrentTimestamp()),
        "version":     "1.0",
        "collections": len(collections),
    }
    
    // Сериализуем в MessagePack
    data, err := serializer.Marshal(exportData)
    if err != nil {
        return fmt.Errorf("failed to marshal export data: %v", err)
    }
    
    // Записываем в файл
    if err := os.WriteFile(fileName, data, 0644); err != nil {
        return fmt.Errorf("failed to write export file: %v", err)
    }
    
    fmt.Printf("✓ Database '%s' exported successfully to %s\n", dbName, fileName)
    fmt.Printf("  Collections exported: %d\n", len(collections))
    
    return nil
}

// ImportData импортирует данные из файла MessagePack в слайс (базу данных)
func ImportData(store *storage.Storage, dbName, fileName string) error {
    // Проверяем существование файла
    if _, err := os.Stat(fileName); os.IsNotExist(err) {
        return fmt.Errorf("import file '%s' not found", fileName)
    }
    
    // Читаем файл
    data, err := os.ReadFile(fileName)
    if err != nil {
        return fmt.Errorf("failed to read import file: %v", err)
    }
    
    // Десериализуем из MessagePack
    var importData map[string]interface{}
    if err := serializer.Unmarshal(data, &importData); err != nil {
        return fmt.Errorf("failed to unmarshal import data: %v", err)
    }
    
    // Проверяем метаданные
    metadata, ok := importData["_metadata"].(map[string]interface{})
    if !ok {
        return fmt.Errorf("invalid import file format: missing metadata")
    }
    
    sourceDB, _ := metadata["database"].(string)
    fmt.Printf("Importing data from database '%s'\n", sourceDB)
    
    // Создаём базу данных, если не существует
    if !store.ExistsDatabase(dbName) {
        if err := store.CreateDatabase(dbName); err != nil {
            return fmt.Errorf("failed to create database: %v", err)
        }
        fmt.Printf("Created database '%s'\n", dbName)
    }
    
    // Получаем базу данных
    db, err := store.GetDatabase(dbName)
    if err != nil {
        return fmt.Errorf("failed to get database: %v", err)
    }
    
    importedCollections := 0
    importedDocuments := 0
    skippedDocuments := 0
    failedDocuments := 0
    
    // Импортируем коллекции
    for key, value := range importData {
        if key == "_metadata" {
            continue
        }
        
        collName := key
        collData, ok := value.([]interface{})
        if !ok {
            fmt.Printf("  Warning: collection '%s' has invalid format, skipping\n", collName)
            continue
        }
        
        // Создаём коллекцию, если не существует
        if _, err := db.GetCollection(collName); err != nil {
            if err := db.CreateCollection(collName); err != nil {
                fmt.Printf("  Warning: failed to create collection '%s': %v\n", collName, err)
                continue
            }
            fmt.Printf("  Created collection '%s'\n", collName)
        }
        
        coll, err := db.GetCollection(collName)
        if err != nil {
            fmt.Printf("  Warning: failed to get collection '%s': %v\n", collName, err)
            continue
        }
        
        collectionImported := 0
        collectionSkipped := 0
        collectionFailed := 0
        
        // Импортируем документы
        for _, docRaw := range collData {
            docMap, ok := docRaw.(map[string]interface{})
            if !ok {
                collectionFailed++
                continue
            }
            
            // Получаем ID документа
            var docID string
            if id, ok := docMap["_id"].(string); ok {
                docID = id
            } else {
                // Если нет ID, пропускаем
                collectionFailed++
                continue
            }
            
            // Проверяем, существует ли уже документ с таким ID
            if existingDoc, _ := coll.Find(docID); existingDoc != nil {
                collectionSkipped++
                skippedDocuments++
                continue
            }
            
            // Создаём документ
            doc := storage.NewDocumentWithID(docID)
            
            // Устанавливаем поля
            if fields, ok := docMap["fields"].(map[string]interface{}); ok {
                for k, v := range fields {
                    doc.SetField(k, v)
                }
            }
            
            // Устанавливаем временные метки с правильным преобразованием типов
            if createdAt, ok := docMap["created_at"]; ok {
                switch v := createdAt.(type) {
                case int64:
                    doc.CreatedAt = v
                case int:
                    doc.CreatedAt = int64(v)
                case float64:
                    doc.CreatedAt = int64(v)
                }
            }
            
            if updatedAt, ok := docMap["updated_at"]; ok {
                switch v := updatedAt.(type) {
                case int64:
                    doc.UpdatedAt = v
                case int:
                    doc.UpdatedAt = int64(v)
                case float64:
                    doc.UpdatedAt = int64(v)
                }
            }
            
            if version, ok := docMap["version"]; ok {
                switch v := version.(type) {
                case uint64:
                    doc.Version = v
                case int:
                    doc.Version = uint64(v)
                case float64:
                    doc.Version = uint64(v)
                }
            }
            
            // Вставляем документ
            if err := coll.Insert(doc); err != nil {
                fmt.Printf("  Warning: failed to insert document %s: %v\n", doc.ID, err)
                collectionFailed++
                failedDocuments++
                continue
            }
            
            collectionImported++
            importedDocuments++
        }
        
        if collectionImported > 0 || collectionSkipped > 0 || collectionFailed > 0 {
            fmt.Printf("  Collection '%s': %d imported, %d skipped, %d failed\n", 
                collName, collectionImported, collectionSkipped, collectionFailed)
        }
        importedCollections++
    }
    
    fmt.Printf("✓ Database '%s' imported successfully from %s\n", dbName, fileName)
    fmt.Printf("  Collections imported: %d\n", importedCollections)
    fmt.Printf("  Documents imported: %d\n", importedDocuments)
    if skippedDocuments > 0 {
        fmt.Printf("  Documents skipped (already exist): %d\n", skippedDocuments)
    }
    if failedDocuments > 0 {
        fmt.Printf("  Documents failed: %d\n", failedDocuments)
    }
    
    return nil
}

// ExecuteExport выполняет команду экспорта
func ExecuteExport(store *storage.Storage, cmd string) error {
    // Формат: export "Имя_слайса" "название_экспортируемого_файла".msgpack
    parts := strings.SplitN(cmd, " ", 3)
    if len(parts) < 3 {
        return fmt.Errorf("usage: export \"database_name\" \"filename.msgpack\"")
    }
    
    dbName := strings.Trim(parts[1], "\"")
    fileName := strings.Trim(parts[2], "\"")
    
    // Проверяем расширение файла
    if !strings.HasSuffix(fileName, ".msgpack") {
        fileName = fileName + ".msgpack"
    }
    
    return ExportData(store, dbName, fileName)
}

// ExecuteImport выполняет команду импорта
func ExecuteImport(store *storage.Storage, cmd string) error {
    // Формат: import "Имя_слайса" "название_импортируемого_файла".msgpack
    parts := strings.SplitN(cmd, " ", 3)
    if len(parts) < 3 {
        return fmt.Errorf("usage: import \"database_name\" \"filename.msgpack\"")
    }
    
    dbName := strings.Trim(parts[1], "\"")
    fileName := strings.Trim(parts[2], "\"")
    
    // Проверяем расширение файла
    if !strings.HasSuffix(fileName, ".msgpack") {
        fileName = fileName + ".msgpack"
    }
    
    return ImportData(store, dbName, fileName)
}
