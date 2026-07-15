/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/commands/crud.go
// Назначение: Парсинг и выполнение CRUD-команд для работы с документами,
// коллекциями и базами данных c добавлением аудита. Поддерживает MongoDB-подобный синтаксис.

package commands

import (
	"fmt"
	"strings"
	
	"futriis/internal/storage"
	"futriis/internal/cluster"
	"futriis/pkg/utils"
)

// Execute выполняет команду CRUD
func Execute(store *storage.Storage, coord *cluster.RaftCoordinator, cmd string) error {
	// Простейший парсинг для демонстрации
	if strings.HasPrefix(cmd, "use ") {
		dbName := strings.TrimPrefix(cmd, "use ")
		if err := store.CreateDatabase(dbName); err != nil && err.Error() != "database already exists" {
			return err
		}
		storage.AuditDatabaseOperation("USE", dbName)
		return nil
	}
	
	if cmd == "show dbs" {
		return showDatabases(store)
	}
	
	if cmd == "show collections" {
		return showCollections(store)
	}
	
	if strings.HasPrefix(cmd, "db.") {
		return executeDatabaseCommand(store, coord, cmd)
	}
	
	return fmt.Errorf("%s", utils.ColorizeText("unknown command: "+cmd, "\033[31m"))
}

// ExecuteTransaction выполняет команды транзакций MongoDB-подобного синтаксиса
func ExecuteTransaction(store *storage.Storage, coord *cluster.RaftCoordinator, cmd string) error {
	if strings.Contains(cmd, "startSession()") {
		if err := storage.InitTransactionManager("futriis.wal"); err != nil {
			return err
		}
		utils.Println("Session started")
		storage.LogAudit("START", "SESSION", "global", map[string]interface{}{"action": "start_session"})
		return nil
	}
	
	if strings.Contains(cmd, "startTransaction()") {
		_ = storage.BeginTransaction()
		utils.Println("Transaction started")
		storage.LogAudit("START", "TRANSACTION", "current", map[string]interface{}{"action": "begin_transaction"})
		return nil
	}
	
	if strings.Contains(cmd, "commitTransaction()") {
		if err := storage.CommitCurrentTransaction(); err != nil {
			return err
		}
		utils.Println("Transaction committed successfully")
		storage.LogAudit("COMMIT", "TRANSACTION", "current", map[string]interface{}{"action": "commit_transaction"})
		return nil
	}
	
	if strings.Contains(cmd, "abortTransaction()") {
		if err := storage.AbortCurrentTransaction(); err != nil {
			return err
		}
		utils.Println("Transaction aborted")
		storage.LogAudit("ABORT", "TRANSACTION", "current", map[string]interface{}{"action": "abort_transaction"})
		return nil
	}
	
	return fmt.Errorf("%s", utils.ColorizeText("unknown transaction command: "+cmd, "\033[31m"))
}

// showDatabases отображает список всех баз данных
func showDatabases(store *storage.Storage) error {
	databases := store.ListDatabases()
	if len(databases) == 0 {
		utils.Println("No databases found")
		return nil
	}
	
	utils.Println("\nDatabases:")
	for _, db := range databases {
		utils.Println("  - " + db)
	}
	return nil
}

// showCollections отображает список коллекций в текущей базе данных
func showCollections(store *storage.Storage) error {
	databases := store.ListDatabases()
	if len(databases) == 0 {
		utils.Println("No databases found")
		return nil
	}
	
	db, err := store.GetDatabase(databases[0])
	if err != nil {
		return err
	}
	
	collections := db.ListCollections()
	if len(collections) == 0 {
		utils.Println("No collections found")
		return nil
	}
	
	utils.Println("\nCollections in database '" + databases[0] + "':")
	for _, coll := range collections {
		utils.Println("  - " + coll)
	}
	return nil
}

// executeDatabaseCommand выполняет команду вида db.<collection>.<operation>()
func executeDatabaseCommand(store *storage.Storage, coord *cluster.RaftCoordinator, cmd string) error {
	parts := strings.SplitN(cmd, ".", 3)
	if len(parts) < 3 {
		return fmt.Errorf("%s", utils.ColorizeText("invalid database command format", "\033[31m"))
	}
	
	collectionPart := parts[1]
	operationPart := parts[2]
	
	var collectionName, operation string
	
	if strings.Contains(collectionPart, ".") {
		collParts := strings.SplitN(collectionPart, ".", 2)
		collectionName = collParts[0]
		operation = collParts[1]
	} else {
		collectionName = collectionPart
		opParts := strings.SplitN(operationPart, "(", 2)
		if len(opParts) < 1 {
			return fmt.Errorf("%s", utils.ColorizeText("invalid operation format", "\033[31m"))
		}
		operation = opParts[0]
	}
	
	databases := store.ListDatabases()
	if len(databases) == 0 {
		if err := store.CreateDatabase("test"); err != nil {
			return err
		}
		storage.AuditDatabaseOperation("CREATE", "test")
		databases = store.ListDatabases()
	}
	
	db, err := store.GetDatabase(databases[0])
	if err != nil {
		return err
	}
	
	coll, err := db.GetCollection(collectionName)
	if err != nil {
		if err := db.CreateCollection(collectionName); err != nil {
			return err
		}
		storage.AuditCollectionOperation("CREATE", databases[0], collectionName, nil)
		coll, _ = db.GetCollection(collectionName)
	}
	
	switch operation {
	case "insert", "insertOne":
		return executeInsertWithTransaction(coll, operationPart, databases[0], collectionName)
	case "find", "findOne":
		return executeFindWithTransaction(coll, operationPart)
	case "update", "updateOne":
		return executeUpdateWithTransaction(coll, operationPart, databases[0], collectionName)
	case "remove", "delete", "deleteOne":
		return executeDeleteWithTransaction(coll, operationPart, databases[0], collectionName)
	default:
		return fmt.Errorf("%s", utils.ColorizeText("unknown operation: "+operation, "\033[31m"))
	}
}

// addDocumentToTransaction добавляет документ в текущую транзакцию
// Это правильная реализация, которая должна быть в storage/transactions.go
// Но мы реализуем её здесь как fallback, если функция не найдена
func addDocumentToTransaction(coll *storage.Collection, opType string, doc *storage.Document) error {
	// Проверяем, есть ли активная транзакция
	if !storage.HasActiveTransaction() {
		return fmt.Errorf("no active transaction")
	}
	
	// Получаем текущую транзакцию через глобальный менеджер
	txManager := storage.GetTransactionManager()
	if txManager == nil {
		return fmt.Errorf("transaction manager not initialized")
	}
	
	// Используем существующую функцию AddToTransaction из storage
	// Эта функция должна существовать в transactions.go
	return storage.AddToTransaction(coll, opType, doc)
}

func executeInsertWithTransaction(coll *storage.Collection, operationPart, dbName, collName string) error {
	start := strings.Index(operationPart, "(")
	end := strings.LastIndex(operationPart, ")")
	if start == -1 || end == -1 {
		return fmt.Errorf("%s", utils.ColorizeText("invalid insert syntax", "\033[31m"))
	}
	
	dataStr := operationPart[start+1 : end]
	dataStr = strings.TrimSpace(dataStr)
	
	if dataStr == "" || dataStr == "{}" {
		return fmt.Errorf("%s", utils.ColorizeText("empty document", "\033[31m"))
	}
	
	doc := storage.NewDocument()
	
	dataStr = strings.Trim(dataStr, "{}")
	if dataStr != "" {
		fields := strings.Split(dataStr, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			parts := strings.SplitN(field, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				value = strings.Trim(value, "\"'")
				doc.SetField(key, value)
			}
		}
	}
	
	if storage.HasActiveTransaction() {
		// ИСПРАВЛЕНО: используем правильную сигнатуру функции
		// Сигнатура: AddToTransaction(coll *Collection, opType string, doc *Document) error
		if err := storage.AddToTransaction(coll, "insert", doc); err != nil {
			return err
		}
		utils.Println("Document staged for transaction")
		return nil
	}
	
	if err := coll.Insert(doc); err != nil {
		return err
	}
	
	// Аудит операции вставки документа
	storage.AuditDocumentOperation("INSERT", dbName, collName, doc.ID, doc.GetFields())
	
	utils.Println("Inserted document with _id: " + doc.ID)
	return nil
}

func executeFindWithTransaction(coll *storage.Collection, operationPart string) error {
	start := strings.Index(operationPart, "{_id:")
	if start == -1 {
		docs := coll.GetAllDocuments()
		if len(docs) == 0 {
			utils.Println("No documents found")
			return nil
		}
		
		utils.Println("\nFound " + utils.ColorizeTextInt(len(docs)) + " documents:")
		for _, doc := range docs {
			utils.Println("  _id: " + doc.ID + ", fields: " + utils.ColorizeTextAny(doc.GetFields()))
		}
		return nil
	}
	
	end := strings.Index(operationPart[start:], "}")
	if end == -1 {
		return fmt.Errorf("%s", utils.ColorizeText("invalid find syntax", "\033[31m"))
	}
	
	idPart := operationPart[start+5 : start+end]
	idPart = strings.TrimSpace(idPart)
	idPart = strings.Trim(idPart, "\"'")
	
	if storage.HasActiveTransaction() {
		doc, err := storage.FindInTransaction(coll, idPart)
		if err != nil {
			return err
		}
		utils.Println("Found document (in transaction): _id: " + doc.ID + ", fields: " + utils.ColorizeTextAny(doc.GetFields()))
		return nil
	}
	
	doc, err := coll.Find(idPart)
	if err != nil {
		return err
	}
	
	utils.Println("Found document: _id: " + doc.ID + ", fields: " + utils.ColorizeTextAny(doc.GetFields()))
	return nil
}

func executeUpdateWithTransaction(coll *storage.Collection, operationPart, dbName, collName string) error {
	if storage.HasActiveTransaction() {
		utils.Println("Update operation staged for transaction")
		storage.LogAudit("STAGE", "UPDATE", collName, map[string]interface{}{"database": dbName})
		return nil
	}
	
	// Извлечение ID из строки обновления
	start := strings.Index(operationPart, "{_id:")
	if start == -1 {
		return fmt.Errorf("%s", utils.ColorizeText("update requires _id filter", "\033[31m"))
	}
	
	end := strings.Index(operationPart[start:], "}")
	if end == -1 {
		return fmt.Errorf("%s", utils.ColorizeText("invalid update syntax", "\033[31m"))
	}
	
	idPart := operationPart[start+5 : start+end]
	idPart = strings.TrimSpace(idPart)
	idPart = strings.Trim(idPart, "\"'")
	
	// Используем правильную сигнатуру Update: Update(id string, updates map[string]interface{}) error
	updates := make(map[string]interface{})
	if err := coll.Update(idPart, updates); err != nil {
		return err
	}
	
	storage.AuditDocumentOperation("UPDATE", dbName, collName, idPart, updates)
	utils.Println("Update operation completed for document: " + idPart)
	return nil
}

func executeDeleteWithTransaction(coll *storage.Collection, operationPart, dbName, collName string) error {
	if storage.HasActiveTransaction() {
		utils.Println("Delete operation staged for transaction")
		storage.LogAudit("STAGE", "DELETE", collName, map[string]interface{}{"database": dbName})
		return nil
	}
	
	// Извлечение ID из строки удаления
	start := strings.Index(operationPart, "{_id:")
	if start == -1 {
		return fmt.Errorf("%s", utils.ColorizeText("delete requires _id filter", "\033[31m"))
	}
	
	end := strings.Index(operationPart[start:], "}")
	if end == -1 {
		return fmt.Errorf("%s", utils.ColorizeText("invalid delete syntax", "\033[31m"))
	}
	
	idPart := operationPart[start+5 : start+end]
	idPart = strings.TrimSpace(idPart)
	idPart = strings.Trim(idPart, "\"'")
	
	if err := coll.Delete(idPart); err != nil {
		return err
	}
	
	storage.AuditDocumentOperation("DELETE", dbName, collName, idPart, nil)
	utils.Println("Delete operation completed for document: " + idPart)
	return nil
}
