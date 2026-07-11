/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/commands/commands.go
// Назначение: Реализация MongoDB-подобных команд CRUD и команд управления кластером.
// Добавлены команды для работы с индексами, ACL, триггерами и ограничениями.

package commands

import (
    "futriis/pkg/utils"
)

// ShowHelp отображает справку по всем доступным командам
func ShowHelp() {
    helpText := `
=== FUTRIIS DATABASE COMMANDS ===

DATABASE MANAGEMENT:
  use <db>                     - Switch to database
  show dbs                     - List all databases
  show collections             - List collections in current database

COLLECTION OPERATIONS:
  db.createCollection("<name>") - Create new collection
  db.<collection>.insert({...}) - Insert document into collection
  db.<collection>.find({_id: "..."}) - Find document by ID
  db.<collection>.find()       - Find all documents in collection
  db.<collection>.findByIndex("<index>", "<value>") - Find by secondary index
  db.<collection>.update({_id: "..."}, {...}) - Update document
  db.<collection>.remove({_id: "..."}) - Delete document

INDEX MANAGEMENT:
  db.<collection>.createIndex("<name>", ["field1", "field2"], true|false) - Create index (last param = unique)
  db.<collection>.dropIndex("<name>") - Drop index
  db.<collection>.listIndexes() - List all indexes

CONSTRAINTS:
  db.<collection>.addRequired("<field>") - Add required field constraint
  db.<collection>.addUnique("<field>") - Add unique constraint
  db.<collection>.addMin("<field>", <value>) - Add minimum value constraint
  db.<collection>.addMax("<field>", <value>) - Add maximum value constraint
  db.<collection>.addEnum("<field>", [values]) - Add enum constraint

TRIGGERS (MongoDB-like syntax):
  db.<collection>.createTrigger("<name>", "<event>", {
    condition: { field: "<field>", operator: "<op>", value: <value> },
    action: "<action>",
    operations: [
      { type: "set", field: "<field>", value: "<value>" },
      { type: "inc", field: "<field>", value: <number> },
      { type: "currentDate", field: "<field>" }
    ]
  })
  
  Events: BEFORE_INSERT, AFTER_INSERT, BEFORE_UPDATE, AFTER_UPDATE, BEFORE_DELETE, AFTER_DELETE
  Actions: abort (cancel operation), skip (skip operation), modify (modify document), log (write to log), notify (send notification)
  Special values: $$NOW (current timestamp), $$USER (current user), $$ROLE (current role)
  
  db.<collection>.dropTrigger("<name>") - Drop trigger
  db.<collection>.listTriggers() - List all triggers on collection
  db.<collection>.enableTrigger("<name>") - Enable trigger
  db.<collection>.disableTrigger("<name>") - Disable trigger
  db.getTriggerLog() - Show trigger execution log

TRIGGER EXAMPLES:
  // Auto-set updated_at timestamp on every update
  db.users.createTrigger("update_timestamp", "BEFORE_UPDATE", {
    action: "modify",
    operations: [{ type: "set", field: "updated_at", value: "$$NOW" }]
  })
  
  // Prevent deletion of active users
  db.users.createTrigger("protect_active", "BEFORE_DELETE", {
    condition: { field: "status", operator: "eq", value: "active" },
    action: "abort"
  })
  
  // Log all inserts
  db.orders.createTrigger("audit_log", "AFTER_INSERT", {
    action: "log",
    description: "Log all order creations"
  })
  
  // Increment counter on document insert
  db.stats.createTrigger("inc_counter", "AFTER_INSERT", {
    action: "modify",
    operations: [{ type: "inc", field: "counter", value: 1 }]
  })

ACL MANAGEMENT:
  acl createUser "<username>" "<password>" [roles] - Create new user
  acl createRole "<rolename>" - Create new role
  acl grant "<rolename>" "<permission>" - Grant permission to role
  acl addUserRole "<username>" "<rolename>" - Add role to user
  acl login "<username>" "<password>" - Login (returns session token)
  acl logout - Logout current session
  acl listUsers - List all users
  acl listRoles - List all roles

TRANSACTIONS (MongoDB-like syntax):
  session = db.startSession()  - Start a new session
  session.startTransaction()   - Begin a transaction
  session.commitTransaction()  - Commit current transaction
  session.abortTransaction()   - Abort/Rollback current transaction

EXPORT/IMPORT (MessagePack format):
  export "database_name" "filename.msgpack"   - Export entire database
  import "database_name" "filename.msgpack"   - Import database from .msgpack file

CLUSTER MANAGEMENT:
  cluster status               - Show cluster status
  cluster nodes                - List all cluster nodes
  cluster add <ip> <port>      - Add node to cluster
  cluster remove <node_id>     - Remove node from cluster
  cluster sync <db> <coll>     - Sync collection across cluster
  cluster replication-factor [n] - Get or set replication factor
  cluster leader               - Show cluster leader
  cluster health               - Check cluster health

HTTP API:
  The database also exposes HTTP RESTful API on port 8080 (configurable)
  See documentation for endpoints: /api/db/, /api/index/, /api/acl/, /api/constraint/, /api/trigger/

UTILITIES:
  help                         - Show this help message
  exit / quit                  - Exit database

`
    utils.Println(helpText)
}
