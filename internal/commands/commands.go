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
// Добавлены команды для работы с индексами, ACL, триггерами, ограничениями и кросс-датацентровой миграцией.

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

CROSS-DATACENTER MIGRATION COMMANDS:
  migration status [task_id]                - Show migration status (current or specific)
  migration list                            - List all migration tasks
  migration start <source_dc> <target_dc> [database] [collection] - Start new migration
  migration pause <task_id>                 - Pause a running migration
  migration resume <task_id>                - Resume a paused migration
  migration cancel <task_id>                - Cancel a migration
  migration stats                           - Show migration statistics
  migration config                          - Show migration configuration
  migration queue                           - Show change queue status
  migration validate <task_id>              - Validate a completed migration

MIGRATION EXAMPLES:
  // List all migration tasks
  migration list
  
  // Check status of current migration
  migration status
  
  // Check status of specific task
  migration status mig_1234567890_dc-primary
  
  // Start migration: all databases, all collections
  migration start dc-primary dc-secondary
  
  // Start migration: specific database, all collections
  migration start dc-primary dc-secondary mydb
  
  // Start migration: specific database and collection
  migration start dc-primary dc-secondary mydb users
  
  // Pause running migration
  migration pause mig_1234567890_dc-primary
  
  // Resume paused migration
  migration resume mig_1234567890_dc-primary
  
  // Cancel migration
  migration cancel mig_1234567890_dc-primary
  
  // Show migration statistics
  migration stats
  
  // Show current configuration
  migration config
  
  // Show queue status
  migration queue

MIGRATION MODES (config.toml):
  manual    - Fully manual: all operations require explicit commands
  semi_auto - Semi-automatic: main migration manual, delta sync automatic (default)
  auto      - Fully automatic: continuous migration with delta sync

MIGRATION STATUS COLORS:
  🟢 completed  - Migration successfully finished
  🟡 migrating  - Migration in progress
  🟡 delta_sync - Delta synchronization in progress
  🔵 preparing  - Preparing metadata
  🔵 validating - Validating data
  🟠 paused     - Paused by user
  🔴 failed     - Migration failed

================================================================================

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
  See documentation for endpoints: /api/db/, /api/index/, /api/acl/, /api/constraint/, /api/trigger/, /api/migration/

UTILITIES:
  help                         - Show this help message
  exit / quit                  - Exit database

`
    utils.Println(helpText)
}

// ShowMigrationHelp отображает справку по командам миграции
func ShowMigrationHelp() {
    migrationHelp := `
=== CROSS-DATACENTER MIGRATION HELP ===

OVERVIEW:
  Cross-datacenter migration allows seamless data transfer between geographically
  distributed futriix clusters without downtime.

LIFECYCLE:
  Idle → Preparing → Migrating → Delta Sync → Validating → Completed
                      ↘           ↘            ↘
                       Paused    Failed       (errors)

COMMANDS:
  migration status [task_id]     Show migration status
  migration list                 List all migration tasks
  migration start <src> <tgt> [database] [collection] - Start new migration
  migration pause <task_id>      Pause migration
  migration resume <task_id>     Resume migration
  migration cancel <task_id>     Cancel migration
  migration stats                Show statistics
  migration config               Show configuration
  migration queue                Show queue status
  migration validate <task_id>   Validate migrated data

CONFIGURATION (config.toml):
  [migration]
    enabled = false
    mode = "semi_auto"          # manual, semi_auto, auto
    
    [migration.source]
      name = "dc-primary"
      endpoint = "localhost:8080"
      timeout_sec = 30
    
    [migration.target]
      name = "dc-secondary"
      endpoint = "localhost:8081"
      timeout_sec = 30
    
    [migration.settings]
      batch_size = 1000
      workers = 4
      compression = "snappy"    # snappy, lz4, zstd
      resume_enabled = true
      checkpoint_interval_sec = 30
      max_retries = 3
      retry_backoff_sec = 5
      collections = []
      exclude_collections = []
    
    [migration.delta]
      enabled = true
      interval_sec = 60
      max_lag_sec = 300
    
    [migration.validation]
      enabled = true
      sample_percent = 10
      max_errors = 100

FEATURES:
  ✅ Resume after interruption (checkpoints)
  ✅ Parallel transfer (configurable workers)
  ✅ Compression (Snappy, LZ4, Zstd)
  ✅ Delta sync (continuous replication)
  ✅ Data validation (checksums)
  ✅ Fault-tolerant (automatic retries)
  ✅ Idempotent operations
  ✅ Persistent change queue
  ✅ Audit logging
  ✅ Cross-datacenter support

EXAMPLES:
  # Start migration all databases
  migration start dc-primary dc-secondary

  # Start migration specific database
  migration start dc-primary dc-secondary mydb

  # Start migration specific collection
  migration start dc-primary dc-secondary mydb users

  # Check migration status
  migration status

  # Check specific task
  migration status mig_1234567890_dc-primary

  # List all migration tasks
  migration list

  # Validate migrated data
  migration validate mig_1234567890_dc-primary

  # Show migration statistics
  migration stats

  # Show queue status
  migration queue
`
    utils.Println(migrationHelp)
}

// ShowMigrationStatusInfo отображает подробную информацию о статусе миграции
func ShowMigrationStatusInfo() {
    statusInfo := `
=== MIGRATION STATUS DETAILS ===

FIELD               DESCRIPTION
─────────────────────────────────────────────────────────────────────────────
ID                  Unique task identifier
Source DC           Source datacenter name
Target DC           Target datacenter name
Status              Current migration state
Progress            Migration progress percentage
Total Docs          Total documents to migrate
Migrated Docs       Successfully migrated documents
Failed Docs         Failed documents
Skipped Docs        Skipped documents
Start Time          Migration start time
End Time            Migration completion time
Last Checkpoint     Last checkpoint timestamp
Error               Error message if migration failed

STATUS VALUES:
  idle       - No active migration
  preparing  - Collecting metadata
  migrating  - Data transfer in progress
  delta_sync - Synchronizing changes
  validating - Validating data
  completed  - Migration finished successfully
  failed     - Migration failed with error
  paused     - Migration paused by user

TROUBLESHOOTING:
  If migration fails, check:
  1. Network connectivity between datacenters
  2. Target datacenter is running
  3. Configuration file is correct
  4. Logs for detailed error messages

  To recover from failure:
  1. Check migration status
  2. If status is "failed", check error message
  3. Fix the issue
  4. Resume migration: migration resume <task_id>
  5. Or cancel and start new: migration cancel <task_id> && migration start <source> <target>
`
    utils.Println(statusInfo)
}

// ShowMigrationConfigInfo отображает информацию о конфигурации миграции
func ShowMigrationConfigInfo() {
    configInfo := `
=== MIGRATION CONFIGURATION GUIDE ===

GENERAL SETTINGS:
  enabled                  - Enable/disable migration system
  mode                     - Manual, semi_auto, or auto

SOURCE DATACENTER:
  name                     - Source datacenter identifier
  endpoint                 - HTTP endpoint URL
  timeout_sec              - Request timeout in seconds

TARGET DATACENTER:
  name                     - Target datacenter identifier
  endpoint                 - HTTP endpoint URL
  timeout_sec              - Request timeout in seconds

MIGRATION SETTINGS:
  batch_size               - Documents per batch (default: 1000)
  workers                  - Parallel workers (default: 4)
  compression              - Compression algorithm: snappy, lz4, zstd
  resume_enabled           - Enable resume from checkpoint
  checkpoint_interval_sec  - Checkpoint save interval
  max_retries              - Maximum retry attempts
  retry_backoff_sec        - Backoff between retries
  collections              - Specific collections to migrate (empty = all)
  exclude_collections      - Collections to exclude

DELTA SYNC SETTINGS:
  enabled                  - Enable delta synchronization
  interval_sec             - Sync interval in seconds
  max_lag_sec              - Maximum acceptable lag

VALIDATION SETTINGS:
  enabled                  - Enable data validation
  sample_percent           - Percentage of documents to validate
  max_errors               - Maximum errors before stopping

EXAMPLE CONFIGURATION:
  [migration]
    enabled = true
    mode = "semi_auto"
    
    [migration.settings]
      batch_size = 2000
      workers = 8
      compression = "zstd"
      resume_enabled = true
      checkpoint_interval_sec = 60
`
    utils.Println(configInfo)
}
