## futriix — lightweight distributed in-memory DBMS in Go without locks, with Lua plugin support, optimized for Linux/Illumos/OpenIndiana/Solaris systems

<a id="readme-top"></a>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <img src="Logo.png" height="100" alt="Logo.png">
  <h3><b>Project Brief Documentation</b></h3>
</div>

1. [About the Project](#about-the-project)
2. [License](#license)
3. [Glossary](#glossary)
4. [Why Illumos OpenIndiana?](#why-illumos-openindiana)
5. [Architectural Notes and Security Proposals](#architectural-notes-and-security-proposals)
6. [Algorithms and Data Structures](#algorithms-and-data-structures)
7. [System Requirements](#system-requirements)
8. [Configuration File](#configuration-file)
9. [Quick Start](#quick-start)
10. [Logging](#logging)
11. [Testing](#testing)
12. [CRUD Operations](#crud-operations)
13. [Indexes](#indexes)
14. [Transactions](#transactions)
15. [Clustering and Sharding](#clustering-and-sharding)
16. [Backpressure](#backpressure)
17. [Geo-Distributed Migration](#geo-distributed-migration)
18. [Constraints](#constraints)
19. [Import-Export](#import-export)
20. [HTTP API](#http-api)
21. [Access Control](#access-control)
22. [Lua Plugins](#lua-plugins)
23. [Triggers](#triggers)
24. [Data Compression](#data-compression)
25. [Graphical User Interface](#graphical-user-interface)
26. [FAQ](#faq)
27. [Roadmap](#roadmap)
28. [Contacts](#contacts)

## About the Project

> [!CAUTION]
> **ALPHA VERSION**
> The project is stable enough in test scenarios, but it is categorically not recommended for production use until version 3.0 is released. We are open to suggestions and very grateful for feedback!

**futriix** is a lightweight distributed NoSQL DBMS written in Go with a MongoDB-compatible interface. It operates in memory (in-memory), utilizes lock-free and wait-free non-blocking s, and relies on the Raft consensus algorithm, providing predictable performance under high loads.

This is an **HTAP system**: it combines OLTP (with ACID transactions) and OLAP (via indexes, triggers, Lua plugins, and analytical functions) to process and analyze data almost in real time.

Data management is fundamentally based on **timestamps**: they provide record versioning, correct MVCC behavior, and event ordering in a distributed environment. This allows for precise change tracking, guarantees data consistency between nodes, and correctly resolves conflicts during parallel updates.

### Key mechanisms for reliability and scalability:
* **WAL** — fault tolerance
* **MVCC** — transaction isolation
* **Horizontal scaling**

Features a **WUI interface** for administration. The project is developed within the OpenIndiana ecosystem, distributed under the CDDL license, and is compatible with OS choices based on Illumos/Solaris (OpenIndiana Hipster, Oracle Solaris) as well as popular Linux distributions (Debian, Ubuntu, Fedora).

> [!IMPORTANT]
> **Use Cases Where futriix is the Right Choice:**
> 1. **Rapid Prototyping and R&D**: When you need to quickly test a hypothesis or build a working prototype. Forget about locks and rigid schemas — just insert documents and change structures whenever you want without extra setups.
> 2. **Embedded Storage for Desktop Applications**: When developing a computer application. Get a ready-to-use distributed storage running directly inside your program instead of an external server — easily embedded and works out of the box.
> 3. **Metrics Processing and Storage**: When you need to collect, save, and quickly process high-velocity streams of metrics.
> 4. **Edge Scenarios as Part of a Distributed System**: When autonomous, fault-tolerant storage is required on the digital periphery (edge). Futriix allows deploying a lightweight node anywhere, including a Closed Software Environment (CSE/ЗПС) — an isolated perimeter where security and controlled execution are critical. The node operates without a constant connection to the core, synchronizes asynchronously, and automatically integrates into any shared distributed network. Compatibility with MongoDB simplifies data unification between the core and the edge.

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## License

The project is distributed under the **CDDL 1.0 license**. Details can be found in the LICENSE files. This license allows copying, modification, distribution, incorporation into other projects, obtaining patent rights, and distributing binaries along with access to their source code. It prohibits adding new restrictions, hiding changes, removing original notices, violating CDDL 1.0 terms upon redistribution, or incorrect linking with other licenses.

All additional software (including the project compilation script `build.sh`) is provided "as is", without warranties or liabilities from the developers. The developers bear no responsibility for direct or indirect damage caused by using the open-source code of Futriix/futriix or technical solutions utilizing this code.

> [!IMPORTANT]
> **IMPORTANT!!!**
> The attached `NOTICE` file lists the third-party dependencies used in the project, their authors, and the licenses under which they are distributed.

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Glossary

* **Database (DB)**: A structured, organized repository of data that enables convenient collection, storage, management, and extraction of information.
* **Database Management System (DBMS)**: Software that enables the creation, management, and interaction with databases.
* **Multi-model DBMS**: A DBMS that combines support for multiple data models (relational, document, graph, key-value, etc.) within a single integrated core.
* **In-Memory (Resident) DBMS**: A DBMS that runs continuously in random access memory (RAM).
* **Eventual Consistency**: A consistency model in distributed systems where, after new updates stop, all data replicas eventually converge to the identical state.

### Key features of Eventual Consistency applied to DBMS architecture:
1. **Allows temporal discrepancies**: In the interval between a write operation and replication completion, different nodes may serve different versions of the same data. This is normal for the model and built into the design.
2. **Asynchronous replication**: Updates spread across nodes without waiting for confirmation from all participants — this increases availability and lowers latency but introduces a window of inconsistency.
3. **Convergence guarantee**: If no new changes occur, the system is guaranteed to reach a consistent state on all replicas. The exact moment this happens is not fixed beforehand.
4. **Dependence on conflict resolution mechanisms**: Since concurrent changes can occur on different nodes, correct eventual consistency relies on conflict resolution strategies (e.g., via timestamp, vector clocks, last-write-wins incorporating metadata, etc.).

* **Slice**: In Futriix DBMS terms, it has two meanings:
  1. A synonym for "database".
  2. A term originating from MongoDB terminology, namely: A logically and physically isolated fragment of a document collection obtained through horizontal partitioning (sharding) and placed on a specific cluster node to scale performance and data volume.
* **Collection**: An analog to a table.
* **Field**: A document attribute.
* **Tuple**: An embedded document within the Futriix DBMS.
* **Instance**: A running copy of the database.
* **Timestamp**: An automatically captured time value of object creation, modification, or deletion in the DBMS, represented in Unix milliseconds format (number of milliseconds since January 1, 1970) with the capability of human-readable display as `YYYY-MM-DD HH:MM:SS.mmm`. Essential for understanding transaction sequences during failures, identifying system behavior anomalies, archiving old records, tracking unauthorized changes, auto-logging out of sessions with limited lifespans, and activity statistics per period.
* **Node (synonyms: host, node, shard)**: A separate server (physical or virtual) that is part of a cluster or distributed system and performs a part of the overall workload.
* **Cluster**: A group of computers connected by high-speed communication channels to solve complex computational tasks, representing a group of servers acting as a single system from the user's perspective.
* **Replica Set**: A group of DBMS servers combined into a fault-tolerant configuration where one node acts as primary (accepting write operations) and one or more others act as secondary (synchronizing their data with the primary and serving reads), with automatic re-election of the primary node in case of failure.
* **OLTP (Online Transactional Processing)**: A real-time transaction processing technology. Its primary task is ensuring fast and reliable execution of operations occurring every second in a business. They provide rapid execution of insert, update, and delete operations while maintaining transaction integrity and reliability.
* **OLAP (Online Analytical Processing)**: A technology that works with historical arrays of information, extracting patterns and analyzing large volumes of data; supports multi-dimensional queries and complex analytical operations. This technology is optimized for executing complex queries and providing summary info for management decisions.
* **HTAP (Hybrid Transactional and Analytical Processing)**: A technology that efficiently combines operational and analytical requests, i.e., OLTP and OLAP classes.
* **WUI (Web User Interface)**: A futriix project term denoting a web-based interface (interface running in a web browser).
* **Workflow**: A principle of business process organization where repeating tasks are represented as a sequence of standard steps.
* **Lock-free algorithms**: Algorithms guaranteeing that at least one execution thread makes progress (completes an operation) in a finite number of steps, even if other threads are delayed or suspended. Individual threads may experience delays, but the system as a whole continues progressing.
* **Wait-free algorithms**: Algorithms guaranteeing that every thread completes its operation within a bounded (finite) number of steps, regardless of the state or behavior of remaining threads. This ensures an absolute absence of delays and starvation for any concurrent participant.
* **CSE (Closed Software Environment / ЗПС)**: A local network of an enterprise or organization, usually without internet access, acting as a "filter" containing a whitelist of software allowed to run on the computer, while any software not listed will be prohibited from executing.
* The `#` prompt symbol in the documentation marks commands executed with superuser (root) privileges.
* The `$` prompt symbol in the documentation marks commands executed with regular user privileges.

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Why Illumos OpenIndiana?

While `futriix` maintains cross-platform compatibility and fully supports Linux, **Illumos (OpenIndiana)** is chosen as the primary target platform. For an in-memory NoSQL DBMS operating within Closed Software Environments (CSE / ЗПС), general-purpose operating systems like Linux often introduce resource jitter and architectural overhead. Illumos provides a mission-critical foundation built on strict determinism and textbook system design.

Key architectural reasons for prioritizing the Illumos kernel include:

* **Predictable Kernel Determinism & Real-Time Scheduling:** Unlike the Linux Completely Fair Scheduler (CFS/EEVDF), which balances throughput and interactivity for general workloads, the Illumos dispatcher guarantees rigid, deterministic CPU resource allocation. This eliminates latency spikes (jitter) which are fatal for wait-free/lock-free in-memory transaction processing.
* **Scalable Event Ports (`port_create`):** For asynchronous, high-concurrency I/O, `futriix` leverages Illumos Event Ports rather than Linux `epoll`. Architectural advantages of Event Ports include native mitigation of the "thundering herd" problem at the kernel level and superior cache-locality when driving thread pools.
* **Native Fault Management Architecture (FMA):** Operating an in-memory database poses severe risks of hardware-induced data corruption. Illumos FMA continuously monitors hardware telemetry; if a memory module starts degrading, FMA isolates bad pages on the fly *before* a kernel panic occurs, ensuring continuous DBMS availability.
* **Flawless WAL Integrity via Native ZFS:** The segmented Write-Ahead Log (WAL) requires absolute storage reliability. In Illumos, ZFS is native and deeply integrated into the OS virtual memory layer. It validates data blocks using cryptographic checksums and self-heals silent data corruption, providing zero-overhead durability guarantees.
* **Rigid OS-Level Isolation (Zones):** Solaris-heritage Zones are baked into the kernel core, rather than being bolted-on via namespaces and cgroups like Linux containers. This provides pure, mathematically sound logical and physical isolation for distributed nodes running on shared hardware.
* **CDDL Licensing for Closed Environments:** The Common Development and Distribution License (CDDL) allows the modification and hardening of the core OS to meet specific domestic security standards without triggering strict GPL copyleft requirements. This simplifies building trusted, certified hardware-software complexes.

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

## Architectural Notes and Security Proposals

### Architectural Notes
Futriix was originally developed as a monolith whose internal core (including the framework for WUI implementation) carried the code name "Futriis". With the transition to the final name "Futriix", external interfaces were renamed, but internal variables retained the original name to minimize changes. Also, the framework for the WUI implementation kept its former name "Futriis" and was integrated into the database core. Consider `futriis` an internal alias for `futriix`, and remember that currently: *"Futriis is the historical name of the Futriix core"*.

### Web Interface Authentication
**General Provisions**  
The DBMS web interface implements basic authentication based on a "user identifier" and "password" pair using the SHA-256 encryption algorithm. Since Futriix is operated predominantly in a Closed Software Environment (CSE) where organizational and network measures eliminate traffic interception and credential brute-forcing, the implemented authentication mechanism is sufficient within the accepted threat model.

* **Password Requirements**: Minimum length of 4 characters.
* **Connection Procedure**:
  1. The CSE Administrator opens the local IP address of the DBMS (port XXXX) via a browser located within the same network segment.
  2. The system prompts for a login and password.
  3. Since the environment is closed, physical or logical access to the workstation is implied to be restricted to authorized personnel only.

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Algorithms and Data Structures

This section lists the primary algorithms and data structures utilized in the futriix DBMS.

### Data Structures

| Structure | Component | Description |
| :--- | :--- | :--- |
| **Storage** | `sync.Map` | Concurrent hash map for database storage, provides wait-free reads and writes. |
| | Atomic counters (`atomic.Int64`) | For tracking the total number of documents without locking. |
| **Database** | `sync.Map` of collections | A synonym for "database" in RDBMS. |
| | Mutexes (`sync.RWMutex`) | For structural modification operations (creating/deleting collections). |
| **Collection** | `sync.Map` of documents | Key: document ID, value: pointer to `Document`. |
| | `sync.Map` of indexes | Separate storage for inverted indexes. |
| | Atomic counters | Document count (`docCount`) and size in bytes (`sizeBytes`). |
| **Index** | `sync.Map` (value &rarr; ID) | Inverted index implementation. |
| | Unique index | Direct lookup operation $O(1)$. |
| | Non-unique index | Range-scanning with optimized comparison (`compareValues`). |
| | Composite index | Concatenation of field values with a "`" delimiter. |
| **Document** | `map[string]interface{}` | Dynamic field schema. |
| | Version (`uint64`) | For optimistic locking. |
| | Timestamps | Creation, update, deletion (Unix milliseconds). |
| **Transaction** | WAL (`Write-Ahead Log`) | Write-ahead logging for durability. |
| | Operations cache | Local storage of changes prior to commit. |
| | Lock list | Tracking of affected documents. |
| **Trigger** | `sync.Map` with a composite key | `collection` + event string. |
| | Conditions (`TriggerCondition`) | Stored as a structure with field/operator/value fields. |
| | Execution queue | Buffered channel for asynchronous processing. |
| **ACL (Access Control List)** | `map[string]bool` | Defined for each operation: `read`, `write`, `delete`, `admin`. |
| | Change history | Array with timestamps for auditing. |

### Complexity Characteristics of Algorithms

1. **Wait-free access operations**
   * *Operations*: 
     * Read: `sync.Map.Load()` — atomic lock-free operation
     * Write: `sync.Map.Store()` — atomic value replacement
     * Delete: `sync.Map.Delete()` — atomic deletion
   * *Complexity*: $O(1)$ amortized
   * *Parameters*: Amortized complexity represents the average cost of an operation over a series of operations, even if individual ones might be expensive.

2. **Index Lookup**
   * *Operations*:
     * Unique index: 
       1. `index.data.Load(value)` &rarr; retrieves document ID
       2. `docs.Load(ID)` &rarr; returns document
     * Non-unique index:
       1. `index.data.Range()` &rarr; scans all index records
       2. Value comparison accounting for types (`compareValues`)
       3. Accumulating all matching IDs
   * *Complexity*: Unique: $O(1)$; Non-unique: $O(n)$
   * *Parameters*: $n$ — size of the index

3. **Optimistic Transaction Locking**
   * *Operations*:
     1. `BeginTransaction()` &rarr; captures current document versions
     2. Executes operations in local cache
     3. `Prepare()` &rarr; verifies versions of all affected documents
     4. `Commit()` &rarr; applies changes, increments versions
     5. On conflict &rarr; `Abort()` and retry
   * *Complexity*: $E[T] = rac{1}{1 - P_{conflict}} 	imes [O(M) + O(M 	imes (V + K)) + O(WAL)]$
   * *Parameters*: $\mathbb{E}[T]$ — expected execution time; $1 - P_{conflict}$ — probability of a successful commit; $M$ — number of documents in the transaction; $V$ — number of fields in the document; $K$ — number of indexes; $WAL$ — log log size.

4. **Constraint Validation**
   * *Operations*: Sequential check of a document:
     1. Required fields &rarr; presence verification
     2. Minimum values (`MinValues`) &rarr; numeric comparison
     3. Maximum values (`MaxValues`) &rarr; numeric comparison
     4. Regex patterns &rarr; string matching
     5. Enum values &rarr; search within an allowed list
   * *Complexity*: $O(k + m)$
   * *Parameters*: $k$ — number of fields in the document; $m$ — number of constraints

5. **Trigger Execution**
   * *Operations*: For each event trigger:
     1. Check `Enabled`
     2. Condition evaluation (`TriggerCondition`)
     3. Mapping operator to document field
     4. Action execution (`abort` / `skip` / `modify` / `log`)
     5. Logging with timestamp and duration
   * *Complexity*: $O(T 	imes (C + O))$
   * *Parameters*: $T$ — number of triggers per event; $C$ — number of conditions in the trigger; $O$ — number of operations in the trigger

6. **Soft Deletion**
   * *Operations*:
     * SoftDelete: 
       1. `doc.DeletedAt = now`
       2. `doc.Version++`
       3. `docs.Store(id, doc)` — preservation, not deletion
       4. `removeFromIndexes(doc)` — exclusion from search lookup
     * Restore:
       1. `doc.DeletedAt = 0`
       2. `doc.Version++`
       3. `docs.Store(id, doc)`
       4. `addToIndexes(doc)` — return to indexes
   * *Complexity*: $O(1) + O(K)$
   * *Parameters*: $K$ — number of indexes in the collection

7. **TTL Cleanup**
   * *Operations*: Background loop with an interval of $TTL/2$ seconds:
     1. `Scan` of all documents in a collection
     2. Check: `now - doc.CreatedAt > TTL * 1000`
     3. Adding expired IDs to a buffer
     4. Batch deletion (accounting for SoftDelete)
   * *Complexity*: $O(N 	imes (1 + K)) + O(D 	imes (1 + K))$
   * *Parameters*: $N$ — total number of documents; $K$ — number of indexes; $D$ — number of expired documents ($D \leq N$)

8. **MessagePack Serialization**
   * *Operations*:
     * Export:
       1. Traverse all DB collections
       2. Collect documents, metadata, indexes, constraints
       3. Add export metadata (time, version)
       4. Marshal into binary format
     * Import:
       1. Unmarshal from binary format
       2. Reconstruct DB and collection structure
       3. Retain original timestamps
       4. Log import metadata
   * *Complexity*: $O(N 	imes F) + O(C 	imes I)$
   * *Parameters*: $N$ — total number of documents; $F$ — average number of fields per document; $C$ — number of collections; $I$ — average number of indexes per collection

9. **Distributed Consensus (Raft)**
   * *Operations*:
     * Cluster operations:
       1. Leader accepts write request
       2. Replicates log to followers
       3. Commitment upon confirmation from majority (quorum)
       4. Commit and apply to state machine
       5. Response to client
     * Leader election:
       1. Heartbeat timeout
       2. Transition to candidate state
       3. Request votes (`RequestVote`)
       4. Acquire majority votes
       5. Become leader
   * *Complexity*: Write operation: $O(L) + O(R) + O(Commit)$; Leader Election: Average: $O(R 	imes \log N)$, Worst: $O(R 	imes N)$
   * *Parameters*: $L$ — size of the operation log; $R$ — RTT to followers (network latency); $Commit$ — time to apply to state machine; $N$ — number of nodes in the cluster

10. **Lua Plugin Operations**
    * *Operations*:
      * Loading:
        1. Read `.lua` file
        2. Create isolated Lua state
        3. Register database access functions
        4. Execute script in protected mode
        5. Retain pointer to the state
      * Execution:
        1. Find function in global Lua scope
        2. Convert Go &rarr; Lua values
        3. Protected call against panic
        4. Measure execution duration
        5. Convert result Lua &rarr; Go
        6. Log event
    * *Complexity*: Loading: $O(P + C)$; Function execution: $O(convert\_args + execution + convert\_result)$
    * *Parameters*: $P$ — plugin file size (bytes); $C$ — Lua compilation complexity (usually $O(P)$); $convert\_args = O(N\_args 	imes V)$; $execution = O(L)$ — script runtime; $convert\_result = O(V)$; $V$ — value conversion complexity (depends on nested depth)

### Key Implementation Features

| Feature | Implementation | Advantage |
| :--- | :--- | :--- |
| **Wait-free reading** | `sync.Map` + atomic operations | No locks during read operations |
| **Separate indexes** | Indexes in `sync.Map`, detached from documents | Parallel access to data and indexes |
| **Versioning** | `uint64` inside each document | Optimistic locking |
| **Flexible schema** | `map[string]interface{}` | Dynamic structure modification |
| **Soft deletion** | `deleted_at` flag + index exclusion | Restoration capability |
| **Asynchronous triggers**| Buffered channel | Do not block core execution flows |
| **Sandboxed plugins** | Isolated Lua states | Secure feature extensibility |

### Complexity Summary Reference

* **Document Insertion**: $O(1) + O(k)$ where $k$ is the number of indexes
* **Lookup by ID**: $O(1)$ via direct lookup in `sync.Map`
* **Lookup by Unique Index**: $O(1)$ via two lookup operations
* **Lookup by Non-Unique Index**: $O(n)$ where $n$ is index size
* **Document Update**: $O(1) + O(k)$ + version verification
* **Document Deletion**: $O(1) + O(k)$ + potential index cleanup
* **Transaction (Commit)**: $O(m) + 	ext{Raft}$ where $m$ is operation count
* **Constraint Validation**: $O(f + c)$ where $f$ represents fields and $c$ constraints
* **Database Export**: $O(N)$ where $N$ is total documents

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## System Requirements

> [!CAUTION]
> **The DBMS works only on Unix-like operating systems. Windows and macOS support is not provided!**  
> Due to architectural specifics (low-level system calls native to POSIX-compliant kernels), running on Windows (including WSL) and macOS is not supported!

### Supported Software Platforms
* **Linux (kernel 5.4+)**: Debian, Ubuntu, Linux Mint, Fedora, and similar variants.
* **Illumos-based**: OmniOS, OpenIndiana, and similar installations.

### Hardware Requirements
* **Processor**: 64-bit Intel or AMD (x86-64)
* **RAM**:
  * Linux: Minimum 6 GB
  * Illumos: Minimum 8 GB (due to specific memory management and default ZFS footprint)

> [!IMPORTANT]
> For load testing, sharding, and parallel operation of multiple nodes (5 and above), a minimum of **16 GB of RAM per node** is highly recommended.

### Software Dependencies and Utilities
The target OS must have the following utilities available:
* `curl` — for HTTP communication and inter-node routing queries.
* `zip` / `unzip` — for processing compressed formats during backups and migrations.

Verification commands:
```sh
curl --version
zip -v
unzip -v
```

### Requirements for Developers
> [!IMPORTANT]
> To compile and develop the source code:
> * **Go**: Version 1.25 or higher (`go version` to check)
> * It is recommended to use `go mod` and verify that `$GOPATH` and `$GOROOT` are correctly exported.

### Production / High-Load Additional Guidelines
* **Disk Subsystem**: SSD/NVMe drives; HDD is acceptable for local testing environments only.
* **File System**: Preferably ZFS (especially on Illumos) or XFS/ext4 on Linux servers.
* **Network**: Stable connection between nodes, preferably 1 Gbps+; low latency is critical for Raft clusters.
* **Time**: Time synchronization between nodes (NTP/Chrony) is mandatory for correct MVCC, WAL, and transaction integrity.

### Pre-installation Verification Commands
```sh
# OS and Kernel
uname -s -r
# Memory (free and total)
free -h
# Utilities existence
which curl zip unzip
# Go environment
go version
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Configuration File

The primary configuration file for futriix DBMS is `config.toml`, which is used **exclusively during the very first cluster execution** to initialize base properties. It outlines all DBMS sub-parameters:
* `[cluster]` — clustering specs (IPs, ports, Raft thresholds, timeouts)
* `[storage]` — engine values (page dimensions, limit boundaries)
* `[wal]` — write-ahead logging configs (segments, sync intervals)
* `[mvcc]` — multi-version concurrency control (version counts, retention TTL)
* `[saga]` — distributed transactions orchestrated parameters
* `[replication]` — data replication characteristics
* `[backpressure]` — overload safety boundaries

> [!IMPORTANT]
> **Warning**: After the initial deployment run, the `config.toml` file is no longer checked or parsed for configuration adjustments — **all settings management is controlled exclusively through Raft!**

### Configuration in a Raft Cluster
Following initialization, structural configuration adjustments are preserved in the Raft log rather than a local file:
```
Raft log (raft_data/)
├── configuration adjustment commands
├── state snapshots
└── history of all changes tracked with version marks
```

### Advantages of This Approach:
* Uniform configuration across all active cluster nodes.
* Adjustments apply synchronously through Raft consensus.
* Rollback capability to a previous version configuration state.
* Auditable lineage mapping change origins alongside authors.

### Interaction Vectors:
* **REPL**: `config set cluster.heartbeat_timeout_ms 2000`
* **HTTP API**: `POST /api/v1/config`
* **CLI**: `futriix config set --key=... --value=...`
* **Subscription**: Internal processes monitor configuration changes in real time.

### Commands Overview

| Command | Description |
| :--- | :--- |
| `config show` | Display current configurations |
| `config show --section=cluster` | Target a specific block display |
| `config set key=value` | Define a specific parameter |
| `config set --dry-run key=value` | Verify syntax adjustments without applying |
| `config history` | Review configuration revision updates |
| `config history --limit=10` | Filter to the last 10 historical entries |
| `config rollback` | Roll back to the immediate preceding version |
| `config rollback --version=5` | Target roll back to specific version 5 |
| `config diff --version=3` | Evaluate variance against version 3 |
| `config export --file=cfg.json` | Export settings to JSON |
| `config import --file=cfg.json` | Load settings from JSON |
| `config watch` | Listen to settings update streams |
| `config validate` | Run syntax validation tests |

### Example Changes via REPL
```sh
# Initialize REPL interface
./futriix repl

# Check properties
> config show

# Apply single specification alteration
> config set cluster.heartbeat_timeout_ms 2000

# Alter multiple specifications concurrently
> config set storage.page_size_mb 128 replication.enabled true

# Display lineage
> config history

# Execute rollback
> config rollback
```

### JSON Structure Properties
* `changes`: Map containing target key-value parameters. Keys use dot notation paths (e.g., `cluster.heartbeat_timeout_ms`).
* `description`: Audit explanation text outlining the reason for modification.
* `changed_by`: Author identifier executing the command (for tracking and logging).

### Example Changes via HTTP API
```sh
# Fetch properties
curl -X GET http://localhost:8080/api/v1/config

# Alter parameter
curl -X POST http://localhost:8080/api/v1/config -H "Content-Type: application/json" -d '{
"changes": {
"cluster.heartbeat_timeout_ms": 2000,
"storage.page_size_mb": 128
},
"description": "Optimizing for high-load requirements",
"changed_by": "admin@futriix"
}'

# Check log list history
curl -X GET http://localhost:8080/api/v1/config/history

# Apply targeted roll back
curl -X POST http://localhost:8080/api/v1/config/rollback -H "Content-Type: application/json" -d '{"version": 5}'

# Complex multi-parameter environment update example
curl -X POST http://localhost:8080/api/v1/config -H "Content-Type: application/json" -d '{
"changes": {
"cluster.name": "production_cluster",
"cluster.heartbeat_timeout_ms": 2000,
"cluster.election_timeout_ms": 1500,
"storage.page_size_mb": 128,
"storage.max_collections": 500,
"replication.enabled": true,
"replication.sync_replication": true,
"wal.segment_size_mb": 128,
"wal.sync_interval_sec": 3,
"mvcc.max_versions_per_doc": 20,
"mvcc.retention_days": 14,
"saga.enabled": true,
"saga.coordinator_count": 5,
"backpressure.enabled": true,
"backpressure.cpu_threshold": 0.85
},
"description": "Production configurations initialization update",
"changed_by": "admin@futriix.com"
}'
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Quick Start

The quick start guide provides minimal step routines to clone, compile, and trigger the environment.

1. Clone repository destination:
```sh
$ git clone https://github.com/futriix/futriix
$ cd futriix
```

> [!IMPORTANT]
> **Isolated Environments Note**: Steps 1.1 to 1.3 are necessary **only** if you are operating inside a Closed Software Environment (CSE/ЗПС). This permits acquiring prerequisites in an archived `vendor.zip` package offline.

1.1 Fetch dependencies package archive:
```sh
$ curl -L -o vendor.zip  -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"  -H "Referer: https://futriix.ru:8083/"  "https://futriix.ru:8083/fm/?r=/download&path=L3dlYi9mdXRyaWl4LnJ1L3B1YmxpY19odG1s"
```

1.2 Unpack archive contents into the project directory root:
```sh
$ unzip vendor.zip
```

1.3 Compile the source code using the dedicated vendor compilation script for CSE:
```sh
$ ./build_vendor.sh
```

2. Compile and execute routines on standard environments:
```sh
# Standard build route on Linux architectures
$ ./build.sh

# Build route optimized for Illumos operating structures
$ cd scripts/
$ ./build_illumos.sh

# Access helper manual reference text flags
$ ./build.sh --help

# Execute the application binary
$ ./futriix
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Logging

Futriix processes log routing into two isolated files depending on the source vector:
* `futriix.log`: System core journal capturing background engine lifecycle tasks handled via terminal interfaces (server runs, Raft coordination tasks, transaction lifecycles, ACL parsing, and critical core errors).
* `webui.log`: Dedicated administrative interface logging file (login evaluation results, profile image updates, trigger definitions, index generation parameters, and storage migration metrics).

Both environments process files in structured lines using text strings with timestamp patterns for `futriix.log` and JSON layouts for `webui.log`. Rotation mechanisms prevent unbounded storage growth (defaults limit `webui.log` to a maximum ceiling of 10,000 index records).

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Testing

Verification validation uses five core functional test categories developed in Lua: regression testing, smoke test routines, explicit functional evaluations, internal component integration reviews, and system stress load tests. The codebase targets CRUD correctness, verification of access rules, transaction isolations, and Raft consensus behaviors.

The corresponding testing scripts are stored in the `/futriix/tests/` target path folder.

> [!IMPORTANT]
> 1. Confirm that the core storage is running and listening properly on HTTP API port 8080 before triggering tests.
> 2. Performance testing routines can span multiple minutes depending on evaluation data volumes.

Execution command sequences:
```sh
# Establish required environment prerequisites dependencies
sudo apt install lua5.3 lua-socket

# Execute distinct scripts selectively
lua test_regression.lua
lua test_smoke.lua
lua test_functional.lua
lua test_integration.lua
lua test_performance.lua

# Alternative command sequence running all validation suites sequentially
for test in test_regression.lua test_smoke.lua test_functional.lua test_integration.lua test_performance.lua; do
  echo "=== Running $test ==="
  lua "$test"
  echo ""
done
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## CRUD Operations

> [!TIP]
> **MongoDB Compatibility**: Futriix syntax commands align with MongoDB specifications. You can substitute NoSQL drivers and schemas to migrate your workloads directly.

Operations like `insert()`, `find()`, `update()`, and `delete()` use wait-free pipelines ensuring high performance during parallel updates without causing locking deadlocks. The storage engine automatically maps unique `_id` hashes to incoming documents, checks schema compliance rules, and generates Unix millisecond metadata flags. Distributed actions are isolated via MVCC, Raft logs, and a SAGA orchestration structure.

### Command Console Interaction Patterns:
```sh
# Generate separate instances (Slices)
futriix:~> create slice company
✓ Database 'company' created at 2026-01-15 10:30:45.123

futriix:~> create slice shop
✓ Database 'shop' created at 2026-01-15 10:30:46.456

# Shift target focus environment
futriix:~> use company
✓ Switched to database 'company'

# View active database definitions
futriix:~> show databases
Databases:
 company (created: 2026-01-15 10:30:45.123)
 * shop (created: 2026-01-15 10:30:46.456)
 test (created: 2026-01-14 09:15:22.789)

# Erase slice entry parameters
futriix:~> drop database test
✓ Database 'test' dropped at 2026-01-15 10:31:12.789

# Initialize target collections within active environment
futriix:~> use company
futriix:~> create collection employees
✓ Collection 'employees' created in database 'company' at 2026-01-15 10:32:15.234

futriix:~> create collection departments
✓ Collection 'departments' created in database 'company' at 2026-01-15 10:32:18.567

# Audit active structures
futriix:~> show collections
Collections in database 'company':
- employees (created: 2026-01-15 10:32:15.234)
- departments (created: 2026-01-15 10:32:18.567)

# Append documentation entries (Key-Value formatting parameters)
futriix:~> insert employees name=John Doe,position=Developer,age=30,department=IT
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440000 (created at: 2026-01-15 10:34:22.345)

# Target item lookup utilizing primary hash string tracking values
futriix:~> find employees 550e8400-e29b-41d4-a716-446655440000
Document found:
{
"name": "John Doe",
"position": "Developer",
"age": 30,
"department": "IT"
}
created_at: 2026-01-15 10:34:22.345
updated_at: 2026-01-15 10:34:22.345

# Lookup using secondary key indexes paths definitions
futriix:~> findbyindex employees name_idx "John Doe"
Found 1 document(s):
[1] ID: 550e8400-e29b-41d4-a716-446655440000 (updated: 2026-01-15 10:34:22.345)

# Filter criteria mapping timeline windows properties values
futriix:~> findbytime users 2026-01-15 2026-01-16

# Check timeline state tracking logs properties
futriix:~> show timestamps users user123
=== Timestamps for document: user123 ===
Created: 2026-01-15 10:30:45.123
Updated: 2026-01-15 15:22:18.456
Deleted: 2026-01-16 09:15:30.789

# Recover item metrics values after soft deletion execution processes
futriix:~> restore users user123
✓ Document 'user123' restored at 2026-01-16 10:00:00.000

# Execute parameters changes (Triggers timeline tracking field automation metrics)
futriix:~> update employees 550e8400-e29b-41d4-a716-446655440000 age=31,position=Senior Developer
✓ Document '550e8400-e29b-41d4-a716-446655440000' updated at 2026-01-15 10:35:45.234

# Aggregate counter inspection commands tracking deleted files properties
futriix:~> count employees
=== Collection 'employees' statistics ===
Active documents: 2
Deleted documents: 1
Total documents: 3

# Perform soft deletion metrics commands options
futriix:~> delete employees 550e8400-e29b-41d4-a716-446655440002
✓ Document '550e8400-e29b-41d4-a716-446655440002' deleted at 2026-01-15 10:36:15.678

# Review deleted files structures lists
futriix:~> show deleted employees
=== Deleted documents in collection 'employees' ===
[1] ID: 550e8400-e29b-41d4-a716-446655440002 (deleted: 2026-01-15 10:36:15.678)

# Check aggregate cluster audit trail configurations updates logs streams values
futriix:~> audit log
=== Audit Log (last 50 entries) ===
[2026-01-15 10:38:15.456] CREATE_INDEX - COLLECTION: company.employees
[2026-01-15 10:37:05.123] RESTORE - DOCUMENT: company.employees.550e8400-e29b-41d4-a716-446
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Indexes

Futriix uses primary indexes (linked to `_id`) and secondary structures detached from standard record bodies. It supports unique properties constraints, composite multi-key tracking patterns, exact matches, and prefix parameters processing fields lookups.

```sh
# Generate simple key lookup indices configurations
futriix:~> create index employees name_idx name
✓ Index 'name_idx' created on collection 'employees' at 2026-01-15 10:50:15.123

# Enforce unique field value parameters definitions
futriix:~> create index employees email_idx email unique
✓ Index 'email_idx' created on collection 'employees' at 2026-01-15 10:50:18.456

# Establish multi-field composite indexing models structures rules
futriix:~> create index employees dept_age_idx department,age
✓ Index 'dept_age_idx' created on collection 'employees' at 2026-01-15 10:50:21.789

# Review generated indexing specifications matrices
futriix:~> show indexes employees
Indexes on collection 'employees':
- _id_ (created: 2026-01-15 10:32:15.234)
- name_idx (created: 2026-01-15 10:50:15.123)
- email_idx (unique) (created: 2026-01-15 10:50:18.456)

# Discard structural indexing pathways assets
futriix:~> drop index employees dept_age_idx
✓ Index 'dept_age_idx' dropped from collection 'employees' at 2026-01-15 10:51:05.234
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Transactions

Engine fault tolerance uses Write-Ahead Logging (`futriix.wal`). ACID local execution handles operations using Multi-Version Concurrency Control (MVCC) isolation, while cross-node distributed jobs use the SAGA pattern managed by a central supervisor orchestrator (10-second timeout safety step rules). Network synchronization states use eventual consistency mechanics.

### Consistency Guarantee Matrix Summary

| Core Action Vector | Isolation Model | standard operational flow | Network Partition Handling | Crash Restoration Mechanics |
| :--- | :--- | :--- | :--- | :--- |
| **Read Tasks** | Read Committed + MVCC | Serves committed snapshots. Uses Visibility Maps and `ReadTimestampCache` filters. | Retries automatically. Routes to healthy cluster nodes via Raft if target is isolated. | Resumes from tracking limits. Rebuilds snapshot parameters using `documentVersions` properties. |
| **Write Tasks** | Read Committed | Logs tasks sequentially into WAL before modifying in-memory structures. | Drops unconfirmed logs. Handles re-routing to newly elected cluster leaders using `fsync`. | Replays un-indexed actions from WAL logs. Re-aligns status tracks via Log Sequence Numbers (LSN). |
| **Transaction Blocks** | Read Committed / Repeatable Read | Atomic block updates. Supports Savepoint rules and deadlocks detection processing. | Aborts incomplete blocks. Uses SAGA routines to trigger compensation tasks automatically. | Performs asynchronous evaluations scanning WAL properties. Recovery threads execute steps. |
| **Indexing Jobs** | Read Committed | Lock-free updates run concurrently into `sync.Map` modules. | Re-calculates structures automatically from underlying files upon boot initialization. | Validates tracking status boundaries via LSN targets. Checks unique rules constraints values. |
| **Deletion Commands**| Read Committed | Executes soft removal marking `deleted_at` fields before index exclusion. | Blocks definitive disk removal tasks until cluster quorum confirms action. | Restores soft-marked fields values. Synchronizes collection numeric counters parameters. |
| **Replication tasks** | Linearizable Writes / Read Committed Reads | Channels write configurations directly into Raft logs streams. | Re-elects leaders automatically via Raft. Handles parallel lookups routing reads to healthy nodes. | Synchronizes log state tracks using standard Raft snapshot procedures. |

> [!TIP]
> **WAL Function Overview:**
> 1. Logs transaction mutations (`INSERT`, `UPDATE`, `DELETE`) prior to application.
> 2. Sets ordered indexing flags using log sequence numbers (LSN).
> 3. Restores engine state via internal `recoverFromWAL()` methods.
> 4. Structured binary layout containing CRC validation blocks.
> 5. Saved into a singular location path: `/futriix/futriix.wal`.

> [!TIP]
> **Checkpoint System Attributes:**
> * Saves active tracking data properties sequentially every 5 minutes (300 seconds).
> * Uses filename masking formatting rules: `futriis.wal.checkpoint.{timestamp}`.
> * Preserves compressed snapshots (retaining last 5 tracking files at `/futriix/wal.checkpoint.wal`).
> * Recovery maps the closest valid checkpoint before processing remaining updates from WAL records.

### Transaction Session Interactions:
```sh
# Establish session tracking environment parameters
futriix:~> db.startSession()
✓ Session started: session_12345 at 2026-01-15 10:55:15.123

# Open transactional scope bounds block attributes
futriix:~> session.startTransaction()
✓ Transaction started: TX_67890 at 2026-01-15 10:55:18.456

# Process actions inside secure pipeline constraints
futriix:~> insert employees name=New User,position=Trainee,age=22
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440005

futriix:~> update employees 550e8400-e29b-41d4-a716-446655440005 status=active
✓ Document '550e8400-e29b-41d4-a716-446655440005' updated at 2026-01-15 10:55:24.012

# Commit block mutations into global storage engine safely
futriix:~> session.commitTransaction()
✓ Transaction committed successfully at 2026-01-15 10:55:27.345

# Example handling abort rollbacks sequence loops patterns
futriix:~> session.startTransaction()
✓ Transaction started: TX_67891 at 2026-01-15 10:56:10.123

futriix:~> insert employees name=Test User,position=Test,age=25
✓ Document inserted with ID: 550e8400-e29b-41d4-a716-446655440006

# Discard uncommitted records data adjustments
futriix:~> session.abortTransaction()
✓ Transaction aborted, changes rolled back at 2026-01-15 10:56:16.789

# Investigate active operations blocks parameters list
futriix:~> show transactions
=== Active Transactions ===
ID: TX_67892, Status: active, Operations: 2, Started: 2026-01-15 10:57:00.123

# Fetch statistics
futriix:~> stats transactions
=== Transaction Statistics ===
Total transactions: 150
Committed: 145 (96.7%)
Aborted: 5 (3.3%)
Average duration: 234 ms
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Clustering and Sharding

Consensus relies on the Raft algorithm processing node status lookups, leader evaluations, and cluster monitoring tools automatically. Network protection and verification details are delegated to the underlying closed corporate boundary environment (CSE) configurations infrastructure. Network traffic encryption within Raft paths is omitted from application configurations layer frameworks since servers run inside physical isolated corporate layers. Secure proxy configurations (IPsec, WireGuard tunnels) can be set up by infrastructure managers if required.

Horizontal cluster auto-scaling loops use the **PHA (Predictive Horizontal Autoscaler)** logic model framework.

### PHA Evaluation Routine Workflow Flowchart Diagram
```
┌─────────────────────────────────────────────────────────────┐
│ Evaluation Loop Sequence Routine (Triggered every 30 sec)   │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 1. Collect nodes performance indices logs metrics            │
│    - Monitor CPU, Memory usage, QPS rate, Latency, Storage  │
└─────────────────────────────────────────────────────────────┐
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Compute compound node load indexing calculations value  │
│    node_load = Σ(metric_value / metric_threshold) * weight  │
└─────────────────────────────────────────────────────────────┐
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Evaluate cluster-wide average compound load values       │
│    avg_load = Σ(node_load) / total_nodes                    │
└─────────────────────────────────────────────────────────────┐
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Execute prediction tasks (Linear Regression windows)     │
│    predicted_load = avg_load + slope                        │
└─────────────────────────────────────────────────────────────┐
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Parse system cooldown safety intervals boundaries rules   │
│    if time_since_last_scale < cooldown → NoChange           │
└─────────────────────────────────────────────────────────────┐
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Process decision selection workflows operations paths    │
│    - predicted_load > scale_up_threshold   → ScaleUp        │
│    - predicted_load < scale_down_threshold → ScaleDown      │
│    - Else                                  → NoChange       │
└─────────────────────────────────────────────────────────────┘
            │                  │                  │
            ▼                  ▼                  ▼
      ┌──────────┐       ┌──────────┐       ┌──────────┐
      │ ScaleUp  │       │ScaleDown │       │ NoChange │
      └──────────┘       └──────────┘       └──────────┘
            │                  │
            ▼                  ▼
┌─────────────────┐  ┌─────────────────┐
│ 7a. Append nodes│  │ 7b. Drop nodes  │
│  nodes_to_add = │  │nodes_to_remove =│
│ ceil(ratio * N) │  │ ceil(ratio * N) │
└─────────────────┘  └─────────────────┘
```

### Node compound score calculations algorithm properties:
$$	ext{node\_load} = rac{\sum \left( rac{	ext{metric\_value}}{	ext{metric\_threshold}} 
ight) 	imes 	ext{metric\_weight}}{\sum 	ext{metric\_weights}}$$

* `metric_value`: Monitored tracking records parameters metrics data points (real-time usage statistics for CPU utilization %, Memory size footprint GB, raw RPS processing counts, disk throughput execution speeds parameters).
* `metric_threshold`: Standard normative reference limit boundaries (e.g., setting CPU limit reference thresholds line to 80%, RAM memory bounds marker to 90%).
* Ratio values exceeding 1.0 signal cluster resource overload states, prompting expansion logic tasks routines.

### Linear Regression Load Path Tracking Formulations:
$$	ext{slope} = rac{n \sum xy - \sum x \sum y}{n \sum x^2 - \left(\sum x
ight)^2}$$
$$	ext{predicted} = 	ext{avg}(y) + 	ext{slope}$$

* $x$: Sample sequencing intervals step indices records points ($0, 1, 2, \dots$).
* $y$: Compound historical utilization levels indicators data array.
* $n$: Sliding parsing sampling size scope limit bounds (defaults scale to 10 monitoring updates points).

### Capacity Sizing Mutation Equations Matrix:

#### Scale-Up Operations (Appending computing environments capacity handles):
$$	ext{excess} = 	ext{current\_load} - 	ext{scale\_up\_threshold}$$
$$	ext{ratio} = rac{	ext{excess}}{	ext{scale\_up\_threshold}}$$
$$	ext{nodes\_to\_add} = \lceil 	ext{ratio} 	imes 	ext{current\_nodes} 
ceil$$

#### Scale-Down Operations (Trimming computing environments cluster size models):
$$	ext{deficit} = 	ext{scale\_down\_threshold} - 	ext{current\_load}$$
$$	ext{ratio} = rac{	ext{deficit}}{	ext{scale\_down\_threshold}}$$
$$	ext{nodes\_to\_remove} = \lceil 	ext{ratio} 	imes (	ext{current\_nodes} - 1) 
ceil$$

* Boundary limits values configurations restrict cluster horizons within immutable parameters scales: minimum scale limits map to 1 node instance, while maximum size boundaries clip clusters to a total cap of 10 node modules. 
* Mutating steps restrict single operations size transformations steps rules (caps allow additions of up to 3 node items max per single scale step, while removal updates drop a max of 2 nodes per cycle).

### Cluster Status Diagnostics Tracking Commands:
```sh
# Investigate active operations role patterns on primary leader nodes
futriix:~> status
=== Cluster Status ===
✓ Role: LEADER
Cluster Name: production
Node: 192.168.1.100:8080
Health: healthy

# Investigate details from follower environments perspective
futriix:~> status
=== Cluster Status ===
⚠ Role: FOLLOWER
Cluster Name: production
Leader: 192.168.1.100:8080
Last heartbeat: 2026-01-15 15:29:58.456

# Enumerate aggregate connected environments elements list
futriix:~> nodes
=== Cluster Nodes ===
* 192.168.1.100:8080 (LEADER)
  Joined: 2026-01-15 10:30:45.123  Status: active  Uptime: 5h 0m 14s
  
192.168.1.101:8080 (FOLLOWER)
  Joined: 2026-01-15 10:31:12.789  Status: active  Uptime: 4h 59m 46s

# Run comprehensive system diagnostics checks routines
futriix:~> cluster health
=== Cluster Health ===
Overall score: 95.5
Recommendation: Cluster is healthy, all systems operational
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Backpressure

The backpressure engine manages network traffic routing when message processing pipelines experience localized processing queues overflows. If internal memory buffers hit critical saturation markings, traffic processing is automatically throttled or temporarily paused to prevent cascading resource starvation errors.

### Mathematical Rejection Probability Adaptive Algorithm Model Formulations:
$$P_{reject} = f(	ext{level}) 	imes g(	ext{load}) 	imes h(	ext{time})$$

* $P_{reject}$: Computed output probability determining request dropping paths values ($0.0 \le P_{reject} \le 1.0$).
* $f(	ext{level})$: Discontinuous scaling step metrics mapping qualitative overload categories boundaries markings values:
$$f(	ext{level}) = egin{cases} 
0.00 & 	ext{if level} = 	ext{None} \ 
0.00 & 	ext{if level} = 	ext{Low (introduces delay factors only)} \ 
0.30 & 	ext{if level} = 	ext{Medium} \ 
0.70 & 	ext{if level} = 	ext{High} \ 
0.90 & 	ext{if level} = 	ext{Critical} 
\end{cases}$$
* $g(	ext{load})$: Aggregated resource usage metric calculating core resource metrics consumption tracks:
$$g(	ext{load}) = rac{	ext{cpu\_usage} + 	ext{memory\_usage} + 	ext{queue\_factor} + 	ext{connection\_factor}}{4}$$
* $h(	ext{time})$: Exponential smoothing calculation model mitigating thundering herd request spike behaviors:
$$h(	ext{time}) = 1 - e^{-\lambda 	imes \Delta t}$$
where constant velocity decay weights match $\lambda = 0.1$, and $\Delta t$ handles duration spans tracking seconds since the last rejection event.

#### Low Overload Status Delay Formulation:
$$D = D_{base} 	imes (1 + lpha 	imes 	ext{load\_factor})$$
Base reference duration standards align with $D_{base} = 100	ext{ms}$, amplification weight adjustments map to $lpha = 2.0$, and the resource utilization mean equals $	ext{load\_factor} = rac{	ext{cpu\_usage} + 	ext{memory\_usage}}{2}$.

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Geo-Distributed Migration

Cross-datacenter transfers use Change Data Capture (CDC) pipelines and non-blocking asynchronous data mirroring without requiring engine downtime windows.

### Migration Lifecycle States:
```
[ Idle ] ──&gt; [ Preparing ] ──&gt; [ Migrating ] ──&gt; [ Delta Sync ] ──&gt; [ Validating ] ──&gt; [ Completed ]
```

### Operational Execution Modes Matrix:
* `manual`: Operational tasks require manual authorization steps.
* `semi_auto`: Baseline storage transfers are triggered manually; catch-up delta sync loops execute automatically.
* `auto`: Transfers trigger automatically based on internal scheduler settings.

### Control Command Reference Sequences:
* `migration start <source_dc> <target_dc> [db] [collection]`: Launch migration process pipeline.
* `migration status [task_id]`: Check progress metrics parameters data points.
* `migration list`: View active and historical migration tasks.
* `migration pause / resume / cancel <task_id>`: Control target processing tasks.
* `migration stats`: Review performance characteristics dashboards.
* `migration validate <task_id>`: Check integrity using SHA-256 controller checksum blocks (defaults test a 10% sampling population block).

### Example JSON Specification Configuration Interface Template:
```toml
[migration]
enabled = true
mode = "semi_auto"

[migration.source]
name = "dc-primary"
endpoint = "https://dc1.futriix.local:8080"
timeout_sec = 30

[migration.target]
name = "dc-secondary"
endpoint = "https://dc2.futriix.local:8080"
timeout_sec = 30

[migration.settings]
batch_size = 1000
workers = 4
compression = "snappy"
resume_enabled = true
checkpoint_interval_sec = 30
max_retries = 3
retry_backoff_sec = 5
exclude_collections = ["temp", "logs"]

[migration.delta]
enabled = true
interval_sec = 60
max_lag_sec = 300

[migration.validation]
enabled = true
sample_percent = 10
max_errors = 100
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Constraints

Data validity uses schema check validation constraints processing attributes lookups before saving mutations permanently.

```sh
# Impose mandatory properties verification checks lines configurations
futriix:~> add required employees email
✓ Required field 'email' added to collection 'employees'

# Enforce field unique parameters rules constraints bounds
futriix:~> add unique employees phone
✓ Unique constraint added for field 'phone' on collection 'employees'

# Inject value bounds checks attributes paths values
futriix:~> add min employees age 18
✓ Min constraint added for field 'age' on collection 'employees' (min: 18.00)

futriix:~> add max employees age 65
✓ Max constraint added for field 'age' on collection 'employees' (max: 65.00)

# Set discrete value enumerations lists validation checks properties
futriix:~> add enum employees status active,inactive,on_leave
✓ Enum constraint added for field 'status' on collection 'employees' (allowed: [active inactive on_leave])
```

<p align="right">(<a href="#readme-top">Back to top</a>)</p>

---

## Import-Export
To create backups, the DBMS provides `export` and `import` commands, allowing you to dump/load entire databases in **MessagePack** format. Exporting saves documents with metadata (versions, timestamps), while importing supports skipping existing documents and providing detailed statistics.

### Exporting database to a MessagePack file
```bash
futriix:~> export "company" "company_backup.msgpack"
✓ Database 'company' exported to company_backup.msgpack
```

### Exporting with automatic extension addition
```bash
futriix:~> export "shop" "shop_backup"
✓ Database 'shop' exported to shop_backup.msgpack
```

### Importing database from a file
```bash
futriix:~> import "company" "company_backup.msgpack"
Importing data from company_backup.msgpack to database 'company'...
✓ Database 'company' imported successfully from company_backup.msgpack
Collections imported: 2
Documents imported: 150
Documents skipped (already exist): 0
Documents failed: 0
```

### Importing into a new database
```bash
futriix:~> import "company_restore" "company_backup.msgpack"
Created database 'company_restore'
✓ Database 'company_restore' imported successfully from company_backup.msgpack
Collections imported: 2
Documents imported: 150
```

[To top](#readme-top)

---

## HTTP API
The Futriix DBMS provides a convenient network interface for integration with web applications — a **RESTful API** that covers all major operations on data and system service objects.

The API is designed considering the requirements of modern web development:
* **CORS Support**: Allows executing requests to the DBMS from browser applications hosted on external domains.
* **X-Session-ID Authentication**: Provides a simple and reliable session management mechanism: the client receives a session token upon authentication.
* **Available Endpoint Groups**: Each is responsible for its own management area:
  * **CRUD Operations on Collections** `(/api/db/{db}/{collection})` — A full set of actions for documents.
  * **Index Management** `(/api/index/)` — Tools for creating, modifying, and deleting indexes.
  * **Access Control (ACL)** `(/api/acl/)` — Configuring access rules for DBMS objects.
  * **Constraint Management** `(/api/constraint/)` — Managing declarative data integrity constraints.
  * **Cluster Administration** `(/api/cluster/)` — Operations to manage cluster topology.

### Authentication
```bash
curl -X POST http://localhost:8080/api/auth/login \
-H "Content-Type: application/json" \
-d '{"username":"admin","password":"admin"}'
# Response: {"success":true,"data":{"session_id":"abc123"}}
```

### Inserting a Document
```bash
curl -X POST http://localhost:8080/api/db/company/employees \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"name":"API User","position":"Integrator","age":28}'
# Response: {"success":true,"data":{"status":"inserted"}}
```

### Getting a Document by ID
```bash
curl -X GET "http://localhost:8080/api/db/company/employees/550e8400-e29b-41d4-a716-446655440000" \
-H "X-Session-ID: abc123"
# Response: {"success":true,"data":{"_id":"550e8400-...","fields":{...}}}
```

### Getting All Documents with Pagination
```bash
curl -X GET "http://localhost:8080/api/db/company/employees?limit=10&offset=0" \
-H "X-Session-ID: abc123"
```

### Index Search
```bash
curl -X GET "http://localhost:8080/api/db/company/employees?index=name_idx&value=John%20Doe" \
-H "X-Session-ID: abc123"
```

### Updating a Document
```bash
curl -X PUT http://localhost:8080/api/db/company/employees/550e8400-e29b-41d4-a716-446655440000 \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"age":31,"position":"Senior Developer"}'
# Response: {"success":true,"data":{"status":"updated"}}
```

### Deleting a Document
```bash
curl -X DELETE "http://localhost:8080/api/db/company/employees/550e8400-e29b-41d4-a716-446655440000" \
-H "X-Session-ID: abc123"
# Response: {"success":true,"data":{"status":"deleted"}}
```

### Creating an Index via API
```bash
curl -X POST http://localhost:8080/api/index/company/employees/create \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"name":"email_idx","fields":["email"],"unique":true}'
```

### Viewing Indexes
```bash
curl -X GET "http://localhost:8080/api/index/company/employees/list" \
-H "X-Session-ID: abc123"
```

### Cluster Status via API
```bash
curl -X GET "http://localhost:8080/api/cluster/status" \
-H "X-Session-ID: abc123"
```

### Creating a User via API
```bash
curl -X POST http://localhost:8080/api/acl/user/newuser \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"password":"secret","roles":["reader"]}'
```

### Granting Permissions via API
```bash
curl -X POST "http://localhost:8080/api/acl/grant/reader/rw" \
-H "X-Session-ID: abc123"
```

### Creating a Trigger via API
```bash
curl -X POST http://localhost:8080/api/trigger/company/employees/create \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"name":"audit","event":"AFTER_INSERT","action":"log"}'
```

[To top](#readme-top)

---

## Lua Plugins
To extend the functional capabilities of the DBMS **without modifying its source code**, Futriix implements a plugin system via Lua scripts with an isolated environment.  
Plugins have access to the database, transactions, triggers, can log events, and interact via an event bus. They are also accessible in the web interface.  
Furthermore, plugins can be used to write alternative storage engines for the DBMS in Lua without changing its core source code.

### Viewing Plugin System Status
```bash
futriix:~> plugin status
=== Plugin System Status ===
Enabled: true
Plugins Directory: ./plugins
Loaded Plugins: 3
Total Executions: 125
```

### List of Loaded Plugins
```bash
futriix:~> plugin list
=== Loaded Plugins ===
validation (v1.0.0) by admin - Document validation rules
 Status: RUNNING
audit (v2.1.0) by security - Audit trail logger
 Status: RUNNING
notify (v1.2.0) by devops - Email and webhook notifications
 Status: RUNNING
```

### Loading a Plugin from a File
```bash
futriix:~> plugin load email_notifier ./plugins/email_notifier.lua
✓ Plugin 'email_notifier' loaded successfully
Version: 1.0.0
Author: admin
Description: Send email notifications on database events
```

### Starting/Stopping a Plugin
```bash
futriix:~> plugin start email_notifier
✓ Plugin 'email_notifier' started
futriix:~> plugin stop email_notifier
✓ Plugin 'email_notifier' stopped
```

### Unloading a Plugin
```bash
futriix:~> plugin unload email_notifier
✓ Plugin 'email_notifier' unloaded
```

### Document Validation Plugin Example
Add a file named `validation.lua` into the `plugins` directory with the following content:

```lua
-- Plugin Metadata
version = "1.0.0"
author = "admin"
description = "Document validation rules for employees collection"

-- Initialization function
function on_load()
    plugin_log("info", "Validation plugin loaded")
    return true
end

-- Start function
function on_start()
    plugin_log("info", "Validation plugin started")
    return true
end

-- Stop function
function on_stop()
    plugin_log("info", "Validation plugin stopped")
    return true
end

-- Unload function
function on_unload()
    plugin_log("info", "Validation plugin unloaded")
    return true
end

-- Event Handler
function on_event(event)
    plugin_log("debug", "Received event: " .. event.type)
    if event.type == "BEFORE_INSERT" then
        return validate_document(event.data)
    end
    return true
end

-- Document Validation Function
function validate_document(doc)
    -- Required fields check
    if doc.name == nil or doc.name == "" then
        plugin_log("error", "Document missing required field: name")
        return false, "Field 'name' is required"
    end
    
    -- Age check
    if doc.age ~= nil then
        if doc.age < 18 then
            plugin_log("warn", "Age validation failed: ")
            return false, "Employee must be at least 18 years old"
        end
        if doc.age > 65 then
            plugin_log("warn", "Age validation failed: ")
            return false, "Employee cannot be older than 65 years old"
        end
    end
    
    -- Email check
    if doc.email ~= nil then
        if string.match(doc.email, "^[%w._-]+@[%w._-]+%.[%w]+$") == nil then
            plugin_log("error", "Invalid email format: ")
            return false, "Invalid email format"
        end
    end
    
    -- Salary check
    if doc.salary ~= nil then
        if doc.salary < 30000 then
            plugin_log("warn", "Salary below minimum: " .. doc.salary)
            return false, "Salary must be at least 30000"
        end
    end
    
    plugin_log("info", "Document validation passed for: " .. doc.name)
    return true
end

-- Custom function for bulk validation
function validate_collection(collection_name)
    local coll = get_collection("company", collection_name)
    if coll == nil then
        plugin_log("error", "Collection not found: " .. collection_name)
        return 0
    end
    plugin_log("info", "Validating collection: " .. collection_name)
    return 0
end
```

### Using the Document Validation Plugin
```bash
# Creating a database and a collection
futriix:~> create database company
✓ Database 'company' created
futriix:~> use company
✓ Switched to database 'company'
futriix:~> create collection employees
✓ Collection 'employees' created in database 'company'

# Loading and starting the validation plugin
futriix:~> plugin load validation ./plugins/validation.lua
✓ Plugin 'validation' loaded successfully
Version: 1.0.0
Author: admin
Description: Document validation rules for employees collection
futriix:~> plugin start validation
✓ Plugin 'validation' started

# Inserting a valid document
futriix:~> insert employees name=John Doe,age=25,email=john@company.com,salary=45000
✓ Document inserted with ID: emp_001

# Inserting an invalid document (age < 18)
futriix:~> insert employees name=Jane Smith,age=16,email=jane@company.com,salary=20000
Error: Employee must be at least 18 years old

# Inserting an invalid document (invalid email)
futriix:~> insert employees name=Bob Johnson,age=30,email=invalid-email,salary=50000
Error: Invalid email format

# Calling a custom plugin function
futriix:~> plugin call validation validate_collection employees
✓ Function returned: 0
```

### Managing Plugins via HTTP API

```bash
# Get the list of plugins via API
curl -X GET "http://localhost:8080/api/plugin/list" \
-H "X-Session-ID: abc123"

# Load a plugin via API
curl -X POST http://localhost:8080/api/plugin/load \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"name":"validation","path":"./plugins/validation.lua"}'

# Start a plugin via API
curl -X POST "http://localhost:8080/api/plugin/start/validation" \
-H "X-Session-ID: abc123"

# Execute a plugin function via API
curl -X POST http://localhost:8080/api/plugin/call \
-H "Content-Type: application/json" \
-H "X-Session-ID: abc123" \
-d '{"plugin":"validation","function":"validate_collection","args":["employees"]}'
```

---

## Plugins as a Tool for Writing Storage Engines for Futriix
To write a new custom engine (LSM-tree, key-value, or time-series), two files are required: a engine logic file with a `.lua` extension and a plugin manifest file with a `.json` extension.

The **engine plugin manifest** is a JSON file accompanying the Lua plugin script that provides metadata to the system. It is required for proper loading, registration, and management of plugins that implement custom storage engines.

### Location and Naming
The manifest must reside in the same directory as the Lua plugin script (by default `/futriix/plugins`) and share an identical base name with a `.json` extension. Example:
```text
plugins/
├── timescale_engine.lua   # Engine Lua script
└── timescale_engine.json  # Engine Manifest
```

### Manifest Structure
```json
{
  "name": "timescale_engine",
  "version": "1.0.0",
  "author": "Example Corp",
  "description": "Time-series storage engine with automatic partitioning",
  "api_version": "1.0",
  "engine_type": "timescale",
  "min_go_version": "1.21",
  "dependencies": [
    {
      "name": "base_engine",
      "version": "2.0.0",
      "min_version": "2.0.0",
      "max_version": "3.0.0",
      "optional": false
    }
  ],
  "entry_point": "create_engine",
  "created_at": 1704067200000,
  "updated_at": 1704153600000
}
```

### Manifest Fields
| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | Yes | Unique name of the plugin. Must match the Lua filename (without extension). |
| `version` | string | Yes | Plugin version in Semantic Versioning format (X.Y.Z). |
| `author` | string | Yes | Author or developer organization of the plugin. |
| `description` | string | Yes | Brief description of the plugin's functionality. |
| `api_version` | string | Yes | DBMS API version with which the plugin is compatible. |
| `engine_type` | string | Yes | **Key field.** Defines that the plugin acts as a storage engine. The value is used as an engine identifier when creating collections. |
| `min_go_version`| string | No | Minimum Go version required for the plugin. |
| `dependencies` | array | No | List of dependencies on other plugins. |
| `entry_point` | string | Yes | Name of the Lua factory function creating the engine instance. Default: `create_engine`. |
| `created_at` | int64 | Yes | Manifest creation time (Unix timestamp in milliseconds). |
| `updated_at` | int64 | Yes | Manifest last update time (Unix timestamp in milliseconds). |

### Dependency Structure
Each item in the `dependencies` array has the following structure:
| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `name` | string | Yes | Name of the dependent plugin. |
| `version` | string | No | Exact version of the dependent plugin (requires an exact match if specified). |
| `min_version` | string | No | Minimum allowed version (inclusive). |
| `max_version` | string | No | Maximum allowed version (exclusive). |
| `optional` | boolean| No | Whether the dependency is optional. Default: `false`. |

### Plugin Loading Process with Manifest
1. **Discovery**: The system scans the plugins directory and locates `.lua` files.
2. **Manifest Reading**: If a matching `.json` file exists, the system loads and parses it.
   * **Validation**:
     * Verifies mandatory fields (`name`, `version`, `api_version`).
     * Verifies that `engine_type` is not empty (for engines).
     * Verifies dependency version compatibility.
3. **Engine Registration**: If `engine_type` is provided, the system automatically registers the plugin in the `EngineRegistry` under this name.
4. **Lua Script Execution**: The Lua script is executed and must export the factory function (default: `create_engine`).
5. **Factory Invocation**: When an engine instance is created for a specific collection, the factory function is called with the given configuration.

### Example Usage
Creating a manifest for a Time-Series engine:
```json
{
  "name": "ts_engine",
  "version": "1.2.0",
  "author": "Futriix Team",
  "description": "High-performance time-series storage engine with automatic partitioning",
  "api_version": "1.0",
  "engine_type": "timeseries",
  "dependencies": [
    {
      "name": "compression_plugin",
      "min_version": "1.0.0",
      "optional": false
    }
  ],
  "entry_point": "new_timeseries_engine",
  "created_at": 1704067200000,
  "updated_at": 1704153600000
}
```

### Versioning Guidelines
* Use Semantic Versioning (MAJOR.MINOR.PATCH).
* Increment MAJOR version for incompatible engine API changes.
* Increment MINOR version for adding functionality in a backwards-compatible manner.
* Increment PATCH version for backwards-compatible bug fixes.

### Error Handling
If a manifest is missing, the system attempts to load the plugin in a "simplified mode" using default values. However, **for engine plugins, a manifest is strictly required** because the `engine_type` field is mandatory for proper registration.  
Manifest parsing errors are logged but do not prevent the Lua script execution (unless it's an engine plugin). On critical errors (e.g., missing `engine_type` for an engine), loading aborts with an appropriate error message.

[To top](#readme-top)

---

## Access Control
Futriix implements a multi-level access control system based on **ACL (Access Control Lists)** with session-based authentication. It supports the following roles: `read`, `write`, `delete`, and `admin`, providing granular control at the database and collection levels.

### Logging into the System
```bash
futriix:~> acl login admin admin
✓ Logged in as 'admin' with role 'admin'
```

### Logging out
```bash
futriix:~> acl logout
✓ Logged out
```

### Assigning Permissions (after logging in as admin)
```bash
futriix:~> acl login admin admin
✓ Logged in as 'admin' with role 'admin'
futriix:~> use company
✓ Switched to database 'company'

# Granting read permissions
futriix:~> acl grant employees reader r
✓ Permissions 'r' granted to role 'reader' on collection 'employees'

# Granting read and write permissions
futriix:~> acl grant employees editor rw
✓ Permissions 'rw' granted to role 'editor' on collection 'employees'

# Granting full permissions (collection administrator)
futriix:~> acl grant employees admin rwda
✓ Permissions 'rwda' granted to role 'admin' on collection 'employees'
```

### Permission flags explanation:
* `r` — read
* `w` — write
* `d` — delete
* `a` — admin (collection administrator)

[To top](#readme-top)

---

## Triggers
A **Trigger** is a database action automatically executed when a record is inserted, updated, or deleted. Most commonly, triggers are used for mathematical evaluations or auditing (automatically recording events into a log or a tracking database table).  
In Futriix DBMS, triggers are implemented in a **MongoDB-like style** with `BEFORE/AFTER INSERT/UPDATE/DELETE/REPLACE` events. It supports conditions (`eq`, `gt`, `regex`, `in`, `exists`) and actions (`abort`, `modify`, `skip`, `log`, `notify`) with document modification support.

### Creating a Trigger to Log Insertions
```bash
futriix:~> create trigger employees audit_log AFTER_INSERT log
✓ Trigger 'audit_log' created on collection 'employees' for event AFTER_INSERT
```

### Creating a Trigger with an Automatic Timestamp
```bash
futriix:~> create trigger employees set_timestamp BEFORE_INSERT modify --set updated_at $$NOW
✓ Trigger 'set_timestamp' created on collection 'employees' for event BEFORE_INSERT
```

### Creating a Trigger with a Condition (Prevent deleting active users)
```bash
futriix:~> create trigger employees protect_active BEFORE_DELETE abort --condition status eq active
✓ Trigger 'protect_active' created on collection 'employees' for event BEFORE_DELETE
```

### Creating a Trigger that Updates Audit Metadata
```bash
futriix:~> create trigger employees audit BEFORE_UPDATE modify --set modified_by $$USER --set modified_at $$NOW
✓ Trigger 'audit' created on collection 'employees' for event BEFORE_UPDATE
```

### Viewing All Triggers for a Collection
```bash
futriix:~> show triggers employees
=== Triggers on collection 'employees': ===
audit_log (AFTER_INSERT) - enabled [log]
Operations:
- log: = 
set_timestamp (BEFORE_INSERT) - enabled [modify]
Operations:
- set: updated_at = $$NOW
protect_active (BEFORE_DELETE) - enabled [abort]
Condition: status eq active
audit (BEFORE_UPDATE) - enabled [modify]
Operations:
- set: modified_by = $$USER
- set: modified_at = $$NOW
```

### Enabling/Disabling a Trigger
```bash
futriix:~> disable trigger employees BEFORE_INSERT set_timestamp
✓ Trigger 'set_timestamp' disabled
futriix:~> enable trigger employees BEFORE_INSERT set_timestamp
✓ Trigger 'set_timestamp' enabled
```

### Viewing the Trigger Execution Log
```bash
futriix:~> trigger log
=== Trigger Execution Log ===
[1] 2026-04-12 10:30:45 - Trigger: audit_log, Event: AFTER_INSERT, Collection: employees, Doc...
[2] 2026-04-12 10:31:20 - Trigger: protect_active, Event: BEFORE_DELETE, Collection: employees...
```

### Dropping a Trigger
```bash
futriix:~> drop trigger employees BEFORE_INSERT set_timestamp
✓ Trigger 'set_timestamp' dropped from collection 'employees'
```

[To top](#readme-top)

---

## Data Compression
Data compression in Futriix DBMS uses the **Brotli** compression algorithm to reduce the storage footprint of documents in both RAM and hard drive space, enabling efficient resource utilization with massive datasets.

The Brotli algorithm, developed by Google, provides compression ratios **20–26% better** than traditional Gzip at comparable decompression speeds, making it an optimal choice for read-intensive systems.

Key benefits of Brotli include: a predefined dictionary of frequently occurring byte sequences, adaptive variable-length encoding, support for **11 compression levels** (from fastest to tightest), and high decompression speed critical for rapid document access. In Futriix, compression applies automatically when a document exceeds a threshold size (configured via `compression.MinSize`). Each document stores a `Compressed` flag and its `Original Size` to monitor performance efficiency.

### Viewing Compression Configuration
```bash
futriix:~> compression config
=== Compression Configuration ===
Enabled: true
Algorithm: snappy
Level: 3
Min Size: 1 KB

Available Algorithms:
snappy - Fast compression/decompression, good balance (default)
lz4    - Extremely fast, lower compression ratio
zstd   - High compression ratio, slower
```

### Viewing Compression Statistics
```bash
futriix:~> compression stats
=== Compression Statistics ===
Total Documents: 1250
Compressed Documents: 890
Compression Rate: 71.20%
Size Reduction: 45.30%
Original Size: 15.2 MB
Compressed Size: 8.3 MB
Algorithm: snappy
Compression Level: 3
Min Size Threshold: 1 KB
```

### Manual Collection Compression
```bash
futriix:~> compress collection employees
Compressing collection 'employees'...
✓ Compressed 45 documents in collection 'employees'
```

### Viewing Document Compression Info
```bash
futriix:~> doc compression employees 550e8400-e29b-41d4-a716-446655440000
=== Compression Info for Document: 550e8400-e29b-41d4-a716-446655440000 ===
Compressed: true
Ratio: 35.20%
Original Size: 2.5 KB
Current Size: 1.6 KB
```

[To top](#readme-top)

---

## Graphical User Interface
To simplify administration, Futriix includes a **WUI (Web User Interface)**. Using a web browser, users can manage the DBMS quickly, simply, and conveniently. The baseline configuration view is available via the default address.

![wui.png](wui.png)

[To top](#readme-top)

---

## FAQ (Frequently Asked Questions)
This section answers the most common questions to help you quickly understand the project without spending time finding obvious concepts. If you cannot find the required answer, please file an issue; your feedback helps prioritize development.

**Question:** Why is the version designated as "Futriix 2 i²" instead of just "2.0"?  
**Answer:** "2" stands for the version number containing core features (sharding, WAL, MVCC, etc.).  
"i²" is a mathematical metaphor: although $i$ is an imaginary unit, its square yields an absolutely exact, real result ($i^2 = -1$). Similarly, even in a complex distributed environment with parallel operations (wait-free, lock-free), Futriix yields strictly predictable results via Raft, ACID, and MVCC. This aligns with the OpenIndiana/Illumos philosophy: determinism and resource control.

[To top](#readme-top)

---

## Roadmap
* [x] Implement support for stored procedures
* [x] Implement support for triggers (callbacks)
* [x] Implement concurrency support
* [x] Implement non-blocking read/write operations
* [x] Implement non-blocking transactions
* [x] Implement constraints (Data integrity rules)
* [x] Implement asynchronous multi-master replication via config file
* [x] Implement logging system for the DBMS
* [x] Implement synchronous master-master replication support
* [x] Implement baseline Raft protocol support
* [x] Implement index support (primary and secondary indexes)
* [x] Implement MessagePack protocol support
* [x] Write baseline tests (integration, functional, regression, load, and smoke tests for the "futriix" DBMS core)
* [x] Add a Lua plugin mechanism to load scripts at startup to extend functionality without modifying the DBMS source code
* [x] Implement HTTP RESTful API support
* [x] Implement data compression based on the "Brotli" protocol
* [x] Implement DBMS dump import and export in "MessagePack" format
* [x] Fix log journal writing bugs (include current year alongside current time in log records)
* [x] Implement a Web User Interface (WUI) based on the custom "futriis" engine to manage the DBMS via browser
* [x] In the web interface, add support for a small profile picture to the left of the "admin" text in the bottom left corner
* [x] In the web interface, remove hardcoded credentials (`admin`; `admin`) from the source code
* [x] In the web interface, implement the ability to change the authentication login and password (store default `admin`/`admin` credentials in a hidden `.credentials` file located in the `futriix` directory)
* [x] In the web interface, add the ability to read the `futriix.log` log file, which records all operations performed in the WUI during a session (e.g., database created, collection dropped), including failed operations
* [x] In the web interface, add the ability to append a new administrator user whose credentials will be saved in the hidden `.credentials` file inside the `futriix` directory
* [x] In the web interface, add the ability to manage plugins (enable, disable)
* [x] Rewrite build scripts `build.sh` and `vendor_build.sh` to remove dependency on the `gcc` compiler so it does not need to be installed separately in the "OpenIndiana Hipster" operating system
* [x] Replace the `raft-boltdb` library with an embedded file storage engine
* [x] Implement unique and composite indexes
* [x] Implement timestamps for core DBMS entities (tuple, collection, document, field, index, transaction, ACL, cluster node)
* [x] Optimize transactions for high loads (Implement: batch WAL flushing instead of synchronous writes — asynchronous flushing in batches of 100 entries; 64KB buffer for WAL entries; periodic fsync every 5 seconds instead of every write; periodic checkpoints of state; atomic operations using `atomic.Value` and `sync.Map` for wait-free access)
* [x] Implement MVCC versioning — multi-version document concurrency control support
* [x] Implement timestamps for additional DBMS entities (Constraints, Import-Export, Triggers, Lua plugins)
* [x] Implement "split-brain" state handling
* [x] Implement Replication Pipelining — grouping multiple commands into a single Raft log entry to reduce network overhead
* [x] Implement Batch commit — committing multiple operations in a single cycle to lower fsync calls
* [x] Implement Dynamic Shard Redistribution — automatic re-sharding upon adding new nodes to the cluster
* [x] Implement Observability — in a primitive form via the web interface
* [x] Implement "Joint consensus" (safe cluster configuration transition)
* [x] Implement leveled logging (DEBUG/INFO/WARN/ERROR) for tracking events
* [x] Implement Segmented WAL — dividing WAL into 64MB segments with automatic rotation
* [x] Implement Parallel Recovery — parallel transaction recovery using a worker pool (4 workers)
* [x] Implement WAL Index — an index for fast entry lookup by LSN
* [x] Implement Checksum per record — CRC32 containing LSN inside the checksum
* [x] Implement Visibility Map — a version visibility bitmap for fast snapshot reads
* [x] Implement Version Pruning Policy — automatic background cleanup of old versions (configurable policy)
* [x] Implement Read Timestamp Cache — cache for fast access to document versions by timestamp
* [x] Implement Gap Detection — detecting missing gaps in document versions
* [x] Implement Distributed Transactions — two-phase commit (2PC) for distributed transactions
* [x] Implement Deadlock Detection — cyclic deadlock detector with timeouts (DFS on wait-for graph)
* [x] Implement Transaction Timeout — configurable timeout for long-running transactions
* [x] Implement Savepoints — savepoints inside a transaction for partial rollbacks
* [x] Extract MVCC, WAL, and plugin configuration parameters from the project source code into a `config.toml` file
* [x] Bold all informational messages inside compilation and build scripts
* [x] Implement limits on the number of created Lua states inside plugins
* [x] Make WAL recovery asynchronous to speed up DBMS startup times
* [x] Implement SAGA pattern with an orchestrator (Distributed Transactions)
* [x] Node pre-voting before commits in the Raft protocol
* [x] Implement automatic scaling
* [x] Implement TLS and backpressure
* [x] Implement runtime limits for collections and data schema migrations
* [x] Implement a "hot backup" system
* [x] Implement a Golang installation script for Illumos-based operating systems
* [x] Implement Automatic sharding
* [x] Implement TLS for inter-node communication
* [x] Implement certificate validation during mutual authentication and encryption key rotation (in a basic form)
* [x] Implement backpressure (cluster overload control mechanism) triggered during heavy loads
* [x] Implement runtime size constraints on collections and documents
* [x] Implement Automatic scaling (before implementation, briefly explain its underlying algorithm and workflow)
* [x] Implement transparent data schema upgrades
* [x] Implement only a "backup scheduler" and "incrementality (WAL-based)" in the backup system
* [x] Implement a plugin mechanism allowing the creation of full custom database engines in Lua without altering core source code
* [x] Implement atomic writes via a temporary file (ensuring the integrity of database checkpoints/snapshots)
* [x] Implement SHA-256 checksums for checkpoints
* [x] Implement locking during data recovery
* [x] Implement data integrity verification on boot/loading
* [x] Implement data recovery status output info
* [x] Implement locking during recovery
* [x] Implement WAL segment purging after successful backup completion
* [x] Implement backup version checking
* [x] Implement backup statuses (pending-running-completed-failed)
* [x] Implement atomic backup writing (ensuring backup file integrity)
* [x] Optimize DBMS core (removed file `/internal/storage/storage.go`, relocated `Storage` and `NewStorage` methods to `/internal/storage/engine.go`)
* [x] Implement WAL journal batch flushing (`flushBatch`)
* [x] Implement geo-distributed migration between two data centers located in the same country and city
* [x] Implement Multi-Raft (Multiple independent Raft consensus groups for different shards)
* [x] Implement Gossip Protocol (automatic node discovery mechanism, status/health/load exchange between nodes via UDP, seed node support for initial discovery, automated cluster membership tracking updates)
* [x] Implement Self-Healing mechanisms (automatic health monitoring of all cluster nodes, failure detection using a configurable failure threshold, automatic remediation: restart, reconnection, rebalancing, tracking recovery history, and metrics collection)
* [x] Implement Dynamic Configuration (centralized configuration management inside the Raft log, hot parameter updates on the fly without node restarts, versioning and history tracking, component subscription hooks for updates)
* [ ] Implement Linearizable reads via leader leases with a quorum for strict cluster consistency
* [ ] Implement Learner nodes (non-voting replication members for backups)
* [ ] Implement full-featured Raft algorithm support (with automatic leader election and failure domains)
* [ ] Implement geo-distributed migration (Regional zones, multi-AZ support with priority selection, read from closest replica, geographically routing requests, asynchronous cross-region replication to reduce global cluster latencies)
* [ ] Implement Read-only leader (query optimization via local index evaluation)
* [ ] Implement Snapshot streaming (streaming database snapshots via an isolated channel without lock contention)
* [ ] Integration with monitoring systems (Prometheus, Grafana)
* [ ] Implement a full backup ecosystem with structural validation and cross-datacenter automated replication capabilities
* [ ] Implement driver connectors for modern programming languages (C, C++, Java, Python, Go)
* [ ] Implement a server benchmarking utility evaluating read/write throughput parameters

See **Open Issues** for the full list of proposed features and known bugs.

[To top](#readme-top)

---

## Contacts
For operations, performance tuning, and configuration support inquiries, please refer to the contact details provided below.  
Please note that the personal email is a technical channel intended *only* for:
* Reporting critical vulnerabilities (with a detailed PoC)
* Partnership proposals
* Structural proposals for improving Futriix

Please be advised that emails lacking a clearly formulated subject line, question, or proposal will not be reviewed.

* **Grigory Safronov** — E-mail
* **Community** — [Futriix Community](https://github.com)

[To top](#readme-top)
