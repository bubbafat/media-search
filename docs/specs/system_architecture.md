# **MediaSearch v2: Deep-Dive Technical Specification**

## **System Vision & Executive Summary** 

MediaSearch v2 is a highly concurrent, distributed media discovery and AI-processing pipeline designed to index, analyze, and search massive media libraries (2,000,000+ assets and 10,000,000+ video frames).

**The Problem:** Traditional media indexers degrade catastrophically at scale. They suffer from database locking during large deletions, they saturate local networks by repeatedly reading massive 50GB video files for different processing steps (the "Double-Read Penalty"), and they pollute pristine user storage with hidden files and sidecar metadata. Furthermore, at 500,000 to 5,000,000 assets, traditional PostgreSQL full-text search degrades on complex ranked queries, scoring millions of rows before returning results. Standard systems also lack a way to cleanly switch AI models without overwriting old results.

\+1

**The Solution:** V2 is completely re-architected from the ground up to operate as a distributed system utilizing a dual-database architecture. It is built on four core philosophies:

1. **Source Immutability:** The user's original network-attached storage (NAS) is treated strictly as a read-only source. The system never pollutes the source with hidden files; all derivative work (thumbnails, proxies) lives on a fast, sharded local SSD cache. The read-only NAS guarantee remains entirely unchanged.

2. **The Proxy Pipeline:** Network I/O is isolated from GPU-bound ML tasks. A heavy file is pulled across the network exactly once to generate a local proxy. Subsequent AI models (Moondream, CLIP, etc.) execute lightning-fast against the local proxy.  
3. **Decentralized Scale:** There is no master dispatcher. The system relies on a pull-based queue using PostgreSQL's FOR UPDATE SKIP LOCKED, allowing workers to scale horizontally across multiple machines with zero race conditions. The core worker architecture and lease system remain unchanged.

4. **Dedicated Append-Optimized Discovery:** PostgreSQL is strictly the relational source of truth, while Quickwit—an append-optimized search engine—powers the discovery layer. Versioned search documents and multi-index routing enable zero-disruption AI model upgrades, instant rollbacks, and shadow testing.  
   \+2

## **1\. Core Architectural Mandates**

### **1.1 Relational Database Engine**

* **Strict Requirement:** PostgreSQL 16.0 or higher.  
* **Dialect:** Use only sqlalchemy.dialects.postgresql features for non-standard types.  
* **Source of Truth:** PostgreSQL continues to own all transactional and relational data.

* **Concurrency:** Implement the **SKIP LOCKED** pattern for all task acquisitions. This is non-negotiable for distributed scaling to prevent race conditions.

### **1.2 Search & Discovery Engine**

* **Strict Requirement:** Quickwit 0.8.1+ running as a Docker service.

* **Append-Optimized:** Quickwit acts as a disposable, append-optimized index that scales to 100M+ documents. We append new versions on re-analysis and never overwrite in-place.

* **Multi-Index Pattern:** Search is dynamically routed to isolated indexes (e.g., media\_scenes\_moondream\_v2) to eliminate query-time filter penalties and allow garbage collection by simply dropping superseded indexes.

### **1.3 Task Orchestration (The State Machine)**

* **Pull-Based Logic:** Workers determine their own work by querying PostgreSQL. There is no central dispatcher.  
* **Lease Mechanism:** A "Claim" consists of an atomic update setting status='processing', assigning a worker\_id, and setting a lease\_expires\_at timestamp.  
* **Recovery:** Any asset with status='processing' and lease\_expires\_at \< now() is considered "Abandoned" and must be eligible for re-claiming by any healthy worker.

## ---

**2\. Detailed Database Schema**

### **2.1 libraries Table**

* slug (String, PK): URL-safe unique identifier. Acts as the strict primary key to prevent duplication.  
* name (String): Human-readable name.  
* absolute\_path (String): The physical local or network mount path.  
* deleted\_at (DateTime | None): Timestamp for soft-deletion.  
* is\_active (Boolean): The master "Pause" switch.

### **2.2 assets Table**

* id (UUID or BigInt, PK): Primary identifier.  
* library\_id (FK): Reference to the parent library (slug).  
* rel\_path (String): Path relative to the library root.  
* **Indexing:** A **Composite Unique Index** on (library\_id, rel\_path) is mandatory.  
* mtime (Float): Unix timestamp of last filesystem modification. Used for "Dirty Checks" during fast scans. Detects in-place replacements to prevent corruption.  
* status (Enum): pending, proxied, extracting, analyzing, completed, failed, poisoned.  
* tags\_model\_id (FK): Records which AI model produced the *current* relational data.

### **2.3 video\_scenes Table & State Management**

* One row per closed scene for resumable indexing.  
* id (int, PK), asset\_id (FK), start\_ts, end\_ts, description, metadata, rep\_frame\_path.  
* **video\_active\_state Table:** Tracks the "open" scene state for an asset currently being indexed via UPSERT logic, allowing FFmpeg segmentation to resume safely after a crash.

### **2.4 worker\_status Table**

* worker\_id (String, PK): Unique identifier for the worker instance.  
* last\_seen\_at (DateTime): Heartbeat timestamp.  
* state (Enum): idle, processing, paused, offline.

### **2.5 library\_model\_policy Table (New)**

* Controls which AI model version is actively served per library.

* library\_slug (String, PK, FK): Reference to the library.

* active\_index\_name (String): The actively promoted Quickwit index routed to users (Implementation adapted from active\_model\_id to support the Multi-Index pattern).

* shadow\_index\_name (String | None): The index currently being built or evaluated in the background.

* locked (Boolean): Prevents concurrent promotions.

### **2.6 search\_sync\_queue Table (The Outbox Pattern)**

* Resolves split-brain mutations between PostgreSQL and Quickwit.  
* id (Integer, PK, Auto-increment): Queue sequence.  
* asset\_id (Integer): The affected asset.  
* action (Enum: UPSERT or DELETE): The required mutation.  
* created\_at (Timestamp).  
* **Database Triggers:** PostgreSQL strictly enforces synchronization. AFTER UPDATE and AFTER DELETE triggers on assets and video\_scenes tables instantly write to this queue, ensuring no mutation is ever missed.

## ---

**3\. Worker Node Architecture & Conceptual Roles**

MediaSearch v2 uses a **Decentralized Worker Model**. Workers are completely stateless and pull work dynamically.

1. **The Scanner Worker (I/O & DB Bound):** Rapidly traverses the user's read-only network storage (NAS). Inserts DB rows as pending.  
2. **The Image Proxy Worker (Network I/O & CPU Bound):** Claims pending images. Pulls the heavy original file once. Generates a WebP proxy and a UI thumbnail using libvips shrink-on-load capabilities. Updates status to proxied.  
3. **The Video Proxy Worker (Network I/O & CPU Bound):** Claims pending videos. Transcodes to a temporary 720p H.264 file, extracts a 10-second head-clip for UI preview, runs scene indexing, persists scene bounds, and deletes the temp file.  
4. **The ML / AI Worker (GPU Bound):** Claims proxied images. Reads only the lightweight local proxies from the SSD, runs them through local Vision Models (Moondream), extracts tags/embeddings, and updates the asset to completed.  
5. **The Video Worker (GPU Bound):** Claims proxied videos that already have scene bounds. Runs vision analysis only on the persisted scene rep frame images.  
6. **The Garbage Collector Worker (Disk & DB Bound):** Executes chunked hard-deletions on databases for "emptied trash" libraries. It also handles Quickwit index pruning by dropping entirely superseded search indexes via the REST API.  
7. **The Search Sync Worker (DB & Network Bound):** This worker acts as the bridge between PostgreSQL and Quickwit. It claims rows from the search\_sync\_queue using FOR UPDATE SKIP LOCKED. For DELETE actions, it issues an HTTP DELETE to Quickwit. For UPSERT actions, it clears old versions and pushes fresh denormalized documents to the active Quickwit index.  
   \+1

## ---

**4\. The 100-Chunk Extraction Algorithm**

When a Video Worker processes an asset:

1. **Segmenting:** Divide duration by the sampling\_limit into N equal windows.  
2. **Keyframe Probing:** Identify all I-Frames within each segment via ffprobe.  
3. **Selection Logic:** Calculate a Pixel-Wise Mean Frame if multiple I-Frames exist. Select the I-Frame with the highest Structural Similarity (SSIM) to the Mean Frame to identify the most visually representative frame.

## ---

**5\. Observability & The "Black Box" Flight Log**

In a high-throughput system, writing DEBUG or INFO statements to a log file for every transaction burns out SSDs.

* **The Flight Log:** Every worker maintains a high-fidelity, circular in-memory buffer (collections.deque with 50,000 capacity).  
* **Triggered Dump:** On an unhandled exception or a forensic\_dump command, the worker instantly flushes the in-memory buffer to a physical file. Successful processing is not permanently memorialized.

## ---

**6\. The Proxy Pipeline & Data Locality**

To ensure UI responsiveness and absolute security of the source media, the system strictly enforces a **Proxy Architecture**. The user's original media directories must be treated as **Read-Only**.

* **Stage 1 (Discovery):** Scanner finds a file on the NAS. Inserts DB row as pending.  
* **Stage 2 (Proxy Generation):** Proxy workers read the heavy source once to generate SSD-cached web-friendly derivatives, updating status to proxied.  
* **Stage 3 (AI Extraction):** GPU workers claim proxied assets, reading only the local SSD derivatives to run inference, and marking assets as completed.  
* **Stage 4 (Search Sync):** The Search Sync Worker detects the completion via the Outbox queue and pushes the final metadata to the active Quickwit index.

## ---

**7\. API Routing & Fallback Safeguards**

The search API endpoints interact with the QuickwitSearchRepository.

* **Stateless Routing:** The FastAPI application strictly avoids LRU caching for policy states. Every search request checks PostgreSQL for the active multi-index policy, ensuring perfect synchronization across all load-balanced API workers.  
* **Loud Fallbacks:** If Quickwit becomes unreachable, the \_get\_search\_repo dependency gracefully falls back to the existing PostgreSQL SearchRepository. However, this fallback MUST explicitly log a CRITICAL or ERROR traceback, strictly preventing "silent failures."  
