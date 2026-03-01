# MASTER PROJECT PLAN AND DOCUMENTATION: THE VIDEO-FIRST NLE COMPANION
(Or, "Google Photos for your NAS, Optimized for Video")

## INTRODUCTION: THE MACARON PROBLEM
Imagine you are a video editor. You went to Disneyland Paris two years ago, and you know for an absolute fact that you filmed a beautiful, 4-second panning shot of a macaron food stand. You want to drop that exact shot into your Premiere Pro or DaVinci Resolve timeline right now. 

You open your massive 10-Terabyte Network Attached Storage (NAS) drive. You navigate to the "Europe_Trip_2022" folder. Inside, you are greeted by 800 massive 4K video files named `DJI_0184.MP4` through `DJI_0984.MP4`. 

Currently, your only option is to drag all 800 files into your video editor, wait hours for proxies to generate, and manually scrub until your eyes bleed. 

This is the exact problem we are solving. We are building a "Read-Only Discovery Layer" for video editors. It is a lightning-fast, local-first, AI-powered search engine. It watches your NAS, slices massive video files into logical "Scenes," generates lightweight video proxies, and uses local Artificial Intelligence to "watch" the videos, tagging them with objects and reading text off signs in the background. 

When you need that macaron cart, you open a snappy web browser, type "macaron", and you are instantly presented with a playable 10-second clip of that exact moment. You click "Copy File Path", switch back to Premiere Pro, and drag the high-resolution original file directly into your project. 

## PART 1: PRODUCT REQUIREMENTS DOCUMENT (PRD)

### 1. PRODUCT VISION
This software is a consumer-grade, lightning-fast media gallery with prosumer power, specifically designed for local storage. It acts as an NLE (Non-Linear Editor) Companion. Images are supported, but Video is the primary, first-class citizen.

### 2. THE 3 NON-NEGOTIABLE USER JOURNEYS
Every feature must serve one of these three journeys.
* **A) "I remember a moment" (Targeted Retrieval):** Search results return specific scenes, not whole files. Result tiles play instantly on hover. "Copy Path" is a one-click action. 
* **B) "I'm browsing what I shot" (Chronological Skimming):** The default "Scan View" is a dense, chronological grid. Hover previews feel completely instantaneous.
* **C) "My NAS is off / archived" (Offline Planning):** Search results still appear seamlessly using cached proxies. The UI gracefully degrades and clearly communicates "Preview unavailable" vs "Proxy unavailable" vs "Original unavailable".

### 3. ANTI-GOALS (WHAT WE ARE NOT BUILDING)
* **NOT a Plex clone:** No TV streaming, no resume-playback.
* **NOT a culling tool:** No RAW-to-JPEG stacking, no color grading, no XMP sidecar writing.
* **NOT a cloud service:** Local storage only.

### 4. CORE PRODUCT PARADIGMS

#### A. The Absolute Zero-Touch (Read-Only) Guarantee
The system must NEVER write, rename, move, delete, chmod, or create sidecar files inside the user's NAS Libraries. The deployment architecture mounts the NAS strictly as Read-Only (`:ro`). All generated data lives exclusively in our isolated app cache.

#### B. Scene-First Data Model
The search index is built on `scene` rows, not `asset` rows. An asset (file) may contain dozens of scenes; each scene is independently searchable, independently tagged by AI, and independently returned in search results.


#### C. The Offline-Ready Philosophy
Our software must NEVER automatically delete our search index just because a file goes missing. If a drive is detached, the app simply marks the assets as "Offline." 
We have a strict "Offline-Ready Contract." For a video to be fully offline-ready, the system must cache three things on the fast local SSD, represented as DB-derived boolean flags:
* `preview_ready` (10s proxy for grid hovering)
* `playable` (720p full-length proxy for the inspector)
* `searchable` (Video scene metadata and OCR exists in the DB)
* `offline_ready = (preview_ready AND playable AND searchable)`

#### D. Duplicate-Safe Stable Identity
Editors duplicate files constantly. We accomplish move-detection and duplicate tracking through fast content hashing combined with intelligent disk-presence checks. We will NOT enforce database-level uniqueness on content hashes, as that would break legitimate user workflows.

#### E. Trust UI with Provenance
AI can hallucinate. The UI must provide "Trust Badges" backed by hard evidence. When a user opens a search result, the UI explicitly shows the OCR snippet with the matched word highlighted.

#### F. Deterministic Output Contract
* Scene boundaries must be mathematically stable across runs.
* Search ranking must be stable for the same query.
* The UI must never "shuffle" results on refresh.

## PART 2: THE USER GUIDE

### 1. INGESTING & THE SYSTEM DRAWER
Mount your NAS drive and drag your SD card contents into your folders normally. In the background, our Scanner wakes up. At the bottom of your screen is the System Drawer with three critical tabs:
* **Activity:** Shows background workers ("Proxying", "Analyzing").
* **Health:** Shows NAS connectivity and database health.
* **Issues:** Shows exactly what failed ("Unreadable file: Corrupted.mp4", "Waiting for file to finish copying"). 

### 2. BROWSING: THE SCAN VIEW
The "Scan View" is a dense, chronological contact sheet. If you move your mouse over any video thumbnail, it instantly and silently starts playing a 10-second deterministic preview.

### 3. SEARCHING LIKE A PRO
The massive Omnibar supports keywords ("red car") and purely instantaneous Scope Chips like `Library: NAS1`, `Folder: Trip_2022`, `Camera: DJI`, or `availability: offline-ready`.

### 4. DEALING WITH DUPLICATES
Click the "Group Duplicates" toggle to intelligently collapse duplicate files into a single tile. Clicking the "15 Copies" badge opens a picker showing every asset path, defaulting to a copy where the original file is currently online and reachable.

### 5. THE INSPECTOR AND THE "HANDOFF"
Click a clip to open the Detail Inspector. 
* **The Timeline:** Tick marks show every "Scene Cut". If you arrived via search, the matching scene is highlighted.
* **The Evidence:** The "Why this matched" box shows the exact OCR text snippet.
* **The Handoff:** Click the giant "Copy File Path" button, and drag the file directly into Premiere Pro.

## PART 3: THE BACKEND ENGINEERING SPECIFICATION

### 1. THE COPY-IN-PROGRESS STABILIZATION CRITERIA (THE 2-SAMPLE GUARD)
When a user drags a 50GB file to the NAS over Wi-Fi, the transfer might stall. To prevent generating a proxy of a half-broken video, the stabilization rule requires **TWO consecutive scanner passes**: The scanner must observe identical `size_bytes` and `mtime` across two separate scans separated by at least 60 seconds.

### 2. DUPLICATE-SAFE MOVE DETECTION
Users duplicate files. We CANNOT put a "UNIQUE" DB constraint on `content_hash`. 
Logic:
1. Scanner computes `content_hash` of a file at `Path_B`.
2. Asks DB for existing candidate rows with that hash.
3. Asks OS: `os.path.exists(candidate.absolute_path)`.
4. 🚨 **Detached Drive Guardrail:** Before trusting an `os.path.exists() == False` response, the scanner MUST verify that the candidate’s Library root is mounted and reachable. 
5. If the OS says "False" AND the Library is reachable, we found a moved file. Execute: `UPDATE asset SET rel_path = :Path_B … WHERE id = :candidate_id`.
6. If the OS says "True" for candidates, the user created a new copy. Insert as a brand-new asset.

### 3. API SEPARATION OF CONCERNS: THE "NO DISK TOUCH" RULE
The List endpoint MUST NEVER touch the NAS hard drive, AND it must never touch the local SSD cache filesystem. The List API (`/api/scenes`) must rely entirely on the DB-derived boolean flags (`preview_ready`, `playable`) and the Quickwit search engine. Disk touches (`os.path.exists`) are strictly reserved for the Detail Inspector API (`/api/assets/{id}`).