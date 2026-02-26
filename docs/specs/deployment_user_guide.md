# MediaSearch 3-Machine Deployment Checklist (macOS)

You have three Macs: **Machine A** (web + database + scanners + proxy), **Machine B** and **Machine C** (AI workers only). Follow the steps in order.

---

## Part 1: Machine A (Main Server)

### Step 1.1 – Install Homebrew (if needed)

1. Open **Terminal** (Applications → Utilities → Terminal).
2. Run: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
3. Follow the prompts.
4. If the installer asks, run the commands it prints to add Homebrew to your PATH.

---

### Step 1.2 – Install Tools with Homebrew

In Terminal, run these one at a time (press Enter after each):

```bash
brew install git
brew install python@3.11
brew install uv
brew install ffmpeg
brew install vips
brew install --cask docker
```

(If something says it's already installed, that's fine—continue.)

---

### Step 1.3 – Clone the Project

1. In Terminal, go to where you want the project:

   ```bash
   cd ~
   ```

2. Clone the repo (replace with your actual repo URL):

   ```bash
   git clone https://github.com/YOUR_USERNAME/media-search.git
   cd media-search
   ```

---

### Step 1.4 – Start Docker and Run Postgres

1. Open the **Docker** app (from Applications). Wait until it says it's running.
2. In Terminal, ensure you're in the project folder:

   ```bash
   cd ~/media-search
   ```

3. Start Postgres:

   ```bash
   docker compose up -d
   ```

---

### Step 1.5 – Install Python Dependencies

Still in the `media-search` folder:

```bash
uv sync
```

This may take several minutes (PyTorch and friends are large). Wait for it to finish.

---

### Step 1.6 – Create the `.env` File

1. Copy the example:

   ```bash
   cp .env.example .env
   ```

2. Open `.env` in a text editor (e.g. TextEdit):

   ```bash
   open -e .env
   ```

3. For Machine A, ensure it looks like this (localhost is correct—Postgres is on this machine). The template includes an optional `MEDIA_SEARCH_DATA_DIR` line; for Machine A the default `./data` is fine, so leave it commented:

   ```
   DATABASE_URL=postgresql+psycopg2://media_search:media_search@localhost:5432/media_search
   HF_TOKEN=
   # MEDIA_SEARCH_DATA_DIR=  # optional; override where thumbnails, proxies, and video scenes are stored (default: ./data)
   ```

   (You can leave `HF_TOKEN` empty unless you need Hugging Face model access.)  
   Save and close.

---

### Step 1.7 – Run Database Migrations

From the project folder:

```bash
uv run --env-file .env alembic upgrade head
```

You should see migration messages; no errors means success.

---

### Step 1.8 – Create the Data Directory

```bash
mkdir -p data
```

The next step shares this folder with Machines B and C, so it must exist first. You can use a different location by setting `MEDIA_SEARCH_DATA_DIR` in your `.env` file.

---

### Step 1.9 – Share the Data Folder for Machines B and C

1. Open **System Settings** (or **System Preferences** on older macOS).
2. Go to **General → Sharing**.
3. Turn on **File Sharing**.
4. Click the **+** under Shared Folders and add the project's `data` folder (e.g. `~/media-search/data`).
5. Note the share name (often the folder name, e.g. `data`).
6. Click **Options** and ensure **Share files and folders using SMB** is checked.
7. Click **Done**.

---

### Step 1.10 – Start the Web App (keep this terminal open)

```bash
uv run --env-file .env uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

Open a browser and go to **http://localhost:8000/dashboard**. If you see the dashboard, the web app is running. Leave that terminal window open.

---

### Step 1.11 – Start the Proxy Worker (new terminal)

1. Open a **new** Terminal window/tab.
2. Run:

   ```bash
   cd ~/media-search
   uv run --env-file .env media-search proxy
   ```

   Leave it running.

---

### Step 1.12 – Start the Video Worker (another terminal)

1. Open another **new** Terminal window/tab.
2. Run:

   ```bash
   cd ~/media-search
   uv run --env-file .env media-search ai video
   ```

   Leave it running.

---

### Step 1.13 – Add a Library and Run a Scan

In a **new** terminal:

```bash
cd ~/media-search
uv run --env-file .env media-search library add "My Photos" /path/to/your/photos
```

Replace `/path/to/your/photos` with the real path (e.g. `/Users/you/Pictures`).

Then run a one-time scan (replace `my-photos` with the slug from the add command):

```bash
uv run --env-file .env media-search scan my-photos
```

---

## Part 2: Machines B and C (AI Workers Only)

### Step 2.1 – Install Tools (same as Machine A, except Docker)

On **each** of Machine B and Machine C:

1. Open Terminal.
2. Run:

   ```bash
   brew install git
   brew install python@3.11
   brew install uv
   brew install ffmpeg
   brew install vips
   ```

   (No Docker on B or C.)

---

### Step 2.2 – Clone the Project

On **each** of B and C:

```bash
cd ~
git clone https://github.com/YOUR_USERNAME/media-search.git
cd media-search
```

---

### Step 2.3 – Install Python Dependencies

On **each** of B and C:

```bash
uv sync
```

---

### Step 2.4 – Mount Machine A's Data Folder

1. In **Finder**, press **Cmd+K** (or Go → Connect to Server).
2. Enter: `smb://MACHINE-A-IP/data`  
   (Example: `smb://192.168.1.10/data`)
3. Click **Connect** and enter Machine A's username/password if asked.
4. Once the share appears (it may show as a disk), note where it's mounted—often `/Volumes/data`.
5. Do this on both B and C.

---

### Step 2.5 – Create the `.env` File on B and C

1. Copy the example:

   ```bash
   cp .env.example .env
   ```

2. Edit it:

   ```bash
   open -e .env
   ```

3. Put in (replace `MACHINE-A-IP` with Machine A's IP address or hostname, e.g. `192.168.1.10`):

   ```
   DATABASE_URL=postgresql+psycopg2://media_search:media_search@MACHINE-A-IP:5432/media_search
   MEDIA_SEARCH_DATA_DIR=/Volumes/data
   HF_TOKEN=
   ```

   Use whatever mount path you saw (e.g. `/Volumes/data`).  
   Save and close.

---

### Step 2.6 – Start the AI Worker on B and C

On **each** of B and C, in a new terminal:

```bash
cd ~/media-search
uv run --env-file .env media-search ai start
```

Leave it running.

---

## Part 3: Verify Everything Works

- On Machine A: open **http://localhost:8000/dashboard**
- From another machine on the network: **http://MACHINE-A-IP:8000/dashboard**
- In the dashboard, check **System Status**—you should see all workers (proxy, video, AI on B and C).

---

## Summary Checklist

| Machine | What Runs |
|---------|-----------|
| **A** | Docker (Postgres), web app, proxy worker, video worker, data directory, scanners |
| **B** | AI worker only |
| **C** | AI worker only |

---

## Quick Reference

**Machine A – .env:**
```
DATABASE_URL=postgresql+psycopg2://media_search:media_search@localhost:5432/media_search
```

**Machines B & C – .env:**
```
DATABASE_URL=postgresql+psycopg2://media_search:media_search@MACHINE-A-IP:5432/media_search
MEDIA_SEARCH_DATA_DIR=/Volumes/data
```

**Finding Machine A's IP:** On Machine A, run `ipconfig getifaddr en0` in Terminal (or `ifconfig` and look for the IP on `en0`).
