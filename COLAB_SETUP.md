# TelegramDriveBot Setup on Google Colab

This guide describes how to run TelegramDriveBot inside a Google Colab notebook session.

## One-Cell Startup (With Safe Auto-Update)

Paste and run the following in a single Colab cell:

```python
import os
import subprocess
from google.colab import drive

# 1. Mount Google Drive
drive.mount('/content/drive')

# 2. Clone repository or perform safe fast-forward update
repo_dir = '/content/TelegramDriveBot'
repo_url = 'https://github.com/Hus-GPT/TelegramDriveBot.git'

if not os.path.exists(repo_dir):
    print("Cloning repository...")
    subprocess.run(['git', 'clone', repo_url, repo_dir], check=True)
else:
    print("Updating existing repository (fast-forward only)...")
    try:
        subprocess.run(['git', '-C', repo_dir, 'fetch', 'origin'], check=True)
        # Verify no uncommitted local changes before fast-forwarding
        status = subprocess.run(['git', '-C', repo_dir, 'status', '--porcelain'], capture_output=True, text=True, check=True)
        if not status.stdout.strip():
            subprocess.run(['git', '-C', repo_dir, 'merge', '--ff-only', 'origin/main'], check=True)
            print("Successfully updated to latest origin/main.")
        else:
            print("Notice: Local modifications detected in /content/TelegramDriveBot; skipping auto-merge to protect edits.")
    except Exception as exc:
        print(f"Warning: Could not auto-update repository: {exc}")

%cd /content/TelegramDriveBot
!pip install -r requirements.txt

# 3. Launch Bot
!python -m app.colab_start
```

## Secrets Configuration

In the left sidebar of Google Colab, open **Secrets** (key icon) and set:

* `TELEGRAM_BOT_TOKEN`: The API token from `@BotFather`.
* `OWNER_ID`: Your numerical Telegram user ID (from `@userinfobot`).

## Operational Commands & Mobile UX (Milestones 2–6)

* `/start` : Mobile dashboard with Quick Inline Controls (Status, History, Storage, Cancel, Help).
* `/status` : View real-time bot state, active job, queue count, and transfer metrics.
* `/status <job_id>` or `/status_<job_id>` : Inspect full lifecycle metadata, timestamps, hash, and interactive action buttons for a specific job.
* `/storage` : View storage diagnostics (Google Drive destination writability, mount status, local staging disk capacity).
* `/history` : Review recent completed, failed, and cancelled transfer history chronologically with `/status_<id>` links.
* `/cancel` : Safely cancel the currently active transfer, or specify a job ID via `/cancel <job_id>`.
* `/retry <job_id>` : Requeue failed or cancelled transfers with original metadata (increments retry count).
* `/help` : View available commands and operational notes.

## Interactive Owner Control Surface (Milestone 6)

* **Mobile Quick Controls:** The `/start` command provides a clean Inline Keyboard (`📊 الحالة`, `📜 السجل`, `💾 التخزين`, `❌ إلغاء الجارية`, `📖 المساعدة`).
* **Active Cancel Confirmation:** Tapping `[❌ إلغاء الجارية]` displays a one-tap confirmation prompt (`نعم` / `تراجع`), preventing accidental cancellations.
* **State-Aware Job Controls:** Dynamic inline buttons on job inspection and lifecycle messages:
  * `queued` / `downloading` $\to$ `[❌ تأكيد الإلغاء]`
  * `failed` / `cancelled` $\to$ `[🔁 إعادة المحاولة]`
  * `completed` $\to$ `[ℹ️ معلومات تفصيلية]`
* **Two-Step Confirmation UX:** Destructive actions (`cancel`, `retry`) display immediate one-tap confirmation prompts (`نعم` / `تراجع`), preventing accidental taps on mobile.
* **Stale Button Protection:** Callbacks validate live status in `StateStore` before executing. Tapping obsolete buttons (e.g. Cancel on a completed job) displays an alert without corrupting state.
* **Complete Authorization Guard:** All callback queries, commands, and messages verify `update.effective_user.id == config.OWNER_ID` upfront.

## Hardened Download Engine & Redirect SSRF Defense (Milestone 3 & Global Audit)

* **Step-by-Step Redirect SSRF Validation:** Transfers follow redirects manually, re-validating DNS resolution and IP addresses on EVERY redirect target before sending requests. Blocks localhost, 127.0.0.1, private IP ranges (10.x, 172.16.x, 192.168.x), link-local, multicast, and cloud metadata endpoints (`metadata.google.internal`).
* **Exponential Backoff with Jitter:** Retries transient failures with configurable exponential backoff (`RETRY_INITIAL_DELAY` to `RETRY_MAX_DELAY`), respecting user cancellation between retries.
* **Resource Cleanup:** Guaranteed context manager and explicit `.close()` calls on requests response and session across success, error, and cancellation paths.
* **Logging & Error Sanitization:** Eliminates leakage of authorization headers, tokens, or query strings in logs (`sanitize_url_for_logging`) and user messages (`humanize_error`).
* **Clean Retry Semantics (No Resume):** Interrupted downloads restart clean streams from byte 0 to guarantee cryptographic integrity.

## Google Drive Storage & Finalization (Milestone 4)

* **Destination Pre-Validation:** Before starting network streams or finalization, `validate_destination_directory` verifies that the Drive folder exists, is a valid directory, and passes a physical write/unlink probe. If Google Drive is unmounted (`/content/drive/MyDrive` missing), the job fails fast with a clear instruction to mount Drive.
* **Storage Intelligence (`/storage`):** Reports whether the Drive mount is ready and writable, alongside free space on the local staging filesystem (`/tmp`). Note: Google Drive cloud quota is managed by Google Workspace and is distinct from local filesystem storage.
* **Two-Point Cryptographic Verification:** Incremental SHA-256 and byte-size matching computed both on local staging and upon destination `.part_<job_id>_*` landing before destination promotion via `os.replace`.
* **Duplicate & Collision Safety:** Exact SHA-256 matches skip redundant copying (`duplicate_skipped`); different content with identical names receives sequential collision suffixes (`file (1).ext`).
* **Scoped Orphan Cleanup:** Startup recovery removes only `.part_<job_id>_*` files whose associated job is terminal or absent from `StateStore`. Valid files and user folders are never touched.

## Advanced Job Manager & Lifecycle (Milestone 5)

* **Enforced Authoritative Transitions:** `queued` $\to$ `downloading` $\to$ `downloaded` $\to$ `verifying` $\to$ `completed` with strict rejection of illegal jumps.
* **Lifecycle Timestamps:** Automatically records `created_at`, `started_at`, and `completed_at` timestamps.
* **Queue Invariant & Duplicate Prevention:** `safe_enqueue_job` guarantees no job ID can enter the processing queue twice.
* **Safe State Retention:** Bounded history retention prunes only oldest terminal jobs while protecting all active/queued and recovery-critical jobs.
* **Structured Correlated Logging:** Every lifecycle milestone is tagged with `[job=<id>]` for streamlined log analysis.

## How Persistence & Recovery Work

* **State Storage:** Saved directly to your Google Drive at:
  `/content/drive/MyDrive/TelegramDriveBot/.state/state.json`
* **Colab Disconnects:**
  * When a session disconnects, files in `/tmp` disappear with the Colab VM.
  * On restart, `restore_unfinished()` cleans up leftover `.part_<job_id>_*` files on Drive, preserves intact staging files if Colab storage survived, and resets interrupted downloads safely to `queued`.
* **File2URL Forwarding:**
  * Telegram restricts Bot API downloads to 20MB. Media over 20MB is forwarded to `@File2url_rbot`.
  * The single-worker engine waits up to 120s for the external response. If the external bot fails or times out, the job fails cleanly without stalling subsequent transfers.
  * Invariant: exactly one File2URL exchange is active at any time; unsolicited or late messages are safely discarded. Note that `@File2url_rbot` does not echo private job tokens; correlation is achieved strictly via serialized single-worker execution.
* **Google Drive FUSE Filesystem Caveats:**
  * Google Drive mounted via `google.colab.drive.mount` utilizes a userspace FUSE filesystem layer over Google Drive APIs.
  * While `os.replace` operates atomically on local POSIX filesystems, FUSE remote mounts may implement file renaming via multi-step API calls. Therefore, universal transactional/atomic promotion guarantees cannot be assumed across FUSE mounts.
  * To guarantee safety across network drops or Colab kernel deaths on FUSE mounts, the engine employs a defense-in-depth model:
    1. Writes to a hidden job-tagged file (`.part_<job_id>_*`).
    2. Recalculates full SHA-256 and byte-size on the destination `.part` file.
    3. Promotes the `.part` file to the final destination path via `os.replace`.
    4. On restart, startup recovery cleans orphan `.part_<job_id>_*` files and executes duplicate content matching (SHA-256 + size) against the final destination. If an interrupted transfer completed its destination write before state was saved, duplicate recovery marks the job completed without re-copying or creating duplicate collision files.
