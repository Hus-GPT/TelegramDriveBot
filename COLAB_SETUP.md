# TelegramDriveBot Setup on Google Colab

This guide describes how to run TelegramDriveBot inside a Google Colab notebook session.

## One-Cell Startup

Paste and run the following in a single Colab cell:

```python
import os
from google.colab import drive

# 1. Mount Google Drive
drive.mount('/content/drive')

# 2. Clone repository if not present
if not os.path.exists('/content/TelegramDriveBot'):
    !git clone https://github.com/Hus-GPT/TelegramDriveBot.git /content/TelegramDriveBot

%cd /content/TelegramDriveBot
!pip install -r requirements.txt

# 3. Launch Bot
!python -m app.colab_start
```

## Secrets Configuration

In the left sidebar of Google Colab, open **Secrets** (key icon) and set:

* `TELEGRAM_BOT_TOKEN`: The API token from `@BotFather`.
* `OWNER_ID`: Your numerical Telegram user ID (from `@userinfobot`).

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
  * While `os.replace` operates atomically on local POSIX filesystems, FUSE remote mounts may implement file renaming via multi-step API calls.
  * To guarantee safety across network drops or Colab kernel deaths on FUSE mounts, the engine employs a defense-in-depth model:
    1. Writes to a hidden job-tagged file (`.part_<job_id>_*`).
    2. Recalculates full SHA-256 and byte-size on the destination `.part` file.
    3. Promotes the `.part` file to the final destination path via `os.replace`.
    4. On restart, startup recovery cleans orphan `.part_<job_id>_*` files and executes duplicate content matching (SHA-256 + size) against the final destination. If an interrupted transfer completed its destination write before state was saved, duplicate recovery marks the job completed without re-copying or creating duplicate collision files.
