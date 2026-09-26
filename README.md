# TelegramDriveBot

A lightweight, reliable Telegram bot designed to safely stream direct downloads and media files directly into Google Drive from within a Google Colab session.

## Core Capabilities (Milestone 1)

* **Direct URL Streaming:** Streams downloads directly into local Colab staging in incremental chunks without loading entire files into RAM.
* **Telegram Media Acquisition:** Handles Telegram native documents, audio, and video up to 20MB directly via Bot API, and forwards larger media through File2URL.
* **Integrity Verification:** End-to-end SHA-256 and byte-size verification before and after finalization to Google Drive.
* **Atomic State & Recovery:** Deterministic JSON state with atomic file replacements (`os.replace`) preserving transfer tracking across Colab restarts.
* **Duplicate & Collision Safety:** Identical SHA-256 matches are recognized as duplicates; same-name differing-content files receive automated collision suffixes (`file (1).ext`) preventing data loss.
* **Clean Cleanup:** Guarantees removal of temporary staging chunks upon completion, failure, or cancellation.

## Architecture

```text
Telegram Message (Owner Only)
    │
    ▼
URL / Media Identification
    │
    ▼
Job Creation (StateStore: queued)
    │
    ▼
Single-Worker Async Queue
    │
    ▼
Worker Thread Pool:
  1. Status -> "downloading"
  2. Streamed download in 1 MB chunks to /tmp/... (Staging)
  3. Pre-transfer Source Verification (SHA-256 + Byte Size)
  4. Status -> "downloaded"
  5. Check existing target on Drive:
     ├─ Exact Match (SHA-256 + Size) -> Status: "completed" (duplicate_skipped)
     └─ Name collision -> Target renamed to "file (N).ext"
  6. Copy to Drive staging: ".part_<job_id>_<filename>"
  7. Post-transfer Destination Verification (SHA-256 + Byte Size match)
  8. Atomic rename of ".part_<job_id>_<filename>" -> Target Path
  9. Status -> "completed"
 10. Guaranteed cleanup of staging file
```

## Running Tests

Automated tests run locally without requiring Telegram tokens or real Google Drive mounts:

```bash
pytest -q
```

Actual test suite inventory: exactly 14 top-level test functions in `tests/test_core.py`.
