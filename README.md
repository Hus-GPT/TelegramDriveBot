# TelegramDriveBot

A lightweight, reliable Telegram bot designed to safely stream direct downloads and media files directly into Google Drive from within a Google Colab session.

## Core Capabilities (Milestones 1–6 & Hardened Audit)

* **Hardened Direct URL Streaming:** Streams downloads directly into local Colab staging in 1 MB chunks without loading entire files into RAM.
* **Redirect SSRF & DNS Defense:** Validates URL scheme, DNS resolution, and IP ranges on the initial URL and on EVERY redirect target, strictly blocking loopback, link-local, private subnets, and cloud metadata services.
* **Exponential Backoff:** Retries transient failures with configurable exponential backoff and jitter without hanging cancellation.
* **Google Drive Pre-Validation & Diagnostics:** Validates Drive mount status and writability before downloading; provides `/storage` diagnostics.
* **Advanced Job Manager:** Tracks complete lifecycle timestamps (`created_at`, `started_at`, `completed_at`), prevents duplicate enqueueing, and supports per-job inspection via `/status <job_id>` or `/status_<job_id>`.
* **Mobile-First Owner UX:** Intuitive inline dashboard, two-step confirmation prompts for destructive actions, state-aware action buttons, and stale button protection.
* **Telegram Media Acquisition:** Handles Telegram native documents, audio, and video up to 20MB directly via Bot API, and forwards larger media through File2URL.
* **Operational Bot UX & Controls:** Full owner-only control via `/start`, `/status` (overview or per-job), `/storage`, `/history`, `/cancel`, `/retry`, and `/help`.
* **Live Throttled Progress:** Live percentage, transfer speed, and ETA updates debounced to prevent Telegram rate-limiting.
* **Failure Isolation:** Telegram UI edit failures never crash or interrupt ongoing file transfers.
* **Integrity Verification:** End-to-end SHA-256 and byte-size verification before and after staging to Google Drive.
* **Atomic State & Recovery:** Deterministic JSON state with atomic file replacements (`os.replace`) preserving transfer tracking across Colab restarts.
* **Duplicate & Collision Safety:** Identical SHA-256 matches are recognized as duplicates; same-name differing-content files receive automated collision suffixes (`file (1).ext`) preventing data loss.
* **Clean Cleanup:** Guarantees removal of temporary staging chunks upon completion, failure, or cancellation.

## Architecture

```text
Telegram Message (Owner Only)
    │
    ▼
URL / Media Identification & Initial SSRF Validation
    │
    ▼
Job Creation (StateStore: queued, timestamps tracked)
    │
    ▼
Single-Worker Async Queue (Duplicate Enqueue Guard)
    │
    ▼
Worker Thread Pool:
  1. Status -> "downloading" (Pre-validates Drive destination, sets started_at)
  2. Streamed download in 1 MB chunks (Per-redirect SSRF guard, exponential backoff, UI throttled speed/ETA)
  3. Pre-transfer Source Verification (SHA-256 + Byte Size)
  4. Status -> "downloaded"
  5. Check existing target on Drive:
     ├─ Exact Match (SHA-256 + Size) -> Status: "completed" (duplicate_skipped)
     └─ Name collision -> Target renamed to "file (N).ext"
  6. Copy to Drive staging: ".part_<job_id>_<filename>"
  7. Post-transfer Destination Verification (SHA-256 + Byte Size match)
  8. Destination promotion of ".part_<job_id>_<filename>" -> Target Path via os.replace
  9. Status -> "completed" (sets completed_at, UI: final delivery receipt + action button)
 10. Guaranteed cleanup of staging file
```

## Running Tests

Automated tests run locally without requiring Telegram tokens or real Google Drive mounts:

```bash
pytest -q
```

Actual test suite inventory: exactly 54 top-level test functions in `tests/test_core.py`.
