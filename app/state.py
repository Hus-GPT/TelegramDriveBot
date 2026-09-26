"""Persistent state management for TelegramDriveBot."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Authoritative job lifecycle states:
# queued -> downloading -> downloaded -> verifying -> completed
# Terminal states: completed, failed, cancelled
VALID_STATES = {
    "queued",
    "downloading",
    "downloaded",
    "verifying",
    "completed",
    "failed",
    "cancelled",
}

# Legal forward transitions
LEGAL_TRANSITIONS = {
    "queued": {"downloading", "cancelled", "failed"},
    "downloading": {"downloaded", "failed", "cancelled"},
    "downloaded": {"verifying", "failed", "cancelled"},
    "verifying": {"completed", "failed", "cancelled"},
    "completed": set(),
    "failed": {"queued"},       # for manual retry
    "cancelled": {"queued"},    # for manual retry
}


def now_utc_iso() -> str:
    """Return ISO 8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    """Manages thread-safe JSON-backed state for TelegramDriveBot.

    NOTE ON CONCURRENCY:
    This class employs an in-process threading.RLock for thread safety across
    worker and Telegram polling handler threads. This is NOT a distributed
    cluster lock across distinct Colab VM instances or multiple processes.
    """

    def __init__(self, state_path: str, max_history: int = 500):
        self.state_path = os.path.abspath(state_path)
        self.max_history = max(50, max_history)
        self._lock = threading.RLock()
        self.data: Dict[str, Any] = {
            "version": 2,
            "jobs": [],
            "stats": {"completed": 0, "failed": 0, "cancelled": 0},
        }
        self._load()

    def _load(self) -> None:
        """Load state safely, handling missing or malformed state files."""
        with self._lock:
            state_dir = os.path.dirname(self.state_path)
            try:
                os.makedirs(state_dir, exist_ok=True)
            except OSError as exc:
                logger.error("Failed to create state directory %s: %s", state_dir, exc)

            if not os.path.exists(self.state_path):
                logger.info("No state file at %s, initializing fresh state.", self.state_path)
                self._save()
                return

            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict) and "jobs" in raw and isinstance(raw["jobs"], list):
                    self.data = raw
                    if "stats" not in self.data or not isinstance(self.data["stats"], dict):
                        self.data["stats"] = {"completed": 0, "failed": 0, "cancelled": 0}
                    if "version" not in self.data:
                        self.data["version"] = 2
                else:
                    logger.warning("Malformed state file at %s (invalid schema). Creating backup.", self.state_path)
                    self._create_corrupt_backup()
                    self._save()
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("Failed to parse state file %s: %s. Creating backup.", self.state_path, exc)
                self._create_corrupt_backup()
                self._save()

    def _create_corrupt_backup(self) -> None:
        """Backup corrupt state file to avoid data destruction."""
        try:
            backup_path = f"{self.state_path}.corrupt.{int(datetime.now(timezone.utc).timestamp())}"
            if os.path.exists(self.state_path):
                os.replace(self.state_path, backup_path)
                logger.info("Backed up corrupted state to %s", backup_path)
        except OSError as exc:
            logger.error("Failed creating corrupt state backup: %s", exc)

    def _save(self) -> None:
        """Atomically write state using a temporary file in the same directory."""
        state_dir = os.path.dirname(self.state_path)
        os.makedirs(state_dir, exist_ok=True)

        # Enforce history limit to avoid unbounded memory / JSON growth
        jobs = self.data.get("jobs", [])
        if len(jobs) > self.max_history:
            terminal_jobs = [j for j in jobs if j.get("status") in {"completed", "failed", "cancelled"}]
            active_jobs = [j for j in jobs if j.get("status") not in {"completed", "failed", "cancelled"}]
            # Keep all active jobs + newest terminal jobs
            excess = len(jobs) - self.max_history
            if excess > 0 and terminal_jobs:
                pruned_terminal = terminal_jobs[excess:]
                self.data["jobs"] = active_jobs + pruned_terminal

        tmp_fd, tmp_path = tempfile.mkstemp(dir=state_dir, prefix="state_", suffix=".tmp")
        try:
            with open(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self.state_path)
        except Exception:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

    def add_job(
        self,
        job_id: str,
        source_type: str,
        filename: str,
        chat_id: int,
        user_id: int,
        source_url: Optional[str] = None,
        telegram_file_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            job = {
                "id": str(job_id),
                "source_type": source_type,
                "source_url": source_url,
                "telegram_file_id": telegram_file_id,
                "filename": filename,
                "chat_id": chat_id,
                "user_id": user_id,
                "status": "queued",
                "created_at": now_utc_iso(),
                "updated_at": now_utc_iso(),
                "retries": 0,
                "sha256": None,
                "size": None,
                "error": None,
                "destination_path": None,
                "temp_path": None,
                "recovery_from_status": None,
                "recovery_at": None,
            }
            self.data.setdefault("jobs", []).append(job)
            self._save()
            return dict(job)

    def update_job(self, job_id: str, **kwargs: Any) -> Optional[Dict[str, Any]]:
        with self._lock:
            for job in self.data.get("jobs", []):
                if job.get("id") == str(job_id):
                    new_status = kwargs.get("status")
                    if new_status and new_status != job.get("status"):
                        if new_status not in VALID_STATES:
                            logger.warning("Attempted invalid state transition to %s for job %s", new_status, job_id)
                        old_status = job.get("status")
                        # Validate transition if not an explicit reset/recovery
                        if "recovery_from_status" not in kwargs and new_status not in LEGAL_TRANSITIONS.get(old_status, set()):
                            logger.warning("Unorthodox state transition from %s to %s for job %s", old_status, new_status, job_id)

                        # Update stats on transition into terminal states
                        if new_status in {"completed", "failed", "cancelled"}:
                            stats = self.data.setdefault("stats", {"completed": 0, "failed": 0, "cancelled": 0})
                            stats[new_status] = stats.get(new_status, 0) + 1

                    job.update(kwargs)
                    job["updated_at"] = now_utc_iso()
                    self._save()
                    return dict(job)
            return None

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            for job in self.data.get("jobs", []):
                if job.get("id") == str(job_id):
                    return dict(job)
            return None

    def unfinished_jobs(self) -> List[Dict[str, Any]]:
        with self._lock:
            active_statuses = {"queued", "downloading", "downloaded", "verifying"}
            return [dict(j) for j in self.data.get("jobs", []) if j.get("status") in active_statuses]

    def all_jobs(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(j) for j in self.data.get("jobs", [])]

    def clear_active_for_recovery(self) -> None:
        """Utility for test isolation."""
        with self._lock:
            self.data["jobs"] = [j for j in self.data.get("jobs", []) if j.get("status") in {"completed", "failed", "cancelled"}]
            self._save()
