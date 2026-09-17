import json
import os
import threading
from datetime import datetime, timezone


class StateStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self.data = {"jobs": [], "history": []}
        self.load()

    def load(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            self.save()
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                self.data = loaded
                self.data.setdefault("jobs", [])
                self.data.setdefault("history", [])
        except (OSError, json.JSONDecodeError):
            self.data = {"jobs": [], "history": []}
            self.save()

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with self._lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)

    def add_job(self, job: dict):
        self.data["jobs"].append(job)
        self.save()

    def update_job(self, job_id: str, **changes):
        for job in self.data["jobs"]:
            if job.get("id") == job_id:
                job.update(changes)
                job["updated_at"] = now()
                self.save()
                return job
        return None

    def add_history(self, record: dict):
        self.data["history"].append(record)
        self.data["history"] = self.data["history"][-1000:]
        self.save()

    def unfinished_jobs(self):
        return [
            j for j in self.data["jobs"]
            if j.get("status") in {"queued", "running", "verifying"}
        ]


def now():
    return datetime.now(timezone.utc).isoformat()
