"""Configuration management for TelegramDriveBot."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _get_val(key: str, default: str = "") -> str:
    """Read value from environment or Colab secrets."""
    val = os.environ.get(key, "").strip()
    if not val:
        try:
            from google.colab import userdata
            val = str(userdata.get(key) or "").strip()
        except Exception:
            pass
    return val or default


@dataclass(frozen=True)
class Config:
    TELEGRAM_BOT_TOKEN: str
    OWNER_ID: int
    DRIVE_DESTINATION: str = "/content/drive/MyDrive/TelegramDriveBot"
    LOCAL_STAGING_DIR: str = "/tmp/telegram_drive_staging"
    STATE_PATH: str = "/content/drive/MyDrive/TelegramDriveBot/.state/state.json"
    MAX_RETRIES: int = 3
    FILE2URL_BOT_USERNAME: str = "File2url_rbot"
    FILE2URL_TIMEOUT: int = 120
    PROGRESS_INTERVAL: float = 3.0  # seconds between Telegram message progress edits
    STATUS_HISTORY_COUNT: int = 5   # number of recent jobs to show in /status and /history

    @classmethod
    def from_env(cls) -> Config:
        token = _get_val("TELEGRAM_BOT_TOKEN")
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required in environment or Colab secrets.")

        owner_raw = _get_val("OWNER_ID")
        if not owner_raw or not owner_raw.isdigit():
            raise ValueError("OWNER_ID is required and must be an integer ID.")

        drive_dest = _get_val("DRIVE_DESTINATION", "/content/drive/MyDrive/TelegramDriveBot")
        state_path = _get_val("STATE_PATH", os.path.join(drive_dest, ".state", "state.json"))
        staging_dir = _get_val("LOCAL_STAGING_DIR", "/tmp/telegram_drive_staging")

        return cls(
            TELEGRAM_BOT_TOKEN=token,
            OWNER_ID=int(owner_raw),
            DRIVE_DESTINATION=drive_dest,
            LOCAL_STAGING_DIR=staging_dir,
            STATE_PATH=state_path,
            MAX_RETRIES=int(_get_val("MAX_RETRIES", "3")),
            FILE2URL_BOT_USERNAME=_get_val("FILE2URL_BOT_USERNAME", "File2url_rbot"),
            FILE2URL_TIMEOUT=int(_get_val("FILE2URL_TIMEOUT", "120")),
            PROGRESS_INTERVAL=float(_get_val("PROGRESS_INTERVAL", "3.0")),
            STATUS_HISTORY_COUNT=int(_get_val("STATUS_HISTORY_COUNT", "5")),
        )
