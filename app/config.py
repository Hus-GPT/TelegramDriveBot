from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Config:
    telegram_token: str
    owner_id: int
    drive_destination: str
    state_file: str
    temp_root: str
    max_retries: int = 3


def load_config() -> Config:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    owner = os.getenv("OWNER_TELEGRAM_ID", "").strip()
    destination = os.getenv(
        "DRIVE_DESTINATION",
        "/content/drive/MyDrive/TelegramDriveBot",
    ).strip()

    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured.")
    if not owner.isdigit():
        raise RuntimeError("OWNER_TELEGRAM_ID must be a numeric Telegram user ID.")

    return Config(
        telegram_token=token,
        owner_id=int(owner),
        drive_destination=destination,
        state_file=os.path.join(destination, ".telegramdrive_state.json"),
        temp_root="/content/TelegramDriveBot_tmp",
    )
