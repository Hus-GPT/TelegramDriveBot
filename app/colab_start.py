import os

from .bot import TelegramDriveBot
from .config import load_config
from .state import StateStore


def secret(name: str, default: str = "") -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    try:
        from google.colab import userdata
        return str(userdata.get(name) or default).strip()
    except Exception:
        return default


def main():
    try:
        from google.colab import drive
    except ImportError as exc:
        raise RuntimeError("هذا المشغل مخصص لبيئة Google Colab.") from exc

    drive.mount("/content/drive", force_remount=False)

    os.environ["TELEGRAM_BOT_TOKEN"] = secret("TELEGRAM_BOT_TOKEN")
    os.environ["OWNER_TELEGRAM_ID"] = secret("OWNER_TELEGRAM_ID")
    os.environ["DRIVE_DESTINATION"] = secret(
        "DRIVE_DESTINATION",
        "/content/drive/MyDrive/TelegramDriveBot",
    )
    os.environ["FILE2URL_BOT_USERNAME"] = secret(
        "FILE2URL_BOT_USERNAME",
        "@File2url_rbot",
    )

    config = load_config()
    os.makedirs(config.drive_destination, exist_ok=True)
    os.makedirs(config.temp_root, exist_ok=True)

    state = StateStore(config.state_file)
    bot = TelegramDriveBot(config, state)
    application = bot.build_application()

    print("🟢 TelegramDriveBot starting...")
    print(f"📁 Drive destination: {config.drive_destination}")
    print("🔐 Owner-only access enabled")
    application.run_polling(allowed_updates=None, drop_pending_updates=False)


if __name__ == "__main__":
    main()
