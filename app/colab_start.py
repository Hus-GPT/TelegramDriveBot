"""Google Colab startup and orchestration script."""

import logging
import os
import sys

from app.config import Config

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("colab_start")


def main() -> None:
    logger.info("Initializing TelegramDriveBot...")

    try:
        config = Config.from_env()
    except Exception as exc:
        logger.error("Configuration failed: %s", exc)
        sys.exit(1)

    # Check Drive destination
    if not os.path.exists("/content/drive/MyDrive"):
        logger.warning(
            "Drive does not appear to be mounted at /content/drive/MyDrive! "
            "Please call drive.mount('/content/drive') first if running in Colab."
        )

    try:
        os.makedirs(config.DRIVE_DESTINATION, exist_ok=True)
        os.makedirs(config.LOCAL_STAGING_DIR, exist_ok=True)
    except Exception as exc:
        logger.error("Failed to prepare directories: %s", exc)
        sys.exit(1)

    logger.info("Configuration validated.")
    logger.info("• Drive Destination: %s", config.DRIVE_DESTINATION)
    logger.info("• Staging Directory: %s", config.LOCAL_STAGING_DIR)
    logger.info("• State Storage: %s", config.STATE_PATH)
    logger.info("• Authorized Owner ID: %d", config.OWNER_ID)

    from app.bot import TelegramDriveBotApp

    bot_app = TelegramDriveBotApp(config)
    total_loaded = len(bot_app.state.all_jobs())
    logger.info("StateStore loaded %d historical jobs.", total_loaded)

    bot_app.restore_unfinished()
    queued_count = bot_app.queue.qsize()
    logger.info("Recovery completed. %d unfinished jobs reconstructed in queue.", queued_count)

    app = bot_app.build_application()

    # Start single async worker alongside Telegram polling
    async def post_init(application):
        import asyncio
        bot_app.worker_task = asyncio.create_task(bot_app.worker_loop())
        logger.info("Single-worker queue processing loop started.")

    app.post_init = post_init

    logger.info("Starting Telegram polling mode...")
    app.run_polling()


if __name__ == "__main__":
    main()
