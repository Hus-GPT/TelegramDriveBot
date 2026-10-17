"""Google Colab startup and orchestration script."""

import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path

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

    inbox_dir = Path(os.path.dirname(config.STATE_PATH)) / "colab_link_inbox"
    inbox_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Configuration validated.")
    logger.info("• Drive Destination: %s", config.DRIVE_DESTINATION)
    logger.info("• Staging Directory: %s", config.LOCAL_STAGING_DIR)
    logger.info("• State Storage: %s", config.STATE_PATH)
    logger.info("• Colab link inbox: %s", inbox_dir)
    logger.info("• Authorized Owner ID: %d", config.OWNER_ID)

    from app.bot import TelegramDriveBotApp
    from app.transfer import (
        NonRetryableTransferError,
        extract_filename_from_url,
        sanitize_url_for_logging,
        validate_url_security,
    )

    bot_app = TelegramDriveBotApp(config)
    total_loaded = len(bot_app.state.all_jobs())
    logger.info("StateStore loaded %d historical jobs.", total_loaded)

    bot_app.restore_unfinished()
    queued_count = bot_app.queue.qsize()
    logger.info("Recovery completed. %d unfinished jobs reconstructed in queue.", queued_count)

    app = bot_app.build_application()

    async def poll_colab_link_inbox():
        """Turn atomically submitted Colab UI link files into normal queued jobs."""
        while True:
            try:
                for entry_path in sorted(inbox_dir.glob("*.json")):
                    try:
                        with entry_path.open("r", encoding="utf-8") as stream:
                            entry = json.load(stream)
                        url = str(entry.get("url", "")).strip()
                        if not url:
                            raise ValueError("The submitted link is empty.")
                        validate_url_security(url)
                        job_id = entry_path.stem[:8] or uuid.uuid4().hex[:8]
                        filename = extract_filename_from_url(url, f"file_{job_id}.bin")
                        job = bot_app.state.add_job(
                            job_id=job_id,
                            source_type="direct_url",
                            filename=filename,
                            chat_id=config.OWNER_ID,
                            user_id=config.OWNER_ID,
                            source_url=url,
                        )
                        if job.get("status") == "queued":
                            await bot_app.safe_enqueue_job(job)
                        entry_path.unlink(missing_ok=True)
                        logger.info(
                            "[job=%s] Accepted link from Colab paste panel: %s",
                            job_id,
                            sanitize_url_for_logging(url),
                        )
                    except NonRetryableTransferError as exc:
                        logger.warning("Rejected link submitted through Colab paste panel: %s", exc)
                        entry_path.unlink(missing_ok=True)
                    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                        logger.warning("Could not process Colab link inbox item %s: %s", entry_path.name, exc)
                        # Leave malformed/transient files for inspection or retry.
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error while polling the Colab link inbox.")
            await asyncio.sleep(2)

    async def post_init(application):
        bot_app.worker_task = asyncio.create_task(bot_app.worker_loop())
        bot_app.colab_inbox_task = asyncio.create_task(poll_colab_link_inbox())
        logger.info("Single-worker queue and Colab link inbox started.")

    app.post_init = post_init

    logger.info("Starting Telegram polling mode...")
    app.run_polling()


if __name__ == "__main__":
    main()
