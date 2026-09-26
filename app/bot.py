"""Telegram handlers, worker loop, and lifecycle management for TelegramDriveBot."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import uuid
from typing import Any, Dict, Optional

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.config import Config
from app.state import StateStore
from app.transfer import (
    NonRetryableTransferError,
    RetryableTransferError,
    TransferError,
    download_url,
    extract_filename_from_url,
    finalize_to_drive,
    safe_filename,
)

logger = logging.getLogger(__name__)

URL_REGEX = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


class File2URLProvider:
    """Isolated provider for large Telegram files via Bot-to-Bot forward."""

    def __init__(self, bot_username: str, timeout: int = 120):
        self.bot_username = bot_username.lstrip("@")
        self.timeout = timeout
        self._waiters: Dict[str, asyncio.Future[str]] = {}
        self._lock = asyncio.Lock()

    async def register_waiter(self, job_id: str) -> asyncio.Future[str]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        async with self._lock:
            self._waiters[job_id] = future
        return future

    async def complete_waiter(self, url: str) -> bool:
        async with self._lock:
            # Complete the earliest active waiter FIFO
            for job_id, fut in list(self._waiters.items()):
                if not fut.done():
                    fut.set_result(url)
                    del self._waiters[job_id]
                    return True
        return False

    async def cancel_waiter(self, job_id: str) -> None:
        async with self._lock:
            fut = self._waiters.pop(job_id, None)
            if fut and not fut.done():
                fut.cancel()


class TelegramDriveBotApp:
    def __init__(self, config: Config):
        self.config = config
        self.state = StateStore(config.STATE_PATH)
        self.file2url = File2URLProvider(config.FILE2URL_BOT_USERNAME, timeout=config.FILE2URL_TIMEOUT)
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.cancel_events: Dict[str, threading.Event] = {}
        self.worker_task: Optional[asyncio.Task[None]] = None
        self.application: Optional[Application] = None

    # Authorization
    def is_authorized(self, update: Update) -> bool:
        user = update.effective_user
        if not user:
            return False
        return int(user.id) == int(self.config.OWNER_ID)

    async def check_auth_or_reject(self, update: Update) -> bool:
        if not self.is_authorized(update):
            if update.effective_message:
                await update.effective_message.reply_text("⛔ عذراً، هذا البوت خاص وغير مصرح لك باستخدامه.")
            return False
        return True

    # Telegram Handlers
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        msg = (
            "👋 مرحباً بك في TelegramDriveBot\n\n"
            "الأوامر المدعومة:\n"
            "/status - عرض حالة النظام والمهام الجارية\n"
            "/retry <job_id> - إعادة تشغيل مهمة فاشلة\n"
            "/cancel <job_id> - إلغاء مهمة جارية\n"
            "/help - مساعدة\n\n"
            "أرسل أي ملف، مستند، فيديو، أو رابط مباشر وسأقوم بحفظه في Google Drive بأمان."
        )
        await update.effective_message.reply_text(msg)

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        await self.cmd_start(update, context)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        jobs = self.state.all_jobs()
        active = [j for j in jobs if j.get("status") in {"queued", "downloading", "downloaded", "verifying"}]
        stats = self.state.data.get("stats", {})

        report = (
            f"📊 حالة النظام:\n"
            f"• المهام النشطة: {len(active)}\n"
            f"• المكتملة: {stats.get('completed', 0)}\n"
            f"• الفاشلة: {stats.get('failed', 0)}\n"
            f"• الملغاة: {stats.get('cancelled', 0)}\n\n"
        )
        if active:
            report += "المهام الجارية حالياً:\n"
            for j in active[:5]:
                report += f"- [{j.get('id')}] {j.get('filename')} ({j.get('status')})\n"
        else:
            report += "لا توجد مهام نشطة حالياً."

        await update.effective_message.reply_text(report)

    async def cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        args = context.args or []
        if not args:
            await update.effective_message.reply_text("يرجى تحديد معرّف المهمة: /cancel <job_id>")
            return
        job_id = args[0].strip()
        job = self.state.get_job(job_id)
        if not job:
            await update.effective_message.reply_text(f"لم يتم العثور على المهمة {job_id}")
            return

        if job.get("status") in {"completed", "failed", "cancelled"}:
            await update.effective_message.reply_text(f"المهمة في حالة نهائية بالفعل ({job.get('status')}).")
            return

        # Trigger cancellation event
        event = self.cancel_events.get(job_id)
        if event:
            event.set()
        await self.file2url.cancel_waiter(job_id)
        self.state.update_job(job_id, status="cancelled", error="تم الإلغاء بواسطة المستخدم")
        await update.effective_message.reply_text(f"✅ تم إرسال أمر الإلغاء للمهمة {job_id}")

    async def cmd_retry(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        args = context.args or []
        if not args:
            await update.effective_message.reply_text("يرجى تحديد معرّف المهمة: /retry <job_id>")
            return
        job_id = args[0].strip()
        job = self.state.get_job(job_id)
        if not job:
            await update.effective_message.reply_text(f"المهمة {job_id} غير موجودة.")
            return

        if job.get("status") not in {"failed", "cancelled"}:
            await update.effective_message.reply_text(f"المهمة في حالة ({job.get('status')}) ولا يمكن إعادة تشغيلها.")
            return

        self.state.update_job(job_id, status="queued", retries=0, error=None)
        await self.queue.put(self.state.get_job(job_id))
        await update.effective_message.reply_text(f"🔄 تمت إعادة جدولة المهمة {job_id} في صف الانتظار.")

    # Media and Link Handling
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return

        # File2URL response detection: if received from external bot
        sender_username = (update.effective_user.username or "") if update.effective_user else ""
        if sender_username.lower() == self.file2url.bot_username.lower():
            text = update.effective_message.text or ""
            match = URL_REGEX.search(text)
            if match:
                completed = await self.file2url.complete_waiter(match.group(0))
                if completed:
                    return

        if not await self.check_auth_or_reject(update):
            return

        message = update.effective_message
        text = message.text or message.caption or ""

        # Direct URL check
        url_match = URL_REGEX.search(text)
        if url_match:
            url = url_match.group(0)
            job_id = str(uuid.uuid4())[:8]
            fname = extract_filename_from_url(url, f"file_{job_id}.bin")
            job = self.state.add_job(
                job_id=job_id,
                source_type="direct_url",
                filename=fname,
                chat_id=message.chat_id,
                user_id=message.from_user.id,
                source_url=url,
            )
            await self.queue.put(job)
            await message.reply_text(f"📥 تم استلام الرابط وإضافته لصف الانتظار:\n• المعرّف: `{job_id}`\n• الملف: `{fname}`", parse_mode="Markdown")
            return

        # Document or Video attachment
        media = message.document or message.video or message.audio
        if media:
            job_id = str(uuid.uuid4())[:8]
            file_name = getattr(media, "file_name", None) or f"media_{job_id}.bin"
            file_name = safe_filename(file_name)
            file_size = getattr(media, "file_size", 0)

            # Check Telegram 20MB limit
            source_type = "telegram_media" if file_size <= 20 * 1024 * 1024 else "telegram_large"
            job = self.state.add_job(
                job_id=job_id,
                source_type=source_type,
                filename=file_name,
                chat_id=message.chat_id,
                user_id=message.from_user.id,
                telegram_file_id=media.file_id,
            )
            await self.queue.put(job)
            await message.reply_text(f"📦 تم استلام الملف:\n• المعرّف: `{job_id}`\n• الاسم: `{file_name}`\n• الحجم: {round(file_size / (1024*1024), 2)} MB", parse_mode="Markdown")
            return

    # Worker Loop
    async def worker_loop(self) -> None:
        logger.info("Worker loop started.")
        while True:
            try:
                job = await self.queue.get()
                job_id = job["id"]
                self.active_jobs[job_id] = job
                cancel_event = threading.Event()
                self.cancel_events[job_id] = cancel_event

                try:
                    await self.process_job(job, cancel_event)
                except Exception as exc:
                    logger.exception("Error processing job %s: %s", job_id, exc)
                finally:
                    self.active_jobs.pop(job_id, None)
                    self.cancel_events.pop(job_id, None)
                    self.queue.task_done()
            except asyncio.CancelledError:
                logger.info("Worker loop cancelled.")
                break
            except Exception as exc:
                logger.exception("Unexpected error in worker loop: %s", exc)

    async def process_job(self, job: Dict[str, Any], cancel_event: threading.Event) -> None:
        job_id = job["id"]
        chat_id = job["chat_id"]
        source_type = job["source_type"]
        temp_dir = self.config.LOCAL_STAGING_DIR
        dest_dir = self.config.DRIVE_DESTINATION

        if cancel_event.is_set():
            self.state.update_job(job_id, status="cancelled")
            return

        self.state.update_job(job_id, status="downloading")
        temp_file_path: Optional[str] = None

        try:
            # 1. Obtain URL
            download_link: Optional[str] = None

            if source_type == "direct_url":
                download_link = job.get("source_url")
            elif source_type == "telegram_media":
                tg_file = await self.application.bot.get_file(job["telegram_file_id"])
                download_link = tg_file.file_path
            elif source_type == "telegram_large":
                # Forward to File2URL if configured
                waiter = await self.file2url.register_waiter(job_id)
                try:
                    # Forward message logic
                    await self.application.bot.forward_message(
                        chat_id=f"@{self.file2url.bot_username}",
                        from_chat_id=chat_id,
                        message_id=job.get("telegram_message_id", 0),
                    )
                    download_link = await asyncio.wait_for(waiter, timeout=self.file2url.timeout)
                except asyncio.TimeoutError:
                    raise NonRetryableTransferError("انتهت مهلة انتظار الرابط من بوت File2URL.")
                except Exception as exc:
                    raise NonRetryableTransferError(f"فشل التحويل عبر File2URL: {exc}")

            if not download_link:
                raise NonRetryableTransferError("تعذر تحديد رابط التحميل للمهمة.")

            # 2. Download to local staging in separate thread
            loop = asyncio.get_running_loop()
            temp_file_path, resolved_name, size = await loop.run_in_executor(
                None,
                download_url,
                download_link,
                temp_dir,
                job.get("filename"),
                cancel_event,
                self.config.MAX_RETRIES,
            )

            self.state.update_job(job_id, status="downloaded", temp_path=temp_file_path, filename=resolved_name, size=size)

            # 3. Finalize to Drive
            result = await loop.run_in_executor(
                None,
                finalize_to_drive,
                temp_file_path,
                resolved_name,
                dest_dir,
                self.state,
                job_id,
                cancel_event,
            )

            # 4. Notify owner
            dup_msg = " (تم تخطي النسخ لوجود ملف مطابق تماماً)" if result.is_duplicate else ""
            collision_msg = " (تمت إعادة التسمية لمنع الاستبدال)" if result.action == "collision_renamed" else ""
            msg = (
                f"✅ اكتمل حفظ الملف بنجاح!{dup_msg}{collision_msg}\n"
                f"• الملف: `{os.path.basename(result.destination_path)}`\n"
                f"• الحجم: {round(result.size / (1024*1024), 2)} MB\n"
                f"• المعرّف: `{job_id}`"
            )
            await self.application.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

        except NonRetryableTransferError as exc:
            self.state.update_job(job_id, status="failed", error=str(exc))
            await self.application.bot.send_message(chat_id=chat_id, text=f"❌ فشلت المهمة {job_id}: {exc}")
        except Exception as exc:
            self.state.update_job(job_id, status="failed", error=str(exc))
            await self.application.bot.send_message(chat_id=chat_id, text=f"❌ خطأ غير متوقع في المهمة {job_id}: {exc}")
        finally:
            # Assure staging cleanup
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except OSError:
                    pass

    # Recovery
    def restore_unfinished(self) -> None:
        """Deterministically restore incomplete jobs on startup."""
        unfinished = self.state.unfinished_jobs()
        logger.info("Restoring %d unfinished jobs from state.", len(unfinished))
        for job in unfinished:
            status = job.get("status")
            job_id = job["id"]

            # If staged temp file is gone (new Colab session), we must re-queue from source
            temp_path = job.get("temp_path")
            if status in {"downloaded", "verifying"} and (not temp_path or not os.path.exists(temp_path)):
                logger.info("Job %s temp staging file lost after restart; resetting to queued.", job_id)
                self.state.update_job(job_id, status="queued", recovery_from_status=status, temp_path=None)
            else:
                self.state.update_job(job_id, status="queued", recovery_from_status=status)

            self.queue.put_nowait(self.state.get_job(job_id))

    def build_application(self) -> Application:
        self.application = ApplicationBuilder().token(self.config.TELEGRAM_BOT_TOKEN).build()
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_help))
        self.application.add_handler(CommandHandler("status", self.cmd_status))
        self.application.add_handler(CommandHandler("cancel", self.cmd_cancel))
        self.application.add_handler(CommandHandler("retry", self.cmd_retry))
        self.application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self.handle_message))
        return self.application
