"""Telegram handlers, worker loop, and lifecycle management for TelegramDriveBot."""

from __future__ import annotations

import asyncio
import glob
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
    clean_orphan_drive_partials,
    download_url,
    extract_filename_from_url,
    finalize_to_drive,
    hash_file,
    safe_filename,
)

logger = logging.getLogger(__name__)

URL_REGEX = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


class File2URLProvider:
    """Serialized single-worker provider for large Telegram files via Bot-to-Bot forward.

    Architectural Invariant on Correlation:
    The third-party bot (@File2url_rbot) communicates in standard Telegram messages
    and does NOT echo back private job tokens or correlation tags.
    In this architecture:
    1. Only ONE File2URL exchange is ever active at any given moment.
    2. A registration assigns an active future bound to the specific job_id.
    3. Unsolicited or late incoming messages arriving when no active waiter is present are discarded.
    4. Cancellation and timeouts immediately clean up and unregister the waiter, preventing
       subsequent jobs from receiving stale responses.
    """

    def __init__(self, bot_username: str, timeout: int = 120):
        self.bot_username = bot_username.lstrip("@")
        self.timeout = timeout
        self._current_job_id: Optional[str] = None
        self._current_waiter: Optional[asyncio.Future[str]] = None
        self._lock = asyncio.Lock()

    async def register_waiter(self, job_id: str) -> asyncio.Future[str]:
        async with self._lock:
            if self._current_waiter and not self._current_waiter.done():
                self._current_waiter.cancel()
            loop = asyncio.get_running_loop()
            future: asyncio.Future[str] = loop.create_future()
            self._current_job_id = str(job_id)
            self._current_waiter = future
            return future

    async def complete_waiter(self, url: str) -> bool:
        async with self._lock:
            if self._current_waiter and not self._current_waiter.done():
                self._current_waiter.set_result(url)
                self._current_waiter = None
                self._current_job_id = None
                return True
            return False

    async def cancel_waiter(self, job_id: str) -> None:
        async with self._lock:
            if self._current_job_id == str(job_id):
                if self._current_waiter and not self._current_waiter.done():
                    self._current_waiter.cancel()
                self._current_waiter = None
                self._current_job_id = None

    async def cleanup_job(self, job_id: str) -> None:
        await self.cancel_waiter(job_id)


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

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        msg = (
            "👋 مرحباً بك في TelegramDriveBot\n\n"
            "الأوامر المدعومة:\n"
            "/status - عرض حالة النظام والمهام الجارية\n"
            "/retry <job_id> - إعادة تشغيل مهمة فاشلة أو ملغاة\n"
            "/cancel <job_id> - إلغاء مهمة جارية أو في الانتظار\n"
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
            f"📊 حالة النظام (المهام الحالية):\n"
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

        current_status = job.get("status")
        if current_status in {"completed", "failed", "cancelled"}:
            await update.effective_message.reply_text(f"المهمة في حالة نهائية بالفعل ({current_status}).")
            return

        event = self.cancel_events.get(job_id)
        if event:
            event.set()
        await self.file2url.cancel_waiter(job_id)

        try:
            self.state.update_job(job_id, status="cancelled", error="تم الإلغاء بواسطة المستخدم")
            await update.effective_message.reply_text(f"✅ تم إلغاء المهمة {job_id} بنجاح.")
        except Exception as exc:
            await update.effective_message.reply_text(f"تعذر إلغاء المهمة: {exc}")

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

        try:
            updated = self.state.retry_job(job_id)
            if updated:
                await self.queue.put(updated)
                await update.effective_message.reply_text(f"🔄 تمت إعادة جدولة المهمة {job_id} في صف الانتظار.")
        except Exception as exc:
            await update.effective_message.reply_text(f"خطأ أثناء إعادة المحاولة: {exc}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return

        # Check for File2URL external response
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
                telegram_message_id=message.message_id,
            )
            await self.queue.put(job)
            await message.reply_text(f"📥 تم استلام الرابط وإضافته لصف الانتظار:\n• المعرّف: `{job_id}`\n• الملف: `{fname}`", parse_mode="Markdown")
            return

        # Media Attachment
        media = message.document or message.video or message.audio
        if media:
            job_id = str(uuid.uuid4())[:8]
            file_name = getattr(media, "file_name", None) or f"media_{job_id}.bin"
            file_name = safe_filename(file_name)
            file_size = getattr(media, "file_size", 0)

            source_type = "telegram_media" if file_size <= 20 * 1024 * 1024 else "telegram_large"
            job = self.state.add_job(
                job_id=job_id,
                source_type=source_type,
                filename=file_name,
                chat_id=message.chat_id,
                user_id=message.from_user.id,
                telegram_file_id=media.file_id,
                telegram_message_id=message.message_id,
            )
            await self.queue.put(job)
            await message.reply_text(f"📦 تم استلام الملف:\n• المعرّف: `{job_id}`\n• الاسم: `{file_name}`\n• الحجم: {round(file_size / (1024*1024), 2)} MB", parse_mode="Markdown")
            return

    async def worker_loop(self) -> None:
        logger.info("Worker loop started.")
        while True:
            try:
                job = await self.queue.get()
                job_id = job["id"]

                # Fresh state check before executing (skips jobs cancelled while queued)
                current_job = self.state.get_job(job_id)
                if not current_job or current_job.get("status") == "cancelled":
                    logger.info("Job %s was cancelled while queued; skipping worker execution.", job_id)
                    self.queue.task_done()
                    continue

                self.active_jobs[job_id] = current_job
                cancel_event = threading.Event()
                self.cancel_events[job_id] = cancel_event

                try:
                    await self.process_job(current_job, cancel_event)
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

        # Fresh cancellation check
        fresh = self.state.get_job(job_id)
        if fresh and fresh.get("status") == "cancelled":
            return
        if cancel_event.is_set():
            return

        temp_file_path: Optional[str] = job.get("temp_path")
        resolved_name: str = job.get("filename", f"file_{job_id}.bin")

        try:
            loop = asyncio.get_running_loop()

            # Skip download if recovering a job whose local staging is already fully intact
            skip_download = False
            if job.get("status") == "downloaded" and temp_file_path and os.path.exists(temp_file_path):
                try:
                    v_sha, v_size = hash_file(temp_file_path)
                    if job.get("sha256") == v_sha and job.get("size") == v_size and v_size > 0:
                        skip_download = True
                        logger.info("Job %s staging verified intact; skipping download directly to finalization.", job_id)
                except Exception:
                    pass

            if not skip_download:
                self.state.update_job(job_id, status="downloading")

                download_link: Optional[str] = None
                if source_type == "direct_url":
                    download_link = job.get("source_url")
                elif source_type == "telegram_media":
                    tg_file = await self.application.bot.get_file(job["telegram_file_id"])
                    download_link = tg_file.file_path
                elif source_type == "telegram_large":
                    waiter = await self.file2url.register_waiter(job_id)
                    try:
                        msg_id = job.get("telegram_message_id")
                        if not msg_id:
                            raise NonRetryableTransferError("تعذر تحويل الملف الكبير لعدم توفر معرّف الرسالة الأصلي.")
                        await self.application.bot.forward_message(
                            chat_id=f"@{self.file2url.bot_username}",
                            from_chat_id=chat_id,
                            message_id=msg_id,
                        )
                        download_link = await asyncio.wait_for(waiter, timeout=self.file2url.timeout)
                    except asyncio.TimeoutError:
                        raise NonRetryableTransferError("انتهت مهلة انتظار الرابط من بوت File2URL.")
                    except Exception as exc:
                        raise NonRetryableTransferError(f"فشل التحويل عبر File2URL: {exc}")
                    finally:
                        await self.file2url.cleanup_job(job_id)

                if not download_link:
                    raise NonRetryableTransferError("تعذر تحديد رابط التحميل للمهمة.")

                temp_file_path, resolved_name, size = await loop.run_in_executor(
                    None,
                    download_url,
                    download_link,
                    temp_dir,
                    job.get("filename"),
                    cancel_event,
                    self.config.MAX_RETRIES,
                )

                sha256_val, _ = hash_file(temp_file_path)
                self.state.update_job(
                    job_id,
                    status="downloaded",
                    temp_path=temp_file_path,
                    filename=resolved_name,
                    size=size,
                    sha256=sha256_val,
                )

            # Finalize to Drive
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
            fresh_status = (self.state.get_job(job_id) or {}).get("status")
            if fresh_status != "cancelled":
                self.state.update_job(job_id, status="failed", error=str(exc))
                await self.application.bot.send_message(chat_id=chat_id, text=f"❌ فشلت المهمة {job_id}: {exc}")
        except Exception as exc:
            fresh_status = (self.state.get_job(job_id) or {}).get("status")
            if fresh_status != "cancelled":
                self.state.update_job(job_id, status="failed", error=str(exc))
                await self.application.bot.send_message(chat_id=chat_id, text=f"❌ خطأ غير متوقع في المهمة {job_id}: {exc}")
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                fresh_status = (self.state.get_job(job_id) or {}).get("status")
                if fresh_status in {"completed", "failed", "cancelled"}:
                    try:
                        os.remove(temp_file_path)
                    except OSError:
                        pass

    def restore_unfinished(self) -> None:
        """Deterministically restore incomplete jobs on startup without violating state machine."""
        dest_dir = self.config.DRIVE_DESTINATION
        clean_orphan_drive_partials(dest_dir, self.state)

        unfinished = self.state.unfinished_jobs()
        logger.info("Restoring %d unfinished jobs from state.", len(unfinished))

        for job in unfinished:
            status = job.get("status")
            job_id = job["id"]
            temp_path = job.get("temp_path")

            if status in {"queued", "downloading"}:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass
                self.state.recover_job(job_id, "queued", temp_path=None)
                self.queue.put_nowait(self.state.get_job(job_id))

            elif status == "downloaded":
                if temp_path and os.path.exists(temp_path):
                    try:
                        v_sha, v_size = hash_file(temp_path)
                        if v_size > 0 and (not job.get("sha256") or job.get("sha256") == v_sha):
                            self.state.recover_job(job_id, "downloaded", sha256=v_sha, size=v_size)
                            self.queue.put_nowait(self.state.get_job(job_id))
                            continue
                    except Exception:
                        pass
                self.state.recover_job(job_id, "queued", temp_path=None)
                self.queue.put_nowait(self.state.get_job(job_id))

            elif status == "verifying":
                target_fname = safe_filename(job.get("filename", f"file_{job_id}.bin"))
                dest_part_path = os.path.join(dest_dir, f".part_{job_id}_{target_fname}")
                if os.path.exists(dest_part_path):
                    try:
                        os.remove(dest_part_path)
                    except OSError:
                        pass

                if temp_path and os.path.exists(temp_path):
                    try:
                        v_sha, v_size = hash_file(temp_path)
                        if v_size > 0 and (not job.get("sha256") or job.get("sha256") == v_sha):
                            self.state.recover_job(job_id, "downloaded", sha256=v_sha, size=v_size)
                            self.queue.put_nowait(self.state.get_job(job_id))
                            continue
                    except Exception:
                        pass

                self.state.recover_job(job_id, "queued", temp_path=None)
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
