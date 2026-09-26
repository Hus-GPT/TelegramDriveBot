"""Telegram handlers, worker loop, and lifecycle management for TelegramDriveBot."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

from telegram import CallbackQuery, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
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
    get_storage_diagnostics,
    hash_file,
    safe_filename,
    sanitize_url_for_logging,
    validate_destination_directory,
    validate_url_security,
)
from app.ui import (
    ProgressTracker,
    build_confirmation_keyboard,
    build_job_action_keyboard,
    build_main_keyboard,
    format_bytes,
    humanize_error,
)

logger = logging.getLogger(__name__)

URL_REGEX = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


class File2URLProvider:
    """Serialized single-worker provider for large Telegram files via Bot-to-Bot forward."""

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
        self.queued_ids: set[str] = set()
        self.worker_task: Optional[asyncio.Task[None]] = None
        self.application: Optional[Application] = None

    def is_authorized(self, update: Update) -> bool:
        user = update.effective_user
        if not user:
            return False
        return int(user.id) == int(self.config.OWNER_ID)

    async def check_auth_or_reject(self, update: Update) -> bool:
        if not self.is_authorized(update):
            if update.callback_query:
                try:
                    await update.callback_query.answer("⛔ هذا البوت شخصي وخاص بالمالك فقط.", show_alert=True)
                except Exception:
                    pass
            elif update.effective_message:
                await update.effective_message.reply_text("⛔ عذراً، هذا البوت شخصي وخاص بالمالك فقط.")
            return False
        return True

    async def safe_enqueue_job(self, job: Dict[str, Any]) -> bool:
        """Enqueue job preventing duplicate queue entries."""
        job_id = str(job["id"])
        if job_id in self.queued_ids or job_id in self.active_jobs:
            logger.warning("[job=%s] Attempted duplicate enqueue; skipped.", job_id)
            return False
        self.queued_ids.add(job_id)
        await self.queue.put(job)
        return True

    async def safe_edit_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: str = "Markdown",
        reply_markup: Optional[Any] = None,
    ) -> bool:
        """Safely edit a message, gracefully ignoring errors to prevent UI failures from stopping transfers."""
        if not self.application:
            return False
        try:
            await self.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
            return True
        except Exception as exc:
            logger.debug("Non-critical failure editing Telegram UI message %s: %s", message_id, exc)
            return False

    # ---------------------------------------------------------
    # Reusable Presentation Methods
    # ---------------------------------------------------------

    def render_status_text(self) -> str:
        """Construct system operational status overview."""
        jobs = self.state.all_jobs()
        active = [j for j in jobs if j.get("status") in {"queued", "downloading", "downloaded", "verifying"}]
        stats = self.state.data.get("stats", {})

        worker_status = "⚡ جاري النقل" if self.active_jobs else "💤 في وضع الخمول (Idle)"

        report = (
            f"📊 *حالة النظام التشغيلية*\n"
            f"• حالة العامل: {worker_status}\n"
            f"• المهام النشطة: {len(active)} | ⏳ في الانتظار: {self.queue.qsize()}\n"
            f"• الإحصائيات: ✅ {stats.get('completed', 0)} مكتملة | "
            f"❌ {stats.get('failed', 0)} فاشلة | 🚫 {stats.get('cancelled', 0)} ملغاة\n\n"
        )

        if self.active_jobs:
            report += "🔄 *المهمة الجارية حالياً:*\n"
            for j_id, j_data in self.active_jobs.items():
                fname = j_data.get("filename", "غير معروف")
                st = j_data.get("status", "نشطة")
                sz = format_bytes(j_data.get("size"))
                report += f"• المعرّف: `{j_id}`\n  الملف: `{fname}`\n  الحالة: {st} | الحجم: {sz}\n\n"

        queued_jobs = [j for j in active if j.get("status") == "queued"]
        if queued_jobs:
            report += "⏳ *مهام في طابور الانتظار:*\n"
            for q in queued_jobs[:3]:
                report += f"- `{q.get('id')}` : `{q.get('filename')}`\n"
            if len(queued_jobs) > 3:
                report += f"  (و {len(queued_jobs) - 3} مهام أخرى في الانتظار...)\n"

        if not active:
            report += "✨ لا توجد عمليات جارية حالياً."

        return report

    def render_job_detail_text(self, job: Dict[str, Any]) -> str:
        """Construct detailed metadata view for a specific job, sanitizing errors and paths."""
        job_id = job.get("id")
        st = job.get("status")
        fname = job.get("filename", "بدون اسم")
        sz = format_bytes(job.get("size"))
        created = (job.get("created_at") or "--")[:19].replace("T", " ")
        started = (job.get("started_at") or "--")[:19].replace("T", " ")
        completed = (job.get("completed_at") or "--")[:19].replace("T", " ")
        sha = job.get("sha256") or "لم تُحسب بعد"
        raw_err = job.get("error")
        retries = job.get("retries", 0)

        # Sanitize destination path (show only base folder and filename)
        dest_raw = job.get("destination_path")
        if dest_raw:
            dest = f".../{os.path.basename(os.path.dirname(dest_raw))}/{os.path.basename(dest_raw)}"
        else:
            dest = "غير محدد بعد"

        detail = (
            f"📋 *تفاصيل المهمة:* `{job_id}`\n\n"
            f"• الملف: `{fname}`\n"
            f"• الحالة: `{st}`\n"
            f"• الحجم: {sz}\n"
            f"• نوع المصدر: `{job.get('source_type')}`\n"
            f"• عدد المحاولات: {retries}\n"
            f"• تاريخ الإنشاء: `{created}`\n"
            f"• تاريخ البدء: `{started}`\n"
            f"• تاريخ الانتهاء: `{completed}`\n"
            f"• مسار الوجهة: `{dest}`\n"
            f"• البصمة (SHA-256): `{sha}`\n"
        )
        if raw_err:
            sanitized_err = humanize_error(Exception(raw_err))
            detail += f"• سبب التعثر: {sanitized_err}\n"
        return detail

    def render_storage_text(self) -> str:
        """Construct storage diagnostics overview."""
        diag = get_storage_diagnostics(self.config.LOCAL_STAGING_DIR, self.config.DRIVE_DESTINATION)
        mount_status = "✅ متصل (Mounted)" if diag.is_mount_likely else "⚠️ غير مؤكد أو غير متصل"
        drive_write = "✅ متاح للكتابة" if diag.drive_writable else "❌ غير متاح للكتابة أو محمي"
        stg_free_str = format_bytes(diag.staging_free_bytes)

        return (
            "💽 *تشخيص وسائط التخزين (Storage Intelligence)*\n\n"
            "☁️ *Google Drive Destination:*\n"
            f"• المسار: `{diag.drive_path}`\n"
            f"• حالة التثبيت: {mount_status}\n"
            f"• إمكانية الكتابة: {drive_write}\n\n"
            "📦 *Local Staging (Colab VM):*\n"
            f"• المسار: `{diag.staging_path}`\n"
            f"• المساحة الحرة بالقرص المحلي: `{stg_free_str}`\n"
            f"• الحد الأقصى المسموح للملف: `{format_bytes(self.config.MAX_DOWNLOAD_SIZE)}`\n\n"
            "💡 *ملاحظة:* سعة Google Drive السحابية تُدار عبر حسابك وليست قرصاً محلياً مباشراً."
        )

    def render_history_text(self) -> str:
        """Construct recent completed/failed history overview."""
        limit = self.config.STATUS_HISTORY_COUNT
        all_jobs = self.state.all_jobs()
        terminal_jobs = [j for j in all_jobs if j.get("status") in {"completed", "failed", "cancelled"}]

        if not terminal_jobs:
            return "📂 سجل المهام فارغ حتى الآن."

        terminal_jobs.sort(key=lambda x: x.get("completed_at") or x.get("updated_at") or "", reverse=True)
        recent = terminal_jobs[:limit]

        text = f"📜 *آخر {len(recent)} مهام منتهية:*\n\n"
        for j in recent:
            st = j.get("status")
            icon = "✅" if st == "completed" else ("❌" if st == "failed" else "🚫")
            fname = j.get("filename", "بدون اسم")
            j_id = j.get("id")
            sz = format_bytes(j.get("size"))
            time_str = (j.get("completed_at") or j.get("updated_at") or "")[:19].replace("T", " ")
            text += f"{icon} `/status_{j_id}`\n   📄 `{fname}` ({sz})\n   الحالة: {st} | الوقت: {time_str}\n\n"

        return text

    # ---------------------------------------------------------
    # Command Handlers
    # ---------------------------------------------------------

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return

        dest_name = os.path.basename(self.config.DRIVE_DESTINATION)
        active_cnt = len(self.active_jobs)
        queue_cnt = self.queue.qsize()

        msg = (
            "🤖 *TelegramDriveBot — مدير النقل المباشر*\n\n"
            "الحالة: جاهز ومستعد لاستقبال الملفات والروابط.\n"
            f"📁 مجلد الوجهة: `.../{dest_name}`\n"
            f"⚡ العمليات الجارية: {active_cnt} | ⏳ في الانتظار: {queue_cnt}\n\n"
            "📥 *طريقة الاستخدام:*\n"
            "• أرسل أي رابط تحميل مباشر (`https://...`)\n"
            "• أرسل أي ملف، فيديو، مستند، أو صوت عبر تيليجرام\n\n"
            "🔘 يمكنك استخدام لوحة التحكم السريعة أدناه أو كتابة الأوامر مباشرة:"
        )
        await update.effective_message.reply_text(msg, parse_mode="Markdown", reply_markup=build_main_keyboard())

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return

        msg = (
            "📖 *دليل أوامر TelegramDriveBot:*\n\n"
            "• `/status` : تقرير شامل عن العمليات النشطة وطابور الانتظار.\n"
            "• `/status <معرّف>` : تفاصيل المهمة الدقيقة مع أزرار التحكم بها.\n"
            "• `/storage` : فحص اتصال Google Drive وصلاحية الكتابة ومساحة Staging.\n"
            "• `/history` : عرض سجل بآخر العمليات المنتهية.\n"
            "• `/cancel` : إلغاء العملية الجارية فوراً.\n"
            "• `/cancel <معرّف>` : إلغاء مهمة محددة بالاسم.\n"
            "• `/retry <معرّف>` : إعادة جدولة مهمة فاشلة أو ملغاة.\n"
            "• `/start` : رسالة الترحيب ولوحة التحكم السريعة.\n\n"
            "💡 *ملاحظة:* الملفات الأكبر من 20 ميجابايت تُحوّل تلقائياً عبر File2URL."
        )
        await update.effective_message.reply_text(msg, parse_mode="Markdown")

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return

        args = context.args or []
        if args:
            job_id = args[0].strip().lstrip("_")
            job = self.state.get_job(job_id)
            if not job:
                await update.effective_message.reply_text(f"❓ لم يتم العثور على مهمة بالمعرّف `{job_id}`.")
                return

            detail = self.render_job_detail_text(job)
            kb = build_job_action_keyboard(job_id, job.get("status", ""))
            await update.effective_message.reply_text(detail, parse_mode="Markdown", reply_markup=kb)
            return

        report = self.render_status_text()
        await update.effective_message.reply_text(report, parse_mode="Markdown")

    async def cmd_status_deep_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle deep link commands formatted as /status_<job_id>."""
        if not await self.check_auth_or_reject(update):
            return
        message = update.effective_message
        text = (message.text or "").strip()
        match = re.match(r"^/status_([A-Za-z0-9_-]+)$", text)
        if match:
            job_id = match.group(1)
            job = self.state.get_job(job_id)
            if not job:
                await message.reply_text(f"❓ لم يتم العثور على مهمة بالمعرّف `{job_id}`.")
                return
            detail = self.render_job_detail_text(job)
            kb = build_job_action_keyboard(job_id, job.get("status", ""))
            await message.reply_text(detail, parse_mode="Markdown", reply_markup=kb)
        else:
            await self.cmd_status(update, context)

    async def cmd_storage(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        report = self.render_storage_text()
        await update.effective_message.reply_text(report, parse_mode="Markdown")

    async def cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return
        text = self.render_history_text()
        await update.effective_message.reply_text(text, parse_mode="Markdown")

    async def execute_cancel_job(self, job_id: str) -> Tuple[bool, str]:
        """Perform logical cancellation safely and return (success, message)."""
        job = self.state.get_job(job_id)
        if not job:
            return False, f"❓ لم يتم العثور على مهمة بالمعرّف `{job_id}`."

        current_status = job.get("status")
        if current_status == "completed":
            return False, f"⚠️ المهمة `{job_id}` مكتملة بالفعل ولا يمكن إلغاؤها."
        if current_status in {"failed", "cancelled"}:
            return False, f"ℹ️ المهمة `{job_id}` في حالة منتهية بالفعل ({current_status})."

        event = self.cancel_events.get(job_id)
        if event:
            event.set()
        await self.file2url.cancel_waiter(job_id)
        self.queued_ids.discard(job_id)

        try:
            self.state.update_job(job_id, status="cancelled", error="تم الإلغاء بواسطة المستخدم")
            logger.info("[job=%s] Cancelled successfully.", job_id)
            return True, f"🚫 تم إلغاء المهمة `{job_id}` بنجاح."
        except Exception as exc:
            return False, f"تعذر تحديث حالة الإلغاء: {exc}"

    async def cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return

        args = context.args or []
        job_id: Optional[str] = None

        if args:
            job_id = args[0].strip().lstrip("_")
        else:
            if self.active_jobs:
                job_id = next(iter(self.active_jobs.keys()))
            else:
                await update.effective_message.reply_text("ℹ️ لا توجد عملية نشطة حالياً لإلغائها.\nلإلغاء مهمة محددة: `/cancel <معرّف>`")
                return

        _, msg = await self.execute_cancel_job(job_id)
        await update.effective_message.reply_text(msg, parse_mode="Markdown")

    async def execute_retry_job(self, job_id: str) -> Tuple[bool, str]:
        """Perform logical retry safely and return (success, message)."""
        job = self.state.get_job(job_id)
        if not job:
            return False, f"❓ المهمة `{job_id}` غير موجودة بالسجل."

        current_status = job.get("status")
        if current_status in {"queued", "downloading", "downloaded", "verifying"}:
            return False, f"⚠️ المهمة `{job_id}` جارية أو في الانتظار بالفعل ({current_status})."
        if current_status == "completed":
            return False, f"✅ المهمة `{job_id}` مكتملة بنجاح في Google Drive بالفعل ولا تحتاج لإعادة المحاولة."

        try:
            updated = self.state.retry_job(job_id)
            if updated:
                enqueued = await self.safe_enqueue_job(updated)
                if enqueued:
                    logger.info("[job=%s] Re-enqueued after retry command (attempt %d).", job_id, updated.get("retries", 1))
                    return True, f"🔄 تمت إعادة جدولة المهمة `{job_id}` بنجاح (المحاولة {updated.get('retries')}).\nالملف: `{updated.get('filename')}`"
                else:
                    return False, f"⚠️ المهمة `{job_id}` موجودة بالفعل في صف الانتظار."
            return False, "فشل غير متوقع أثناء تحديث السجل."
        except Exception as exc:
            return False, f"❌ تعذر إعادة الجدولة: {exc}"

    async def cmd_retry(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self.check_auth_or_reject(update):
            return

        args = context.args or []
        if not args:
            await update.effective_message.reply_text("يرجى تحديد معرّف المهمة المراد إعادة تشغيلها:\n`/retry <job_id>`")
            return

        job_id = args[0].strip().lstrip("_")
        _, msg = await self.execute_retry_job(job_id)
        await update.effective_message.reply_text(msg, parse_mode="Markdown")

    # ---------------------------------------------------------
    # Callback Query Handler
    # ---------------------------------------------------------

    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle inline keyboard interactions safely with owner auth and state validation."""
        query = update.callback_query
        if not query:
            return

        if not await self.check_auth_or_reject(update):
            return

        data = query.data or ""
        chat_id = query.message.chat_id if query.message else None
        msg_id = query.message.message_id if query.message else None

        try:
            await query.answer()
        except Exception:
            pass

        # 1. Dashboard Navigation
        if data == "nav_status":
            text = self.render_status_text()
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, text, reply_markup=build_main_keyboard())
            return

        if data == "nav_history":
            text = self.render_history_text()
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, text, reply_markup=build_main_keyboard())
            return

        if data == "nav_storage":
            text = self.render_storage_text()
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, text, reply_markup=build_main_keyboard())
            return

        if data == "nav_help":
            help_text = (
                "📖 *تعليمات سريعة:*\n\n"
                "• أرسل أي رابط مباشر وسيبدأ البوت بتنزيله فوراً.\n"
                "• لإلغاء أي مهمة: اضغط زر الإلغاء أو استخدم `/cancel <معرّف>`.\n"
                "• لإعادة تشغيل مهمة فاشلة: استخدم `/retry <معرّف>`."
            )
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, help_text, reply_markup=build_main_keyboard())
            return

        # 2. Cancel Active Fast Action -> MUST REQUIRE CONFIRMATION
        if data == "act_cancel_active":
            if self.active_jobs:
                act_id = next(iter(self.active_jobs.keys()))
                job = self.state.get_job(act_id)
                prompt = f"⚠️ هل أنت متأكد من إلغاء المهمة الجارية `{act_id}` (`{job.get('filename') if job else ''}`)؟"
                kb = build_confirmation_keyboard("cancel", act_id)
                if chat_id and msg_id:
                    await self.safe_edit_text(chat_id, msg_id, prompt, reply_markup=kb)
            else:
                if query.message:
                    await query.message.reply_text("ℹ️ لا توجد عملية نشطة حالياً لإلغائها.")
            return

        # 3. Job Detail View: job_detail_<id>
        if data.startswith("job_detail_"):
            job_id = data.replace("job_detail_", "")
            job = self.state.get_job(job_id)
            if not job:
                if chat_id and msg_id:
                    await self.safe_edit_text(chat_id, msg_id, f"❓ لم يتم العثور على المهمة `{job_id}`.")
                return
            detail = self.render_job_detail_text(job)
            kb = build_job_action_keyboard(job_id, job.get("status", ""))
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, detail, reply_markup=kb)
            return

        # 4. Confirmation Prompts: ask_cancel_<id>, ask_retry_<id>
        if data.startswith("ask_cancel_"):
            job_id = data.replace("ask_cancel_", "")
            job = self.state.get_job(job_id)
            if not job or job.get("status") in {"completed", "failed", "cancelled"}:
                try:
                    await query.answer("⚠️ لا يمكن إلغاء هذه المهمة (حالتها تغيرت بالفعل).", show_alert=True)
                except Exception:
                    pass
                return
            prompt = f"⚠️ هل أنت متأكد من إلغاء المهمة `{job_id}` (`{job.get('filename')}`)?\n\nاضغط تأكيد للإلغاء أو تراجع للمحافظة على المهمة."
            kb = build_confirmation_keyboard("cancel", job_id)
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, prompt, reply_markup=kb)
            return

        if data.startswith("ask_retry_"):
            job_id = data.replace("ask_retry_", "")
            job = self.state.get_job(job_id)
            if not job or job.get("status") not in {"failed", "cancelled"}:
                try:
                    await query.answer("⚠️ هذه المهمة غير مؤهلة لإعادة المحاولة (حالتها تغيرت).", show_alert=True)
                except Exception:
                    pass
                return
            prompt = f"🔄 هل ترغب في إعادة جدولة المهمة `{job_id}` في طابور الانتظار؟"
            kb = build_confirmation_keyboard("retry", job_id)
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, prompt, reply_markup=kb)
            return

        # 5. Executing Actions: do_cancel_<id>, do_retry_<id>
        if data.startswith("do_cancel_"):
            job_id = data.replace("do_cancel_", "")
            # Re-verify latest state before executing (prevents canceling already-completed jobs)
            job_now = self.state.get_job(job_id)
            if not job_now or job_now.get("status") in {"completed", "failed", "cancelled"}:
                try:
                    await query.answer("⚠️ تعذر الإلغاء: حالة المهمة تغيرت بالفعل في النظام.", show_alert=True)
                except Exception:
                    pass
                if chat_id and msg_id and job_now:
                    detail = self.render_job_detail_text(job_now)
                    kb = build_job_action_keyboard(job_id, job_now.get("status", ""))
                    await self.safe_edit_text(chat_id, msg_id, detail, reply_markup=kb)
                return

            _, cancel_msg = await self.execute_cancel_job(job_id)
            job = self.state.get_job(job_id)
            kb = build_job_action_keyboard(job_id, job.get("status", "")) if job else None
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, cancel_msg, reply_markup=kb)
            return

        if data.startswith("do_retry_"):
            job_id = data.replace("do_retry_", "")
            job_now = self.state.get_job(job_id)
            if not job_now or job_now.get("status") not in {"failed", "cancelled"}:
                try:
                    await query.answer("⚠️ تعذر إعادة الجدولة: المهمة لم تعد قابلة للمحاولة.", show_alert=True)
                except Exception:
                    pass
                if chat_id and msg_id and job_now:
                    detail = self.render_job_detail_text(job_now)
                    kb = build_job_action_keyboard(job_id, job_now.get("status", ""))
                    await self.safe_edit_text(chat_id, msg_id, detail, reply_markup=kb)
                return

            _, retry_msg = await self.execute_retry_job(job_id)
            job = self.state.get_job(job_id)
            kb = build_job_action_keyboard(job_id, job.get("status", "")) if job else None
            if chat_id and msg_id:
                await self.safe_edit_text(chat_id, msg_id, retry_msg, reply_markup=kb)
            return

    # ---------------------------------------------------------
    # Input Handling (Media & URLs)
    # ---------------------------------------------------------

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_message:
            return

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
        text = (message.text or message.caption or "").strip()

        # Direct URL check
        url_match = URL_REGEX.search(text)
        if url_match:
            url = url_match.group(0)

            try:
                validate_url_security(url)
            except NonRetryableTransferError as exc:
                await message.reply_text(f"⛔ {exc}")
                return

            job_id = str(uuid.uuid4())[:8]
            fname = extract_filename_from_url(url, f"file_{job_id}.bin")

            ack_msg = await message.reply_text(
                f"⏳ تم قبول الرابط وإدراجه في طابور الانتظار...\n"
                f"• المعرّف: `{job_id}`\n"
                f"• الملف المتوقع: `{fname}`",
                parse_mode="Markdown",
                reply_markup=build_job_action_keyboard(job_id, "queued"),
            )

            job = self.state.add_job(
                job_id=job_id,
                source_type="direct_url",
                filename=fname,
                chat_id=message.chat_id,
                user_id=message.from_user.id,
                source_url=url,
                telegram_message_id=message.message_id,
            )
            job["ui_message_id"] = ack_msg.message_id
            await self.safe_enqueue_job(job)
            sanitized_log_url = sanitize_url_for_logging(url)
            logger.info("[job=%s] Accepted direct URL job into queue: %s (source: %s)", job_id, fname, sanitized_log_url)
            return

        # Media Attachment
        media = message.document or message.video or message.audio
        if media:
            job_id = str(uuid.uuid4())[:8]
            file_name = getattr(media, "file_name", None) or f"media_{job_id}.bin"
            file_name = safe_filename(file_name)
            file_size = getattr(media, "file_size", 0)

            source_type = "telegram_media" if file_size <= 20 * 1024 * 1024 else "telegram_large"
            sz_str = format_bytes(file_size)

            ack_msg = await message.reply_text(
                f"📦 تم استلام الملف وإدراجه في طابور الانتظار:\n"
                f"• المعرّف: `{job_id}`\n"
                f"• الاسم: `{file_name}`\n"
                f"• الحجم: {sz_str}",
                parse_mode="Markdown",
                reply_markup=build_job_action_keyboard(job_id, "queued"),
            )

            job = self.state.add_job(
                job_id=job_id,
                source_type=source_type,
                filename=file_name,
                chat_id=message.chat_id,
                user_id=message.from_user.id,
                telegram_file_id=media.file_id,
                telegram_message_id=message.message_id,
            )
            job["ui_message_id"] = ack_msg.message_id
            await self.safe_enqueue_job(job)
            logger.info("[job=%s] Accepted media job into queue: %s (%s)", job_id, file_name, source_type)
            return

        await message.reply_text(
            "ℹ️ لم يتم التعرف على المدخل.\n"
            "يرجى إرسال رابط مباشر يبدأ بـ `http://` أو `https://`، أو ملف/مستند/فيديو مباشرة، أو استخدام `/help` لعرض الأوامر.",
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(),
        )

    # ---------------------------------------------------------
    # Worker Loop & Execution
    # ---------------------------------------------------------

    async def worker_loop(self) -> None:
        logger.info("Worker loop started.")
        while True:
            try:
                job = await self.queue.get()
                job_id = job["id"]
                self.queued_ids.discard(job_id)

                current_job = self.state.get_job(job_id)
                if not current_job or current_job.get("status") == "cancelled":
                    logger.info("[job=%s] Skipped execution as status is cancelled or job not found.", job_id)
                    self.queue.task_done()
                    continue

                if "ui_message_id" in job:
                    current_job["ui_message_id"] = job["ui_message_id"]

                self.active_jobs[job_id] = current_job
                cancel_event = threading.Event()
                self.cancel_events[job_id] = cancel_event

                try:
                    await self.process_job(current_job, cancel_event)
                except Exception as exc:
                    logger.exception("[job=%s] Unhandled exception in process_job: %s", job_id, exc)
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
        ui_msg_id = job.get("ui_message_id")
        source_type = job["source_type"]
        temp_dir = self.config.LOCAL_STAGING_DIR
        dest_dir = self.config.DRIVE_DESTINATION

        fresh = self.state.get_job(job_id)
        if fresh and fresh.get("status") == "cancelled":
            return
        if cancel_event.is_set():
            return

        temp_file_path: Optional[str] = job.get("temp_path")
        resolved_name: str = job.get("filename", f"file_{job_id}.bin")

        logger.info("[job=%s] Processing started. Source: %s, file: %s", job_id, source_type, resolved_name)

        try:
            loop = asyncio.get_running_loop()

            await loop.run_in_executor(None, validate_destination_directory, dest_dir)

            skip_download = False
            if job.get("status") == "downloaded" and temp_file_path and os.path.exists(temp_file_path):
                try:
                    v_sha, v_size = hash_file(temp_file_path)
                    if job.get("sha256") == v_sha and job.get("size") == v_size and v_size > 0:
                        skip_download = True
                        logger.info("[job=%s] Staging verified intact; skipping download directly to finalization.", job_id)
                except Exception:
                    pass

            if not skip_download:
                self.state.update_job(job_id, status="downloading")
                logger.info("[job=%s] Download phase initiated.", job_id)

                if ui_msg_id:
                    await self.safe_edit_text(
                        chat_id,
                        ui_msg_id,
                        f"⚡ *بدء تنزيل الملف:* `{resolved_name}`\n• المعرّف: `{job_id}`\n• جاري الاتصال بالخادم المصدر...",
                        reply_markup=build_job_action_keyboard(job_id, "downloading"),
                    )

                download_link: Optional[str] = None
                if source_type == "direct_url":
                    download_link = job.get("source_url")
                elif source_type == "telegram_media":
                    tg_file = await self.application.bot.get_file(job["telegram_file_id"])
                    download_link = tg_file.file_path
                elif source_type == "telegram_large":
                    if ui_msg_id:
                        await self.safe_edit_text(
                            chat_id,
                            ui_msg_id,
                            f"🔄 جاري تحويل الملف الكبير عبر خدمة File2URL...\n• المعرّف: `{job_id}`",
                            reply_markup=build_job_action_keyboard(job_id, "downloading"),
                        )

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

                tracker = ProgressTracker(resolved_name, min_interval=self.config.PROGRESS_INTERVAL)

                def on_progress(bytes_written: int, total_expected: Optional[int]) -> None:
                    if ui_msg_id and tracker.should_update(bytes_written, total_expected):
                        prog_text = tracker.build_progress_text(bytes_written, total_expected)
                        asyncio.run_coroutine_threadsafe(
                            self.safe_edit_text(
                                chat_id,
                                ui_msg_id,
                                prog_text,
                                reply_markup=build_job_action_keyboard(job_id, "downloading"),
                            ),
                            loop,
                        )

                timeout_tuple = (self.config.CONNECT_TIMEOUT, self.config.READ_TIMEOUT)
                temp_file_path, resolved_name, size = await loop.run_in_executor(
                    None,
                    lambda: download_url(
                        url=download_link,
                        temp_root=temp_dir,
                        custom_filename=job.get("filename"),
                        cancel_event=cancel_event,
                        max_retries=self.config.MAX_RETRIES,
                        progress_callback=on_progress,
                        timeout=timeout_tuple,
                        max_download_size=self.config.MAX_DOWNLOAD_SIZE,
                        max_redirects=self.config.MAX_REDIRECTS,
                        retry_base_delay=self.config.RETRY_INITIAL_DELAY,
                        retry_max_delay=self.config.RETRY_MAX_DELAY,
                    ),
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
                logger.info("[job=%s] Downloaded successfully: %s (%d bytes).", job_id, resolved_name, size)

            # Stage 2: Finalize to Google Drive
            logger.info("[job=%s] Drive finalization phase initiated.", job_id)
            if ui_msg_id:
                await self.safe_edit_text(
                    chat_id,
                    ui_msg_id,
                    f"☁️ *جاري الحفظ في Google Drive:*\n"
                    f"• الملف: `{resolved_name}`\n"
                    f"• المرحلة: فحص وتأكيد سلامة التشفير (SHA-256)...",
                    reply_markup=build_job_action_keyboard(job_id, "verifying"),
                )

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

            dup_note = " (تم تخطي النقل لوجود ملف مطابق بالبصمة)" if result.is_duplicate else ""
            col_note = " (تمت إعادة التسمية لمنع استبدال ملف سابق)" if result.action == "collision_renamed" else ""
            final_name = os.path.basename(result.destination_path)
            size_formatted = format_bytes(result.size)

            logger.info("[job=%s] Completed successfully. Target: %s, Action: %s", job_id, result.destination_path, result.action)

            completion_msg = (
                f"✅ *اكتمل النقل بنجاح!*{dup_note}{col_note}\n\n"
                f"📄 الملف: `{final_name}`\n"
                f"📦 الحجم: {size_formatted}\n"
                f"🔐 البصمة: SHA-256 مطابقة وموثقة\n"
                f"📁 المسار: `.../{os.path.basename(dest_dir)}/{final_name}`\n"
                f"🆔 المعرّف: `/status_{job_id}`"
            )

            kb = build_job_action_keyboard(job_id, "completed")
            if ui_msg_id:
                edited = await self.safe_edit_text(chat_id, ui_msg_id, completion_msg, reply_markup=kb)
                if not edited:
                    await self.application.bot.send_message(chat_id=chat_id, text=completion_msg, parse_mode="Markdown", reply_markup=kb)
            else:
                await self.application.bot.send_message(chat_id=chat_id, text=completion_msg, parse_mode="Markdown", reply_markup=kb)

        except NonRetryableTransferError as exc:
            fresh_status = (self.state.get_job(job_id) or {}).get("status")
            if fresh_status != "cancelled":
                self.state.update_job(job_id, status="failed", error=str(exc))
                logger.warning("[job=%s] NonRetryable transfer failure: %s", job_id, exc)
                user_msg = humanize_error(exc)
                err_text = (
                    f"❌ *تعذر إكمال النقل للمهمة `{job_id}`*\n\n"
                    f"• السبب: {user_msg}\n"
                    f"• الملف: `{resolved_name}`\n\n"
                    f"💡 للإعادة بعد تصحيح الخلل: `/retry {job_id}`"
                )
                kb = build_job_action_keyboard(job_id, "failed")
                if ui_msg_id:
                    await self.safe_edit_text(chat_id, ui_msg_id, err_text, reply_markup=kb)
                else:
                    await self.application.bot.send_message(chat_id=chat_id, text=err_text, parse_mode="Markdown", reply_markup=kb)

        except Exception as exc:
            fresh_status = (self.state.get_job(job_id) or {}).get("status")
            if fresh_status != "cancelled":
                self.state.update_job(job_id, status="failed", error=str(exc))
                logger.exception("[job=%s] Unexpected failure: %s", job_id, exc)
                user_msg = humanize_error(exc)
                err_text = (
                    f"❌ *خطأ في المهمة `{job_id}`*\n\n"
                    f"• {user_msg}\n"
                    f"• الملف: `{resolved_name}`\n\n"
                    f"💡 يمكنك إعادة المحاولة عبر: `/retry {job_id}`"
                )
                kb = build_job_action_keyboard(job_id, "failed")
                if ui_msg_id:
                    await self.safe_edit_text(chat_id, ui_msg_id, err_text, reply_markup=kb)
                else:
                    await self.application.bot.send_message(chat_id=chat_id, text=err_text, parse_mode="Markdown", reply_markup=kb)

        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                fresh_status = (self.state.get_job(job_id) or {}).get("status")
                if fresh_status in {"completed", "failed", "cancelled"}:
                    try:
                        os.remove(temp_file_path)
                    except OSError:
                        pass

    # ---------------------------------------------------------
    # Startup Recovery
    # ---------------------------------------------------------

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
                if job_id not in self.queued_ids:
                    self.queued_ids.add(job_id)
                    self.queue.put_nowait(self.state.get_job(job_id))

            elif status == "downloaded":
                if temp_path and os.path.exists(temp_path):
                    try:
                        v_sha, v_size = hash_file(temp_path)
                        if v_size > 0 and (not job.get("sha256") or job.get("sha256") == v_sha):
                            self.state.recover_job(job_id, "downloaded", sha256=v_sha, size=v_size)
                            if job_id not in self.queued_ids:
                                self.queued_ids.add(job_id)
                                self.queue.put_nowait(self.state.get_job(job_id))
                            continue
                    except Exception:
                        pass
                self.state.recover_job(job_id, "queued", temp_path=None)
                if job_id not in self.queued_ids:
                    self.queued_ids.add(job_id)
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
                            if job_id not in self.queued_ids:
                                self.queued_ids.add(job_id)
                                self.queue.put_nowait(self.state.get_job(job_id))
                            continue
                    except Exception:
                        pass

                self.state.recover_job(job_id, "queued", temp_path=None)
                if job_id not in self.queued_ids:
                    self.queued_ids.add(job_id)
                    self.queue.put_nowait(self.state.get_job(job_id))

    def build_application(self) -> Application:
        self.application = ApplicationBuilder().token(self.config.TELEGRAM_BOT_TOKEN).build()
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_help))
        self.application.add_handler(CommandHandler("status", self.cmd_status))
        self.application.add_handler(CommandHandler("storage", self.cmd_storage))
        self.application.add_handler(CommandHandler("history", self.cmd_history))
        self.application.add_handler(CommandHandler("cancel", self.cmd_cancel))
        self.application.add_handler(CommandHandler("retry", self.cmd_retry))
        # Support deep-link commands formatted as /status_<job_id>
        self.application.add_handler(MessageHandler(filters.Regex(r"^/status_[A-Za-z0-9_-]+$"), self.cmd_status_deep_link))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback_query))
        self.application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self.handle_message))
        return self.application
