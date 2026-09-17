import asyncio
import os
import re
import shutil
import uuid
from urllib.parse import urlparse

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .config import Config
from .state import now
from .transfer import TransferError, download_url, finalize_to_drive, safe_filename

URL_RE = re.compile(r"https?://[^\s<>]+", re.I)


class TelegramDriveBot:
    def __init__(self, config: Config, state_store):
        self.config = config
        self.state = state_store
        self.queue: asyncio.Queue = asyncio.Queue()
        self.active_cancel: asyncio.Event | None = None
        self.file2url_waiter: asyncio.Future | None = None
        self.file2url_username = os.getenv("FILE2URL_BOT_USERNAME", "@File2url_rbot").strip()

    def authorized(self, update: Update) -> bool:
        return bool(update.effective_user and update.effective_user.id == self.config.owner_id)

    async def reject(self, update: Update):
        if update.effective_chat:
            await update.effective_chat.send_message("🚫 غير مصرح لك باستخدام هذا البوت.")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.authorized(update):
            return await self.reject(update)
        await update.effective_chat.send_message(
            "📦 TelegramDriveBot جاهز.\n\nأرسل ملفًا أو رابطًا، وسأنقله إلى Google Drive."
        )

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.authorized(update):
            return await self.reject(update)
        if self.active_cancel and not self.active_cancel.is_set():
            self.active_cancel.set()
            await update.effective_chat.send_message("🛑 تم طلب إلغاء العملية الحالية.")
        else:
            await update.effective_chat.send_message("لا توجد عملية قيد التنفيذ حاليًا.")

    async def enqueue(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.authorized(update):
            return await self.reject(update)
        message = update.effective_message
        url = self.extract_url(message)
        media = self.extract_media(message)
        if not url and not media:
            return

        job_id = uuid.uuid4().hex
        job = {
            "id": job_id,
            "status": "queued",
            "created_at": now(),
            "source": "url" if url else "telegram",
            "chat_id": update.effective_chat.id,
            "message_id": message.message_id,
            "url": url,
            "filename": media[1] if media else None,
            "telegram_file_id": media[0] if media else None,
            "telegram_file_size": media[2] if media else None,
        }
        self.state.add_job(job)
        position = self.queue.qsize() + 1
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية الحالية", callback_data="cancel_current")]])
        await message.reply_text(
            f"📥 تمت إضافة الطلب إلى الطابور.\nالترتيب: {position}",
            reply_markup=keyboard,
        )
        await self.queue.put(job)

    async def callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if not self.authorized(update):
            return await query.edit_message_text("🚫 غير مصرح لك.")
        if query.data == "cancel_current":
            if self.active_cancel and not self.active_cancel.is_set():
                self.active_cancel.set()
                await query.edit_message_text("🛑 تم طلب إلغاء العملية الحالية.")
            else:
                await query.edit_message_text("لا توجد عملية قيد التنفيذ حاليًا.")

    async def bot_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.effective_message
        if not message or not message.from_user or not message.from_user.is_bot:
            return
        username = (message.from_user.username or "").lower()
        expected = self.file2url_username.lstrip("@").lower()
        if username != expected or not self.file2url_waiter or self.file2url_waiter.done():
            return
        text = message.text or message.caption or ""
        match = URL_RE.search(text)
        if match:
            self.file2url_waiter.set_result(match.group(0).rstrip(".,);]"))

    async def worker(self, application: Application):
        while True:
            job = await self.queue.get()
            self.active_cancel = asyncio.Event()
            try:
                await self.process_job(application, job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.update_job(job["id"], status="failed", error=str(exc))
                try:
                    await application.bot.send_message(job["chat_id"], f"❌ فشلت العملية: {exc}")
                except Exception:
                    pass
            finally:
                self.active_cancel = None
                self.queue.task_done()

    async def process_job(self, application: Application, job: dict):
        chat_id = job["chat_id"]
        job_id = job["id"]
        self.state.update_job(job_id, status="running")
        status_message = await application.bot.send_message(chat_id, "⏳ جاري تجهيز الملف…")
        temp_path = None
        try:
            await application.bot.send_chat_action(chat_id, ChatAction.UPLOAD_DOCUMENT)
            source = job.get("source")
            if source == "url":
                await self.update_status(status_message, "⬇️ جاري تنزيل المصدر…")
                temp_path, filename = await asyncio.to_thread(
                    download_url, job["url"], self.config.temp_root
                )
            else:
                size = job.get("telegram_file_size")
                if size and size > 20 * 1024 * 1024:
                    await self.update_status(status_message, "🔗 الملف كبير؛ جاري تمريره لمسار الرابط…")
                    url = await self.request_file_url(application, job)
                    job["source"] = "telegram_via_file2url"
                    self.state.update_job(job_id, source=job["source"], url=url)
                    temp_path, filename = await asyncio.to_thread(
                        download_url, url, self.config.temp_root
                    )
                else:
                    await self.update_status(status_message, "⬇️ جاري الحصول على الملف من Telegram…")
                    temp_path, filename = await self.download_telegram_media(application, job)

            self.ensure_not_cancelled()
            await self.update_status(status_message, "☁️ جاري حفظ الملف في Google Drive…")
            result = await asyncio.to_thread(
                finalize_to_drive,
                temp_path,
                filename,
                self.config.drive_destination,
                self.state,
                job.get("source", "telegram"),
                job_id,
            )
            temp_path = None
            if result["status"] == "duplicate":
                await self.update_status(status_message, "ℹ️ الملف موجود مسبقًا")
            else:
                await self.update_status(
                    status_message,
                    f"✅ تم الحفظ بنجاح\n📄 {result['filename']}\n📦 {result['size']:,} بايت",
                )
        except Exception:
            if temp_path and os.path.exists(temp_path):
                # Keep failed temporary data for recovery; startup recovery can inspect it later.
                pass
            raise

    async def download_telegram_media(self, application: Application, job: dict):
        os.makedirs(self.config.temp_root, exist_ok=True)
        message = await application.bot.get_updates(timeout=0)  # harmless connectivity check
        del message
        # The message object is not persisted between updates, so retrieve it from Telegram is not
        # possible through Bot API. The first implementation therefore stores the file_id and uses get_file.
        file_id = job["telegram_file_id"]
        tg_file = await application.bot.get_file(file_id)
        filename = safe_filename(job.get("filename") or f"telegram_{job['message_id']}")
        path = os.path.join(self.config.temp_root, f"{job['id']}_{filename}")
        await tg_file.download_to_drive(path)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            raise TransferError("تعذر الحصول على الملف من Telegram.")
        return path, filename

    async def request_file_url(self, application: Application, job: dict) -> str:
        if not self.file2url_username:
            raise TransferError("مسار الملفات الكبيرة غير مهيأ.")
        loop = asyncio.get_running_loop()
        self.file2url_waiter = loop.create_future()
        try:
            await application.bot.forward_message(
                chat_id=self.file2url_username,
                from_chat_id=job["chat_id"],
                message_id=job["message_id"],
            )
            return await asyncio.wait_for(self.file2url_waiter, timeout=90)
        except asyncio.TimeoutError:
            raise TransferError("لم يصل رابط الملف الكبير من خدمة التحويل ضمن المهلة.")
        finally:
            self.file2url_waiter = None

    def ensure_not_cancelled(self):
        if self.active_cancel and self.active_cancel.is_set():
            raise TransferError("تم إلغاء العملية.")

    @staticmethod
    def extract_url(message):
        text = message.text or message.caption or ""
        match = URL_RE.search(text)
        return match.group(0).rstrip(".,);]") if match else None

    @staticmethod
    def extract_media(message):
        if message.document:
            return message.document.file_id, message.document.file_name or "document", message.document.file_size
        if message.video:
            return message.video.file_id, message.video.file_name or "video.mp4", message.video.file_size
        if message.audio:
            return message.audio.file_id, message.audio.file_name or "audio", message.audio.file_size
        if message.voice:
            return message.voice.file_id, "voice.ogg", message.voice.file_size
        if message.photo:
            item = message.photo[-1]
            return item.file_id, "photo.jpg", item.file_size
        return None

    @staticmethod
    async def update_status(message, text):
        try:
            await message.edit_text(text)
        except Exception:
            pass

    def build_application(self):
        application = Application.builder().token(self.config.telegram_token).build()
        application.add_handler(MessageHandler(filters.ALL & filters.UpdateType.MESSAGE & filters.User(self.config.owner_id), self.bot_message), group=-2)
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.enqueue))
        application.add_handler(MessageHandler(filters.ATTACHMENT, self.enqueue))
        application.add_handler(CallbackQueryHandler(self.callback))
        application.add_handler(MessageHandler(filters.COMMAND & filters.Regex(r"^/start$"), self.start))
        application.add_handler(MessageHandler(filters.COMMAND & filters.Regex(r"^/cancel$"), self.cancel))
        return application

    async def post_init(self, application: Application):
        application.create_task(self.worker(application))
