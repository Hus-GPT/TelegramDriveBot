"""UI formatting, progress throttling, keyboards, and error presentation helpers for Telegram."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def format_bytes(num_bytes: Optional[int]) -> str:
    """Format bytes into human-readable string (KB, MB, GB)."""
    if num_bytes is None or num_bytes < 0:
        return "غير معروف"
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    elif num_bytes < 1024 * 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{num_bytes / (1024 * 1024 * 1024):.2f} GB"


def format_duration(seconds: Optional[float]) -> str:
    """Format seconds into human-readable duration (e.g. 1m 24s)."""
    if seconds is None or seconds < 0:
        return "--"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m = s // 60
    rem_s = s % 60
    if m < 60:
        return f"{m}m {rem_s}s"
    h = m // 60
    rem_m = m % 60
    return f"{h}h {rem_m}m"


def humanize_error(exc: Exception) -> str:
    """Translate technical internal exceptions to clean, actionable user-facing messages.

    Guarantees secrets, tokens, and raw stack traces are never exposed to Telegram.
    """
    msg = str(exc)

    if "تم إلغاء عملية النقل بواسطة المستخدم" in msg or "cancelled" in msg.lower():
        return "تم إلغاء العملية بأمر منك."

    if "وحدة تخزين Google Drive غير مثبتة" in msg or "Unmounted" in msg:
        return "وحدة تخزين Google Drive غير متصلة بالجلسة. يرجى تفعيل drive.mount في كولاب أولاً."
    if "لا توجد صلاحية كتابة في مجلد" in msg or "غير متاح للكتابة" in msg:
        return "مجلد الوجهة في Google Drive محمي أو لا توجد صلاحيات كتابة فيه."
    if "ليس مجلداً صالحاً" in msg:
        return "مسار الحفظ المحدد في Google Drive غير صالح."

    if "SSRF Protection" in msg or "محظور: لا يمكن تحميل عناوين" in msg or "نطاقات الشبكة الداخلية" in msg:
        return "تم حظر الرابط لأسباب أمنية (الرابط يشير إلى خادم محلي أو شبكة خاصة محظورة)."

    if "404" in msg:
        return "الرابط المطلوب غير موجود أو تم حذفه من المصدر (رمز 404)."
    if "403" in msg or "401" in msg:
        return "تم رفض الوصول من الخادم المصدر؛ الرابط يتطلب صلاحيات أو محمي (رمز 401/403)."
    if "رمز HTTP غير قابل لإعادة المحاولة" in msg:
        return f"المصدر رفض طلب التنزيل برمز استجابة دائم ({msg})."

    if "text/html" in msg or "صفحة ويب" in msg:
        return "الرابط يشير إلى صفحة ويب (HTML) وليس إلى ملف تنزيل مباشر."
    if "صفر بايت" in msg or "empty" in msg.lower():
        return "الملف المستلم فارغ من المصدر (حجمه صفر بايت)."

    if "يتجاوز الحد الأقصى المسموح" in msg:
        return "حجم الملف المطلوب يتجاوز الحد الأقصى المسموح بتنزيله في إعدادات النظام."

    if "File2URL" in msg or "مهلة انتظار" in msg:
        return "تعذر استخراج رابط الملف الكبير عبر خدمة التحويل الخارجي (انتهت المهلة أو الخدمة متوقفة)."

    if "حجم الملف غير مكتمل" in msg:
        return "انقطع الاتصال قبل اكتمال تحميل كامل حجم الملف."
    if "خطأ مؤقت قابل لإعادة المحاولة" in msg or "50" in msg:
        return "حدث خطأ مؤقت في خادم المصدر أو في الشبكة أثناء القراءة."

    if "تكامل الملف" in msg or "SHA" in msg:
        return "فشل التحقق من سلامة البصمة المشفرة (SHA-256) للملف بعد نقله إلى Google Drive."
    if "المؤقت المصدر غير موجود" in msg:
        return "فُقد الملف المؤقت في جلسة العمل قبل إتمام نقله إلى Drive."

    return "تعذر إكمال عملية النقل بسبب خطأ غير متوقع. تفاصيل العملية مسجلة بالسجلات الفنية."


class ProgressTracker:
    """Throttled progress tracker to safely compute speed, percentage, and ETA without spamming Telegram."""

    def __init__(self, filename: str, min_interval: float = 3.0):
        self.filename = filename
        self.min_interval = max(1.0, min_interval)
        self.start_time = time.time()
        self.last_update_time = 0.0
        self.last_bytes = 0

    def should_update(self, current_bytes: int, total_bytes: Optional[int]) -> bool:
        """Return True if enough time elapsed since last UI message edit."""
        now = time.time()
        if now - self.last_update_time >= self.min_interval:
            self.last_update_time = now
            self.last_bytes = current_bytes
            return True
        return False

    def build_progress_text(self, current_bytes: int, total_bytes: Optional[int]) -> str:
        """Construct user-friendly progress display string."""
        now = time.time()
        elapsed = max(0.1, now - self.start_time)
        speed = current_bytes / elapsed  # bytes per second
        speed_str = f"{format_bytes(int(speed))}/s"

        if total_bytes and total_bytes > 0:
            pct = min(100.0, (current_bytes / total_bytes) * 100)
            rem_bytes = max(0, total_bytes - current_bytes)
            eta_s = rem_bytes / speed if speed > 0 else None
            eta_str = format_duration(eta_s)
            curr_str = format_bytes(current_bytes)
            tot_str = format_bytes(total_bytes)

            return (
                f"📥 جاري التنزيل: `{self.filename}`\n"
                f"• التقدم: {pct:.1f}% ({curr_str} / {tot_str})\n"
                f"• السرعة: {speed_str} | المتبقي تقريباً: {eta_str}"
            )
        else:
            curr_str = format_bytes(current_bytes)
            return (
                f"📥 جاري التنزيل: `{self.filename}`\n"
                f"• تم تنزيل: {curr_str} (الحجم الإجمالي غير محدد)\n"
                f"• السرعة: {speed_str}"
            )


# ---------------------------------------------------------
# Milestone 6: Keyboards & Interactive Inline UI
# ---------------------------------------------------------

def build_main_keyboard() -> InlineKeyboardMarkup:
    """Build compact, mobile-friendly main owner dashboard keyboard."""
    keyboard = [
        [
            InlineKeyboardButton("📊 الحالة", callback_data="nav_status"),
            InlineKeyboardButton("📜 السجل", callback_data="nav_history"),
            InlineKeyboardButton("💾 التخزين", callback_data="nav_storage"),
        ],
        [
            InlineKeyboardButton("❌ إلغاء الجارية", callback_data="act_cancel_active"),
            InlineKeyboardButton("📖 المساعدة", callback_data="nav_help"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def build_job_action_keyboard(job_id: str, status: str) -> Optional[InlineKeyboardMarkup]:
    """Generate state-aware action buttons for a specific job."""
    buttons: List[List[InlineKeyboardButton]] = []

    if status in {"queued", "downloading", "verifying"}:
        buttons.append([
            InlineKeyboardButton("❌ تأكيد الإلغاء", callback_data=f"ask_cancel_{job_id}"),
            InlineKeyboardButton("🔄 تحديث", callback_data=f"job_detail_{job_id}"),
        ])
    elif status in {"failed", "cancelled"}:
        buttons.append([
            InlineKeyboardButton("🔁 إعادة المحاولة", callback_data=f"ask_retry_{job_id}"),
            InlineKeyboardButton("🔄 تحديث", callback_data=f"job_detail_{job_id}"),
        ])
    elif status == "completed":
        buttons.append([
            InlineKeyboardButton("ℹ️ معلومات تفصيلية", callback_data=f"job_detail_{job_id}")
        ])

    return InlineKeyboardMarkup(buttons) if buttons else None


def build_confirmation_keyboard(action: str, job_id: str) -> InlineKeyboardMarkup:
    """Build single-tap confirmation keyboard with cancel/back options."""
    if action == "cancel":
        keyboard = [
            [
                InlineKeyboardButton("⚠️ نعم، إلغاء المهمة", callback_data=f"do_cancel_{job_id}"),
                InlineKeyboardButton("تراجع", callback_data=f"job_detail_{job_id}"),
            ]
        ]
    else:  # retry
        keyboard = [
            [
                InlineKeyboardButton("🔄 نعم، إعادة المحاولة", callback_data=f"do_retry_{job_id}"),
                InlineKeyboardButton("تراجع", callback_data=f"job_detail_{job_id}"),
            ]
        ]
    return InlineKeyboardMarkup(keyboard)
