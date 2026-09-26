"""Core download, streaming, hashing, and Drive finalization engine."""

from __future__ import annotations

import glob
import hashlib
import ipaddress
import logging
import os
import re
import shutil
import socket
import tempfile
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MB chunk for streaming and hashing
DEFAULT_TIMEOUT = (15, 60)  # (connect_timeout, read_timeout) in seconds
MAX_STREAM_SIZE = 100 * 1024 * 1024 * 1024  # 100 GB safety sanity limit
MAX_REDIRECTS = 10


class TransferError(Exception):
    """Base class for transfer failures."""
    pass


class NonRetryableTransferError(TransferError):
    """Explicitly non-retryable transfer failure (e.g. 400, 401, 403, 404, 405, 410, SSRF, bad request, cancelled)."""
    pass


class RetryableTransferError(TransferError):
    """Failure that may succeed upon a subsequent retry attempt (timeouts, 408, 429, 5xx)."""
    pass


@dataclass
class StorageDiagnostics:
    staging_path: str
    staging_exists: bool
    staging_writable: bool
    staging_free_bytes: Optional[int]
    drive_path: str
    drive_exists: bool
    drive_is_dir: bool
    drive_writable: bool
    is_mount_likely: bool


@dataclass
class FinalizeResult:
    destination_path: str
    sha256: str
    size: int
    is_duplicate: bool
    action: str  # "copied", "duplicate_skipped", "collision_renamed"


def validate_destination_directory(destination: str) -> str:
    """Validate that Google Drive destination exists or can be created, is a directory, and is writable."""
    dest_path = os.path.abspath(destination)

    if os.path.exists(dest_path):
        if not os.path.isdir(dest_path):
            raise NonRetryableTransferError(f"مسار التخزين المحدد ليس مجلداً صالحاً: {dest_path}")
        if not os.access(dest_path, os.W_OK | os.X_OK):
            raise NonRetryableTransferError(f"لا توجد صلاحية كتابة في مجلد Google Drive: {dest_path}")
    else:
        # Check parent directory accessibility
        parent = os.path.dirname(dest_path)
        if not os.path.exists(parent):
            # If standard Colab mount path parent /content/drive/MyDrive is missing
            if "/content/drive" in dest_path and not os.path.exists("/content/drive/MyDrive"):
                raise NonRetryableTransferError(
                    "وحدة تخزين Google Drive غير مثبتة (Unmounted)! يرجى تنفيذ drive.mount('/content/drive') في كولاب أولاً."
                )
        try:
            os.makedirs(dest_path, exist_ok=True)
        except OSError as exc:
            raise NonRetryableTransferError(f"تعذر إنشاء مجلد الوجهة في Google Drive ({exc}): {dest_path}") from exc

    # Probe writability with a volatile test file
    test_probe = os.path.join(dest_path, f".write_probe_{os.getpid()}_{int(hashlib.md5(dest_path.encode()).hexdigest()[:8], 16)}")
    try:
        with open(test_probe, "w", encoding="utf-8") as f:
            f.write("probe")
        os.remove(test_probe)
    except OSError as exc:
        if os.path.exists(test_probe):
            try:
                os.remove(test_probe)
            except OSError:
                pass
        raise NonRetryableTransferError(f"فحص الكتابة فشل؛ مجلد Google Drive غير متاح للكتابة: {exc}") from exc

    return dest_path


def get_storage_diagnostics(staging_dir: str, drive_dir: str) -> StorageDiagnostics:
    """Inspect local staging and Google Drive storage availability without assuming universal FUSE quotas."""
    staging_path = os.path.abspath(staging_dir)
    drive_path = os.path.abspath(drive_dir)

    # 1. Staging evaluation
    stg_exists = os.path.exists(staging_path)
    stg_writable = False
    stg_free: Optional[int] = None
    if stg_exists and os.path.isdir(staging_path):
        stg_writable = os.access(staging_path, os.W_OK)
        try:
            usage = shutil.disk_usage(staging_path)
            stg_free = usage.free
        except OSError:
            pass

    # 2. Drive evaluation
    drv_exists = os.path.exists(drive_path)
    drv_is_dir = os.path.isdir(drive_path) if drv_exists else False
    drv_writable = False
    if drv_exists and drv_is_dir:
        drv_writable = os.access(drive_path, os.W_OK | os.X_OK)

    # Mount detection heuristic for Colab runtime
    is_mount = False
    if "/content/drive" in drive_path:
        is_mount = os.path.exists("/content/drive/MyDrive")
    else:
        # Generic local/POSIX destination
        is_mount = drv_exists and drv_is_dir

    return StorageDiagnostics(
        staging_path=staging_path,
        staging_exists=stg_exists,
        staging_writable=stg_writable,
        staging_free_bytes=stg_free,
        drive_path=drive_path,
        drive_exists=drv_exists,
        drive_is_dir=drv_is_dir,
        drive_writable=drv_writable,
        is_mount_likely=is_mount,
    )


def validate_url_security(url: str) -> None:
    """Validate URL scheme and protect against SSRF targets (localhost, private subnets, metadata IPs)."""
    if not url or len(url) > 2048:
        raise NonRetryableTransferError("الرابط غير صالح أو يتجاوز الحد الأقصى للطول (2048 حرفاً).")

    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise NonRetryableTransferError(f"بروتوكول الرابط غير مدعوم ({parsed.scheme}). الروابط المدعومة هي http و https فقط.")

    hostname = parsed.hostname
    if not hostname:
        raise NonRetryableTransferError("الرابط غير صالح: لا يحتوي على اسم مضيف (hostname).")

    lower_host = hostname.lower().strip("[]")
    if lower_host in {"localhost", "127.0.0.1", "::1", "metadata.google.internal", "metadata.local"}:
        raise NonRetryableTransferError("محظور: لا يمكن تحميل عناوين الخوادم المحلية أو خدمات البيانات الوصفية (SSRF Protection).")

    try:
        ip = ipaddress.ip_address(lower_host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise NonRetryableTransferError("محظور: لا يمكن التحميل من نطاقات الشبكة الداخلية أو الخاصة (SSRF Protection).")
    except ValueError:
        pass


def safe_filename(name: str, fallback: str = "download.bin") -> str:
    """Sanitize arbitrary input filename preventing path traversal, while preserving valid Unicode (Arabic, etc.)."""
    if not name:
        return fallback

    cleaned = name.split("?")[0].split("#")[0]
    cleaned = os.path.basename(cleaned)
    cleaned = re.sub(r'[\x00-\x1f\\/:\*\?"<>\|]', "_", cleaned)
    cleaned = cleaned.strip(". \t\r\n")
    if not cleaned or cleaned in {".", ".."}:
        return fallback

    return cleaned[:255]


def extract_filename_from_url(url: str, default: str = "download.bin") -> str:
    """Extract a clean filename from a URL path, or return fallback."""
    try:
        parsed = urllib.parse.urlsplit(url)
        path = urllib.parse.unquote(parsed.path)
        base = os.path.basename(path.rstrip("/"))
        return safe_filename(base, fallback=default)
    except Exception:
        return default


def parse_content_disposition(header: str) -> Optional[str]:
    """Parse filename from Content-Disposition header with RFC 5987 / RFC 6266 support."""
    if not header:
        return None

    match_star = re.search(r"filename\*\s*=\s*(?:UTF-8''|utf-8'')([^;]+)", header, re.IGNORECASE)
    if match_star:
        val = match_star.group(1).strip("\"' ")
        try:
            decoded = urllib.parse.unquote(val)
            return safe_filename(decoded)
        except Exception:
            pass

    match_standard = re.search(r'filename\s*=\s*"([^"]+)"', header, re.IGNORECASE)
    if not match_standard:
        match_standard = re.search(r'filename\s*=\s*([^; ]+)', header, re.IGNORECASE)
    if match_standard:
        return safe_filename(match_standard.group(1).strip("\"' "))

    return None


def hash_file(filepath: str, chunk_size: int = CHUNK_SIZE) -> Tuple[str, int]:
    """Calculate SHA-256 hash and exact byte size incrementally in chunks without loading file to RAM."""
    hasher = hashlib.sha256()
    total_bytes = 0
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
            total_bytes += len(chunk)
    return hasher.hexdigest(), total_bytes


def check_cancellation(cancel_event: Optional[Any] = None) -> None:
    """Check cancellation event and raise NonRetryableTransferError if set."""
    if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
        raise NonRetryableTransferError("تم إلغاء عملية النقل بواسطة المستخدم.")


def download_url(
    url: str,
    temp_root: str,
    custom_filename: Optional[str] = None,
    cancel_event: Optional[Any] = None,
    max_retries: int = 3,
    progress_callback: Optional[Callable[[int, Optional[int]], None]] = None,
    timeout: Tuple[int, int] = DEFAULT_TIMEOUT,
    max_download_size: int = MAX_STREAM_SIZE,
    max_redirects: int = MAX_REDIRECTS,
) -> Tuple[str, str, int]:
    """Stream download a direct URL into a local temporary staging file safely."""
    validate_url_security(url)
    os.makedirs(temp_root, exist_ok=True)
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        check_cancellation(cancel_event)
        temp_fd, temp_path = tempfile.mkstemp(dir=temp_root, prefix="dl_", suffix=".part")
        os.close(temp_fd)

        try:
            session = requests.Session()
            session.max_redirects = max_redirects
            response = session.get(
                url,
                stream=True,
                allow_redirects=True,
                timeout=timeout,
                headers={"User-Agent": "TelegramDriveBot/4.0"},
            )

            if response.status_code in {400, 401, 403, 404, 405, 410}:
                raise NonRetryableTransferError(f"فشل التحميل (رمز HTTP غير قابل لإعادة المحاولة: {response.status_code})")
            if response.status_code in {408, 429} or response.status_code >= 500:
                raise RetryableTransferError(f"خطأ مؤقت قابل لإعادة المحاولة (HTTP {response.status_code})")
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" in content_type and not (custom_filename and custom_filename.endswith(".html")):
                raise NonRetryableTransferError("الرابط يشير إلى صفحة ويب (HTML) وليس إلى ملف تحميل مباشر.")

            content_length_hdr = response.headers.get("Content-Length")
            total_expected: Optional[int] = None
            if content_length_hdr and content_length_hdr.strip().isdigit():
                total_expected = int(content_length_hdr.strip())
                if total_expected > max_download_size:
                    raise NonRetryableTransferError(
                        f"حجم الملف ({total_expected} بايت) يتجاوز الحد الأقصى المسموح به للنظام ({max_download_size} بايت)."
                    )

            cd_name = parse_content_disposition(response.headers.get("Content-Disposition", ""))
            resolved_filename = (
                safe_filename(custom_filename)
                if custom_filename
                else (cd_name or extract_filename_from_url(response.url or url))
            )

            bytes_written = 0
            with open(temp_path, "wb") as out_file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    check_cancellation(cancel_event)
                    if chunk:
                        out_file.write(chunk)
                        bytes_written += len(chunk)

                        if bytes_written > max_download_size:
                            raise NonRetryableTransferError(
                                f"تم إيقاف التحميل: حجم البيانات المستلمة تجاوز الحد الأقصى المسموح ({max_download_size} بايت)."
                            )

                        if progress_callback:
                            try:
                                progress_callback(bytes_written, total_expected)
                            except Exception:
                                pass

            check_cancellation(cancel_event)

            if bytes_written == 0:
                raise NonRetryableTransferError("فشل التنزيل: تم استقبال ملف فارغ بحجم صفر بايت.")

            if total_expected is not None and bytes_written != total_expected:
                raise RetryableTransferError(f"حجم الملف غير مكتمل (تم تنزيل {bytes_written} من {total_expected} بايت).")

            return temp_path, resolved_filename, bytes_written

        except NonRetryableTransferError:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

        except Exception as exc:
            last_error = exc
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

            check_cancellation(cancel_event)
            logger.warning("Download attempt %d/%d failed for %s: %s", attempt, max_retries, url, exc)
            if attempt == max_retries:
                break

    raise RetryableTransferError(f"تعذر تنزيل الملف بعد {max_retries} محاولات: {last_error}")


def clean_orphan_drive_partials(destination_dir: str, state_store: Any) -> List[str]:
    """Safely removes only orphaned .part_<job_id>_* files from Google Drive destination.

    A partial file is an orphan IF AND ONLY IF:
    1. It strictly matches the project-specific naming convention '.part_<job_id>_*'
    2. The associated job does not exist in StateStore, OR the job is in a terminal state (completed, failed, cancelled).
    Never touches any valid destination files or unrelated files.
    """
    cleaned: List[str] = []
    if not os.path.exists(destination_dir):
        return cleaned

    pattern = os.path.join(destination_dir, ".part_*_*")
    for part_path in glob.glob(pattern):
        filename = os.path.basename(part_path)
        match = re.match(r"^\.part_([^_]+)_(.+)$", filename)
        if not match:
            continue

        job_id = match.group(1)
        job = state_store.get_job(job_id) if hasattr(state_store, "get_job") else None

        if not job or job.get("status") in {"completed", "failed", "cancelled"}:
            try:
                os.remove(part_path)
                cleaned.append(part_path)
                logger.info("Cleaned orphan partial Drive file: %s", part_path)
            except OSError as exc:
                logger.warning("Failed cleaning orphan partial file %s: %s", part_path, exc)

    return cleaned


def finalize_to_drive(
    temp_path: str,
    filename: str,
    destination: str,
    state_store: Any,
    job_id: str,
    cancel_event: Optional[Any] = None,
) -> FinalizeResult:
    """Safely verify integrity, validate destination, check duplicate/collision, and copy to Google Drive destination.

    Hardened Drive Architecture:
    1. Pre-transfer destination validation (existence, directory, writability probe).
    2. Pre-copy source validation & state transition to 'verifying'.
    3. Content duplicate detection (exact SHA-256 and size match avoids redundant transfer).
    4. Collision renaming (safely appends suffix if differing content exists).
    5. Copy to .part_<job_id>_<filename> in destination.
    6. Post-copy cryptographic verification (destination SHA-256 + size match).
    7. Destination promotion via os.replace.
    8. State transition to 'completed'.
    9. Local staging file removal.
    """
    check_cancellation(cancel_event)

    if not os.path.exists(temp_path):
        raise NonRetryableTransferError(f"الملف المؤقت المصدر غير موجود: {temp_path}")

    # Validate destination directory writability upfront
    dest_path = validate_destination_directory(destination)

    source_sha, source_size = hash_file(temp_path)
    if source_size == 0:
        raise NonRetryableTransferError("الملف المؤقت فارغ (حجمه صفر).")

    target_filename = safe_filename(filename)
    target_path = os.path.join(dest_path, target_filename)

    state_store.update_job(
        job_id,
        status="verifying",
        sha256=source_sha,
        size=source_size,
        temp_path=temp_path,
        destination_path=target_path,
    )

    action = "copied"
    is_duplicate = False

    if os.path.exists(target_path):
        try:
            existing_sha, existing_size = hash_file(target_path)
            if existing_size == source_size and existing_sha == source_sha:
                logger.info("Identical file already exists at destination: %s", target_path)
                state_store.update_job(job_id, status="completed", destination_path=target_path)
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
                return FinalizeResult(
                    destination_path=target_path,
                    sha256=source_sha,
                    size=source_size,
                    is_duplicate=True,
                    action="duplicate_skipped",
                )
        except OSError as exc:
            logger.warning("Could not hash existing file at %s: %s", target_path, exc)

        action = "collision_renamed"
        stem, ext = os.path.splitext(target_filename)
        counter = 1
        while os.path.exists(target_path):
            try:
                cand_sha, cand_size = hash_file(target_path)
                if cand_size == source_size and cand_sha == source_sha:
                    state_store.update_job(job_id, status="completed", destination_path=target_path)
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass
                    return FinalizeResult(
                        destination_path=target_path,
                        sha256=source_sha,
                        size=source_size,
                        is_duplicate=True,
                        action="duplicate_skipped",
                    )
            except OSError:
                pass
            target_filename = f"{stem} ({counter}){ext}"
            target_path = os.path.join(dest_path, target_filename)
            counter += 1

    check_cancellation(cancel_event)

    dest_part_path = os.path.join(dest_path, f".part_{job_id}_{target_filename}")
    try:
        shutil.copyfile(temp_path, dest_part_path)
        check_cancellation(cancel_event)

        dest_sha, dest_size = hash_file(dest_part_path)
        if dest_size != source_size or dest_sha != source_sha:
            raise TransferError(
                f"فشل التحقق من تكامل الملف في درايف: المصدر ({source_sha}, {source_size}) != الهدف ({dest_sha}, {dest_size})"
            )

        os.replace(dest_part_path, target_path)

    except Exception:
        if os.path.exists(dest_part_path):
            try:
                os.remove(dest_part_path)
            except OSError:
                pass
        raise

    state_store.update_job(
        job_id,
        status="completed",
        destination_path=target_path,
        sha256=source_sha,
        size=source_size,
    )

    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    except OSError as exc:
        logger.warning("Failed to clean up staging temp file %s: %s", temp_path, exc)

    return FinalizeResult(
        destination_path=target_path,
        sha256=source_sha,
        size=source_size,
        is_duplicate=is_duplicate,
        action=action,
    )
