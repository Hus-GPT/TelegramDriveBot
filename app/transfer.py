"""Core download, streaming, hashing, SSRF protection, and Drive finalization engine."""

from __future__ import annotations

import glob
import hashlib
import ipaddress
import logging
import os
import random
import re
import shutil
import socket
import tempfile
import time
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


def sanitize_url_for_logging(url: Optional[str]) -> str:
    """Return a sanitized version of URL for logs, stripping sensitive query params, userinfo, and tokens."""
    if not url:
        return ""
    try:
        parsed = urllib.parse.urlsplit(url.strip())
        netloc = parsed.netloc
        if "@" in netloc:
            # Strip user:password
            netloc = netloc.split("@")[-1]

        # Redact query string entirely if it contains any query parameters
        query = "[redacted]" if parsed.query else ""
        clean_parts = (parsed.scheme, netloc, parsed.path, query, "")
        return urllib.parse.urlunsplit(clean_parts)
    except Exception:
        return "[sanitized_url]"


def is_ip_disallowed(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Check if an IPv4 or IPv6 address is private, loopback, link-local, reserved, multicast, or unspecified."""
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def validate_url_security(url: str, resolve_dns: bool = True) -> None:
    """Validate URL scheme and protect against SSRF targets across IPv4/IPv6, hostnames, and cloud metadata."""
    if not url or len(url) > 2048:
        raise NonRetryableTransferError("الرابط غير صالح أو يتجاوز الحد الأقصى للطول (2048 حرفاً).")

    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise NonRetryableTransferError(f"بروتوكول الرابط غير مدعوم ({parsed.scheme}). الروابط المدعومة هي http و https فقط.")

    hostname = parsed.hostname
    if not hostname:
        raise NonRetryableTransferError("الرابط غير صالح: لا يحتوي على اسم مضيف (hostname).")

    lower_host = hostname.lower().strip("[]")

    # Block well-known loopback and cloud metadata names
    if lower_host in {"localhost", "127.0.0.1", "::1", "metadata.google.internal", "metadata.local"}:
        raise NonRetryableTransferError("محظور: لا يمكن تحميل عناوين الخوادم المحلية أو خدمات البيانات الوصفية (SSRF Protection).")

    # If host is an IP literal
    try:
        ip = ipaddress.ip_address(lower_host)
        if is_ip_disallowed(ip):
            raise NonRetryableTransferError("محظور: لا يمكن التحميل من نطاقات الشبكة الداخلية أو الخاصة (SSRF Protection).")
        return
    except ValueError:
        pass

    # Resolve DNS to check if hostname resolves to private/loopback/cloud metadata IP
    if resolve_dns:
        try:
            addr_info = socket.getaddrinfo(lower_host, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM)
            for family, _, _, _, sockaddr in addr_info:
                ip_str = sockaddr[0]
                try:
                    ip_obj = ipaddress.ip_address(ip_str)
                    if is_ip_disallowed(ip_obj):
                        raise NonRetryableTransferError(
                            f"محظور: اسم المضيف {lower_host} يحل إلى عنوان IP داخلي/محلي ({ip_str}) (SSRF Protection)."
                        )
                except ValueError:
                    pass
        except socket.gaierror as exc:
            logger.debug("DNS resolution failed for %s during SSRF validation: %s", lower_host, exc)
            # Will be caught as connection error in requests session if invalid host


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


def compute_retry_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 30.0) -> float:
    """Calculate exponential backoff delay with jitter."""
    exp_delay = base_delay * (2 ** (attempt - 1))
    jitter = random.uniform(0.0, 0.5 * base_delay)
    return min(max_delay, exp_delay + jitter)


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
    sleep_fn: Callable[[float], None] = time.sleep,
    retry_base_delay: float = 1.0,
    retry_max_delay: float = 30.0,
) -> Tuple[str, str, int]:
    """Stream download a direct URL into a local temporary staging file safely.

    Hardened Features:
    - SSRF validation on initial URL AND every redirect target before requesting.
    - Full resource cleanup: closes response/session across success, errors, and cancellations.
    - Exponential backoff with jitter on retryable failures.
    - Streaming without buffering to RAM.
    """
    validate_url_security(url)
    os.makedirs(temp_root, exist_ok=True)
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        check_cancellation(cancel_event)
        temp_fd, temp_path = tempfile.mkstemp(dir=temp_root, prefix="dl_", suffix=".part")
        os.close(temp_fd)

        session = requests.Session()
        session_closed = False
        response: Optional[requests.Response] = None

        try:
            current_url = url
            redirect_count = 0

            # Step-by-step redirect following with strict SSRF validation per redirect
            while True:
                validate_url_security(current_url)

                if response is not None:
                    response.close()

                response = session.get(
                    current_url,
                    stream=True,
                    allow_redirects=False,
                    timeout=timeout,
                    headers={"User-Agent": "TelegramDriveBot/5.0"},
                )

                if response.is_redirect or response.status_code in {301, 302, 303, 307, 308}:
                    redirect_count += 1
                    if redirect_count > max_redirects:
                        raise NonRetryableTransferError(f"تم تجاوز الحد الأقصى لإعادة التوجيه ({max_redirects}).")

                    location = response.headers.get("Location")
                    if not location:
                        raise NonRetryableTransferError("استجابة إعادة التوجيه لا تحتوي على ترويسة Location.")

                    next_url = urllib.parse.urljoin(current_url, location)
                    current_url = next_url
                    continue
                else:
                    break

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
                else (cd_name or extract_filename_from_url(current_url))
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
            sanitized_src = sanitize_url_for_logging(url)
            logger.warning("Download attempt %d/%d failed for %s: %s", attempt, max_retries, sanitized_src, exc)

            if attempt < max_retries:
                backoff = compute_retry_backoff(attempt, base_delay=retry_base_delay, max_delay=retry_max_delay)
                try:
                    sleep_fn(backoff)
                except Exception:
                    pass
                check_cancellation(cancel_event)

        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
            if not session_closed:
                try:
                    session.close()
                except Exception:
                    pass

    raise RetryableTransferError(f"تعذر تنزيل الملف بعد {max_retries} محاولات: {last_error}")


def validate_destination_directory(destination: str) -> str:
    """Validate that Google Drive destination exists or can be created, is a directory, and is writable."""
    dest_path = os.path.abspath(destination)

    if os.path.exists(dest_path):
        if not os.path.isdir(dest_path):
            raise NonRetryableTransferError(f"مسار التخزين المحدد ليس مجلداً صالحاً: {dest_path}")
        if not os.access(dest_path, os.W_OK | os.X_OK):
            raise NonRetryableTransferError(f"لا توجد صلاحية كتابة في مجلد Google Drive: {dest_path}")
    else:
        parent = os.path.dirname(dest_path)
        if not os.path.exists(parent):
            if "/content/drive" in dest_path and not os.path.exists("/content/drive/MyDrive"):
                raise NonRetryableTransferError(
                    "وحدة تخزين Google Drive غير مثبتة (Unmounted)! يرجى تنفيذ drive.mount('/content/drive') في كولاب أولاً."
                )
        try:
            os.makedirs(dest_path, exist_ok=True)
        except OSError as exc:
            raise NonRetryableTransferError(f"تعذر إنشاء مجلد الوجهة في Google Drive ({exc}): {dest_path}") from exc

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

    drv_exists = os.path.exists(drive_path)
    drv_is_dir = os.path.isdir(drive_path) if drv_exists else False
    drv_writable = False
    if drv_exists and drv_is_dir:
        drv_writable = os.access(drive_path, os.W_OK | os.X_OK)

    is_mount = False
    if "/content/drive" in drive_path:
        is_mount = os.path.exists("/content/drive/MyDrive")
    else:
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


def clean_orphan_drive_partials(destination_dir: str, state_store: Any) -> List[str]:
    """Safely removes only orphaned .part_<job_id>_* files from Google Drive destination."""
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
    """Safely verify integrity, validate destination, check duplicate/collision, and copy to Google Drive destination."""
    check_cancellation(cancel_event)

    if not os.path.exists(temp_path):
        raise NonRetryableTransferError(f"الملف المؤقت المصدر غير موجود: {temp_path}")

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
