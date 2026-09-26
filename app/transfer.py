"""Core download, streaming, hashing, and Drive finalization engine."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import tempfile
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MB chunk for streaming and hashing
DEFAULT_TIMEOUT = (15, 60)  # (connect_timeout, read_timeout) in seconds
MAX_STREAM_SIZE = 100 * 1024 * 1024 * 1024  # 100 GB safety sanity limit


class TransferError(Exception):
    """Base class for transfer failures."""
    pass


class NonRetryableTransferError(TransferError):
    """Explicitly non-retryable transfer failure (e.g. 404, 403, bad request, cancelled)."""
    pass


class RetryableTransferError(TransferError):
    """Failure that may succeed upon a subsequent retry attempt (timeouts, 5xx)."""
    pass


@dataclass
class FinalizeResult:
    destination_path: str
    sha256: str
    size: int
    is_duplicate: bool
    action: str  # "copied", "duplicate_skipped", "collision_renamed"


def safe_filename(name: str, fallback: str = "download.bin") -> str:
    """Sanitize arbitrary input filename preventing path traversal and unsafe characters."""
    if not name:
        return fallback

    # Strip URL fragments / query parameters if accidentally passed
    cleaned = name.split("?")[0].split("#")[0]
    cleaned = os.path.basename(cleaned)
    # Remove control and reserved filesystem characters
    cleaned = re.sub(r'[\x00-\x1f\\/:\*\?"<>\|]', "_", cleaned)
    # Strip leading/trailing dots and spaces
    cleaned = cleaned.strip(". ")
    if not cleaned or cleaned in {".", ".."}:
        return fallback
    return cleaned[:255]


def extract_filename_from_url(url: str, default: str = "download.bin") -> str:
    """Extract a clean filename from a URL path, or return fallback."""
    try:
        parsed = urllib.parse.urlparse(url)
        path = urllib.parse.unquote(parsed.path)
        base = os.path.basename(path)
        return safe_filename(base, fallback=default)
    except Exception:
        return default


def parse_content_disposition(header: str) -> Optional[str]:
    """Parse filename from Content-Disposition header with RFC 5987 / RFC 6266 support."""
    if not header:
        return None

    # Try filename* (UTF-8) first
    match_star = re.search(r"filename\*\s*=\s*(?:UTF-8''|utf-8'')([^;]+)", header, re.IGNORECASE)
    if match_star:
        val = match_star.group(1).strip("\"' ")
        try:
            return safe_filename(urllib.parse.unquote(val))
        except Exception:
            pass

    # Standard filename="..."
    match_standard = re.search(r'filename\s*=\s*"([^"]+)"', header, re.IGNORECASE)
    if not match_standard:
        match_standard = re.search(r'filename\s*=\s*([^; ]+)', header, re.IGNORECASE)
    if match_standard:
        return safe_filename(match_standard.group(1).strip("\"' "))

    return None


def hash_file(filepath: str, chunk_size: int = CHUNK_SIZE) -> Tuple[str, int]:
    """Calculate SHA-256 hash and exact byte size incrementally."""
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
) -> Tuple[str, str, int]:
    """Stream download a direct URL into a temporary file safely.

    Returns:
        (temp_file_path, resolved_filename, byte_size)
    """
    os.makedirs(temp_root, exist_ok=True)
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        check_cancellation(cancel_event)
        temp_fd, temp_path = tempfile.mkstemp(dir=temp_root, prefix="dl_", suffix=".part")
        os.close(temp_fd)

        try:
            session = requests.Session()
            response = session.get(
                url,
                stream=True,
                allow_redirects=True,
                timeout=DEFAULT_TIMEOUT,
                headers={"User-Agent": "TelegramDriveBot/2.0"},
            )

            # Classify status code
            if response.status_code in {400, 401, 403, 404, 405, 410}:
                raise NonRetryableTransferError(f"فشل التحميل (رمز HTTP غير قابل لإعادة المحاولة: {response.status_code})")
            if response.status_code >= 500:
                raise RetryableTransferError(f"خطأ خادم مؤقت (HTTP {response.status_code})")
            response.raise_for_status()

            # Reject HTML landing pages masquerading as file downloads
            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" in content_type and not (custom_filename and custom_filename.endswith(".html")):
                raise NonRetryableTransferError("الرابط يشير إلى صفحة ويب (HTML) وليس إلى ملف تحميل مباشر.")

            # Resolve filename
            cd_name = parse_content_disposition(response.headers.get("Content-Disposition", ""))
            resolved_filename = (
                safe_filename(custom_filename)
                if custom_filename
                else (cd_name or extract_filename_from_url(response.url or url))
            )

            # Download streaming
            content_length_hdr = response.headers.get("Content-Length")
            total_expected = int(content_length_hdr) if content_length_hdr and content_length_hdr.isdigit() else None

            bytes_written = 0
            with open(temp_path, "wb") as out_file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    check_cancellation(cancel_event)
                    if chunk:
                        out_file.write(chunk)
                        bytes_written += len(chunk)
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


def _find_job(state_store: Any, job_id: str) -> Optional[Dict[str, Any]]:
    if not state_store or not hasattr(state_store, "data"):
        return None
    for j in state_store.data.get("jobs", []):
        if j.get("id") == str(job_id):
            return j
    return None


def finalize_to_drive(
    temp_path: str,
    filename: str,
    destination: str,
    state_store: Any,
    job_id: str,
    cancel_event: Optional[Any] = None,
) -> FinalizeResult:
    """Safely verify integrity, check duplicate/collision, and copy to Google Drive destination.

    Crash-Safe & Idempotency Rules:
    1. Pre-copy hash & size verification of source in staging.
    2. Duplicate content check (exact SHA-256 match in destination directory).
    3. Safe collision avoidance for same-name different-content files.
    4. Destination staging via '.part-<job_id>' temporary file inside destination.
    5. Post-copy SHA-256 and size verification of destination before replacing target.
    6. State update to 'completed'.
    7. Safe cleanup of local source temp file.
    """
    check_cancellation(cancel_event)

    if not os.path.exists(temp_path):
        raise NonRetryableTransferError(f"الملف المؤقت المصدر غير موجود: {temp_path}")

    # Step 1: Hash and measure local source
    source_sha, source_size = hash_file(temp_path)
    if source_size == 0:
        raise NonRetryableTransferError("الملف المؤقت فارغ (حجمه صفر).")

    os.makedirs(destination, exist_ok=True)
    target_filename = safe_filename(filename)
    target_path = os.path.join(destination, target_filename)

    state_store.update_job(
        job_id,
        status="verifying",
        sha256=source_sha,
        size=source_size,
        temp_path=temp_path,
        destination_path=target_path,
    )

    # Step 2: Content duplicate & collision resolution
    action = "copied"
    is_duplicate = False

    # Check if target_path exists
    if os.path.exists(target_path):
        try:
            existing_sha, existing_size = hash_file(target_path)
            if existing_size == source_size and existing_sha == source_sha:
                # Exact identical file already exists at target
                logger.info("Identical file already exists at destination: %s", target_path)
                state_store.update_job(job_id, status="completed", destination_path=target_path)
                # Cleanup local temp
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

        # Same filename, different content -> Resolve collision deterministically
        action = "collision_renamed"
        stem, ext = os.path.splitext(target_filename)
        counter = 1
        while os.path.exists(target_path):
            # Also check if collision candidate happens to be exact content duplicate
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
            target_path = os.path.join(destination, target_filename)
            counter += 1

    check_cancellation(cancel_event)

    # Step 3: Copy to destination using a job-tagged partial file for atomic landing
    dest_part_path = os.path.join(destination, f".part_{job_id}_{target_filename}")
    try:
        shutil.copyfile(temp_path, dest_part_path)

        check_cancellation(cancel_event)

        # Step 4: True integrity verification of destination file
        dest_sha, dest_size = hash_file(dest_part_path)
        if dest_size != source_size or dest_sha != source_sha:
            raise TransferError(
                f"فشل التحقق من تكامل الملف في درايف: المصدر ({source_sha}, {source_size}) != الهدف ({dest_sha}, {dest_size})"
            )

        # Step 5: Atomic move into final destination filename
        os.replace(dest_part_path, target_path)

    except Exception:
        if os.path.exists(dest_part_path):
            try:
                os.remove(dest_part_path)
            except OSError:
                pass
        raise

    # Step 6: Mark completed in state
    state_store.update_job(
        job_id,
        status="completed",
        destination_path=target_path,
        sha256=source_sha,
        size=source_size,
    )

    # Step 7: Clean up local staging temp file
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
