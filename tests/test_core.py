"""Comprehensive automated test suite for TelegramDriveBot Core Reliability, UX & Milestone 5 Advanced Job Manager."""

import asyncio
import hashlib
import json
import os
import tempfile
import threading
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.config import Config
from app.state import InvalidStateTransitionError, StateStore, now_utc_iso
from app.transfer import (
    FinalizeResult,
    NonRetryableTransferError,
    RetryableTransferError,
    clean_orphan_drive_partials,
    download_url,
    extract_filename_from_url,
    finalize_to_drive,
    get_storage_diagnostics,
    hash_file,
    parse_content_disposition,
    safe_filename,
    validate_destination_directory,
    validate_url_security,
)
from app.bot import File2URLProvider, TelegramDriveBotApp
from app.ui import ProgressTracker, format_bytes, format_duration, humanize_error


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as staging, tempfile.TemporaryDirectory() as drive:
        yield staging, drive


# ---------------------------------------------------------
# 1. State Machine & Transition Tests (Milestone 1 Core)
# ---------------------------------------------------------

def test_state_store_legal_transitions(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    job = store.add_job("j1", "direct_url", "f.bin", 1, 2)
    assert job["status"] == "queued"

    j1 = store.update_job("j1", status="downloading")
    assert j1["status"] == "downloading"

    j2 = store.update_job("j1", status="downloaded")
    assert j2["status"] == "downloaded"

    j3 = store.update_job("j1", status="verifying")
    assert j3["status"] == "verifying"

    j4 = store.update_job("j1", status="completed")
    assert j4["status"] == "completed"


def test_state_store_illegal_transitions(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    store.add_job("j_illegal", "direct_url", "f.bin", 1, 2)

    with pytest.raises(InvalidStateTransitionError):
        store.update_job("j_illegal", status="completed")

    store.update_job("j_illegal", status="downloading")

    with pytest.raises(InvalidStateTransitionError):
        store.update_job("j_illegal", status="completed")


def test_state_store_retry_and_recovery(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    store.add_job("j_retry", "direct_url", "f.bin", 1, 2)
    store.update_job("j_retry", status="failed")

    with pytest.raises(InvalidStateTransitionError):
        store.update_job("j_retry", status="downloading")

    retried = store.retry_job("j_retry")
    assert retried["status"] == "queued"
    assert retried["recovery_from_status"] == "failed"

    store.update_job("j_retry", status="downloading")
    recovered = store.recover_job("j_retry", "queued")
    assert recovered["status"] == "queued"
    assert recovered["recovery_from_status"] == "downloading"


def test_terminal_state_statistics(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    store.add_job("j1", "direct_url", "f1.bin", 1, 2)
    store.update_job("j1", status="failed")
    assert store.data["stats"]["failed"] == 1

    store.retry_job("j1")
    assert store.data["stats"]["failed"] == 0

    store.update_job("j1", status="failed")
    assert store.data["stats"]["failed"] == 1

    reloaded = StateStore(state_file)
    assert reloaded.data["stats"]["failed"] == 1
    assert reloaded.data["stats"]["completed"] == 0


# ---------------------------------------------------------
# 2. Filename, Content Disposition, and Hashing (Milestone 1)
# ---------------------------------------------------------

def test_safe_filename():
    assert safe_filename("normal.pdf") == "normal.pdf"
    assert safe_filename("../../etc/passwd") == "passwd"
    assert safe_filename("test?query=1#frag") == "test"
    assert safe_filename("") == "download.bin"
    assert safe_filename("   ") == "download.bin"
    assert safe_filename("bad:name*<>.txt") == "bad_name___.txt"


def test_parse_content_disposition():
    assert parse_content_disposition('attachment; filename="report.pdf"') == "report.pdf"
    assert parse_content_disposition("attachment; filename*=UTF-8''my%20file.zip") == "my file.zip"
    assert parse_content_disposition("") is None


def test_hash_file_and_empty(temp_dirs):
    staging, _ = temp_dirs
    filepath = os.path.join(staging, "sample.txt")
    with open(filepath, "wb") as f:
        f.write(b"Hello TelegramDriveBot!")

    sha, size = hash_file(filepath)
    assert sha == hashlib.sha256(b"Hello TelegramDriveBot!").hexdigest()
    assert size == len(b"Hello TelegramDriveBot!")

    empty_path = os.path.join(staging, "empty.bin")
    with open(empty_path, "wb") as f:
        pass
    e_sha, e_size = hash_file(empty_path)
    assert e_size == 0
    assert e_sha == hashlib.sha256(b"").hexdigest()


# ---------------------------------------------------------
# 3. Finalize to Drive, Duplicates, and Collisions (Milestone 1)
# ---------------------------------------------------------

def test_finalize_to_drive_success(temp_dirs):
    staging, drive = temp_dirs
    src = os.path.join(staging, "src.dat")
    with open(src, "wb") as f:
        f.write(b"Reliable core payload")

    store = StateStore(os.path.join(drive, "state.json"))
    store.add_job("j_fin", "direct_url", "data.bin", 1, 2)
    store.update_job("j_fin", status="downloading")
    store.update_job("j_fin", status="downloaded")

    res = finalize_to_drive(src, "data.bin", drive, store, "j_fin")

    assert res.action == "copied"
    assert not res.is_duplicate
    assert os.path.exists(res.destination_path)
    assert not os.path.exists(src)

    dest_sha, dest_size = hash_file(res.destination_path)
    assert dest_sha == res.sha256
    assert dest_size == res.size
    assert store.get_job("j_fin")["status"] == "completed"


def test_finalize_duplicate_content(temp_dirs):
    staging, drive = temp_dirs
    dest_existing = os.path.join(drive, "doc.pdf")
    with open(dest_existing, "wb") as f:
        f.write(b"Same exact bytes")

    src = os.path.join(staging, "doc_temp.pdf")
    with open(src, "wb") as f:
        f.write(b"Same exact bytes")

    store = StateStore(os.path.join(drive, "state.json"))
    store.add_job("j_dup", "direct_url", "doc.pdf", 1, 2)
    store.update_job("j_dup", status="downloading")
    store.update_job("j_dup", status="downloaded")

    res = finalize_to_drive(src, "doc.pdf", drive, store, "j_dup")

    assert res.is_duplicate is True
    assert res.action == "duplicate_skipped"
    assert res.destination_path == dest_existing
    assert not os.path.exists(src)
    assert store.get_job("j_dup")["status"] == "completed"


def test_finalize_filename_collision(temp_dirs):
    staging, drive = temp_dirs
    dest_existing = os.path.join(drive, "file.txt")
    with open(dest_existing, "wb") as f:
        f.write(b"Original file version")

    src = os.path.join(staging, "file.txt")
    with open(src, "wb") as f:
        f.write(b"New different file version")

    store = StateStore(os.path.join(drive, "state.json"))
    store.add_job("j_col", "direct_url", "file.txt", 1, 2)
    store.update_job("j_col", status="downloading")
    store.update_job("j_col", status="downloaded")

    res = finalize_to_drive(src, "file.txt", drive, store, "j_col")

    assert res.action == "collision_renamed"
    assert res.destination_path == os.path.join(drive, "file (1).txt")
    assert os.path.exists(dest_existing)
    assert os.path.exists(res.destination_path)


def test_finalize_multiple_collisions(temp_dirs):
    staging, drive = temp_dirs

    for name, content in [("data.txt", b"v0"), ("data (1).txt", b"v1"), ("data (2).txt", b"v2")]:
        with open(os.path.join(drive, name), "wb") as f:
            f.write(content)

    src = os.path.join(staging, "data.txt")
    with open(src, "wb") as f:
        f.write(b"new v3")

    store = StateStore(os.path.join(drive, "state.json"))
    store.add_job("j_col3", "direct_url", "data.txt", 1, 2)
    store.update_job("j_col3", status="downloading")
    store.update_job("j_col3", status="downloaded")

    res = finalize_to_drive(src, "data.txt", drive, store, "j_col3")

    assert res.action == "collision_renamed"
    assert res.destination_path == os.path.join(drive, "data (3).txt")
    assert os.path.exists(res.destination_path)
    assert not os.path.exists(src)


def test_duplicate_recovery_when_target_already_completed_on_drive(temp_dirs):
    staging, drive = temp_dirs

    content = b"Content transferred successfully before crash"
    sha, size = hashlib.sha256(content).hexdigest(), len(content)

    target_path = os.path.join(drive, "crash_target.bin")
    with open(target_path, "wb") as f:
        f.write(content)

    local_staging = os.path.join(staging, "crash_staging.bin")
    with open(local_staging, "wb") as f:
        f.write(content)

    store = StateStore(os.path.join(drive, "state.json"))
    store.add_job("j_crash", "direct_url", "crash_target.bin", 1, 2)
    store.update_job("j_crash", status="downloading")
    store.update_job("j_crash", status="downloaded", temp_path=local_staging, sha256=sha, size=size)

    res = finalize_to_drive(local_staging, "crash_target.bin", drive, store, "j_crash")

    assert res.is_duplicate is True
    assert res.action == "duplicate_skipped"
    assert res.destination_path == target_path
    assert not os.path.exists(os.path.join(drive, "crash_target (1).bin"))
    assert store.get_job("j_crash")["status"] == "completed"
    assert not os.path.exists(local_staging)


# ---------------------------------------------------------
# 4. Safe Orphan Drive Partial Cleanup (Milestone 1)
# ---------------------------------------------------------

def test_clean_orphan_drive_partials(temp_dirs):
    _, drive = temp_dirs
    store = StateStore(os.path.join(drive, "state.json"))

    store.add_job("j_active", "direct_url", "active.bin", 1, 2)
    part_active = os.path.join(drive, ".part_j_active_active.bin")
    with open(part_active, "wb") as f:
        f.write(b"in flight")

    store.add_job("j_done", "direct_url", "done.bin", 1, 2)
    store.update_job("j_done", status="completed")
    part_done = os.path.join(drive, ".part_j_done_done.bin")
    with open(part_done, "wb") as f:
        f.write(b"leftover")

    unrelated = os.path.join(drive, ".part_notmatchingconvention")
    with open(unrelated, "wb") as f:
        f.write(b"do not touch")

    cleaned = clean_orphan_drive_partials(drive, store)
    assert part_done in cleaned
    assert not os.path.exists(part_done)

    assert os.path.exists(part_active)
    assert os.path.exists(unrelated)


# ---------------------------------------------------------
# 5. File2URL Serialized Provider Tests (Milestone 1)
# ---------------------------------------------------------

@pytest.mark.asyncio
async def test_file2url_success_and_cleanup():
    provider = File2URLProvider("file2url_rbot", timeout=5)

    fut = await provider.register_waiter("job_f1")
    assert not fut.done()

    delivered = await provider.complete_waiter("https://cdn.example.com/file.mp4")
    assert delivered is True
    res = await fut
    assert res == "https://cdn.example.com/file.mp4"

    dropped = await provider.complete_waiter("https://cdn.example.com/stale.mp4")
    assert dropped is False


@pytest.mark.asyncio
async def test_file2url_cancellation():
    provider = File2URLProvider("file2url_rbot", timeout=5)

    fut = await provider.register_waiter("job_f2")
    await provider.cancel_waiter("job_f2")
    assert fut.cancelled()


# ---------------------------------------------------------
# 6. Cancellation & Worker Safety (Milestone 1)
# ---------------------------------------------------------

@pytest.mark.asyncio
async def test_queued_job_cancellation_skips_worker(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=123456,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    job = app.state.add_job("j_cancel_q", "direct_url", "test.bin", 123456, 123456, source_url="http://mock.com/t.bin")
    await app.safe_enqueue_job(job)

    app.state.update_job("j_cancel_q", status="cancelled")

    with patch.object(app, "process_job", new_callable=AsyncMock) as mock_process:
        worker_task = asyncio.create_task(app.worker_loop())
        await app.queue.join()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        mock_process.assert_not_called()
        assert app.state.get_job("j_cancel_q")["status"] == "cancelled"


# ---------------------------------------------------------
# 7. Deterministic Recovery Tests (Milestone 1)
# ---------------------------------------------------------

def test_restore_unfinished_branches(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=123456,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    part_stage = os.path.join(staging, "part1.bin")
    with open(part_stage, "wb") as f:
        f.write(b"broken")
    app.state.add_job("j_dl", "direct_url", "f1.bin", 1, 2)
    app.state.update_job("j_dl", status="downloading", temp_path=part_stage)

    valid_stage = os.path.join(staging, "valid.bin")
    with open(valid_stage, "wb") as f:
        f.write(b"intact content")
    v_sha, v_size = hash_file(valid_stage)
    app.state.add_job("j_valid", "direct_url", "valid.bin", 1, 2)
    app.state.update_job("j_valid", status="downloading")
    app.state.update_job("j_valid", status="downloaded", temp_path=valid_stage, sha256=v_sha, size=v_size)

    app.state.add_job("j_lost", "direct_url", "lost.bin", 1, 2)
    app.state.update_job("j_lost", status="downloading")
    app.state.update_job("j_lost", status="downloaded", temp_path=os.path.join(staging, "missing.bin"))

    dest_part = os.path.join(drive, ".part_j_ver_valid2.bin")
    with open(dest_part, "wb") as f:
        f.write(b"leftover dest part")
    valid_stage2 = os.path.join(staging, "valid2.bin")
    with open(valid_stage2, "wb") as f:
        f.write(b"intact content 2")
    v_sha2, v_size2 = hash_file(valid_stage2)
    app.state.add_job("j_ver", "direct_url", "valid2.bin", 1, 2)
    app.state.update_job("j_ver", status="downloading")
    app.state.update_job("j_ver", status="downloaded")
    app.state.update_job("j_ver", status="verifying", temp_path=valid_stage2, sha256=v_sha2, size=v_size2)

    app.restore_unfinished()

    assert not os.path.exists(part_stage)
    assert app.state.get_job("j_dl")["status"] == "queued"

    assert app.state.get_job("j_valid")["status"] == "downloaded"
    assert os.path.exists(valid_stage)

    assert app.state.get_job("j_lost")["status"] == "queued"

    assert not os.path.exists(dest_part)
    assert app.state.get_job("j_ver")["status"] == "downloaded"


def test_recovery_idempotency(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=123456,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    valid_stage = os.path.join(staging, "idem.bin")
    with open(valid_stage, "wb") as f:
        f.write(b"stable content")
    v_sha, v_size = hash_file(valid_stage)

    app.state.add_job("j_idem", "direct_url", "idem.bin", 1, 2)
    app.state.update_job("j_idem", status="downloading")
    app.state.update_job("j_idem", status="downloaded", temp_path=valid_stage, sha256=v_sha, size=v_size)

    app.restore_unfinished()
    state_after_1 = json.dumps(app.state.data, sort_keys=True)
    q_size_1 = app.queue.qsize()

    while not app.queue.empty():
        app.queue.get_nowait()
    app.queued_ids.clear()

    app.restore_unfinished()
    state_after_2 = json.dumps(app.state.data, sort_keys=True)
    q_size_2 = app.queue.qsize()

    assert q_size_1 == q_size_2 == 1
    assert app.state.get_job("j_idem")["status"] == "downloaded"
    assert len(app.state.all_jobs()) == 1


# ---------------------------------------------------------
# 8. Metadata & Download Errors (Milestone 1)
# ---------------------------------------------------------

def test_telegram_message_id_persisted(temp_dirs):
    _, drive = temp_dirs
    store = StateStore(os.path.join(drive, "state.json"))

    job = store.add_job(
        job_id="j_msg",
        source_type="telegram_large",
        filename="video.mp4",
        chat_id=123,
        user_id=456,
        telegram_file_id="fid_123",
        telegram_message_id=98765,
    )
    assert job["telegram_message_id"] == 98765

    reloaded = StateStore(os.path.join(drive, "state.json"))
    assert reloaded.get_job("j_msg")["telegram_message_id"] == 98765


def test_download_error_classification(temp_dirs):
    import requests
    staging, _ = temp_dirs

    class MockResponse:
        def __init__(self, status_code, content=b"", headers=None):
            self.status_code = status_code
            self._content = content
            self.headers = headers or {}
            self.url = "http://example.com/file.bin"

        def raise_for_status(self):
            if 400 <= self.status_code < 600:
                raise requests.HTTPError(f"HTTP {self.status_code}")

        def iter_content(self, chunk_size=1024):
            yield self._content

    with patch("requests.Session.get", return_value=MockResponse(404)):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://example.com/404", staging, max_retries=1)

    with patch("requests.Session.get", return_value=MockResponse(403)):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://example.com/403", staging, max_retries=1)

    with patch("requests.Session.get", return_value=MockResponse(200, b"<html></html>", {"Content-Type": "text/html"})):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://example.com/html", staging, max_retries=1)

    with patch("requests.Session.get", return_value=MockResponse(200, b"", {"Content-Type": "application/octet-stream"})):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://example.com/empty", staging, max_retries=1)

    with patch("requests.Session.get", return_value=MockResponse(500)):
        with pytest.raises(RetryableTransferError):
            download_url("http://example.com/500", staging, max_retries=2)

    with patch("requests.Session.get", return_value=MockResponse(429)):
        with pytest.raises(RetryableTransferError):
            download_url("http://example.com/429", staging, max_retries=2)


def test_owner_authorization(temp_dirs):
    _, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=999888,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR="/tmp",
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    mock_auth_update = MagicMock()
    mock_auth_update.effective_user.id = 999888
    assert app.is_authorized(mock_auth_update) is True

    mock_unauth_update = MagicMock()
    mock_unauth_update.effective_user.id = 111222
    assert app.is_authorized(mock_unauth_update) is False


# ---------------------------------------------------------
# 9. UI, Commands & Failure Isolation (Milestone 2)
# ---------------------------------------------------------

def test_ui_helpers_formatting():
    assert format_bytes(500) == "500 B"
    assert format_bytes(1536) == "1.5 KB"
    assert format_bytes(5 * 1024 * 1024) == "5.00 MB"
    assert format_bytes(2 * 1024 * 1024 * 1024) == "2.00 GB"
    assert format_bytes(None) == "غير معروف"

    assert format_duration(35) == "35s"
    assert format_duration(95) == "1m 35s"
    assert format_duration(3665) == "1h 1m"
    assert format_duration(None) == "--"


def test_humanize_error_translations():
    err_404 = humanize_error(Exception("فشل التحميل (رمز HTTP غير قابل لإعادة المحاولة: 404)"))
    assert "غير موجود" in err_404

    err_403 = humanize_error(Exception("403 Forbidden"))
    assert "تم رفض الوصول" in err_403

    err_html = humanize_error(Exception("الرابط يشير إلى صفحة ويب (HTML)"))
    assert "صفحة ويب" in err_html

    err_cancel = humanize_error(Exception("تم إلغاء عملية النقل بواسطة المستخدم."))
    assert "تم إلغاء العملية بأمر منك" in err_cancel

    err_empty = humanize_error(Exception("الملف فارغ بحجم صفر بايت"))
    assert "فارغ" in err_empty

    err_secret = humanize_error(Exception("SensitiveToken123456 unexpected failure"))
    assert "SensitiveToken123456" not in err_secret
    assert "تعذر إكمال عملية النقل" in err_secret


def test_progress_tracker_throttling():
    tracker = ProgressTracker("test.zip", min_interval=2.0)
    assert tracker.should_update(100, 1000) is True
    assert tracker.should_update(200, 1000) is False

    text = tracker.build_progress_text(500, 1000)
    assert "test.zip" in text
    assert "50.0%" in text

    text_unknown = tracker.build_progress_text(500, None)
    assert "غير محدد" in text_unknown


@pytest.mark.asyncio
async def test_cmd_start_and_help(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    update = MagicMock()
    update.effective_user.id = 777
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock()

    await app.cmd_start(update, context)
    update.effective_message.reply_text.assert_called_once()
    start_reply = update.effective_message.reply_text.call_args[0][0]
    assert "TelegramDriveBot" in start_reply
    assert "/status" in start_reply

    update.effective_message.reply_text.reset_mock()
    await app.cmd_help(update, context)
    help_reply = update.effective_message.reply_text.call_args[0][0]
    assert "/retry" in help_reply
    assert "/cancel" in help_reply


@pytest.mark.asyncio
async def test_cmd_status_and_history(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    app.state.add_job("j_done1", "direct_url", "file1.bin", 777, 777)
    app.state.update_job("j_done1", status="completed", size=1024 * 1024)

    app.state.add_job("j_act", "direct_url", "file_active.bin", 777, 777)
    app.state.update_job("j_act", status="downloading", size=2 * 1024 * 1024)
    app.active_jobs["j_act"] = app.state.get_job("j_act")

    update = MagicMock()
    update.effective_user.id = 777
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock()

    await app.cmd_status(update, context)
    status_text = update.effective_message.reply_text.call_args[0][0]
    assert "file_active.bin" in status_text
    assert "j_act" in status_text

    update.effective_message.reply_text.reset_mock()
    await app.cmd_history(update, context)
    hist_text = update.effective_message.reply_text.call_args[0][0]
    assert "file1.bin" in hist_text
    assert "j_done1" in hist_text


@pytest.mark.asyncio
async def test_cmd_cancel_modes(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    update = MagicMock()
    update.effective_user.id = 777
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock()

    context.args = []
    await app.cmd_cancel(update, context)
    assert "لا توجد عملية نشطة" in update.effective_message.reply_text.call_args[0][0]

    app.state.add_job("j_active_canc", "direct_url", "act.bin", 777, 777)
    app.state.update_job("j_active_canc", status="downloading")
    app.active_jobs["j_active_canc"] = app.state.get_job("j_active_canc")
    evt = threading.Event()
    app.cancel_events["j_active_canc"] = evt

    update.effective_message.reply_text.reset_mock()
    await app.cmd_cancel(update, context)
    assert evt.is_set()
    assert "تم إلغاء المهمة" in update.effective_message.reply_text.call_args[0][0]
    assert app.state.get_job("j_active_canc")["status"] == "cancelled"

    app.state.add_job("j_done_canc", "direct_url", "done.bin", 777, 777)
    app.state.update_job("j_done_canc", status="completed")
    context.args = ["j_done_canc"]
    update.effective_message.reply_text.reset_mock()
    await app.cmd_cancel(update, context)
    assert "مكتملة بالفعل ولا يمكن إلغاؤها" in update.effective_message.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_cmd_retry_validation(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    update = MagicMock()
    update.effective_user.id = 777
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock()

    context.args = []
    await app.cmd_retry(update, context)
    assert "يرجى تحديد معرّف المهمة" in update.effective_message.reply_text.call_args[0][0]

    app.state.add_job("j_act_ret", "direct_url", "a.bin", 777, 777)
    app.state.update_job("j_act_ret", status="downloading")
    context.args = ["j_act_ret"]
    update.effective_message.reply_text.reset_mock()
    await app.cmd_retry(update, context)
    assert "جارية أو في الانتظار بالفعل" in update.effective_message.reply_text.call_args[0][0]

    app.state.add_job("j_comp_ret", "direct_url", "c.bin", 777, 777)
    app.state.update_job("j_comp_ret", status="completed")
    context.args = ["j_comp_ret"]
    update.effective_message.reply_text.reset_mock()
    await app.cmd_retry(update, context)
    assert "مكتملة بنجاح" in update.effective_message.reply_text.call_args[0][0]

    app.state.add_job("j_fail_ret", "direct_url", "f.bin", 777, 777)
    app.state.update_job("j_fail_ret", status="failed")
    context.args = ["j_fail_ret"]
    update.effective_message.reply_text.reset_mock()
    await app.cmd_retry(update, context)
    assert "تمت إعادة جدولة المهمة" in update.effective_message.reply_text.call_args[0][0]
    assert app.state.get_job("j_fail_ret")["status"] == "queued"
    assert app.queue.qsize() == 1


@pytest.mark.asyncio
async def test_ui_failure_isolation():
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION="/tmp",
        LOCAL_STAGING_DIR="/tmp",
        STATE_PATH="/tmp/state.json",
    )
    app = TelegramDriveBotApp(cfg)
    app.application = MagicMock()
    app.application.bot.edit_message_text = AsyncMock(side_effect=Exception("Telegram Network Timeout"))

    res = await app.safe_edit_text(123, 456, "Sample text")
    assert res is False


# ---------------------------------------------------------
# 10. Download Engine Hardening (Milestone 3)
# ---------------------------------------------------------

def test_url_security_ssrf_and_schemes():
    validate_url_security("https://example.com/file.zip")
    validate_url_security("http://cdn.example.org:8080/data?key=123")

    with pytest.raises(NonRetryableTransferError) as exc:
        validate_url_security("ftp://example.com/file.zip")
    assert "غير مدعوم" in str(exc.value)

    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://localhost/admin")
    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://127.0.0.1:8000/secret")
    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://[::1]/secret")

    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://metadata.google.internal/computeMetadata/v1/")

    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://10.0.0.1/file")
    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://192.168.1.1/backup.tar")
    with pytest.raises(NonRetryableTransferError):
        validate_url_security("http://172.16.0.5/conf")


def test_unicode_and_arabic_safe_filename():
    assert safe_filename("تقرير_المشروع_2026.pdf") == "تقرير_المشروع_2026.pdf"
    assert safe_filename("ملف مستند هام.docx") == "ملف مستند هام.docx"
    assert safe_filename("../../ملف_سري.zip") == "ملف_سري.zip"
    assert safe_filename("path/to/ملف.tar.gz") == "ملف.tar.gz"


def test_download_streaming_and_content_length_limit(temp_dirs):
    staging, _ = temp_dirs

    class MockResponse:
        def __init__(self, status_code=200, headers=None, chunks=None):
            self.status_code = status_code
            self.headers = headers or {}
            self.url = "https://example.com/stream.bin"
            self._chunks = chunks or [b"chunk1", b"chunk2", b"chunk3"]

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=1024):
            for c in self._chunks:
                yield c

    with patch("requests.Session.get", return_value=MockResponse(headers={"Content-Length": "18"})):
        path, fname, size = download_url("https://example.com/stream.bin", staging)
        assert os.path.exists(path)
        assert size == 18
        assert fname == "stream.bin"
        os.remove(path)

    with patch("requests.Session.get", return_value=MockResponse(headers={"Content-Length": "1000"})):
        with pytest.raises(NonRetryableTransferError) as exc:
            download_url("https://example.com/stream.bin", staging, max_download_size=500)
        assert "يتجاوز الحد الأقصى" in str(exc.value)

    with patch("requests.Session.get", return_value=MockResponse(headers={}, chunks=[b"a" * 300, b"b" * 300])):
        with pytest.raises(NonRetryableTransferError) as exc:
            download_url("https://example.com/stream.bin", staging, max_download_size=500)
        assert "تجاوز الحد الأقصى" in str(exc.value)


def test_download_cancellation_during_streaming_cleans_partial(temp_dirs):
    staging, _ = temp_dirs
    cancel_evt = threading.Event()

    class CancellableResponse:
        def __init__(self):
            self.status_code = 200
            self.headers = {"Content-Length": "5000"}
            self.url = "https://example.com/large.bin"

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=1024):
            yield b"first chunk"
            cancel_evt.set()
            yield b"second chunk"

    with patch("requests.Session.get", return_value=CancellableResponse()):
        with pytest.raises(NonRetryableTransferError) as exc:
            download_url("https://example.com/large.bin", staging, cancel_event=cancel_evt)
        assert "تم إلغاء عملية النقل" in str(exc.value)

    staged_files = os.listdir(staging)
    assert len(staged_files) == 0


def test_download_redirect_and_content_disposition_precedence(temp_dirs):
    staging, _ = temp_dirs

    class RedirectResponse:
        def __init__(self):
            self.status_code = 200
            self.headers = {
                "Content-Length": "12",
                "Content-Disposition": 'attachment; filename="final_document.pdf"',
            }
            self.url = "https://cdn.example.org/downloads/v1/download?id=999"

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size=1024):
            yield b"valid content"

    with patch("requests.Session.get", return_value=RedirectResponse()):
        path, resolved_name, size = download_url(
            "https://short.link/xyz",
            staging,
            custom_filename=None,
        )
        assert resolved_name == "final_document.pdf"
        assert size == 13
        os.remove(path)


# ---------------------------------------------------------
# 11. Drive Storage Intelligence & Validation (Milestone 4)
# ---------------------------------------------------------

def test_validate_destination_directory_success(temp_dirs):
    _, drive = temp_dirs
    target_sub = os.path.join(drive, "subfolder", "target")
    validated = validate_destination_directory(target_sub)
    assert os.path.exists(validated)
    assert os.path.isdir(validated)


def test_validate_destination_directory_is_file(temp_dirs):
    _, drive = temp_dirs
    file_path = os.path.join(drive, "some_file.txt")
    with open(file_path, "w") as f:
        f.write("I am a file")

    with pytest.raises(NonRetryableTransferError) as exc:
        validate_destination_directory(file_path)
    assert "ليس مجلداً صالحاً" in str(exc.value)


def test_validate_destination_unmounted_colab():
    with pytest.raises(NonRetryableTransferError) as exc:
        validate_destination_directory("/content/drive/MyDrive/NonExistentTestDir")
    assert "غير مثبتة" in str(exc.value) or "Unmounted" in str(exc.value)


def test_get_storage_diagnostics(temp_dirs):
    staging, drive = temp_dirs
    diag = get_storage_diagnostics(staging, drive)

    assert diag.staging_exists is True
    assert diag.staging_writable is True
    assert diag.staging_free_bytes is not None
    assert diag.staging_free_bytes > 0
    assert diag.drive_exists is True
    assert diag.drive_is_dir is True
    assert diag.drive_writable is True


@pytest.mark.asyncio
async def test_cmd_storage(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    update = MagicMock()
    update.effective_user.id = 777
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock()

    await app.cmd_storage(update, context)
    update.effective_message.reply_text.assert_called_once()
    msg = update.effective_message.reply_text.call_args[0][0]
    assert "تشخيص وسائط التخزين" in msg
    assert "Google Drive Destination" in msg
    assert "Local Staging" in msg


# =========================================================
# MILESTONE 5: Advanced Job Manager Tests
# =========================================================

def test_job_metadata_lifecycle_timestamps(temp_dirs):
    _, drive = temp_dirs
    store = StateStore(os.path.join(drive, "state.json"))

    job = store.add_job("j_meta", "direct_url", "test.bin", 1, 2)
    assert job["created_at"] is not None
    assert job["started_at"] is None
    assert job["completed_at"] is None

    store.update_job("j_meta", status="downloading")
    j_dl = store.get_job("j_meta")
    assert j_dl["started_at"] is not None

    store.update_job("j_meta", status="downloaded")
    store.update_job("j_meta", status="verifying")
    store.update_job("j_meta", status="completed")
    j_comp = store.get_job("j_meta")
    assert j_comp["completed_at"] is not None


def test_state_retention_preserves_active_jobs(temp_dirs):
    _, drive = temp_dirs
    # Configure tight max_history of 50
    store = StateStore(os.path.join(drive, "state.json"), max_history=50)

    # Add 55 completed jobs
    for i in range(55):
        j_id = f"c_{i}"
        store.add_job(j_id, "direct_url", f"file_{i}.bin", 1, 2)
        store.update_job(j_id, status="downloading")
        store.update_job(j_id, status="downloaded")
        store.update_job(j_id, status="verifying")
        store.update_job(j_id, status="completed")

    # Add 2 active queued jobs
    store.add_job("active_1", "direct_url", "a1.bin", 1, 2)
    store.add_job("active_2", "direct_url", "a2.bin", 1, 2)

    # Active jobs must NEVER be pruned
    all_j = store.all_jobs()
    ids = {j["id"] for j in all_j}
    assert "active_1" in ids
    assert "active_2" in ids
    # Oldest completed jobs should have been pruned to keep under limit
    assert "c_0" not in ids
    assert "c_54" in ids


@pytest.mark.asyncio
async def test_queue_duplicate_enqueue_prevention(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    job = app.state.add_job("j_dup_q", "direct_url", "dup.bin", 777, 777)

    # First enqueue succeeds
    first = await app.safe_enqueue_job(job)
    assert first is True
    assert app.queue.qsize() == 1

    # Second enqueue of identical job ID is rejected
    second = await app.safe_enqueue_job(job)
    assert second is False
    assert app.queue.qsize() == 1


@pytest.mark.asyncio
async def test_cmd_status_job_detail_mode(temp_dirs):
    staging, drive = temp_dirs
    cfg = Config(
        TELEGRAM_BOT_TOKEN="mock_token",
        OWNER_ID=777,
        DRIVE_DESTINATION=drive,
        LOCAL_STAGING_DIR=staging,
        STATE_PATH=os.path.join(drive, "state.json"),
    )
    app = TelegramDriveBotApp(cfg)

    app.state.add_job("job_detail_1", "direct_url", "report.pdf", 777, 777)
    app.state.update_job("job_detail_1", status="downloading")
    app.state.update_job("job_detail_1", status="downloaded")
    app.state.update_job("job_detail_1", status="verifying")
    app.state.update_job("job_detail_1", status="completed", sha256="abcd1234ef", size=1024 * 1024, destination_path="/drive/report.pdf")

    update = MagicMock()
    update.effective_user.id = 777
    update.effective_message.reply_text = AsyncMock()
    context = MagicMock()

    # /status job_detail_1
    context.args = ["job_detail_1"]
    await app.cmd_status(update, context)
    update.effective_message.reply_text.assert_called_once()
    msg = update.effective_message.reply_text.call_args[0][0]
    assert "تفاصيل المهمة" in msg
    assert "job_detail_1" in msg
    assert "report.pdf" in msg
    assert "abcd1234ef" in msg
    assert "completed" in msg
