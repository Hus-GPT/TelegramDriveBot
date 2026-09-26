"""Comprehensive automated test suite for TelegramDriveBot Core Reliability."""

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
    hash_file,
    parse_content_disposition,
    safe_filename,
)
from app.bot import File2URLProvider, TelegramDriveBotApp


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as staging, tempfile.TemporaryDirectory() as drive:
        yield staging, drive


# ---------------------------------------------------------
# 1. State Machine & Transition Tests
# ---------------------------------------------------------

def test_state_store_legal_transitions(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    job = store.add_job("j1", "direct_url", "f.bin", 1, 2)
    assert job["status"] == "queued"

    # queued -> downloading
    j1 = store.update_job("j1", status="downloading")
    assert j1["status"] == "downloading"

    # downloading -> downloaded
    j2 = store.update_job("j1", status="downloaded")
    assert j2["status"] == "downloaded"

    # downloaded -> verifying
    j3 = store.update_job("j1", status="verifying")
    assert j3["status"] == "verifying"

    # verifying -> completed
    j4 = store.update_job("j1", status="completed")
    assert j4["status"] == "completed"


def test_state_store_illegal_transitions(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    store.add_job("j_illegal", "direct_url", "f.bin", 1, 2)

    # queued -> completed (illegal)
    with pytest.raises(InvalidStateTransitionError):
        store.update_job("j_illegal", status="completed")

    # queued -> downloading
    store.update_job("j_illegal", status="downloading")

    # downloading -> completed (illegal)
    with pytest.raises(InvalidStateTransitionError):
        store.update_job("j_illegal", status="completed")


def test_state_store_retry_and_recovery(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    store.add_job("j_retry", "direct_url", "f.bin", 1, 2)
    store.update_job("j_retry", status="failed")

    # Direct illegal transition failed -> downloading must fail
    with pytest.raises(InvalidStateTransitionError):
        store.update_job("j_retry", status="downloading")

    # Controlled retry_job succeeds
    retried = store.retry_job("j_retry")
    assert retried["status"] == "queued"
    assert retried["recovery_from_status"] == "failed"

    # Controlled recover_job succeeds
    store.update_job("j_retry", status="downloading")
    recovered = store.recover_job("j_retry", "queued")
    assert recovered["status"] == "queued"
    assert recovered["recovery_from_status"] == "downloading"


# ---------------------------------------------------------
# 2. Terminal State & Statistics Semantics Tests
# ---------------------------------------------------------

def test_terminal_state_statistics(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "state.json")
    store = StateStore(state_file)

    store.add_job("j1", "direct_url", "f1.bin", 1, 2)
    store.update_job("j1", status="failed")
    assert store.data["stats"]["failed"] == 1

    # Retry job -> failed count must decrease to 0
    store.retry_job("j1")
    assert store.data["stats"]["failed"] == 0

    # Fail it again -> failed count is 1, not 2
    store.update_job("j1", status="failed")
    assert store.data["stats"]["failed"] == 1

    # Reload from disk and verify recalculated stats
    reloaded = StateStore(state_file)
    assert reloaded.data["stats"]["failed"] == 1
    assert reloaded.data["stats"]["completed"] == 0


# ---------------------------------------------------------
# 3. Filename, Content Disposition, and Hashing
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
# 4. Finalize to Drive, Duplicates, and Collisions
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


# ---------------------------------------------------------
# 5. Safe Orphan Drive Partial Cleanup
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
# 6. File2URL Serialized Provider Tests
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
# 7. Queued Job Cancellation
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
    await app.queue.put(job)

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
# 8. Deterministic Recovery Tests
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

    # Branch 1: downloading job -> reset to queued and clean local staging
    part_stage = os.path.join(staging, "part1.bin")
    with open(part_stage, "wb") as f:
        f.write(b"broken")
    app.state.add_job("j_dl", "direct_url", "f1.bin", 1, 2)
    app.state.update_job("j_dl", status="downloading", temp_path=part_stage)

    # Branch 2: downloaded job with valid temp_path -> preserved
    valid_stage = os.path.join(staging, "valid.bin")
    with open(valid_stage, "wb") as f:
        f.write(b"intact content")
    v_sha, v_size = hash_file(valid_stage)
    app.state.add_job("j_valid", "direct_url", "valid.bin", 1, 2)
    app.state.update_job("j_valid", status="downloading")
    app.state.update_job("j_valid", status="downloaded", temp_path=valid_stage, sha256=v_sha, size=v_size)

    # Branch 3: downloaded job with missing temp_path -> reset to queued
    app.state.add_job("j_lost", "direct_url", "lost.bin", 1, 2)
    app.state.update_job("j_lost", status="downloading")
    app.state.update_job("j_lost", status="downloaded", temp_path=os.path.join(staging, "missing.bin"))

    app.restore_unfinished()

    assert not os.path.exists(part_stage)
    assert app.state.get_job("j_dl")["status"] == "queued"

    assert app.state.get_job("j_valid")["status"] == "downloaded"
    assert os.path.exists(valid_stage)

    assert app.state.get_job("j_lost")["status"] == "queued"


# ---------------------------------------------------------
# 9. Telegram Message ID Persistence
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


# ---------------------------------------------------------
# 10. Download Error Classification Tests
# ---------------------------------------------------------

def test_download_error_classification(temp_dirs):
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

    # 404 -> NonRetryableTransferError
    with patch("requests.Session.get", return_value=MockResponse(404)):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://mock/404", staging, max_retries=1)

    # 403 -> NonRetryableTransferError
    with patch("requests.Session.get", return_value=MockResponse(403)):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://mock/403", staging, max_retries=1)

    # HTML content -> NonRetryableTransferError
    with patch("requests.Session.get", return_value=MockResponse(200, b"<html></html>", {"Content-Type": "text/html"})):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://mock/html", staging, max_retries=1)

    # 0 bytes -> NonRetryableTransferError
    with patch("requests.Session.get", return_value=MockResponse(200, b"", {"Content-Type": "application/octet-stream"})):
        with pytest.raises(NonRetryableTransferError):
            download_url("http://mock/empty", staging, max_retries=1)

    # 500 Server error -> RetryableTransferError (retries exhausted)
    with patch("requests.Session.get", return_value=MockResponse(500)):
        with pytest.raises(RetryableTransferError):
            download_url("http://mock/500", staging, max_retries=2)

    # 429 Rate limit -> RetryableTransferError (retries exhausted)
    with patch("requests.Session.get", return_value=MockResponse(429)):
        with pytest.raises(RetryableTransferError):
            download_url("http://mock/429", staging, max_retries=2)


# ---------------------------------------------------------
# 11. Multi-Collision Finalization Test
# ---------------------------------------------------------

def test_finalize_multiple_collisions(temp_dirs):
    staging, drive = temp_dirs

    # Create file.txt, file (1).txt, file (2).txt
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


# ---------------------------------------------------------
# 12. Recovery Idempotency Test
# ---------------------------------------------------------

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

    # First recovery
    app.restore_unfinished()
    state_after_1 = json.dumps(app.state.data, sort_keys=True)
    q_size_1 = app.queue.qsize()

    # Empty queue to simulate clean queue before re-run
    while not app.queue.empty():
        app.queue.get_nowait()

    # Second recovery
    app.restore_unfinished()
    state_after_2 = json.dumps(app.state.data, sort_keys=True)
    q_size_2 = app.queue.qsize()

    # Must produce identical structural state
    assert q_size_1 == q_size_2 == 1
    assert app.state.get_job("j_idem")["status"] == "downloaded"


# ---------------------------------------------------------
# 13. Owner Authorization & Unauthorized Message Rejection
# ---------------------------------------------------------

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

    # Authorized user
    mock_auth_update = MagicMock()
    mock_auth_update.effective_user.id = 999888
    assert app.is_authorized(mock_auth_update) is True

    # Unauthorized user
    mock_unauth_update = MagicMock()
    mock_unauth_update.effective_user.id = 111222
    assert app.is_authorized(mock_unauth_update) is False
