"""Automated test suite for TelegramDriveBot Core Reliability."""

import hashlib
import json
import os
import tempfile
import threading
import pytest

from app.state import StateStore, now_utc_iso
from app.transfer import (
    FinalizeResult,
    NonRetryableTransferError,
    RetryableTransferError,
    extract_filename_from_url,
    finalize_to_drive,
    hash_file,
    parse_content_disposition,
    safe_filename,
)


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as staging, tempfile.TemporaryDirectory() as drive:
        yield staging, drive


class DummyStateStore:
    def __init__(self):
        self.data = {"jobs": []}

    def update_job(self, job_id, **kwargs):
        for j in self.data["jobs"]:
            if j["id"] == job_id:
                j.update(kwargs)
                return j
        j = {"id": job_id, **kwargs}
        self.data["jobs"].append(j)
        return j


# 1. safe_filename tests
def test_safe_filename():
    assert safe_filename("normal.pdf") == "normal.pdf"
    assert safe_filename("../../etc/passwd") == "passwd"
    assert safe_filename("test?query=1#frag") == "test"
    assert safe_filename("") == "download.bin"
    assert safe_filename("   ") == "download.bin"
    assert safe_filename("bad:name*<>.txt") == "bad_name___.txt"


# 2. Content disposition parser
def test_parse_content_disposition():
    assert parse_content_disposition('attachment; filename="report.pdf"') == "report.pdf"
    assert parse_content_disposition("attachment; filename*=UTF-8''my%20file.zip") == "my file.zip"
    assert parse_content_disposition("") is None


# 3. Hash file
def test_hash_file_and_empty(temp_dirs):
    staging, _ = temp_dirs
    filepath = os.path.join(staging, "sample.txt")
    with open(filepath, "wb") as f:
        f.write(b"Hello TelegramDriveBot!")

    sha, size = hash_file(filepath)
    expected_sha = hashlib.sha256(b"Hello TelegramDriveBot!").hexdigest()
    assert sha == expected_sha
    assert size == len(b"Hello TelegramDriveBot!")

    empty_path = os.path.join(staging, "empty.bin")
    with open(empty_path, "wb") as f:
        pass
    e_sha, e_size = hash_file(empty_path)
    assert e_size == 0
    assert e_sha == hashlib.sha256(b"").hexdigest()


# 4. Finalize to Drive - Successful copy and verification
def test_finalize_to_drive_success(temp_dirs):
    staging, drive = temp_dirs
    src = os.path.join(staging, "src.dat")
    with open(src, "wb") as f:
        f.write(b"Reliable core payload")

    store = DummyStateStore()
    res = finalize_to_drive(src, "data.bin", drive, store, "job1")

    assert res.action == "copied"
    assert not res.is_duplicate
    assert os.path.exists(res.destination_path)
    assert not os.path.exists(src)  # Temporary local source cleaned up

    dest_sha, dest_size = hash_file(res.destination_path)
    assert dest_sha == res.sha256
    assert dest_size == res.size


# 5. Finalize to Drive - Duplicate content detection
def test_finalize_duplicate_content(temp_dirs):
    staging, drive = temp_dirs
    # Existing identical file in drive
    dest_existing = os.path.join(drive, "doc.pdf")
    with open(dest_existing, "wb") as f:
        f.write(b"Same exact bytes")

    # Staged identical file
    src = os.path.join(staging, "doc_temp.pdf")
    with open(src, "wb") as f:
        f.write(b"Same exact bytes")

    store = DummyStateStore()
    res = finalize_to_drive(src, "doc.pdf", drive, store, "job_dup")

    assert res.is_duplicate is True
    assert res.action == "duplicate_skipped"
    assert res.destination_path == dest_existing
    assert not os.path.exists(src)


# 6. Finalize to Drive - Collision avoidance (same name, different content)
def test_finalize_filename_collision(temp_dirs):
    staging, drive = temp_dirs
    dest_existing = os.path.join(drive, "file.txt")
    with open(dest_existing, "wb") as f:
        f.write(b"Original file version")

    src = os.path.join(staging, "file.txt")
    with open(src, "wb") as f:
        f.write(b"New different file version")

    store = DummyStateStore()
    res = finalize_to_drive(src, "file.txt", drive, store, "job_col")

    assert res.action == "collision_renamed"
    assert res.destination_path == os.path.join(drive, "file (1).txt")
    assert os.path.exists(dest_existing)
    assert os.path.exists(res.destination_path)


# 7. Finalize to Drive - Cancellation
def test_finalize_cancellation(temp_dirs):
    staging, drive = temp_dirs
    src = os.path.join(staging, "cancel.bin")
    with open(src, "wb") as f:
        f.write(b"content")

    cancel_evt = threading.Event()
    cancel_evt.set()

    store = DummyStateStore()
    with pytest.raises(NonRetryableTransferError):
        finalize_to_drive(src, "cancel.bin", drive, store, "job_c", cancel_event=cancel_evt)


# 8. StateStore - Atomic writes and schema resilience
def test_state_store_lifecycle(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, ".state", "state.json")
    store = StateStore(state_file)

    job = store.add_job("j1", "direct_url", "test.bin", 1234, 5678, source_url="http://example.com/test.bin")
    assert job["status"] == "queued"

    updated = store.update_job("j1", status="downloading")
    assert updated["status"] == "downloading"

    store.update_job("j1", status="completed")
    retrieved = store.get_job("j1")
    assert retrieved["status"] == "completed"
    assert store.data["stats"]["completed"] == 1

    # Reload store from disk
    reloaded = StateStore(state_file)
    assert reloaded.get_job("j1") is not None
    assert reloaded.get_job("j1")["status"] == "completed"


# 9. StateStore - Malformed state handling
def test_state_store_malformed(temp_dirs):
    _, drive = temp_dirs
    state_file = os.path.join(drive, "bad_state.json")
    with open(state_file, "w") as f:
        f.write("{ invalid json")

    store = StateStore(state_file)
    assert store.data["jobs"] == []
    assert os.path.exists(state_file)  # Restored valid JSON
