import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

from .state import now


class TransferError(Exception):
    pass


def safe_filename(name: str, fallback: str = "file") -> str:
    name = unquote(name or "").strip().replace("\\", "_").replace("/", "_")
    name = re.sub(r"[\x00-\x1f<>:\"|?*]", "_", name)
    name = name.strip(" .")
    return name[:240] or fallback


def hash_file(path: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def known_hashes(state_store) -> set[str]:
    return {
        item.get("sha256")
        for item in state_store.data.get("history", [])
        if item.get("status") == "completed" and item.get("sha256")
    }


def download_url(url: str, temp_root: str) -> tuple[str, str]:
    os.makedirs(temp_root, exist_ok=True)
    response = requests.get(
        url,
        stream=True,
        timeout=(20, 60),
        allow_redirects=True,
        headers={"User-Agent": "TelegramDriveBot/1.0"},
    )
    response.raise_for_status()

    content_type = (response.headers.get("content-type") or "").lower()
    if "text/html" in content_type:
        raise TransferError("المصدر أعاد صفحة ويب بدل ملف قابل للتنزيل.")

    name = ""
    disposition = response.headers.get("content-disposition", "")
    match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)', disposition, re.I)
    if match:
        name = match.group(1)
    if not name:
        name = Path(urlparse(response.url).path).name
    name = safe_filename(name, "download")

    fd, path = tempfile.mkstemp(prefix="tdb_", suffix="_" + name, dir=temp_root)
    os.close(fd)
    try:
        with open(path, "wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
    except Exception:
        try:
            os.remove(path)
        except OSError:
            pass
        raise
    finally:
        response.close()

    if os.path.getsize(path) == 0:
        os.remove(path)
        raise TransferError("الملف الناتج فارغ.")
    return path, name


def finalize_to_drive(
    temp_path: str,
    filename: str,
    destination: str,
    state_store,
    source: str,
    job_id: str,
) -> dict:
    os.makedirs(destination, exist_ok=True)
    sha256, size = hash_file(temp_path)

    if sha256 in known_hashes(state_store):
        os.remove(temp_path)
        result = {
            "status": "duplicate",
            "filename": filename,
            "sha256": sha256,
            "size": size,
            "source": source,
            "job_id": job_id,
            "timestamp": now(),
        }
        state_store.add_history(result)
        return result

    target = os.path.join(destination, safe_filename(filename))
    if os.path.exists(target):
        stem, ext = os.path.splitext(target)
        counter = 1
        while os.path.exists(target):
            target = f"{stem} ({counter}){ext}"
            counter += 1

    state_store.update_job(job_id, status="verifying", filename=os.path.basename(target), sha256=sha256, size=size)
    shutil.copy2(temp_path, target)

    if not os.path.isfile(target) or os.path.getsize(target) != size:
        raise TransferError("تعذر التحقق من الملف بعد نقله إلى Drive.")

    state_store.update_job(job_id, status="completed", filename=os.path.basename(target), sha256=sha256, size=size)
    record = {
        "status": "completed",
        "filename": os.path.basename(target),
        "sha256": sha256,
        "size": size,
        "source": source,
        "method": "temporary_colab_to_mounted_drive",
        "job_id": job_id,
        "timestamp": now(),
    }
    state_store.add_history(record)
    os.remove(temp_path)
    return record
