"""Параллельный воркер скачивания безлимита (photoslice).

Берёт pending файлы пачками, для каждой просит Яндекс собрать ZIP,
скачивает, распаковывает в DOWNLOAD_DIR/_unlim/, удаляет ZIP.

Можно запускать несколько процессов параллельно — каждый атомарно
забирает свою пачку из БД и не мешает другим.

Запуск:
  python unlim_worker.py --id 1
  python unlim_worker.py --id 2

Коды выхода: 0 работа закончена, 1 слишком много ошибок подряд,
3 сессия истекла (нужно обновить cookies.json).
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sqlite3
import sys
import time
import zipfile

import requests
from dotenv import load_dotenv

from unlim_common import (
    BASE,
    SessionExpired,
    ensure_schema,
    get_sk_uid,
    make_session,
    read_cookies,
)
from utils import sanitize_component

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT, ".env"))

COOKIES_FILE = os.path.join(PROJECT, "cookies.json")
DB_FILE = os.path.join(PROJECT, "unlim_state.db")
LOG_FILE = os.path.join(PROJECT, "logs", "unlim_worker.log")

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", os.path.join(PROJECT, "downloads")).rstrip("/\\")
EXTRACT_DIR = os.path.join(DOWNLOAD_DIR, "_unlim")
ZIP_TMP_DIR = os.path.join(DOWNLOAD_DIR, "_unlim_tmp_zips")
BATCH_SIZE = 250
MAX_ATTEMPTS = 5
MAX_FAILED_BATCHES_IN_ROW = 3
EXIT_TOO_MANY_ERRORS = 1
EXIT_SESSION_EXPIRED = 3

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, encoding="utf-8",
                    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")


def _connect() -> sqlite3.Connection:
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def migrate() -> None:
    c = _connect()
    try:
        ensure_schema(c)
        c.commit()
    finally:
        c.close()


def take_batch(worker_id: int, size: int) -> list[tuple[str, str]]:
    """Атомарно забирает пачку pending под себя (status='in_progress')."""
    c = _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        rows = c.execute(
            "SELECT file_id, COALESCE(name, '') FROM unlim_files "
            "WHERE status='pending' ORDER BY attempts, rowid LIMIT ?",
            (size,),
        ).fetchall()
        if not rows:
            c.execute("COMMIT")
            return []
        c.executemany(
            "UPDATE unlim_files SET status='in_progress', worker_id=?, "
            "assigned_at=CURRENT_TIMESTAMP WHERE file_id=?",
            [(worker_id, file_id) for file_id, _ in rows],
        )
        c.execute("COMMIT")
        return [(file_id, name) for file_id, name in rows]
    except Exception:
        try: c.execute("ROLLBACK")
        except sqlite3.Error: pass
        raise
    finally:
        c.close()


def mark_done(file_ids: list[str]) -> None:
    if not file_ids:
        return
    c = _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        c.executemany(
            "UPDATE unlim_files SET status='downloaded', downloaded_at=CURRENT_TIMESTAMP, "
            "worker_id=NULL, error=NULL WHERE file_id=?",
            [(i,) for i in file_ids],
        )
        c.execute("COMMIT")
    finally:
        c.close()


def mark_attempt_failed(file_ids: list[str], error: str) -> None:
    if not file_ids:
        return
    c = _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        c.executemany(
            "UPDATE unlim_files SET attempts=attempts+1, worker_id=NULL, error=?, "
            "status=CASE WHEN attempts+1 >= ? THEN 'failed' ELSE 'pending' END "
            "WHERE file_id=?",
            [(error[:500], MAX_ATTEMPTS, i) for i in file_ids],
        )
        c.execute("COMMIT")
    finally:
        c.close()


def release(file_ids: list[str]) -> None:
    if not file_ids:
        return
    c = _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        c.executemany(
            "UPDATE unlim_files SET status='pending', worker_id=NULL "
            "WHERE file_id=? AND status='in_progress'",
            [(i,) for i in file_ids],
        )
        c.execute("COMMIT")
    finally:
        c.close()


def recover_stuck(worker_id: int, timeout_sec: int = 1800) -> int:
    """Возвращает в pending пачку прошлого процесса с этим же id и всё, что зависло дольше timeout."""
    c = _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        n = c.execute(
            "UPDATE unlim_files SET status='pending', worker_id=NULL "
            "WHERE status='in_progress' AND (worker_id=? OR "
            "(julianday(CURRENT_TIMESTAMP) - julianday(assigned_at)) * 86400 > ?)",
            (worker_id, timeout_sec),
        ).rowcount
        c.execute("COMMIT")
        return n
    finally:
        c.close()


# ── HTTP ────────────────────────────────────────────────────────────────────


def prepare_zip(s: requests.Session, sk: str, uid: str, paths: list[str]) -> str | None:
    body = {"sk": sk,
            "connection_id": f"{uid}{int(time.time()*1000)}{random.randint(100,999)}",
            "apiMethod": "mpfs/bulk-download-prepare",
            "requestParams": {"items": paths}}
    try:
        r = s.post(f"{BASE}/models-v2",
                   params={"m": "mpfs/bulk-download-prepare"},
                   json=body, timeout=60)
    except requests.RequestException as e:
        logging.warning(f"prepare exc: {e}")
        return None
    if r.status_code in (401, 403):
        raise SessionExpired(f"HTTP {r.status_code}")
    if r.status_code != 200:
        logging.warning(f"prepare HTTP {r.status_code}: {r.text[:200]}")
        return None
    try:
        url = r.json().get("download_url")
    except ValueError as e:
        logging.warning(f"prepare bad json: {e}")
        return None
    if url and url.startswith("//"):
        url = "https:" + url
    return url


def member_target(extract_dir: str, member_name: str) -> str | None:
    parts = [p for p in member_name.replace("\\", "/").split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        return None
    return os.path.join(extract_dir, *(sanitize_component(p) for p in parts))


def extract_zip(zip_path: str, extract_dir: str, worker_id: int) -> tuple[list[str], int]:
    names: list[str] = []
    total_bytes = 0
    with zipfile.ZipFile(zip_path) as zf:
        for m in zf.infolist():
            if m.is_dir():
                continue
            dst = member_target(extract_dir, m.filename)
            if dst is None:
                logging.warning(f"w{worker_id} skip unsafe member {m.filename!r}")
                continue
            base = m.filename.replace("\\", "/").rsplit("/", 1)[-1]
            if os.path.exists(dst) and os.path.getsize(dst) == m.file_size:
                names.append(base)
                total_bytes += m.file_size
                continue
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            part = dst + ".part"
            try:
                with zf.open(m) as src, open(part, "wb") as f:
                    while True:
                        chunk = src.read(1024 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                os.replace(part, dst)
                names.append(base)
                total_bytes += m.file_size
            except Exception as e:
                logging.warning(f"w{worker_id} extract {m.filename}: {e}")
                try: os.remove(part)
                except OSError: pass
    return names, total_bytes


def split_batch(batch: list[tuple[str, str]],
                extracted_names: list[str]) -> tuple[list[str], list[str]]:
    ids = [file_id for file_id, _ in batch]
    present = set(extracted_names)
    done = [file_id for file_id, name in batch
            if (name or file_id.rsplit("/", 1)[-1]) in present]
    if not done and extracted_names and len(extracted_names) == len(batch):
        return ids, []
    done_set = set(done)
    return done, [i for i in ids if i not in done_set]


def download_zip(s: requests.Session, url: str, zip_path: str, worker_id: int) -> bool:
    tmp = zip_path + ".part"
    try:
        with s.get(url, stream=True, timeout=900) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(2 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
        os.replace(tmp, zip_path)
        return True
    except (requests.RequestException, OSError) as e:
        logging.error(f"w{worker_id} download {os.path.basename(zip_path)}: {e}")
        try: os.remove(tmp)
        except OSError: pass
        return False


def download_and_extract(s: requests.Session, url: str, worker_id: int,
                         batch_idx: int) -> tuple[list[str], int]:
    os.makedirs(ZIP_TMP_DIR, exist_ok=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    zip_path = os.path.join(ZIP_TMP_DIR, f"w{worker_id}_b{batch_idx}.zip")
    if not download_zip(s, url, zip_path, worker_id):
        return [], 0
    try:
        return extract_zip(zip_path, EXTRACT_DIR, worker_id)
    except (zipfile.BadZipFile, OSError) as e:
        logging.error(f"w{worker_id} b{batch_idx} bad zip: {e}")
        return [], 0
    finally:
        try:
            os.remove(zip_path)
        except OSError:
            pass


def run(worker_id: int, batch_size: int) -> int:
    migrate()
    recovered = recover_stuck(worker_id)
    if recovered:
        print(f"[w{worker_id}] вернул в очередь зависшие файлы: {recovered}")

    try:
        s = make_session(read_cookies(COOKIES_FILE))
        sk, uid = get_sk_uid(s)
    except SessionExpired as e:
        print(f"[w{worker_id}] сессия истекла: {e}")
        return EXIT_SESSION_EXPIRED
    print(f"[w{worker_id}] авторизован uid={uid}")

    batch_idx = 0
    total_files = 0
    total_bytes = 0
    empty_streak = 0
    failed_in_row = 0
    exit_code = 0

    try:
        while True:
            batch = take_batch(worker_id, batch_size)
            if not batch:
                empty_streak += 1
                if empty_streak >= 3:
                    print(f"[w{worker_id}] нет работы, выход")
                    break
                time.sleep(5)
                continue
            empty_streak = 0
            batch_idx += 1
            ids = [file_id for file_id, _ in batch]

            try:
                url = prepare_zip(s, sk, uid, ids)
            except SessionExpired as e:
                release(ids)
                print(f"[w{worker_id}] сессия истекла ({e}), обнови cookies.json")
                exit_code = EXIT_SESSION_EXPIRED
                break

            if url:
                start = time.monotonic()
                names, bytes_ = download_and_extract(s, url, worker_id, batch_idx)
                dur = time.monotonic() - start
                done, missing = split_batch(batch, names)
            else:
                done, missing, bytes_, dur = [], ids, 0, 0.0

            mark_done(done)
            mark_attempt_failed(missing, "prepare failed" if not url else "missing in zip")

            if done:
                failed_in_row = 0
                total_files += len(done)
                total_bytes += bytes_
                mbps = (bytes_ / 1e6) / max(0.1, dur)
                print(f"[w{worker_id}] batch {batch_idx}: ✓ {len(done)} files / "
                      f"{bytes_/1e9:.2f} GB ({mbps:.1f} MB/s, {int(dur)}s)"
                      + (f", не пришло {len(missing)}" if missing else ""))
            else:
                failed_in_row += 1
                print(f"[w{worker_id}] batch {batch_idx}: ✗ FAIL → pending "
                      f"({failed_in_row}/{MAX_FAILED_BATCHES_IN_ROW} подряд)")
                if failed_in_row >= MAX_FAILED_BATCHES_IN_ROW:
                    print(f"[w{worker_id}] слишком много ошибок подряд, выход")
                    exit_code = EXIT_TOO_MANY_ERRORS
                    break
                time.sleep(10)
    finally:
        try:
            s.close()
        except Exception:
            pass
        try:
            if os.path.isdir(ZIP_TMP_DIR) and not os.listdir(ZIP_TMP_DIR):
                os.rmdir(ZIP_TMP_DIR)
        except OSError:
            pass

    print(f"[w{worker_id}] ИТОГ: {total_files} files / {total_bytes/1e9:.2f} GB")
    return exit_code


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True, help="ID воркера (1, 2, ...)")
    ap.add_argument("--batch", type=int, default=BATCH_SIZE)
    args = ap.parse_args()

    print(f"[w{args.id}] старт. batch={args.batch}")
    if not os.path.exists(DB_FILE):
        sys.exit("Нет unlim_state.db — сначала запусти unlim_download.py --collect")
    if not os.path.exists(COOKIES_FILE):
        sys.exit("Нет cookies.json — сначала сохрани куки на вкладке «Настройка»")
    return run(args.id, args.batch)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
