"""Общие части для unlim_download.py и unlim_worker.py: схема БД и HTTP-сессия."""

from __future__ import annotations

import json
import re
import sqlite3

import requests

BASE = "https://disk.yandex.ru"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")

SCHEMA = """
CREATE TABLE IF NOT EXISTS unlim_files (
    file_id TEXT PRIMARY KEY,
    cluster_id TEXT,
    name TEXT,
    size INTEGER,
    md5 TEXT,
    mime TEXT,
    status TEXT DEFAULT 'pending',
    local_path TEXT,
    error TEXT,
    worker_id INTEGER,
    assigned_at TIMESTAMP,
    downloaded_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_unlim_status ON unlim_files(status);
CREATE INDEX IF NOT EXISTS idx_unlim_status_size ON unlim_files(status, size);
CREATE TABLE IF NOT EXISTS unlim_clusters (
    cluster_id TEXT PRIMARY KEY,
    size INTEGER,
    fetched INTEGER DEFAULT 0
);
"""


class SessionExpired(Exception):
    pass


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(unlim_files)")}
    if "attempts" not in columns:
        conn.execute(
            "ALTER TABLE unlim_files ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
        )


def read_cookies(path: str) -> dict[str, str]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, list):
        raise ValueError("cookies.json должен быть массивом (формат Cookie-Editor)")
    return {c["name"]: c["value"] for c in raw
            if isinstance(c, dict) and c.get("name") and c.get("value") is not None}


def make_session(cookies: dict[str, str]) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA, "Accept": "*/*",
        "Accept-Language": "ru,en;q=0.9",
        "Origin": BASE, "Referer": f"{BASE}/client/photo",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
    })
    for k, v in cookies.items():
        s.cookies.set(k, v, domain=".yandex.ru")
    return s


def get_sk_uid(s: requests.Session) -> tuple[str, str]:
    r = s.get(f"{BASE}/client/disk", timeout=30)
    if "passport.yandex" in r.url or r.status_code in (401, 403):
        raise SessionExpired("Куки невалидны, обнови cookies.json")
    if r.status_code != 200:
        raise RuntimeError(f"disk.yandex.ru ответил HTTP {r.status_code}")
    sk = re.search(r'"sk"\s*:\s*"([^"]+)"', r.text)
    uid = re.search(r'"uid"\s*:\s*"?(\d+)"?', r.text)
    if not sk or not uid:
        raise RuntimeError("Не нашёл sk/uid в HTML, Яндекс изменил формат")
    return sk.group(1), uid.group(1)
