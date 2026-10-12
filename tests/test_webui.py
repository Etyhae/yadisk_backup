import sqlite3
import subprocess
import sys
import threading

import pytest
from fastapi.testclient import TestClient

import unlim_common
import webui

TOKEN = "y0_secret_token_value_1234567890"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(webui, "ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(webui, "DB_FILE", tmp_path / "state.db")
    monkeypatch.setattr(webui, "UNLIM_DB_FILE", tmp_path / "unlim_state.db")
    monkeypatch.setattr(webui, "COOKIES_FILE", tmp_path / "cookies.json")
    monkeypatch.setenv("YA_DISK_TOKEN", TOKEN)
    monkeypatch.setenv("DOWNLOAD_DIR", str(tmp_path / "dl"))
    monkeypatch.setattr(webui, "_fetch_token_info", lambda _: {"ok": True, "login": "me"})
    webui.invalidate_token_cache()
    return TestClient(webui.app, base_url="http://127.0.0.1:8000")


def test_should_not_expose_raw_token_when_status_is_polled(client):
    response = client.get("/api/status")

    assert TOKEN not in response.text


def test_should_reject_request_when_host_is_foreign(client):
    response = client.get("/api/status", headers={"host": "evil.example:8000"})

    assert response.status_code == 403


@pytest.mark.parametrize("origin, status", [
    ("http://127.0.0.1:8000", 200),
    ("http://127.0.0.1:8000.evil.example", 403),
    ("http://evil.example", 403),
    ("null", 403),
])
def test_should_check_origin_exactly_when_request_changes_state(client, origin, status):
    response = client.post("/api/cookies/clear", headers={"origin": origin})

    assert response.status_code == status


def test_should_answer_without_waiting_for_yandex_when_token_is_not_cached(client, monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(webui, "_fetch_token_info", lambda _: release.wait(5) and {"ok": True})

    result = webui.token_status(TOKEN)
    release.set()

    assert result.get("pending") is True


def test_should_reject_download_dir_when_it_is_filesystem_root(client):
    response = client.post("/api/config/download_dir", json={"download_dir": "/"})

    assert response.status_code == 400


def test_should_refuse_selection_when_disk_download_is_running(client, monkeypatch):
    monkeypatch.setattr(webui.tm, "is_running", lambda slot: slot == "disk")
    started = []
    monkeypatch.setattr(webui.tm, "start", lambda *a: started.append(a) or (True, ""))

    response = client.post("/api/disk/download_selected", json={"categories": ["photos"]})

    assert (response.json()["ok"], started) == (False, [])


def _make_unlim_db(path, rows):
    conn = sqlite3.connect(path)
    unlim_common.ensure_schema(conn)
    conn.executemany(
        "INSERT INTO unlim_files (file_id, name, size, status, worker_id) VALUES (?, ?, 10, ?, ?)", rows)
    conn.commit()
    conn.close()


def test_should_categorize_unlim_by_name_when_file_id_has_no_extension(client):
    _make_unlim_db(webui.UNLIM_DB_FILE, [("res:123", "IMG_1.jpg", "pending", None)])

    response = client.get("/api/unlim/preview")

    assert response.json()["categories"]["photos"]["count"] == 1


def test_should_release_only_stopped_workers_when_unlim_is_stopped(client):
    _make_unlim_db(webui.UNLIM_DB_FILE, [("a", "a.jpg", "in_progress", 1), ("b", "b.jpg", "in_progress", 2)])

    webui.release_unlim_workers(["unlim_w1"])

    with sqlite3.connect(webui.UNLIM_DB_FILE) as conn:
        assert dict(conn.execute("SELECT file_id, status FROM unlim_files")) == {"a": "pending", "b": "in_progress"}


def test_should_report_stopped_by_user_when_task_is_terminated():
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"],
                            stdout=subprocess.PIPE, text=True)
    task = webui.Task("disk", proc, "main.py", ["download"])

    task.stop()

    assert (task.stopped_by_user, task.alive()) == (True, False)


def test_should_use_default_workers_when_env_value_is_invalid(client, monkeypatch):
    monkeypatch.setenv("MAX_WORKERS", "four")

    response = client.get("/api/status")

    assert response.json()["config"]["max_workers"] == 4
