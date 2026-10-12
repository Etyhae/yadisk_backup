import sqlite3
import zipfile

import pytest

import unlim_common
import unlim_worker


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "unlim_state.db"
    monkeypatch.setattr(unlim_worker, "DB_FILE", str(path))
    conn = sqlite3.connect(path)
    unlim_common.ensure_schema(conn)
    conn.executemany(
        "INSERT INTO unlim_files (file_id, name, size, status, worker_id) VALUES (?, ?, 1, ?, ?)",
        [("/photounlim/a.jpg", "a.jpg", "pending", None),
         ("/photounlim/b.jpg", "b.jpg", "in_progress", 1),
         ("/photounlim/c.jpg", "c.jpg", "in_progress", 2)],
    )
    conn.commit()
    conn.close()
    return path


def _statuses(path) -> dict:
    with sqlite3.connect(path) as conn:
        return dict(conn.execute("SELECT file_id, status FROM unlim_files"))


BATCH = [("/p/a.jpg", "a.jpg"), ("/p/b.jpg", "b.jpg"), ("/p/c.jpg", "c.jpg")]


@pytest.mark.parametrize("names, done, missing", [
    (["a.jpg", "b.jpg", "c.jpg"], ["/p/a.jpg", "/p/b.jpg", "/p/c.jpg"], []),
    (["a.jpg", "c.jpg"], ["/p/a.jpg", "/p/c.jpg"], ["/p/b.jpg"]),
    (["renamed.jpg"], [], ["/p/a.jpg", "/p/b.jpg", "/p/c.jpg"]),
    (["x.jpg", "y.jpg", "z.jpg"], ["/p/a.jpg", "/p/b.jpg", "/p/c.jpg"], []),
    ([], [], ["/p/a.jpg", "/p/b.jpg", "/p/c.jpg"]),
])
def test_should_mark_only_received_files_done_when_zip_is_partial(names, done, missing):
    assert unlim_worker.split_batch(BATCH, names) == (done, missing)


def test_should_keep_file_pending_when_zip_has_other_files_only():
    batch = [("/p/b.jpg", "b.jpg")]

    assert unlim_worker.split_batch(batch, ["a.jpg", "c.jpg"]) == ([], ["/p/b.jpg"])


@pytest.mark.parametrize("member", ["../evil.jpg", "a/../../evil.jpg", "./x", ""])
def test_should_reject_member_when_path_escapes_target(tmp_path, member):
    assert unlim_worker.member_target(str(tmp_path), member) is None


def test_should_sanitize_member_when_it_looks_like_windows_drive(tmp_path):
    result = unlim_worker.member_target(str(tmp_path), "C:/Windows/x.jpg")

    assert result == str(tmp_path / "C_" / "Windows" / "x.jpg")


def test_should_extract_files_and_report_names_when_zip_is_valid(tmp_path):
    zip_path = tmp_path / "b.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("disk/a.jpg", b"12345")
    out = tmp_path / "out"

    names, size = unlim_worker.extract_zip(str(zip_path), str(out), 1)

    assert (names, size, (out / "disk" / "a.jpg").read_bytes()) == (["a.jpg"], 5, b"12345")


def test_should_mark_failed_when_attempts_are_exhausted(db):
    for _ in range(unlim_worker.MAX_ATTEMPTS):
        unlim_worker.mark_attempt_failed(["/photounlim/a.jpg"], "prepare failed")

    assert _statuses(db)["/photounlim/a.jpg"] == "failed"


def test_should_keep_pending_when_attempts_remain(db):
    unlim_worker.mark_attempt_failed(["/photounlim/a.jpg"], "prepare failed")

    assert _statuses(db)["/photounlim/a.jpg"] == "pending"


def test_should_recover_own_batch_when_worker_restarts(db):
    recovered = unlim_worker.recover_stuck(worker_id=1)

    assert (recovered, _statuses(db)["/photounlim/c.jpg"]) == (1, "in_progress")


def test_should_exit_with_session_code_when_cookies_expire(db, monkeypatch):
    monkeypatch.setattr(unlim_worker, "read_cookies", lambda _: {})
    monkeypatch.setattr(unlim_worker, "get_sk_uid", lambda _: ("sk", "1"))

    def expired(*_):
        raise unlim_common.SessionExpired("HTTP 401")

    monkeypatch.setattr(unlim_worker, "prepare_zip", expired)

    code = unlim_worker.run(worker_id=9, batch_size=10)

    assert (code, _statuses(db)["/photounlim/a.jpg"]) == (unlim_worker.EXIT_SESSION_EXPIRED, "pending")


def test_should_add_attempts_column_when_database_is_old(tmp_path):
    conn = sqlite3.connect(tmp_path / "old.db")
    conn.execute("CREATE TABLE unlim_files (file_id TEXT PRIMARY KEY, name TEXT, size INTEGER, status TEXT)")

    unlim_common.ensure_schema(conn)

    assert "attempts" in {row[1] for row in conn.execute("PRAGMA table_info(unlim_files)")}
