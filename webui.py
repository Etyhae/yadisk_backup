"""Веб-дашборд для бэкапа Яндекс.Диска.

Запуск:
  python webui.py
  → http://localhost:8000

Что умеет:
  - Сохранить токен в .env через UI
  - Сохранить cookies.json через UI (для безлимита)
  - Запуск scan / download / verify / retry для обычного диска
  - Запуск collect / worker для photoslice (безлимита)
  - Запуск organize (сортировка по полочкам)
  - Live прогресс через polling
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv, set_key
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent
ENV_FILE = PROJECT / ".env"
load_dotenv(ENV_FILE)

DB_FILE = PROJECT / "state.db"
UNLIM_DB_FILE = PROJECT / "unlim_state.db"
COOKIES_FILE = PROJECT / "cookies.json"
TEMPLATES_DIR = PROJECT / "webui_assets"


def get_config() -> dict:
    """Перечитывает .env заново."""
    load_dotenv(ENV_FILE, override=True)
    return {
        "token": (os.getenv("YA_DISK_TOKEN", "") or "").strip(),
        "download_dir": (os.getenv("DOWNLOAD_DIR", str(PROJECT / "downloads")) or "").strip(),
        "max_workers": _int_env("MAX_WORKERS", 4),
    }


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


def mask_token(token: str) -> str:
    """Маскирует токен для отображения: y0_xxxx****xxxx"""
    if not token or len(token) < 8:
        return ""
    if len(token) <= 12:
        return token[:3] + "***"
    return f"{token[:6]}…{token[-4:]} ({len(token)} симв.)"


app = FastAPI(title="Yandex.Disk Backup UI")


# ── CSRF и DNS rebinding: пускаем только запросы к localhost ───────────────

_ALLOWED_HOSTNAMES = {"localhost", "127.0.0.1", "[::1]"}


def _hostname(host: str) -> str:
    if host.startswith("["):
        return host.split("]", 1)[0] + "]"
    return host.rsplit(":", 1)[0]


@app.middleware("http")
async def csrf_guard(request: Request, call_next):
    """Host проверяем всегда (защита от DNS rebinding, иначе чужой сайт прочитает
    /api/status). Для state-changing запросов Origin должен точно совпадать с Host.
    """
    host = request.headers.get("host", "")
    if _hostname(host) not in _ALLOWED_HOSTNAMES:
        return JSONResponse({"detail": "Forbidden host"}, status_code=403)
    if request.method in ("POST", "PUT", "DELETE", "PATCH"):
        origin = request.headers.get("origin")
        if origin is not None and origin != f"http://{host}":
            return JSONResponse({"detail": "Forbidden origin"}, status_code=403)
    return await call_next(request)


# ── Категории файлов ────────────────────────────────────────────────────────

CATEGORIES = {
    "photos": {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".gif",
               ".bmp", ".tiff", ".tif", ".raw", ".arw", ".cr2", ".nef",
               ".dng", ".orf"},
    "videos": {".mp4", ".mov", ".avi", ".mkv", ".m4v", ".webm", ".3gp", ".3g2",
               ".mpg", ".mpeg", ".wmv", ".flv", ".vob", ".mts", ".m2ts"},
    "audio": {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".oga", ".aac", ".wma",
              ".opus", ".aiff", ".alac"},
    "documents": {".doc", ".docx", ".pdf", ".txt", ".rtf", ".odt",
                  ".xls", ".xlsx", ".ods", ".csv",
                  ".ppt", ".pptx", ".odp"},
    "archives": {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso",
                 ".cab", ".tgz", ".tbz"},
    "programs": {".exe", ".dll", ".msi", ".bat", ".cmd", ".sys", ".com",
                 ".scr", ".vbs", ".ps1", ".sh", ".jar"},
}

CATEGORY_LABELS = {
    "photos": "Фото",
    "videos": "Видео",
    "audio": "Аудио",
    "documents": "Документы",
    "archives": "Архивы",
    "programs": "Программы",
    "other": "Прочее",
}


CATEGORY_SKIP_MARKER = "not in selected categories"
UNLIM_NAME_EXPR = "COALESCE(NULLIF(name, ''), file_id)"


def categorize(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    for cat, exts in CATEGORIES.items():
        if ext in exts:
            return cat
    return "other"


# ── Менеджер фоновых процессов ──────────────────────────────────────────────


class Task:
    def __init__(self, name: str, proc: subprocess.Popen, script: str, args: list[str]):
        self.name = name
        self.proc = proc
        self.script = script
        self.args = list(args)
        self.stopped_by_user = False
        self.started_at = time.time()
        self.log: deque[str] = deque(maxlen=300)
        self._t = threading.Thread(target=self._read, daemon=True)
        self._t.start()

    def _read(self):
        try:
            for line in self.proc.stdout:  # type: ignore
                line = line.rstrip("\r\n")
                if line:
                    self.log.append(line)
        except Exception:
            pass

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self) -> None:
        if self.alive():
            self.stopped_by_user = True
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


class TaskManager:
    def __init__(self):
        self._tasks: dict[str, Task] = {}  # ключ — название/слот
        self._lock = threading.Lock()

    def start(self, slot: str, script: str, args: list[str]) -> tuple[bool, str]:
        """slot — логическое имя ('disk', 'photoslice_collect', 'worker-1', ...).
        Можно одновременно несколько задач в разных слотах.
        """
        with self._lock:
            existing = self._tasks.get(slot)
            if existing and existing.alive():
                return False, f"Уже идёт: {slot}"
            # Чистка зомби — если предыдущая задача в слоте умерла, закроем pipe и забудем
            if existing and not existing.alive():
                # Сначала дождёмся, что reader-тред допишет последние строки
                try:
                    existing._t.join(timeout=1.0)
                except Exception:
                    pass
                try:
                    if existing.proc.stdout:
                        existing.proc.stdout.close()
                except Exception:
                    pass
                del self._tasks[slot]
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"] = "1"
            env["PYTHONUNBUFFERED"] = "1"
            cmd = [sys.executable, "-u", script] + args
            proc = subprocess.Popen(
                cmd, cwd=str(PROJECT),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace",
                env=env,
            )
            self._tasks[slot] = Task(slot, proc, script, args)
            return True, f"Запущено: {slot}"

    def stop(self, slot: str) -> tuple[bool, str]:
        with self._lock:
            t = self._tasks.get(slot)
            if not t or not t.alive():
                return False, f"Нет активной задачи {slot}"
            t.stop()
            return True, f"Остановлено: {slot}"

    def stop_prefix(self, prefix: str) -> list[str]:
        stopped = []
        with self._lock:
            for slot, t in self._tasks.items():
                if slot.startswith(prefix) and t.alive():
                    t.stop()
                    stopped.append(slot)
        return stopped

    def stop_all(self) -> list[str]:
        return self.stop_prefix("")

    def is_running(self, slot: str) -> bool:
        with self._lock:
            t = self._tasks.get(slot)
            return t is not None and t.alive()

    def any_running(self, prefix: str) -> bool:
        with self._lock:
            return any(slot.startswith(prefix) and t.alive()
                       for slot, t in self._tasks.items())

    def status(self) -> dict:
        out = {}
        with self._lock:
            for slot, t in self._tasks.items():
                out[slot] = {
                    "running": t.alive(),
                    "elapsed": time.time() - t.started_at,
                    "exit_code": t.proc.poll(),
                    "stopped": t.stopped_by_user,
                    "script": t.script,
                    "command": t.args[0] if t.script == "main.py" and t.args else t.script,
                    "args": t.args,
                    "log_tail": list(t.log)[-80:],
                }
        return out


tm = TaskManager()


# ── Pipeline ────────────────────────────────────────────────────────────────


class Pipeline:
    """Автоматическая последовательность: scan → download диска → collect → workers безлимита.
    Сам мониторит окончание каждого шага и переходит к следующему.
    """

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._stop = False
        self._owned_slots: set[str] = set()  # слоты которые pipeline сам запустил
        self.state: str = "idle"  # idle | running | done | failed
        self.step: str = "—"
        self.log: deque[str] = deque(maxlen=120)
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.skip_disk: bool = False
        self.skip_unlim: bool = False
        self.workers: int = 4

    def _own_start(self, slot: str, script: str, args: list[str]) -> tuple[bool, str]:
        """Запускает задачу через tm и помечает её как принадлежащую pipeline'у."""
        ok, msg = tm.start(slot, script, args)
        if ok:
            self._owned_slots.add(slot)
        return ok, msg

    def _log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        self.log.append(f"{ts}  {msg}")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, workers: int = 4, do_disk: bool = True, do_unlim: bool = True) -> tuple[bool, str]:
        with self._lock:
            if self.is_running():
                return False, "Pipeline уже идёт"
            self._stop = False
            self._owned_slots.clear()
            self.state = "running"
            self.step = "старт"
            self.log.clear()
            self.started_at = time.time()
            self.finished_at = None
            self.workers = max(1, min(workers, 6))
            self.skip_disk = not do_disk
            self.skip_unlim = not do_unlim
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return True, "Pipeline запущен"

    def stop(self) -> tuple[bool, str]:
        if not self.is_running():
            return False, "Pipeline не идёт"
        self._stop = True
        # Гасим ТОЛЬКО задачи которые pipeline сам запустил
        stopped_workers = []
        for slot in list(self._owned_slots):
            ok, _ = tm.stop(slot)
            if ok and slot.startswith("unlim_w"):
                stopped_workers.append(slot)
        release_unlim_workers(stopped_workers)
        self._log("⏹ остановлено пользователем")
        return True, "Остановлено"

    def _wait_task(self, slot: str) -> Optional[int]:
        """Блокирующее ожидание завершения задачи в слоте. None если pipeline остановили."""
        while not self._stop:
            with tm._lock:
                t = tm._tasks.get(slot)
            if t and not t.alive():
                return t.proc.poll()
            time.sleep(0.5 if not t else 1.0)
        return None

    def _step(self, slot: str, script: str, args: list[str], title: str) -> Optional[int]:
        ok, msg = self._own_start(slot, script, args)
        if not ok:
            self._log(f"❌ {title}: {msg}")
            return -1
        code = self._wait_task(slot)
        if code is not None:
            self._log(f"{'✓' if code == 0 else '⚠️'} {title}: exit={code}")
        return code

    def _run(self):
        warnings = 0
        try:
            # Шаг 0: токен
            self.step = "проверка токена"
            self._log("🔑 Проверяю токен...")
            tok = check_token(get_config()["token"], force=True)
            if not tok.get("ok"):
                self._log(f"❌ Токен невалиден: {tok.get('msg')}")
                self.state = "failed"; return
            self._log(f"✓ Токен OK: {tok.get('login')}, {tok.get('used',0)/1e9:.1f} ГБ занято")

            # === ДИСК ===
            if not self.skip_disk:
                self.step = "сканирую диск"
                self._log("🔍 Запускаю scan...")
                code = self._step("disk", "main.py", ["scan"], "scan")
                if code is None: self._abort(); return
                if code != 0:
                    self._log("❌ scan не удался, дальше не иду. Подробности на вкладке «Диск»")
                    self.state = "failed"; return

                self.step = "качаю диск"
                self._log(f"⬇️ Запускаю download (workers={self.workers})...")
                code = self._step("disk", "main.py",
                                  ["download", "--workers", str(self.workers), "--no-scan"],
                                  "download диска")
                if code is None: self._abort(); return
                if code != 0:
                    warnings += 1
                    self._log("⚠️ часть файлов диска не скачалась, их можно повторить на вкладке «Диск»")
            else:
                self._log("⏭ Шаг 'диск' пропущен")

            # === БЕЗЛИМИТ ===
            if not self.skip_unlim:
                cookies = check_cookies()
                if not cookies.get("ok"):
                    warnings += 1
                    self._log(f"⚠️ Cookies не загружены, пропускаю безлимит: {cookies.get('msg')}")
                else:
                    self.step = "собираю список безлимита"
                    self._log("🗂 Запускаю unlim collect...")
                    code = self._step("unlim_collect", "unlim_download.py", ["--collect"],
                                      "сбор списка безлимита")
                    if code is None: self._abort(); return
                    if code != 0:
                        self._log("❌ список безлимита не собран, проверь cookies")
                        self.state = "failed"; return

                    self.step = f"скачиваю безлимит ({self.workers} воркеров)"
                    self._log(f"🚀 Запускаю {self.workers} воркеров...")
                    slots = []
                    for i in range(1, self.workers + 1):
                        ok, msg = self._own_start(f"unlim_w{i}", "unlim_worker.py", ["--id", str(i)])
                        if ok:
                            slots.append(f"unlim_w{i}")
                            self._log(f"  ✓ воркер {i} запущен")
                        else:
                            self._log(f"  ✗ воркер {i}: {msg}")

                    codes = [self._wait_task(slot) for slot in slots]
                    if self._stop or None in codes:
                        self._abort(); return
                    bad = [c for c in codes if c != 0]
                    if bad:
                        warnings += 1
                        self._log(f"⚠️ воркеров с ошибкой: {len(bad)} (коды {bad})")
                    else:
                        self._log("✓ все воркеры безлимита завершили работу")
            else:
                self._log("⏭ Шаг 'безлимит' пропущен")

            self.state = "done"
            self.step = "финиш" if not warnings else "финиш с предупреждениями"
            self._log("🏆 ВСЁ ГОТОВО!" if not warnings else "🏁 Готово, но есть предупреждения выше")
        except Exception as e:
            self._log(f"❌ Ошибка pipeline: {e}")
            self.state = "failed"
        finally:
            self.finished_at = time.time()

    def _abort(self):
        self.state = "stopped"
        self.step = "остановлено"

    def status(self) -> dict:
        return {
            "state": self.state,
            "running": self.is_running(),
            "step": self.step,
            "started_at": self.started_at,
            "elapsed": (time.time() - self.started_at) if self.started_at and not self.finished_at else
                       ((self.finished_at - self.started_at) if self.started_at else None),
            "log": list(self.log),
        }


pipeline = Pipeline()


# ── Чтение состояния ────────────────────────────────────────────────────────


def db_stats(db_file: Path, table: str = "files",
             id_col: str = "remote_path", with_recent: bool = True) -> dict:
    out = {"total": 0, "pending": 0, "downloaded": 0, "skipped": 0,
           "failed": 0, "in_progress": 0,
           "bytes_total": 0, "bytes_done": 0, "recent": []}
    if not db_file.exists():
        return out
    try:
        c = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=30)
        c.row_factory = sqlite3.Row
        try:
            for r in c.execute(
                f"SELECT status, COUNT(*) AS n, COALESCE(SUM(size),0) AS b "
                f"FROM {table} GROUP BY status"
            ):
                st = r["status"]
                out[st] = r["n"]
                out["total"] += r["n"]
                out["bytes_total"] += r["b"]
                if st in ("downloaded", "skipped"):
                    out["bytes_done"] += r["b"]
            if with_recent:
                out["recent"] = [
                    {"path": r["path"], "status": r["status"], "size": r["size"]}
                    for r in c.execute(
                        f"SELECT {id_col} AS path, status, size FROM {table} "
                        f"WHERE downloaded_at IS NOT NULL "
                        f"ORDER BY downloaded_at DESC LIMIT 10"
                    )
                ]
        finally:
            c.close()
        return out
    except sqlite3.Error as e:
        return {**out, "error": str(e)}


_token_lock = threading.Lock()
_token_check_cache: dict = {"token": None, "at": 0.0, "data": None, "refreshing": False}
_TOKEN_TTL = 300
_TOKEN_PENDING = {"ok": False, "pending": True, "msg": "проверяю…"}


def _token_placeholder(token: str) -> Optional[dict]:
    if not token or token.startswith("AQAAA_paste"):
        return {"ok": False, "msg": "Токен не задан"}
    return None


def _fetch_token_info(token: str) -> dict:
    try:
        import yadisk
        cli = yadisk.Client(token=token)
        try:
            if not cli.check_token():
                return {"ok": False, "msg": "Токен невалиден"}
            info = cli.get_disk_info()
        finally:
            cli.close()
        user = getattr(info, "user", None)
        return {"ok": True, "msg": "OK",
                "used": getattr(info, "used_space", 0) or 0,
                "total": getattr(info, "total_space", 0) or 0,
                "photo_unlim": getattr(info, "photounlim_size", 0) or 0,
                "login": getattr(user, "login", "?") if user else "?"}
    except Exception as e:
        return {"ok": False, "msg": f"{type(e).__name__}: {e}"}


def check_token(token: str, force: bool = False) -> dict:
    placeholder = _token_placeholder(token)
    if placeholder:
        return placeholder
    with _token_lock:
        cache = dict(_token_check_cache)
    if (not force and cache["token"] == token and cache["data"]
            and time.time() - cache["at"] < _TOKEN_TTL):
        return cache["data"]
    data = _fetch_token_info(token)
    with _token_lock:
        _token_check_cache.update({"token": token, "at": time.time(), "data": data})
    return data


def _refresh_token_in_background(token: str) -> None:
    try:
        data = _fetch_token_info(token)
        with _token_lock:
            if _token_check_cache["token"] == token:
                _token_check_cache.update({"at": time.time(), "data": data})
    finally:
        with _token_lock:
            _token_check_cache["refreshing"] = False


def token_status(token: str) -> dict:
    placeholder = _token_placeholder(token)
    if placeholder:
        return placeholder
    with _token_lock:
        cache = _token_check_cache
        if cache["token"] != token:
            cache.update({"token": token, "at": 0.0, "data": None})
        fresh = cache["data"] is not None and time.time() - cache["at"] < _TOKEN_TTL
        data = cache["data"]
        if fresh or cache["refreshing"]:
            return data or _TOKEN_PENDING
        cache["refreshing"] = True
    threading.Thread(target=_refresh_token_in_background, args=(token,), daemon=True).start()
    return data or _TOKEN_PENDING


def invalidate_token_cache() -> None:
    with _token_lock:
        _token_check_cache.update({"token": None, "at": 0.0, "data": None})


def check_cookies() -> dict:
    if not COOKIES_FILE.exists():
        return {"ok": False, "msg": "cookies.json не загружен", "count": 0}
    try:
        raw = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            return {"ok": False, "msg": "Формат не Cookie-Editor", "count": 0}
        names = {c.get("name") for c in raw if isinstance(c, dict)}
        important = ["Session_id", "yandexuid", "sessionid2"]
        missing = [n for n in important if n not in names]
        if missing:
            return {"ok": False,
                    "msg": f"Не хватает: {', '.join(missing)}. "
                           f"Залогинься в браузере и заэкспортируй заново.",
                    "count": len(raw)}
        return {"ok": True, "msg": f"Куки OK ({len(raw)} шт)", "count": len(raw)}
    except Exception as e:
        return {"ok": False, "msg": f"Ошибка: {e}", "count": 0}


# ── Endpoints ───────────────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
def index():
    html_path = TEMPLATES_DIR / "index.html"
    if not html_path.exists():
        return HTMLResponse("<h1>UI не установлен</h1>")
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/status")
def api_status():
    cfg = get_config()
    return {
        "config": {
            "download_dir": cfg["download_dir"],
            "max_workers": cfg["max_workers"],
            "token_masked": mask_token(cfg["token"]),
        },
        "token": token_status(cfg["token"]),
        "cookies": check_cookies(),
        "disk": {"db": db_stats(DB_FILE, "files", "remote_path")},
        "unlim": {"db": db_stats(UNLIM_DB_FILE, "unlim_files", "file_id", with_recent=False)},
        "tasks": tm.status(),
        "pipeline": pipeline.status(),
    }


class StartPayload(BaseModel):
    workers: int = Field(default=4, ge=1, le=6)
    do_disk: bool = True
    do_unlim: bool = True


@app.post("/api/start")
def api_start(payload: StartPayload):
    ok, msg = pipeline.start(workers=payload.workers,
                              do_disk=payload.do_disk,
                              do_unlim=payload.do_unlim)
    return {"ok": ok, "msg": msg}


@app.post("/api/start/stop")
def api_start_stop():
    ok, msg = pipeline.stop()
    return {"ok": ok, "msg": msg}


class TokenPayload(BaseModel):
    token: str = Field(max_length=4096)


def _has_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 for ch in value)


def _write_env_key(key: str, value: str) -> None:
    if not ENV_FILE.exists():
        ENV_FILE.write_text("", encoding="utf-8")
    set_key(str(ENV_FILE), key, value, quote_mode="never")
    _restrict_permissions(ENV_FILE)
    load_dotenv(ENV_FILE, override=True)


def _restrict_permissions(path: Path) -> None:
    if os.name == "posix":
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass


@app.post("/api/config/token/check")
def api_check_token(payload: TokenPayload):
    """Проверка без сохранения."""
    return check_token((payload.token or "").strip(), force=True)


@app.post("/api/config/token")
def api_set_token(payload: TokenPayload):
    token = (payload.token or "").strip()
    if not token:
        raise HTTPException(400, "Пустой токен")
    if _has_control_chars(token) or " " in token:
        raise HTTPException(400, "Токен содержит пробелы или переносы строк")
    _write_env_key("YA_DISK_TOKEN", token)
    return {"ok": True, "check": check_token(token, force=True)}


class DownloadDirPayload(BaseModel):
    download_dir: str = Field(max_length=1024)


def _is_filesystem_root(path: str) -> bool:
    abs_path = os.path.abspath(os.path.expanduser(path))
    return os.path.dirname(abs_path) == abs_path


@app.post("/api/config/download_dir")
def api_set_dir(payload: DownloadDirPayload):
    d = (payload.download_dir or "").strip()
    if not d:
        raise HTTPException(400, "Пустой путь")
    if _has_control_chars(d):
        raise HTTPException(400, "Путь содержит недопустимые символы")
    if _is_filesystem_root(d):
        raise HTTPException(400, "Нельзя качать в корень диска, укажи подпапку, например D:/YandexBackup")
    if os.path.exists(d) and not os.path.isdir(d):
        raise HTTPException(400, "По этому пути лежит файл, а не папка")
    _write_env_key("DOWNLOAD_DIR", d)
    return {"ok": True, "download_dir": d}


class CookiesPayload(BaseModel):
    cookies: list


def _validate_cookies(arr: list) -> dict:
    if not isinstance(arr, list):
        return {"ok": False, "msg": "Должен быть массив"}
    names = {c.get("name") for c in arr if isinstance(c, dict)}
    important = ["Session_id", "yandexuid", "sessionid2"]
    missing = [n for n in important if n not in names]
    if missing:
        return {"ok": False,
                "msg": f"Не хватает кук: {', '.join(missing)}. "
                       f"Залогинься в браузере на disk.yandex.ru и заэкспортируй заново."}
    return {"ok": True, "msg": f"Куки готовы ({len(arr)} шт)"}


@app.post("/api/cookies/check")
def api_check_cookies(payload: CookiesPayload):
    """Проверка без сохранения."""
    return _validate_cookies(payload.cookies)


@app.post("/api/cookies")
def api_set_cookies(payload: CookiesPayload):
    """Сохраняет cookies.json атомарно через tmp+replace."""
    arr = payload.cookies
    check = _validate_cookies(arr)
    if not check["ok"]:
        return {"ok": False, "check": check, "msg": check["msg"]}
    tmp = COOKIES_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(arr, ensure_ascii=False, indent=2), encoding="utf-8")
    _restrict_permissions(tmp)
    os.replace(tmp, COOKIES_FILE)
    return {"ok": True, "check": check_cookies(), "saved_count": len(arr)}


@app.post("/api/cookies/clear")
def api_clear_cookies():
    if COOKIES_FILE.exists():
        try:
            COOKIES_FILE.unlink()
        except OSError as e:
            return {"ok": False, "msg": str(e)}
    return {"ok": True}


@app.post("/api/config/token/clear")
def api_clear_token():
    if ENV_FILE.exists():
        _write_env_key("YA_DISK_TOKEN", "")
    invalidate_token_cache()
    return {"ok": True}


def preview_db(db_file: Path, table: str, name_expr: str) -> dict:
    """Группирует файлы в БД по категориям."""
    if not db_file.exists():
        return {"empty": True, "categories": {}}
    try:
        c = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=30)
        summary: dict[str, dict] = {
            k: {"count": 0, "bytes": 0, "label": CATEGORY_LABELS[k]}
            for k in list(CATEGORY_LABELS.keys())
        }
        total = 0
        try:
            for name, size in c.execute(
                f"SELECT {name_expr}, COALESCE(size, 0) FROM {table} "
                f"WHERE status IN ('pending','downloaded','skipped','in_progress','failed')"
            ):
                cat = categorize(name or "")
                summary[cat]["count"] += 1
                summary[cat]["bytes"] += size
                total += 1
        finally:
            c.close()
        return {"empty": total == 0, "total": total, "categories": summary}
    except sqlite3.Error as e:
        return {"empty": True, "error": str(e)}


def filter_to_categories(db_file: Path, table: str, id_col: str, name_expr: str,
                         selected: list[str]) -> tuple[int, int]:
    """Помечает pending-файлы вне выбранных категорий как 'skipped' (одним проходом).
    Возвращает (skipped_count, kept_pending).
    """
    if not db_file.exists():
        return (0, 0)
    sel = set(selected)
    c = sqlite3.connect(db_file, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        rows = c.execute(
            f"SELECT {id_col}, {name_expr} FROM {table} WHERE status='pending'"
        ).fetchall()
        to_skip = [file_id for file_id, name in rows if categorize(name or "") not in sel]
        if to_skip:
            c.execute("BEGIN")
            try:
                c.executemany(
                    f"UPDATE {table} SET status='skipped', "
                    f"error='{CATEGORY_SKIP_MARKER}' WHERE {id_col}=?",
                    [(p,) for p in to_skip],
                )
                c.execute("COMMIT")
            except Exception:
                try: c.execute("ROLLBACK")
                except sqlite3.Error: pass
                raise
        return (len(to_skip), len(rows) - len(to_skip))
    finally:
        c.close()


def _execute_write(db_file: Path, sql: str, params: tuple = ()) -> int:
    if not db_file.exists():
        return 0
    c = sqlite3.connect(db_file, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    try:
        c.execute("BEGIN")
        try:
            n = c.execute(sql, params).rowcount
            c.execute("COMMIT")
            return n
        except Exception:
            try: c.execute("ROLLBACK")
            except sqlite3.Error: pass
            raise
    finally:
        c.close()


def unskip_category_marker(db_file: Path, table: str) -> int:
    """Сбрасывает skipped-with-marker обратно в pending — на случай повторного выбора."""
    return _execute_write(
        db_file,
        f"UPDATE {table} SET status='pending', error=NULL "
        f"WHERE status='skipped' AND error=?",
        (CATEGORY_SKIP_MARKER,),
    )


def release_unlim_workers(slots: list[str]) -> int:
    ids = [int(s.removeprefix("unlim_w")) for s in slots
           if s.startswith("unlim_w") and s.removeprefix("unlim_w").isdigit()]
    if not ids:
        return 0
    marks = ",".join("?" * len(ids))
    try:
        return _execute_write(
            UNLIM_DB_FILE,
            f"UPDATE unlim_files SET status='pending', worker_id=NULL "
            f"WHERE status='in_progress' AND worker_id IN ({marks})",
            tuple(ids),
        )
    except sqlite3.Error:
        return 0


def reset_unlim_failed() -> int:
    try:
        return _execute_write(
            UNLIM_DB_FILE,
            "UPDATE unlim_files SET status='pending', error=NULL, attempts=0 "
            "WHERE status='failed'",
        )
    except sqlite3.Error:
        return _execute_write(
            UNLIM_DB_FILE,
            "UPDATE unlim_files SET status='pending', error=NULL WHERE status='failed'",
        )


def _start_unlim_workers(count: int) -> dict:
    started = []
    failed = []
    for i in range(1, max(1, min(count, 6)) + 1):
        ok, msg = tm.start(f"unlim_w{i}", "unlim_worker.py", ["--id", str(i)])
        if ok:
            started.append(i)
        else:
            failed.append({"id": i, "msg": msg})
    msg = f"Запущено воркеров: {len(started)}" if started else "Воркеры не запущены"
    return {"ok": bool(started), "msg": msg, "started": started, "failed": failed}


_BUSY_DISK = {"ok": False, "msg": "Задача по диску уже идёт, дождись окончания или нажми «Стоп»"}
_BUSY_UNLIM = {"ok": False, "msg": "Безлимит уже качается, дождись окончания или нажми «Стоп»"}


@app.get("/api/disk/preview")
def disk_preview():
    return preview_db(DB_FILE, "files", "remote_path")


@app.get("/api/unlim/preview")
def unlim_preview():
    return preview_db(UNLIM_DB_FILE, "unlim_files", UNLIM_NAME_EXPR)


class SelectivePayload(BaseModel):
    categories: list[str]
    workers: int = Field(default=4, ge=1, le=6)


@app.post("/api/disk/download_selected")
def disk_download_selected(payload: SelectivePayload):
    if tm.is_running("disk"):
        return _BUSY_DISK
    unskip_category_marker(DB_FILE, "files")
    filter_to_categories(DB_FILE, "files", "remote_path", "remote_path", payload.categories)
    workers = max(1, min(payload.workers, 5))
    return _resp(tm.start("disk", "main.py",
                          ["download", "--workers", str(workers), "--no-scan"]))


@app.post("/api/unlim/download_selected")
def unlim_download_selected(payload: SelectivePayload):
    if tm.any_running("unlim"):
        return _BUSY_UNLIM
    unskip_category_marker(UNLIM_DB_FILE, "unlim_files")
    filter_to_categories(UNLIM_DB_FILE, "unlim_files", "file_id", UNLIM_NAME_EXPR,
                         payload.categories)
    return _start_unlim_workers(payload.workers)


# Disk actions
@app.post("/api/disk/scan")
def disk_scan():
    return _resp(tm.start("disk", "main.py", ["scan"]))


class DownloadOpts(BaseModel):
    workers: int = Field(default=4, ge=1, le=5)


@app.post("/api/disk/download")
def disk_download(opts: DownloadOpts):
    return _resp(tm.start("disk", "main.py",
                          ["download", "--workers", str(opts.workers), "--no-scan"]))


@app.post("/api/disk/retry")
def disk_retry():
    return _resp(tm.start("disk", "main.py", ["retry-failed"]))


@app.post("/api/disk/verify")
def disk_verify():
    return _resp(tm.start("disk", "main.py", ["verify"]))


@app.post("/api/disk/stop")
def disk_stop():
    return _resp(tm.stop("disk"))


def _remove_db(db_file: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        f = db_file.with_name(db_file.name + suffix)
        if f.exists():
            f.unlink()


@app.post("/api/disk/clear")
def disk_clear():
    """Полная очистка БД сканирования диска + остановка задач."""
    tm.stop("disk")
    try:
        _remove_db(DB_FILE)
        return {"ok": True, "msg": "БД диска очищена"}
    except OSError as e:
        return {"ok": False, "msg": str(e)}


@app.post("/api/unlim/clear")
def unlim_clear():
    """Полная очистка БД безлимита + остановка воркеров."""
    tm.stop_prefix("unlim")
    try:
        _remove_db(UNLIM_DB_FILE)
        return {"ok": True, "msg": "БД безлимита очищена"}
    except OSError as e:
        return {"ok": False, "msg": str(e)}


# Photoslice (безлимит)
@app.post("/api/unlim/collect")
def unlim_collect():
    return _resp(tm.start("unlim_collect", "unlim_download.py", ["--collect"]))


class WorkersPayload(BaseModel):
    workers: int = Field(default=4, ge=1, le=6)


@app.post("/api/unlim/start_workers")
def unlim_start_workers(payload: WorkersPayload):
    if tm.any_running("unlim_w"):
        return _BUSY_UNLIM
    return _start_unlim_workers(payload.workers)


@app.post("/api/unlim/retry")
def unlim_retry(payload: WorkersPayload):
    if tm.any_running("unlim"):
        return _BUSY_UNLIM
    n = reset_unlim_failed()
    if n == 0:
        return {"ok": False, "msg": "Нет файлов с ошибками"}
    result = _start_unlim_workers(payload.workers)
    return {**result, "msg": f"Вернул в очередь: {n}. {result['msg']}"}


@app.post("/api/unlim/stop")
def unlim_stop():
    stopped = tm.stop_prefix("unlim")
    released = release_unlim_workers(stopped)
    return {"ok": bool(stopped), "stopped": len(stopped), "released": released,
            "msg": f"Остановлено задач: {len(stopped)}" if stopped else "Нет активных задач"}


# Organize / dedupe
class OrganizePayload(BaseModel):
    input: str
    action: str = "report-only"  # report-only | move


# Запрещённые пути для organize (нельзя бесконтрольно сортировать систему)
_FORBIDDEN_ORGANIZE_PREFIXES = [
    "c:\\windows", "c:\\program files", "c:\\programdata",
    "c:\\users\\all users", "c:\\$recycle.bin",
    "/", "/etc", "/usr", "/bin", "/sbin", "/system", "/library",
]


def _is_safe_organize_path(path: str) -> tuple[bool, str]:
    """Запрещаем сортировку системных папок и корней дисков."""
    try:
        abs_path = os.path.abspath(path)
    except Exception:
        return False, "Невалидный путь"
    norm = abs_path.lower().replace("/", "\\")
    # Корень диска (типа C:\ или D:\) — слишком опасно
    if len(abs_path) <= 3 and abs_path.endswith(":\\"):
        return False, "Нельзя сортировать корень диска целиком"
    for bad in _FORBIDDEN_ORGANIZE_PREFIXES:
        bad_norm = bad.replace("/", "\\")
        if norm == bad_norm or norm.startswith(bad_norm + "\\"):
            return False, f"Системная папка запрещена: {bad}"
    if not os.path.isdir(abs_path):
        return False, "Папка не существует"
    return True, ""


@app.post("/api/organize")
def organize(payload: OrganizePayload):
    ok, msg = _is_safe_organize_path(payload.input)
    if not ok:
        return {"ok": False, "msg": msg}
    if payload.action not in ("report-only", "move"):
        return {"ok": False, "msg": "Неизвестное действие"}
    return _resp(tm.start("organize", "organize.py",
                          ["--input", payload.input,
                           "--action", payload.action]))


@app.post("/api/organize/stop")
def organize_stop():
    return _resp(tm.stop("organize"))


@app.post("/api/stop_all")
def stop_all():
    stopped = tm.stop_all()
    release_unlim_workers(stopped)
    return {"ok": True, "stopped": len(stopped)}


@app.post("/api/task/{slot}/clear_log")
def clear_task_log(slot: str):
    """Очищает буфер логов задачи (или всех slot с префиксом)."""
    cleared = 0
    with tm._lock:
        for s, t in tm._tasks.items():
            if s == slot or (slot.endswith("*") and s.startswith(slot[:-1])):
                t.log.clear()
                cleared += 1
    return {"ok": True, "cleared": cleared}


# ── Helpers ─────────────────────────────────────────────────────────────────


def _resp(t: tuple[bool, str]):
    return {"ok": t[0], "msg": t[1]}


if (TEMPLATES_DIR / "static").exists():
    app.mount("/static",
              StaticFiles(directory=str(TEMPLATES_DIR / "static")),
              name="static")


def _open_browser_when_ready(host: str, port: int):
    """Открывает браузер только когда сервер начал отвечать."""
    import http.client
    import webbrowser
    url = f"http://{host}:{port}"
    for _ in range(60):  # ждём до ~30 сек
        time.sleep(0.5)
        try:
            conn = http.client.HTTPConnection(host, port, timeout=1)
            conn.request("GET", "/api/status")
            resp = conn.getresponse()
            if resp.status == 200:
                conn.close()
                break
            conn.close()
        except Exception:
            pass
    try:
        webbrowser.open(url)
    except Exception:
        pass


def main():
    import uvicorn
    host = "127.0.0.1"
    port = 8000
    url = f"http://{host}:{port}"
    print("=" * 60)
    print("  Yandex.Disk Backup — Web UI")
    print(f"  {url}")
    print("=" * 60)
    print()
    print("  Сервер запускается, браузер откроется автоматически.")
    print("  Чтобы остановить — закрой это окно или нажми Ctrl+C.")
    print()

    # Открыть браузер в фоне когда сервер готов
    threading.Thread(
        target=_open_browser_when_ready,
        args=(host, port),
        daemon=True,
    ).start()

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
