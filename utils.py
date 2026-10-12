"""Утилиты: санитизация путей, MD5, работа с длинными путями Windows."""

from __future__ import annotations

import hashlib
import os
import sys
import unicodedata

# Символы, запрещённые в именах файлов на Windows
_WIN_FORBIDDEN = '<>:"|?*'
# Зарезервированные имена Windows
_WIN_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

IS_WINDOWS = sys.platform.startswith("win")
CASE_INSENSITIVE_FS = IS_WINDOWS or sys.platform == "darwin"
# Порог, после которого включаем префикс \\?\ для длинных путей
WIN_LONG_PATH_THRESHOLD = 240


def sanitize_component(name: str) -> str:
    """Чистит ОДИН компонент пути (имя файла или папки) от запрещённых символов."""
    if not name:
        return "_"
    cleaned = []
    for ch in name:
        if ch in _WIN_FORBIDDEN or ord(ch) < 32:
            cleaned.append("_")
        else:
            cleaned.append(ch)
    out = "".join(cleaned).rstrip(" .")  # Windows не любит хвостовые пробелы и точки
    if not out:
        out = "_"
    # Зарезервированные имена
    stem = out.split(".", 1)[0].upper()
    if stem in _WIN_RESERVED:
        out = "_" + out
    return out


def sanitize_remote_path(remote_path: str) -> str:
    """Превращает /disk/foo:bar/file?.jpg → foo_bar/file_.jpg (относительный путь)."""
    # Убираем префикс disk: если есть
    p = remote_path
    if p.startswith("disk:"):
        p = p[5:]
    p = p.lstrip("/")
    parts = [sanitize_component(part) for part in p.split("/") if part]
    return "/".join(parts)


def ensure_dir(path: str | os.PathLike) -> None:
    """Создаёт директорию и все родительские, если их нет."""
    os.makedirs(path, exist_ok=True)


def to_long_path(path: str) -> str:
    r"""Для Windows — добавляет префикс \\?\ если путь длинный.
    На других платформах возвращает как есть.
    """
    if not IS_WINDOWS:
        return path
    abs_path = os.path.abspath(path)
    if len(abs_path) < WIN_LONG_PATH_THRESHOLD:
        return abs_path
    if abs_path.startswith("\\\\?\\"):
        return abs_path
    # UNC-пути (\\server\share\...) требуют другой префикс
    if abs_path.startswith("\\\\"):
        return "\\\\?\\UNC\\" + abs_path[2:]
    return "\\\\?\\" + abs_path


def resolve_local_path(download_dir: str, remote_path: str) -> str:
    """Собирает локальный путь для удалённого файла с санитизацией и поддержкой длинных путей."""
    rel = sanitize_remote_path(remote_path)
    local = os.path.join(download_dir, rel.replace("/", os.sep))
    return local


def avoid_case_collision(local_path: str) -> str:
    """На Windows и macOS foo.txt и FOO.txt — один и тот же файл.
    Если по этому пути уже лежит файл с другим регистром имени (это другой
    удалённый файл), добавляем суффикс _1, _2 и т.д. Свой файл с точным
    совпадением имени оставляем как есть, чтобы resume работал.
    """
    if not CASE_INSENSITIVE_FS or not os.path.exists(to_long_path(local_path)):
        return local_path
    parent, name = os.path.split(local_path)
    try:
        existing = os.listdir(to_long_path(parent or "."))
    except OSError:
        return local_path
    by_lower = {_fold(e): e for e in existing}
    if _same_name(by_lower.get(_fold(name)), name):
        return local_path
    return _suffix_path(local_path, by_lower)


def _fold(name: str) -> str:
    return unicodedata.normalize("NFC", name).lower()


def _same_name(actual: str | None, wanted: str) -> bool:
    return actual is not None and (
        unicodedata.normalize("NFC", actual) == unicodedata.normalize("NFC", wanted)
    )


def _suffix_path(local_path: str, by_lower: dict[str, str]) -> str:
    base, ext = os.path.splitext(local_path)
    i = 1
    while True:
        candidate = f"{base}_{i}{ext}"
        cand_name = os.path.basename(candidate)
        actual = by_lower.get(_fold(cand_name))
        if actual is None or _same_name(actual, cand_name):
            return candidate
        i += 1


def compute_md5(local_path: str, chunk: int = 1024 * 1024) -> str:
    """Считает MD5 локального файла стримом — не грузит файл целиком в память."""
    h = hashlib.md5()
    path = to_long_path(local_path)
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def file_size_safe(local_path: str) -> int | None:
    """Размер локального файла или None если файла нет."""
    try:
        return os.path.getsize(to_long_path(local_path))
    except OSError:
        return None


def human_bytes(n: int | float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PB"


def human_duration(seconds: float) -> str:
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"
