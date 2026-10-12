import threading

import pytest

import webui


class FakeTask:
    def __init__(self, code):
        self.code = code
        self.proc = self

    def alive(self):
        return False

    def poll(self):
        return self.code


class FakeTaskManager:
    def __init__(self, codes, on_start=None):
        self._lock = threading.Lock()
        self._tasks = {}
        self.codes = codes
        self.started = []
        self.on_start = on_start

    def start(self, slot, script, args):
        self.started.append((slot, args[0] if args else ""))
        self._tasks[slot] = FakeTask(self.codes.get((slot, args[0] if args else ""), 0))
        if self.on_start:
            self.on_start(slot)
        return True, ""

    def stop(self, slot):
        return True, ""


@pytest.fixture
def run_pipeline(monkeypatch):
    monkeypatch.setattr(webui, "check_token", lambda *a, **k: {"ok": True, "login": "me", "used": 0})
    monkeypatch.setattr(webui, "check_cookies", lambda: {"ok": True})

    def run(codes, on_start=None, workers=2):
        fake = FakeTaskManager(codes, on_start)
        monkeypatch.setattr(webui, "tm", fake)
        p = webui.Pipeline()
        p.workers = workers
        p._run()
        return p, fake

    return run


def test_should_fail_and_skip_download_when_scan_fails(run_pipeline):
    p, fake = run_pipeline({("disk", "scan"): 2})

    assert (p.state, fake.started) == ("failed", [("disk", "scan")])


def test_should_finish_with_warning_when_some_disk_files_fail(run_pipeline):
    p, _ = run_pipeline({("disk", "download"): 1})

    assert (p.state, p.step) == ("done", "финиш с предупреждениями")


def test_should_report_stopped_when_user_stops_during_workers(run_pipeline):
    holder = {}

    def stop_on_worker(slot):
        if slot.startswith("unlim_w"):
            holder["p"]._stop = True

    original = webui.Pipeline.__init__

    def init(self):
        original(self)
        holder["p"] = self

    webui.Pipeline.__init__ = init
    try:
        p, _ = run_pipeline({}, on_start=stop_on_worker)
    finally:
        webui.Pipeline.__init__ = original

    assert p.state == "stopped"
