import os
import sys
import threading
import time
import types

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gofile_browser_client as gbc


def setup_function(_function):
    gbc._reset_browser_meta_transport_for_tests()


def teardown_function(_function):
    gbc._reset_browser_meta_transport_for_tests()


def test_get_browser_meta_transport_returns_singleton():
    a = gbc.get_browser_meta_transport()
    b = gbc.get_browser_meta_transport()
    assert a is b


def test_profile_dir_uses_runtime_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIG_DIR", str(tmp_path))
    gbc._reset_browser_meta_transport_for_tests()

    client = gbc.get_browser_meta_transport()

    assert str(tmp_path) in client.profile_dir
    assert client.profile_dir.endswith(".gofile-chrome-profile")


def test_reset_hook_recreates_singleton_instance():
    first = gbc.get_browser_meta_transport()
    gbc._reset_browser_meta_transport_for_tests()

    second = gbc.get_browser_meta_transport()

    assert first is not second


def test_request_text_serializes_concurrent_access(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))

    class _FakeDriver:
        def __init__(self):
            self._active = 0
            self._max_active = 0
            self._state_lock = threading.Lock()

        @property
        def max_active(self):
            return self._max_active

        def get(self, _url):
            return None

        def execute_async_script(self, _script, *_args):
            with self._state_lock:
                self._active += 1
                self._max_active = max(self._max_active, self._active)
            time.sleep(0.05)
            with self._state_lock:
                self._active -= 1
            return '{"ok": true, "status": 200, "text": "ok"}'

    fake_driver = _FakeDriver()
    transport._driver = fake_driver
    monkeypatch.setattr(transport, "_ensure_driver", lambda: None)

    results = []

    def _call_request():
        results.append(transport.request_text("GET", "https://example.com"))

    t1 = threading.Thread(target=_call_request)
    t2 = threading.Thread(target=_call_request)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(results) == 2
    assert results == ["ok", "ok"]
    assert fake_driver.max_active == 1


def test_request_text_warms_gofile_origin_before_first_fetch(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))
    call_order = []

    class _FakeDriver:
        def get(self, url):
            call_order.append(("get", url))

        def execute_async_script(self, _script, *_args):
            call_order.append(("fetch", _args[0]))
            return '{"ok": true, "status": 200, "text": "ok"}'

    transport._driver = _FakeDriver()
    monkeypatch.setattr(transport, "_ensure_driver", lambda: None)

    response_text = transport.request_text("GET", "https://api.gofile.io/accounts")

    assert response_text == "ok"
    assert call_order[0] == ("get", "https://gofile.io/")
    assert call_order[1][0] == "fetch"


def test_ensure_driver_falls_back_to_selenium_when_uc_missing(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))
    captured = {"args": []}

    class _FakeOptions:
        def __init__(self):
            self.arguments = []

        def add_argument(self, value):
            self.arguments.append(value)

    class _FakeChromeDriver:
        def quit(self):
            return None

    def _fake_chrome(options=None):
        assert options is not None
        captured["args"] = list(options.arguments)
        return _FakeChromeDriver()

    def _fake_import_module(name):
        if name == "undetected_chromedriver":
            raise ImportError("uc missing")
        if name == "selenium.webdriver":
            return types.SimpleNamespace(Chrome=_fake_chrome)
        if name == "selenium.webdriver.chrome.options":
            return types.SimpleNamespace(Options=_FakeOptions)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(gbc.importlib, "import_module", _fake_import_module)

    transport._ensure_driver()

    assert transport._driver is not None
    assert "--headless=new" in captured["args"]
    assert f"--user-data-dir={tmp_path}" in captured["args"]


def test_ensure_driver_falls_back_to_selenium_when_uc_launch_fails(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))
    events = []

    class _FakeOptions:
        def __init__(self):
            self.arguments = []

        def add_argument(self, value):
            self.arguments.append(value)

    class _FakeChromeDriver:
        def quit(self):
            return None

    def _fake_uc_chrome(options=None):
        assert options is not None
        events.append("uc")
        raise RuntimeError("session not created")

    def _fake_selenium_chrome(options=None):
        assert options is not None
        events.append("selenium")
        return _FakeChromeDriver()

    def _fake_import_module(name):
        if name == "undetected_chromedriver":
            return types.SimpleNamespace(
                ChromeOptions=_FakeOptions,
                Chrome=_fake_uc_chrome,
            )
        if name == "selenium.webdriver":
            return types.SimpleNamespace(Chrome=_fake_selenium_chrome)
        if name == "selenium.webdriver.chrome.options":
            return types.SimpleNamespace(Options=_FakeOptions)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(gbc.importlib, "import_module", _fake_import_module)

    transport._ensure_driver()

    assert transport._driver is not None
    assert events == ["uc", "selenium"]
