import json
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
    transport._session_bootstrapped = True
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
    transport._session_bootstrapped = True
    monkeypatch.setattr(transport, "_ensure_driver", lambda: None)

    response_text = transport.request_text("GET", "https://api.gofile.io/accounts")

    assert response_text == "ok"
    assert call_order[0] == ("get", "https://gofile.io/")
    assert call_order[1][0] == "fetch"


def test_ensure_driver_uses_playwright_persistent_context(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))
    captured = {"launch": {}, "init_scripts": []}

    class _FakePage:
        def goto(self, _url):
            return None

        def evaluate(self, _script):
            return {"localStorage": {}, "sessionStorage": {}}

    class _FakeContext:
        def __init__(self):
            self.pages = [_FakePage()]

        def cookies(self, _url):
            return []

        def add_init_script(self, script):
            captured["init_scripts"].append(script)

        def close(self):
            return None

    class _FakePlaywright:
        def __init__(self):
            self.chromium = types.SimpleNamespace(launch_persistent_context=self._launch)

        def _launch(self, **kwargs):
            captured["launch"].update(kwargs)
            return _FakeContext()

        def stop(self):
            return None

    class _FakeSyncPlaywrightFactory:
        def start(self):
            return _FakePlaywright()

    def _fake_sync_playwright():
        return _FakeSyncPlaywrightFactory()

    def _fake_import_module(name):
        if name == "playwright.sync_api":
            return types.SimpleNamespace(sync_playwright=_fake_sync_playwright)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(gbc.importlib, "import_module", _fake_import_module)

    transport._ensure_driver()

    assert transport._driver is not None
    launch = captured["launch"]
    assert launch["user_data_dir"] == str(tmp_path)
    assert launch["headless"] is True
    assert launch["user_agent"].startswith("Mozilla/5.0")
    assert launch["locale"] == "en-US"
    assert launch["timezone_id"] == "UTC"
    assert launch["viewport"] == {"width": 1366, "height": 768}
    assert launch["extra_http_headers"]["Accept-Language"] == "en-US,en;q=0.9"
    assert launch["extra_http_headers"]["DNT"] == "1"
    assert "--headless=new" in launch["args"]
    assert "--disable-blink-features=AutomationControlled" in launch["args"]
    assert captured["init_scripts"]
    assert "'webdriver'" in captured["init_scripts"][0]


def test_ensure_driver_creates_new_page_when_context_has_none(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))
    seen = {"new_page_calls": 0}

    class _FakePage:
        def goto(self, _url):
            return None

        def evaluate(self, _script):
            return {"localStorage": {}, "sessionStorage": {}}

    class _FakeContext:
        def __init__(self):
            self.pages = []

        def cookies(self, _url):
            return []

        def new_page(self):
            seen["new_page_calls"] += 1
            return _FakePage()

        def add_init_script(self, _script):
            return None

        def close(self):
            return None

    class _FakePlaywright:
        def __init__(self):
            self.chromium = types.SimpleNamespace(launch_persistent_context=self._launch)

        def _launch(self, **_kwargs):
            return _FakeContext()

        def stop(self):
            return None

    class _FakeSyncPlaywrightFactory:
        def start(self):
            return _FakePlaywright()

    def _fake_sync_playwright():
        return _FakeSyncPlaywrightFactory()

    def _fake_import_module(name):
        if name == "playwright.sync_api":
            return types.SimpleNamespace(sync_playwright=_fake_sync_playwright)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(gbc.importlib, "import_module", _fake_import_module)

    transport._ensure_driver()

    assert transport._driver is not None
    assert seen["new_page_calls"] == 1


def test_ensure_driver_applies_proxy_from_env(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))
    captured = {}

    class _FakePage:
        def goto(self, _url):
            return None

        def evaluate(self, _script):
            return {"localStorage": {}, "sessionStorage": {}}

    class _FakeContext:
        def __init__(self):
            self.pages = [_FakePage()]

        def cookies(self, _url):
            return []

        def add_init_script(self, _script):
            return None

        def close(self):
            return None

    class _FakePlaywright:
        def __init__(self):
            self.chromium = types.SimpleNamespace(launch_persistent_context=self._launch)

        def _launch(self, **kwargs):
            captured.update(kwargs)
            return _FakeContext()

        def stop(self):
            return None

    class _FakeSyncPlaywrightFactory:
        def start(self):
            return _FakePlaywright()

    def _fake_sync_playwright():
        return _FakeSyncPlaywrightFactory()

    def _fake_import_module(name):
        if name == "playwright.sync_api":
            return types.SimpleNamespace(sync_playwright=_fake_sync_playwright)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setenv("GOFILE_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setattr(gbc.importlib, "import_module", _fake_import_module)

    transport._ensure_driver()

    assert captured["proxy"] == {"server": "http://127.0.0.1:7890"}


def test_ensure_driver_persists_gofile_session_state(tmp_path, monkeypatch):
    transport = gbc.BrowserMetaTransport(profile_dir=str(tmp_path))

    class _FakeDriver:
        def get(self, _url):
            return None

        def get_cookies(self):
            return [{"name": "accountToken", "value": "token-123"}]

        def execute_script(self, _script):
            return {
                "localStorage": {"theme": "light"},
                "sessionStorage": {"wt": "wt-abc"},
            }

        def quit(self):
            return None

    monkeypatch.setattr(transport, "_create_browser_driver", lambda: _FakeDriver())

    transport._ensure_driver()

    state_path = os.path.join(str(tmp_path), "gofile-session-state.json")
    assert os.path.exists(state_path)

    with open(state_path, encoding="utf-8") as file_obj:
        state = json.load(file_obj)

    assert state["origin"] == "https://gofile.io/"
    assert state["cookies"] == [{"name": "accountToken", "value": "token-123"}]
    assert state["localStorage"]["theme"] == "light"
    assert state["sessionStorage"]["wt"] == "wt-abc"
