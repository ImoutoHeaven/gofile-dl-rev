import os
import sys
import threading
import time

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
