import json
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run


def _fresh_client(tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.cache_file = str(tmp_path / ".gofile_api_cache.json")
    client._cache_loaded = False
    client.token = ""
    client.wt = ""
    return client


def test_update_token_reads_fresh_cache_without_network(monkeypatch, tmp_path):
    cache_file = tmp_path / ".gofile_api_cache.json"
    cache_file.write_text(
        json.dumps({"token": {"value": "cached-token", "updated_at": time.time()}}),
        encoding="utf-8",
    )

    client = _fresh_client(tmp_path)

    def _fail_post(*_args, **_kwargs):
        raise AssertionError("requests.post should not be called when cache is valid")

    monkeypatch.setattr(run.requests, "post", _fail_post)

    client.update_token()

    assert client.token == "cached-token"


def test_update_token_force_refresh_requests_new_token(monkeypatch, tmp_path):
    cache_file = tmp_path / ".gofile_api_cache.json"
    cache_file.write_text(
        json.dumps({"token": {"value": "old-token", "updated_at": time.time()}}),
        encoding="utf-8",
    )

    client = _fresh_client(tmp_path)
    calls = {"count": 0}

    def _fake_request_json(
        method,
        url,
        headers=None,
        params=None,
        timeout=10,
        credentials="include",
    ):
        del headers, params
        calls["count"] += 1
        assert method == "POST"
        assert url == "https://api.gofile.io/accounts"
        assert timeout == run.DEFAULT_TIMEOUT
        assert credentials == "omit"
        return {"status": "ok", "data": {"token": "fresh-token"}}

    monkeypatch.setattr(client.meta_transport, "request_json", _fake_request_json)

    client.update_token(force_refresh=True)

    assert calls["count"] == 1
    assert client.token == "fresh-token"


def test_update_wt_reads_fresh_cache_without_network(monkeypatch, tmp_path):
    cache_file = tmp_path / ".gofile_api_cache.json"
    cache_file.write_text(
        json.dumps({"wt": {"value": "cached-wt", "updated_at": time.time()}}),
        encoding="utf-8",
    )

    client = _fresh_client(tmp_path)

    def _fail_get(*_args, **_kwargs):
        raise AssertionError("requests.get should not be called when cache is valid")

    monkeypatch.setattr(run.requests, "get", _fail_get)

    client.update_wt()

    assert client.wt == "cached-wt"


def test_update_wt_force_refresh_requests_new_wt(monkeypatch, tmp_path):
    cache_file = tmp_path / ".gofile_api_cache.json"
    cache_file.write_text(
        json.dumps({"wt": {"value": "old-wt", "updated_at": time.time()}}),
        encoding="utf-8",
    )

    client = _fresh_client(tmp_path)
    calls = {"count": 0}

    def _fake_request_text(method, url, headers=None, params=None, timeout=10):
        del headers, params
        calls["count"] += 1
        assert method == "GET"
        assert url == "https://gofile.io/dist/js/config.js"
        assert timeout == run.DEFAULT_TIMEOUT
        return 'appdata.wt = "fresh-wt"'

    monkeypatch.setattr(client.meta_transport, "request_text", _fake_request_text)

    client.update_wt(force_refresh=True)

    assert calls["count"] == 1
    assert client.wt == "fresh-wt"
