import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run


def _fresh_client():
    run.GoFileMeta._instances.clear()
    return run.GoFile()


def test_should_refresh_auth_only_for_auth_related_statuses():
    assert run.should_refresh_auth("error-invalidToken")
    assert run.should_refresh_auth("error-notAuthenticated")
    assert not run.should_refresh_auth("error-notPremium")
    assert not run.should_refresh_auth("error-rateLimit")


def test_parse_api_error_details_prefers_message_fields():
    status, details = run.parse_api_error_details(
        {
            "status": "error-rateLimit",
            "message": "Too many requests",
            "data": {"retryAfter": 60},
        }
    )

    assert status == "error-rateLimit"
    assert "Too many requests" in details
    assert "retryAfter=60" in details


def test_execute_uses_meta_transport_for_contents(monkeypatch, tmp_path):
    client = _fresh_client()
    seen = {}
    legacy_calls = {"count": 0}

    def _fake_request_json(method, url, headers=None, params=None, timeout=10):
        seen["method"] = method
        seen["url"] = url
        seen["headers"] = headers
        seen["params"] = params
        seen["timeout"] = timeout
        return {"status": "ok", "data": {"type": "folder", "name": "demo", "children": {}}}

    def _legacy_get(*_args, **_kwargs):
        legacy_calls["count"] += 1
        raise AssertionError("requests.get metadata path should not be used")

    monkeypatch.setattr(client.meta_transport, "request_json", _fake_request_json)
    monkeypatch.setattr(client, "update_token", lambda force_refresh=False: setattr(client, "token", "tk"))
    monkeypatch.setattr(client, "update_wt", lambda force_refresh=False: setattr(client, "wt", "wt"))
    monkeypatch.setattr(run.requests, "get", _legacy_get)

    client.execute(dir=str(tmp_path), content_id="abc123")

    assert legacy_calls["count"] == 0
    assert seen["method"] == "GET"
    assert seen["url"] == "https://api.gofile.io/contents/abc123"
    assert seen["headers"] == {
        "Authorization": "Bearer tk",
        "X-Website-Token": "wt",
    }
    assert seen["params"] == {
        "contentFilter": "",
        "page": 1,
        "pageSize": 1000,
        "sortField": "name",
        "sortDirection": 1,
    }
    assert seen["timeout"] == run.DEFAULT_TIMEOUT


def test_execute_does_not_force_refresh_on_not_premium(monkeypatch, tmp_path):
    client = _fresh_client()
    token_force_calls = []
    wt_force_calls = []
    request_calls = {"count": 0}
    seen_headers = []
    legacy_calls = {"count": 0}

    def _fake_update_token(force_refresh=False):
        token_force_calls.append(force_refresh)
        client.token = "tk"

    def _fake_update_wt(force_refresh=False):
        wt_force_calls.append(force_refresh)
        client.wt = "wt"

    def _fake_request_json(method, url, headers=None, params=None, timeout=10):
        del method, url, params, timeout
        request_calls["count"] += 1
        seen_headers.append(dict(headers or {}))
        return {"status": "error-notPremium", "data": {}}

    def _legacy_get(*_args, **_kwargs):
        legacy_calls["count"] += 1
        raise AssertionError("requests.get metadata path should not be used")

    monkeypatch.setattr(client, "update_token", _fake_update_token)
    monkeypatch.setattr(client, "update_wt", _fake_update_wt)
    monkeypatch.setattr(client.meta_transport, "request_json", _fake_request_json)
    monkeypatch.setattr(run.requests, "get", _legacy_get)

    client.execute(dir=str(tmp_path), content_id="abc123")

    assert request_calls["count"] == 2
    assert legacy_calls["count"] == 0
    assert seen_headers[0].get("X-Website-Token") == "wt"
    assert "X-Website-Token" not in seen_headers[1]
    assert True not in token_force_calls
    assert True not in wt_force_calls


def test_execute_force_refreshes_once_on_auth_error(monkeypatch, tmp_path):
    client = _fresh_client()
    token_force_calls = []
    wt_force_calls = []
    request_calls = {"count": 0}
    legacy_calls = {"count": 0}

    def _fake_update_token(force_refresh=False):
        token_force_calls.append(force_refresh)
        client.token = "tk"

    def _fake_update_wt(force_refresh=False):
        wt_force_calls.append(force_refresh)
        client.wt = "wt"

    responses = iter(
        [
            {"status": "error-invalidToken", "data": {"message": "token expired"}},
            {"status": "ok", "data": {"type": "folder", "name": "demo", "children": {}}},
        ]
    )

    def _fake_request_json(method, url, headers=None, params=None, timeout=10):
        del method, url, headers, params, timeout
        request_calls["count"] += 1
        return next(responses)

    def _legacy_get(*_args, **_kwargs):
        legacy_calls["count"] += 1
        raise AssertionError("requests.get metadata path should not be used")

    monkeypatch.setattr(client, "update_token", _fake_update_token)
    monkeypatch.setattr(client, "update_wt", _fake_update_wt)
    monkeypatch.setattr(client.meta_transport, "request_json", _fake_request_json)
    monkeypatch.setattr(run.requests, "get", _legacy_get)

    client.execute(dir=str(tmp_path), content_id="abc123")

    assert request_calls["count"] == 2
    assert legacy_calls["count"] == 0
    assert token_force_calls.count(True) == 1
    assert wt_force_calls.count(True) == 1
