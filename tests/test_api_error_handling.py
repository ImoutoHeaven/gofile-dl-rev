import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


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


def test_execute_does_not_force_refresh_on_not_premium(monkeypatch, tmp_path):
    client = _fresh_client()
    token_force_calls = []
    wt_force_calls = []

    def _fake_update_token(force_refresh=False):
        token_force_calls.append(force_refresh)

    def _fake_update_wt(force_refresh=False):
        wt_force_calls.append(force_refresh)

    monkeypatch.setattr(client, "update_token", _fake_update_token)
    monkeypatch.setattr(client, "update_wt", _fake_update_wt)
    monkeypatch.setattr(
        run.requests,
        "get",
        lambda *_args, **_kwargs: _FakeResponse({"status": "error-notPremium", "data": {}}),
    )

    client.execute(dir=str(tmp_path), content_id="abc123")

    assert True not in token_force_calls
    assert True not in wt_force_calls


def test_execute_force_refreshes_once_on_auth_error(monkeypatch, tmp_path):
    client = _fresh_client()
    token_force_calls = []
    wt_force_calls = []

    def _fake_update_token(force_refresh=False):
        token_force_calls.append(force_refresh)

    def _fake_update_wt(force_refresh=False):
        wt_force_calls.append(force_refresh)

    monkeypatch.setattr(client, "update_token", _fake_update_token)
    monkeypatch.setattr(client, "update_wt", _fake_update_wt)

    responses = iter(
        [
            {"status": "error-invalidToken", "data": {"message": "token expired"}},
            {"status": "ok", "data": {"type": "folder", "name": "demo", "children": {}}},
        ]
    )

    monkeypatch.setattr(
        run.requests,
        "get",
        lambda *_args, **_kwargs: _FakeResponse(next(responses)),
    )

    client.execute(dir=str(tmp_path), content_id="abc123")

    assert token_force_calls.count(True) == 1
    assert wt_force_calls.count(True) == 1
