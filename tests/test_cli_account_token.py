import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run


def test_extract_account_token_supports_data_dot_token_assignment():
    token = run.extract_account_token("data.token = 'my-token-123'")

    assert token == "my-token-123"


def test_extract_account_token_supports_accounts_response_json():
    token = run.extract_account_token('{"status":"ok","data":{"token":"json-token-abc"}}')

    assert token == "json-token-abc"


def test_main_uses_account_token_for_execution(tmp_path):
    client = None

    class FakeGoFile:
        def __init__(self):
            self.token = ""
            self.execute_calls = []

        def execute(self, **kwargs):
            self.execute_calls.append((kwargs["url"], self.token))

    def _factory():
        nonlocal client
        client = FakeGoFile()
        return client

    exit_code = run.main(
        argv=[
            "--account-token",
            "data.token = pasted-token-value",
            "https://gofile.io/d/abc123",
            "-d",
            str(tmp_path),
        ],
        gofile_factory=_factory,
    )

    assert exit_code == 0
    assert client is not None
    assert client.execute_calls == [
        ("https://gofile.io/d/abc123", "pasted-token-value"),
    ]


def test_main_account_token_overrides_refresh_auth_token(tmp_path):
    client = None

    class FakeGoFile:
        def __init__(self):
            self.token = ""
            self.execute_calls = []

        def update_token(self, force_refresh=False):
            if force_refresh:
                self.token = "refreshed-token"

        def update_wt(self, force_refresh=False):
            return None

        def execute(self, **kwargs):
            self.execute_calls.append((kwargs["url"], self.token))

    def _factory():
        nonlocal client
        client = FakeGoFile()
        return client

    exit_code = run.main(
        argv=[
            "--refresh-auth",
            "--account-token",
            "data.token=pasted-token-value",
            "https://gofile.io/d/abc123",
            "-d",
            str(tmp_path),
        ],
        gofile_factory=_factory,
    )

    assert exit_code == 0
    assert client is not None
    assert client.execute_calls == [
        ("https://gofile.io/d/abc123", "pasted-token-value"),
    ]
