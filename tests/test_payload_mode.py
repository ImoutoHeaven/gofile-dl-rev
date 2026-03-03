import json
import base64
import os
import sys
import threading
import time
from typing import cast

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run


def test_collect_download_jobs_from_payload_handles_nested_folders(tmp_path):
    payload = {
        "status": "ok",
        "data": {
            "type": "folder",
            "name": "Root",
            "children": {
                "file-1": {
                    "type": "file",
                    "name": "A.txt",
                    "link": "https://cdn.example/a",
                },
                "folder-1": {
                    "type": "folder",
                    "name": "Sub",
                    "children": {
                        "file-2": {
                            "type": "file",
                            "name": "B.txt",
                            "link": "https://cdn.example/b",
                        }
                    },
                },
            },
        },
    }

    jobs = run.collect_download_jobs_from_payload(payload, base_dir=str(tmp_path))

    assert jobs == [
        (
            "https://cdn.example/a",
            os.path.join(str(tmp_path), "Root", "A.txt"),
        ),
        (
            "https://cdn.example/b",
            os.path.join(str(tmp_path), "Root", "Sub", "B.txt"),
        ),
    ]


def test_collect_download_jobs_from_payload_rejects_error_payload(tmp_path):
    payload = {"status": "error-rateLimit", "data": {}}

    with pytest.raises(ValueError):
        run.collect_download_jobs_from_payload(payload, base_dir=str(tmp_path))


def test_main_payload_mode_downloads_without_content_api(tmp_path):
    payload = {
        "status": "ok",
        "data": {
            "type": "folder",
            "name": "Root",
            "children": {
                "file-1": {
                    "type": "file",
                    "name": "A.txt",
                    "link": "https://cdn.example/a",
                }
            },
        },
    }
    payload_file = tmp_path / "payload.json"
    payload_file.write_text(json.dumps(payload), encoding="utf-8")

    calls = []

    class FakeGoFile:
        def __init__(self):
            self.token = ""

        def execute(self, **kwargs):
            raise AssertionError("URL/content API execute path should not run in payload mode")

        def download(self, link, file, **kwargs):
            calls.append((link, file, self.token))

        def execute_payload(self, **kwargs):
            jobs = run.collect_download_jobs_from_payload(kwargs["payload"], kwargs["dir"])
            for link, file in jobs:
                self.download(link, file)

    exit_code = run.main(
        argv=[
            "--content-payload-file",
            str(payload_file),
            "--account-token",
            "data.token=pasted-token",
            "-d",
            str(tmp_path),
        ],
        gofile_factory=FakeGoFile,
    )

    assert exit_code == 0
    assert calls == [
        (
            "https://cdn.example/a",
            os.path.join(str(tmp_path), "Root", "A.txt"),
            "pasted-token",
        )
    ]


def test_load_content_payloads_supports_jsonl_file(tmp_path):
    payload_file = tmp_path / "payloads.jsonl"
    payload_file.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "status": "ok",
                        "data": {
                            "type": "file",
                            "name": "A.txt",
                            "link": "https://cdn.example/a",
                        },
                    }
                ),
                json.dumps(
                    {
                        "status": "ok",
                        "data": {
                            "type": "file",
                            "name": "B.txt",
                            "link": "https://cdn.example/b",
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    payloads = run.load_content_payloads(str(payload_file))

    assert len(payloads) == 2
    assert payloads[0]["data"]["name"] == "A.txt"
    assert payloads[1]["data"]["name"] == "B.txt"


def test_load_content_payloads_supports_blank_line_separated_multiline_json(tmp_path):
    payload_file = tmp_path / "payloads-multiline.json"
    payload_file.write_text(
        """{
  "status": "ok",
  "data": {
    "type": "file",
    "name": "A.txt",
    "link": "https://cdn.example/a"
  }
}

{
  "status": "ok",
  "data": {
    "type": "file",
    "name": "B.txt",
    "link": "https://cdn.example/b"
  }
}
""",
        encoding="utf-8",
    )

    payloads = run.load_content_payloads(str(payload_file))

    assert len(payloads) == 2
    assert payloads[0]["data"]["name"] == "A.txt"
    assert payloads[1]["data"]["name"] == "B.txt"


def test_parse_payload_bundle_supports_json_with_payload_jsonl():
    payload_a = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "A.txt",
            "link": "https://cdn.example/a",
        },
    }
    payload_b = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "B.txt",
            "link": "https://cdn.example/b",
        },
    }
    bundle = {
        "schema": "gofile-payload-bundle/v1",
        "accountToken": "data.token = bundle-token",
        "payloadJsonl": "\n".join([json.dumps(payload_a), json.dumps(payload_b)]),
    }

    token, payloads = run.parse_payload_bundle(json.dumps(bundle))

    assert token == "bundle-token"
    assert [payload["data"]["name"] for payload in payloads] == ["A.txt", "B.txt"]


def test_parse_payload_bundle_supports_base64url_blob():
    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "A.txt",
            "link": "https://cdn.example/a",
        },
    }
    bundle = {
        "schema": "gofile-payload-bundle/v1",
        "accountToken": "bundle-token",
        "payloads": [payload],
    }
    encoded = base64.urlsafe_b64encode(json.dumps(bundle).encode("utf-8")).decode("ascii").rstrip("=")

    token, payloads = run.parse_payload_bundle(encoded)

    assert token == "bundle-token"
    assert payloads == [payload]


def test_parse_payload_bundle_supports_quoted_base64_blob_with_newlines():
    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "A.txt",
            "link": "https://cdn.example/a",
        },
    }
    bundle = {
        "schema": "gofile-payload-bundle/v1",
        "accountToken": "bundle-token",
        "payloads": [payload],
    }
    encoded = base64.urlsafe_b64encode(json.dumps(bundle).encode("utf-8")).decode("ascii").rstrip("=")
    wrapped = f"'{encoded[:24]}\n{encoded[24:]}'"

    token, payloads = run.parse_payload_bundle(wrapped)

    assert token == "bundle-token"
    assert payloads == [payload]


def test_main_payload_bundle_prompt_mode_parses_double_blank_terminated_input(tmp_path):
    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "A.txt",
            "link": "https://cdn.example/a",
        },
    }
    bundle = {
        "schema": "gofile-payload-bundle/v1",
        "accountToken": "data.token=bundle-token",
        "payloads": [payload],
    }
    encoded = base64.urlsafe_b64encode(json.dumps(bundle).encode("utf-8")).decode("ascii").rstrip("=")

    calls = []

    class FakeGoFile:
        def __init__(self):
            self.token = ""

        def download(self, link, file, **_kwargs):
            calls.append((link, file, self.token))

    lines = [encoded[:20], encoded[20:], "", ""]
    line_iter = iter(lines)

    def _input_fn(_prompt=""):
        return next(line_iter)

    exit_code = run.main(
        argv=["-pb", "-d", str(tmp_path)],
        input_fn=_input_fn,
        gofile_factory=FakeGoFile,
    )

    assert exit_code == 0
    assert calls == [
        (
            "https://cdn.example/a",
            os.path.join(str(tmp_path), "A.txt"),
            "bundle-token",
        )
    ]


def test_main_payload_bundle_file_path_reads_bundle_text(tmp_path):
    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "A.txt",
            "link": "https://cdn.example/a",
        },
    }
    bundle = {
        "schema": "gofile-payload-bundle/v1",
        "accountToken": "data.token=bundle-token",
        "payloads": [payload],
    }
    encoded = base64.urlsafe_b64encode(json.dumps(bundle).encode("utf-8")).decode("ascii").rstrip("=")
    bundle_file = tmp_path / "bundle.txt"
    bundle_file.write_text(encoded, encoding="utf-8")

    calls = []

    class FakeGoFile:
        def __init__(self):
            self.token = ""

        def download(self, link, file, **_kwargs):
            calls.append((link, file, self.token))

    exit_code = run.main(
        argv=["-pb", str(bundle_file), "-d", str(tmp_path)],
        gofile_factory=FakeGoFile,
    )

    assert exit_code == 0
    assert calls == [
        (
            "https://cdn.example/a",
            os.path.join(str(tmp_path), "A.txt"),
            "bundle-token",
        )
    ]


def test_main_payload_mode_downloads_multiple_payloads_from_jsonl(tmp_path):
    payload_file = tmp_path / "payloads.jsonl"
    payload_file.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "status": "ok",
                        "data": {
                            "type": "file",
                            "name": "A.txt",
                            "link": "https://cdn.example/a",
                        },
                    }
                ),
                json.dumps(
                    {
                        "status": "ok",
                        "data": {
                            "type": "file",
                            "name": "B.txt",
                            "link": "https://cdn.example/b",
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    calls = []

    class FakeGoFile:
        def __init__(self):
            self.token = ""

        def download(self, link, file, **kwargs):
            calls.append((link, file))

    exit_code = run.main(
        argv=[
            "--content-payload-file",
            str(payload_file),
            "-d",
            str(tmp_path),
        ],
        gofile_factory=FakeGoFile,
    )

    assert exit_code == 0
    assert calls == [
        ("https://cdn.example/a", os.path.join(str(tmp_path), "A.txt")),
        ("https://cdn.example/b", os.path.join(str(tmp_path), "B.txt")),
    ]


def test_collect_download_jobs_from_payload_uses_relative_path(tmp_path):
    payload = {
        "type": "file",
        "name": "ignored.txt",
        "link": "https://cdn.example/a",
        "relativePath": "Root/Sub/A.txt",
    }

    jobs = run.collect_download_jobs_from_payload(payload, base_dir=str(tmp_path))

    assert jobs == [
        ("https://cdn.example/a", os.path.join(str(tmp_path), "Root", "Sub", "A.txt"))
    ]


def test_execute_payload_records_failed_files_for_retry(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.failed_files = []

    def _fake_download(link, file, **_kwargs):
        return not link.endswith("/fail")

    monkeypatch.setattr(client, "download", _fake_download)

    payload = {
        "status": "ok",
        "data": {
            "type": "folder",
            "name": "Root",
            "children": {
                "ok-file": {
                    "type": "file",
                    "name": "A.txt",
                    "link": "https://cdn.example/ok",
                },
                "failed-file": {
                    "type": "file",
                    "name": "B.txt",
                    "link": "https://cdn.example/fail",
                },
            },
        },
    }

    client.execute_payload(dir=str(tmp_path), payload=payload)

    assert len(client.failed_files) == 1
    failed = client.failed_files[0]
    assert failed["type"] == "file"
    assert failed["link"] == "https://cdn.example/fail"
    assert failed["relativePath"] == "Root/B.txt"


def test_main_writes_failed_files_json_parseable_for_payload_retry(tmp_path):
    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "seed.txt",
            "link": "https://cdn.example/seed",
        },
    }
    payload_file = tmp_path / "payload.json"
    payload_file.write_text(json.dumps(payload), encoding="utf-8")

    class FakeGoFile:
        def __init__(self):
            self.token = ""
            self.failed_files = []

        def execute_payload(self, **_kwargs):
            self.failed_files = [
                {
                    "type": "file",
                    "name": "B.txt",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Root/B.txt",
                    "error": "network error",
                }
            ]

    exit_code = run.main(
        argv=["--content-payload-file", str(payload_file), "-d", str(tmp_path)],
        gofile_factory=FakeGoFile,
    )

    failed_path = tmp_path / "failed_files.json"
    assert exit_code == 0
    assert failed_path.exists()

    retry_payloads = run.load_content_payloads(str(failed_path))
    assert len(retry_payloads) == 1
    retry_jobs = run.collect_download_jobs_from_payload(retry_payloads[0], base_dir=str(tmp_path))
    assert retry_jobs == [
        ("https://cdn.example/fail", os.path.join(str(tmp_path), "Root", "B.txt"))
    ]


def test_execute_payload_skips_already_downloaded_file_by_size_md5(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.failed_files = []

    file_dir = tmp_path / "Root"
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / "A.txt"
    file_path.write_bytes(b"hello-world")

    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "A.txt",
            "relativePath": "Root/A.txt",
            "link": "https://cdn.example/a",
            "size": 11,
            "md5": "2095312189753de6ad47dfe20cbe97ec",
        },
    }

    def _unexpected_download(*_args, **_kwargs):
        raise AssertionError("download should be skipped for already downloaded file")

    monkeypatch.setattr(client, "download", _unexpected_download)

    client.execute_payload(dir=str(tmp_path), payload=payload)

    assert client.failed_files == []


def test_execute_payload_flushes_failed_report_immediately(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.failed_files = []
    client.failed_report_dir = str(tmp_path)

    def _always_fail(*_args, **_kwargs):
        return False

    monkeypatch.setattr(client, "download", _always_fail)

    payload = {
        "status": "ok",
        "data": {
            "type": "file",
            "name": "B.txt",
            "relativePath": "Root/B.txt",
            "link": "https://cdn.example/fail",
            "size": 12,
            "md5": "d41d8cd98f00b204e9800998ecf8427e",
        },
    }

    client.execute_payload(dir=str(tmp_path), payload=payload)

    failed_path = tmp_path / "failed_files.json"
    assert failed_path.exists()
    failed_payloads = run.load_content_payloads(str(failed_path))
    assert len(failed_payloads) == 1
    assert failed_payloads[0]["relativePath"] == "Root/B.txt"


def test_download_uses_libcurl_backend(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    observed_session_kwargs = {}
    observed_get_kwargs = {}

    class _FakeCurlResponse:
        def __init__(self):
            self.headers = {"Content-Length": "5"}
            self.closed = False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            del chunk_size
            yield b"hello"

        def close(self):
            self.closed = True

    class _FakeCurlSession:
        def __init__(self, **kwargs):
            observed_session_kwargs.update(kwargs)

        def get(self, *_args, **kwargs):
            observed_get_kwargs.update(kwargs)
            return _FakeCurlResponse()

    def _requests_get_should_not_run(*_args, **_kwargs):
        raise AssertionError("requests.get download path should not be used")

    monkeypatch.setattr(run.requests, "get", _requests_get_should_not_run)
    monkeypatch.setattr(run.curl_requests, "Session", _FakeCurlSession)

    client = run.GoFile()
    client.token = "token-123"

    output_path = tmp_path / "curl" / "ok.bin"
    ok = client.download("https://cdn.example/file", str(output_path), retry_attempts=0)

    assert ok is True
    assert observed_session_kwargs.get("impersonate") == "chrome"
    assert observed_get_kwargs.get("impersonate") is None
    assert observed_get_kwargs.get("cookies") == {"accountToken": "token-123"}
    assert output_path.read_bytes() == b"hello"


def test_download_session_uses_proxy_env(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    observed_session_kwargs = {}

    class _FakeCurlResponse:
        def __init__(self):
            self.headers = {"Content-Length": "2"}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            del chunk_size
            yield b"ok"

        def close(self):
            return None

    class _FakeCurlSession:
        def __init__(self, **kwargs):
            observed_session_kwargs.update(kwargs)

        def get(self, *_args, **_kwargs):
            return _FakeCurlResponse()

    monkeypatch.setenv("GOFILE_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setattr(run.curl_requests, "Session", _FakeCurlSession)

    client = run.GoFile()
    output_path = tmp_path / "curl" / "proxy.bin"
    ok = client.download("https://cdn.example/file", str(output_path), retry_attempts=0)

    assert ok is True
    assert observed_session_kwargs.get("proxy") == "http://127.0.0.1:7890"


def test_execute_payload_downloads_with_two_workers(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()

    payload = {
        "status": "ok",
        "data": {
            "type": "folder",
            "name": "Root",
            "children": {
                "file-1": {"type": "file", "name": "A.bin", "link": "https://cdn.example/a"},
                "file-2": {"type": "file", "name": "B.bin", "link": "https://cdn.example/b"},
                "file-3": {"type": "file", "name": "C.bin", "link": "https://cdn.example/c"},
                "file-4": {"type": "file", "name": "D.bin", "link": "https://cdn.example/d"},
            },
        },
    }

    active_downloads = 0
    max_active_downloads = 0
    lock = threading.Lock()

    def _fake_download(_link=None, _file=None, **_kwargs):
        if _link is None:
            _link = _kwargs.get("link")
        if _file is None:
            _file = _kwargs.get("file")
        assert _link
        assert _file
        nonlocal active_downloads, max_active_downloads
        with lock:
            active_downloads += 1
            max_active_downloads = max(max_active_downloads, active_downloads)
        time.sleep(0.05)
        with lock:
            active_downloads -= 1
        return True

    monkeypatch.setattr(client, "download", _fake_download)

    client.execute_payload(dir=str(tmp_path), payload=payload)

    assert max_active_downloads == 2


def test_execute_downloads_with_two_workers_for_contents_api(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.token = "token-123"
    client.wt = "wt-123"

    class _FakeMetaTransport:
        def request_json(self, method, url, headers=None, params=None, timeout=10, credentials="include"):
            del method, headers, params, timeout, credentials
            content_id = url.rsplit("/", 1)[-1]
            assert content_id == "root-folder"
            return {
                "status": "ok",
                "data": {
                    "type": "folder",
                    "name": "Root",
                    "children": {
                        "file-1": {"type": "file", "name": "A.bin", "link": "https://cdn.example/a"},
                        "file-2": {"type": "file", "name": "B.bin", "link": "https://cdn.example/b"},
                        "file-3": {"type": "file", "name": "C.bin", "link": "https://cdn.example/c"},
                        "file-4": {"type": "file", "name": "D.bin", "link": "https://cdn.example/d"},
                    },
                },
            }

        def request_text(self, method, url, headers=None, params=None, timeout=10, credentials="include"):
            del method, url, headers, params, timeout, credentials
            return 'appdata.wt = "wt-123"'

    client.meta_transport = _FakeMetaTransport()

    active_downloads = 0
    max_active_downloads = 0
    lock = threading.Lock()

    def _fake_download(_link=None, _file=None, **_kwargs):
        if _link is None:
            _link = _kwargs.get("link")
        if _file is None:
            _file = _kwargs.get("file")
        assert _link
        assert _file
        nonlocal active_downloads, max_active_downloads
        with lock:
            active_downloads += 1
            max_active_downloads = max(max_active_downloads, active_downloads)
        time.sleep(0.05)
        with lock:
            active_downloads -= 1
        return True

    monkeypatch.setattr(client, "download", _fake_download)

    client.execute(dir=str(tmp_path), content_id="root-folder")

    assert max_active_downloads == 2


def test_download_pauses_and_recovers_when_10s_average_below_threshold(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    fake_clock = [0.0]
    sleep_calls = []

    class _FakeCurlResponse:
        def __init__(self):
            self.headers = {"Content-Length": str(11 * 10 * 1024)}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            del chunk_size
            for _ in range(11):
                fake_clock[0] += 1.0
                yield b"x" * (10 * 1024)

        def close(self):
            return None

    class _FakeCurlSession:
        def __init__(self, **_kwargs):
            return None

        def get(self, *_args, **_kwargs):
            return _FakeCurlResponse()

    def _fake_time():
        return fake_clock[0]

    def _fake_sleep(seconds):
        sleep_calls.append(seconds)
        fake_clock[0] += seconds

    monkeypatch.setattr(run.curl_requests, "Session", _FakeCurlSession)
    monkeypatch.setattr(run.time, "time", _fake_time)
    monkeypatch.setattr(run.time, "sleep", _fake_sleep)

    client = run.GoFile()
    output_path = tmp_path / "curl" / "slow.bin"
    ok = client.download(
        "https://cdn.example/file",
        str(output_path),
        chunk_size=10 * 1024,
        retry_attempts=0,
    )

    assert ok is True
    assert 3 in sleep_calls


def test_download_low_speed_guard_applies_even_with_low_throttle(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    fake_clock = [0.0]
    sleep_calls = []

    class _FakeCurlResponse:
        def __init__(self):
            self.headers = {"Content-Length": str(11 * 10 * 1024)}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            del chunk_size
            for _ in range(11):
                fake_clock[0] += 1.0
                yield b"x" * (10 * 1024)

        def close(self):
            return None

    class _FakeCurlSession:
        def __init__(self, **_kwargs):
            return None

        def get(self, *_args, **_kwargs):
            return _FakeCurlResponse()

    def _fake_time():
        return fake_clock[0]

    def _fake_sleep(seconds):
        sleep_calls.append(seconds)
        fake_clock[0] += seconds

    monkeypatch.setattr(run.curl_requests, "Session", _FakeCurlSession)
    monkeypatch.setattr(run.time, "time", _fake_time)
    monkeypatch.setattr(run.time, "sleep", _fake_sleep)

    client = run.GoFile()
    output_path = tmp_path / "curl" / "slow-throttled.bin"
    ok = client.download(
        "https://cdn.example/file",
        str(output_path),
        chunk_size=10 * 1024,
        throttle_speed=80,
        retry_attempts=0,
    )

    assert ok is True
    assert 3 in sleep_calls


def test_execute_reuses_existing_nested_folder_in_incremental_mode(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.token = "token-123"
    client.wt = "wt-123"

    root_path = tmp_path / "Root"
    renamed_nested_path = root_path / "Nested Renamed"
    renamed_nested_path.mkdir(parents=True, exist_ok=True)

    class _FakeTracker:
        def find_existing_folder(self, folder_name, parent_dir):
            if folder_name == "Nested" and parent_dir == str(root_path):
                return str(renamed_nested_path)
            return None

        def is_downloaded(self, _file_id, _file_name):
            return False

        def mark_downloaded(self, _file_id, _file_name):
            return None

    tracker = _FakeTracker()

    class _FakeMetaTransport:
        def request_json(self, method, url, headers=None, params=None, timeout=10, credentials="include"):
            del method, headers, params, timeout, credentials
            content_id = url.rsplit("/", 1)[-1]
            if content_id == "root-folder":
                return {
                    "status": "ok",
                    "data": {
                        "type": "folder",
                        "name": "Root",
                        "children": {
                            "nested-folder": {
                                "type": "folder",
                                "name": "Nested",
                            }
                        },
                    },
                }

            if content_id == "nested-folder":
                return {
                    "status": "ok",
                    "data": {
                        "type": "folder",
                        "name": "Nested",
                        "children": {
                            "file-1": {
                                "type": "file",
                                "name": "A.bin",
                                "link": "https://cdn.example/a",
                            }
                        },
                    },
                }

            raise AssertionError(f"unexpected content id {content_id}")

        def request_text(self, method, url, headers=None, params=None, timeout=10, credentials="include"):
            del method, url, headers, params, timeout, credentials
            return 'appdata.wt = "wt-123"'

    client.meta_transport = _FakeMetaTransport()

    downloaded_paths = []

    def _fake_download(_link=None, _file=None, **_kwargs):
        if _file is None:
            _file = _kwargs.get("file")
        assert _file
        downloaded_paths.append(_file)
        return True

    monkeypatch.setattr(client, "download", _fake_download)

    client.execute(
        dir=str(tmp_path),
        content_id="root-folder",
        incremental=True,
        tracker=cast(run.DownloadTracker, tracker),
    )

    assert downloaded_paths == [
        os.path.join(str(renamed_nested_path), "A.bin"),
    ]


def test_download_handles_missing_content_length_without_division_by_zero(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()

    class _FakeCurlResponse:
        def __init__(self):
            self.headers = {}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            del chunk_size
            yield b"hello"

        def close(self):
            return None

    class _FakeCurlSession:
        def __init__(self, **_kwargs):
            return None

        def get(self, *_args, **_kwargs):
            return _FakeCurlResponse()

    monkeypatch.setattr(run.curl_requests, "Session", _FakeCurlSession)

    client = run.GoFile()
    output_path = tmp_path / "curl" / "no-content-length.bin"
    observed_progress = []

    ok = client.download(
        "https://cdn.example/file",
        str(output_path),
        progress_callback=lambda pct: observed_progress.append(pct),
        retry_attempts=0,
    )

    assert ok is True
    assert observed_progress
    assert output_path.read_bytes() == b"hello"


def test_download_overwrites_existing_file_when_rename_hits_windows_exists_error(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()

    class _FakeCurlResponse:
        def __init__(self):
            self.headers = {"Content-Length": "5"}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            del chunk_size
            yield b"hello"

        def close(self):
            return None

    class _FakeCurlSession:
        def __init__(self, **_kwargs):
            return None

        def get(self, *_args, **_kwargs):
            return _FakeCurlResponse()

    real_replace = run.os.replace
    replace_calls = []

    def _fake_rename(src, dst):
        if run.os.path.exists(dst):
            raise FileExistsError(183, "file exists", dst)
        return None

    def _fake_replace(src, dst):
        replace_calls.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr(run.curl_requests, "Session", _FakeCurlSession)
    monkeypatch.setattr(run.os, "rename", _fake_rename)
    monkeypatch.setattr(run.os, "replace", _fake_replace)

    client = run.GoFile()
    output_path = tmp_path / "curl" / "existing.bin"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"old")

    ok = client.download("https://cdn.example/file", str(output_path), retry_attempts=0)

    assert ok is True
    assert replace_calls
    assert output_path.read_bytes() == b"hello"


def test_record_failed_file_keeps_report_snapshots_ordered_under_concurrency(monkeypatch, tmp_path):
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    client.set_failed_report_dir(str(tmp_path))

    observed_lengths = []
    observed_lock = threading.Lock()

    def _fake_write_failed_files_report(failed_files, out_dir):
        del out_dir
        # If writes happen outside the failed-files lock, this delay can reorder snapshots.
        time.sleep((32 - len(failed_files)) * 0.0005)
        with observed_lock:
            observed_lengths.append(len(failed_files))
        return "ignored"

    monkeypatch.setattr(run, "write_failed_files_report", _fake_write_failed_files_report)

    worker_threads = []
    total_failures = 16

    for index in range(total_failures):
        file_path = tmp_path / f"failed-{index}.bin"
        thread = threading.Thread(
            target=client._record_failed_file,
            kwargs={
                "link": f"https://cdn.example/{index}",
                "file_path": str(file_path),
                "error": "network error",
                "base_dir": str(tmp_path),
            },
        )
        worker_threads.append(thread)

    for thread in worker_threads:
        thread.start()
    for thread in worker_threads:
        thread.join()

    assert len(client.failed_files) == total_failures
    assert observed_lengths
    assert observed_lengths == sorted(observed_lengths)
    assert observed_lengths[-1] == total_failures
