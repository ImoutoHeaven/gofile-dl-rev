import json
import os
import sys

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
