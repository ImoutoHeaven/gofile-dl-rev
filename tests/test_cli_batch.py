import os
import sys
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run


def _make_input(lines):
    iterator = iter(lines)

    def _fake_input(_prompt=""):
        return next(iterator)

    return _fake_input


def test_collect_batch_urls_stops_on_double_blank_line():
    input_fn = _make_input(
        [
            " https://gofile.io/d/abc123 ",
            "https://gofile.io/d/xyz789",
            "",
            "   ",
            "https://gofile.io/d/ignored",
        ]
    )

    urls = run.collect_batch_urls(input_fn=input_fn)

    assert urls == ["https://gofile.io/d/abc123", "https://gofile.io/d/xyz789"]


def test_collect_batch_urls_keeps_reading_after_single_blank_line():
    input_fn = _make_input(
        [
            "https://gofile.io/d/abc123",
            "",
            "https://gofile.io/d/xyz789",
            "",
            "",
        ]
    )

    urls = run.collect_batch_urls(input_fn=input_fn)

    assert urls == ["https://gofile.io/d/abc123", "https://gofile.io/d/xyz789"]


def test_filter_gofile_urls_trims_skips_empty_and_invalid_lines():
    valid_urls, invalid_urls = run.filter_gofile_urls(
        [
            "   ",
            " https://gofile.io/d/abc123 ",
            "https://example.com/d/nope",
            "https://gofile.io/f/wrong",
            "not-a-url",
            "https://gofile.io/d/xyz789/",
        ]
    )

    assert valid_urls == ["https://gofile.io/d/abc123", "https://gofile.io/d/xyz789"]
    assert invalid_urls == [
        "https://example.com/d/nope",
        "https://gofile.io/f/wrong",
        "not-a-url",
    ]


def test_main_batch_mode_downloads_only_valid_urls(tmp_path):
    calls = []

    class FakeGoFile:
        def execute(self, **kwargs):
            calls.append(kwargs["url"])

    input_fn = _make_input(
        [
            "https://gofile.io/d/abc123",
            "https://invalid.example.com/d/skip",
            "https://gofile.io/d/xyz789/",
            "",
            "",
        ]
    )

    exit_code = run.main(
        argv=["-d", str(tmp_path)],
        input_fn=input_fn,
        gofile_factory=FakeGoFile,
    )

    assert exit_code == 0
    assert calls == ["https://gofile.io/d/abc123", "https://gofile.io/d/xyz789"]


def test_main_returns_nonzero_when_no_valid_urls(tmp_path):
    class FakeGoFile:
        def execute(self, **kwargs):
            raise AssertionError("execute should not be called")

    input_fn = _make_input(["", ""])

    exit_code = run.main(
        argv=["-d", str(tmp_path)],
        input_fn=input_fn,
        gofile_factory=FakeGoFile,
    )

    assert exit_code == 1


def test_main_url_mode_writes_failed_files_json_for_retry(tmp_path):
    class FakeGoFile:
        def __init__(self):
            self.failed_files = []

        def execute(self, **kwargs):
            self.failed_files = [
                {
                    "type": "file",
                    "name": "bad.bin",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Folder/bad.bin",
                    "error": "download failed after retries",
                }
            ]

        def execute_payload(self, **kwargs):
            self.failed_files = [
                {
                    "type": "file",
                    "name": "bad.bin",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Folder/bad.bin",
                    "error": "download failed after retries",
                }
            ]

    exit_code = run.main(
        argv=[
            "https://gofile.io/d/abc123",
            "-d",
            str(tmp_path),
            "--total-retries",
            "1",
        ],
        gofile_factory=FakeGoFile,
    )

    failed_path = tmp_path / "failed_files.json"
    assert exit_code == 0
    assert failed_path.exists()

    payloads = run.load_content_payloads(str(failed_path))
    assert len(payloads) == 1
    jobs = run.collect_download_jobs_from_payload(payloads[0], base_dir=str(tmp_path))
    assert jobs == [
        ("https://cdn.example/fail", os.path.join(str(tmp_path), "Folder", "bad.bin"))
    ]


def test_parse_total_retries_accepts_positive_int_and_inf():
    assert run.parse_total_retries("1") == 1
    assert run.parse_total_retries("3") == 3
    assert run.parse_total_retries("inf") is None
    assert run.parse_total_retries("INF") is None


def test_parse_total_retries_rejects_invalid_values():
    with pytest.raises(Exception):
        run.parse_total_retries("0")
    with pytest.raises(Exception):
        run.parse_total_retries("-1")
    with pytest.raises(Exception):
        run.parse_total_retries("abc")


def test_main_total_retries_uses_payload_retry_loop(tmp_path):
    client = None

    class FakeGoFile:
        def __init__(self):
            self.failed_files = []
            self.execute_calls = 0
            self.execute_payload_calls = 0

        def clear_failed_files(self):
            self.failed_files = []

        def execute(self, **_kwargs):
            self.execute_calls += 1
            self.failed_files = [
                {
                    "type": "file",
                    "name": "bad.bin",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Folder/bad.bin",
                    "error": "failed attempt 1",
                }
            ]

        def execute_payload(self, **_kwargs):
            self.execute_payload_calls += 1
            if self.execute_payload_calls == 1:
                self.failed_files = [
                    {
                        "type": "file",
                        "name": "bad.bin",
                        "link": "https://cdn.example/fail",
                        "relativePath": "Folder/bad.bin",
                        "error": "failed attempt 2",
                    }
                ]
            else:
                self.failed_files = []

    def _factory():
        nonlocal client
        client = FakeGoFile()
        return client

    exit_code = run.main(
        argv=[
            "https://gofile.io/d/abc123",
            "-d",
            str(tmp_path),
            "--total-retries",
            "2",
        ],
        gofile_factory=_factory,
    )

    assert exit_code == 0
    assert client is not None
    assert client.execute_calls == 1
    assert client.execute_payload_calls == 2


def test_main_total_retries_inf_keeps_retrying_until_clear(tmp_path):
    client = None

    class FakeGoFile:
        def __init__(self):
            self.failed_files = []
            self.execute_calls = 0
            self.execute_payload_calls = 0

        def clear_failed_files(self):
            self.failed_files = []

        def execute(self, **_kwargs):
            self.execute_calls += 1
            self.failed_files = [
                {
                    "type": "file",
                    "name": "bad.bin",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Folder/bad.bin",
                    "error": "failed attempt 1",
                }
            ]

        def execute_payload(self, **_kwargs):
            self.execute_payload_calls += 1
            if self.execute_payload_calls < 4:
                self.failed_files = [
                    {
                        "type": "file",
                        "name": "bad.bin",
                        "link": "https://cdn.example/fail",
                        "relativePath": "Folder/bad.bin",
                        "error": "still failing",
                    }
                ]
            else:
                self.failed_files = []

    def _factory():
        nonlocal client
        client = FakeGoFile()
        return client

    exit_code = run.main(
        argv=[
            "https://gofile.io/d/abc123",
            "-d",
            str(tmp_path),
            "--total-retries",
            "inf",
        ],
        gofile_factory=_factory,
    )

    assert exit_code == 0
    assert client is not None
    assert client.execute_calls == 1
    assert client.execute_payload_calls == 4


def test_main_total_retries_stops_when_limit_reached(tmp_path):
    client = None

    class FakeGoFile:
        def __init__(self):
            self.failed_files = []
            self.execute_calls = 0
            self.execute_payload_calls = 0

        def clear_failed_files(self):
            self.failed_files = []

        def execute(self, **_kwargs):
            self.execute_calls += 1
            self.failed_files = [
                {
                    "type": "file",
                    "name": "bad.bin",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Folder/bad.bin",
                    "error": "failed attempt 1",
                }
            ]

        def execute_payload(self, **_kwargs):
            self.execute_payload_calls += 1
            self.failed_files = [
                {
                    "type": "file",
                    "name": "bad.bin",
                    "link": "https://cdn.example/fail",
                    "relativePath": "Folder/bad.bin",
                    "error": "failed attempt 2",
                }
            ]

    def _factory():
        nonlocal client
        client = FakeGoFile()
        return client

    exit_code = run.main(
        argv=[
            "https://gofile.io/d/abc123",
            "-d",
            str(tmp_path),
            "--total-retries",
            "1",
        ],
        gofile_factory=_factory,
    )

    assert exit_code == 0
    assert client is not None
    assert client.execute_calls == 1
    assert client.execute_payload_calls == 1
    assert (tmp_path / "failed_files.json").exists()
