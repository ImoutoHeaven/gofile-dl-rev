import os
import sys

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
