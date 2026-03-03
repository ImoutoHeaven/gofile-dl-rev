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
