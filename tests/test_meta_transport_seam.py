import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import gofile_browser_client as gbc
import run


def test_gofile_has_meta_transport_attribute():
    run.GoFileMeta._instances.clear()
    client = run.GoFile()
    assert hasattr(client, "meta_transport")


def test_build_meta_transport_uses_browser_meta_singleton(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(gbc, "get_browser_meta_transport", lambda: sentinel)

    assert run.build_meta_transport() is sentinel
