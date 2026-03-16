from pathlib import Path

from config import freshness


def test_check_freshness_skips_remote_probe_without_refresh(monkeypatch, tmp_path):
    local_path = tmp_path / "source"
    local_path.mkdir()

    monkeypatch.setattr(
        freshness,
        "load_cache",
        lambda _: {"last_download": "2026-01-01T00:00:00+00:00"},
    )

    called = {"remote": False}

    def fake_remote_headers(_url, timeout=30):
        called["remote"] = True
        return {"accessible": True}

    monkeypatch.setattr(freshness, "get_remote_headers", fake_remote_headers)

    needs_download, reason, _ = freshness.check_freshness(
        source_name="demo",
        url="https://example.test/demo.zip",
        local_path=local_path,
        max_age_hours=999999,
        refresh=False,
    )

    assert needs_download is False
    assert reason == "fresh_cache_local"
    assert called["remote"] is False


def test_check_freshness_uses_remote_probe_with_refresh(monkeypatch, tmp_path):
    local_path = tmp_path / "source"
    local_path.mkdir()

    monkeypatch.setattr(
        freshness,
        "load_cache",
        lambda _: {
            "last_download": "2026-01-01T00:00:00+00:00",
            "last_modified_remote": "Wed, 01 Jan 2025 00:00:00 GMT",
            "etag": "v1",
            "content_length": "10",
        },
    )
    monkeypatch.setattr(
        freshness,
        "get_remote_headers",
        lambda _url, timeout=30: {
            "accessible": True,
            "last_modified": "Thu, 02 Jan 2025 00:00:00 GMT",
            "etag": "v1",
            "content_length": "10",
        },
    )

    needs_download, reason, _ = freshness.check_freshness(
        source_name="demo",
        url="https://example.test/demo.zip",
        local_path=local_path,
        max_age_hours=999999,
        refresh=True,
    )

    assert needs_download is True
    assert reason == "remote_updated"
