import pytest

pytest.importorskip("fsspec")

from datacore.io import build_storage_adapter, storage_options_from_env


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "S3A_DISABLE_SSL",
    ]:
        monkeypatch.delenv(key, raising=False)


def test_build_storage_adapter_merges_overrides(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA123")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "super-secret")

    env_cfg = {
        "storage": {
            "s3": {
                "storage_options": {"client_kwargs": {"endpoint_url": "https://example.com"}},
                "reader_options": {"encoding": "latin-1"},
                "writer_options": {"compression": "gzip"},
            }
        }
    }

    adapter = build_storage_adapter(
        "s3://bucket/path",
        env_cfg,
        {
            "storage_options": {"anon": False},
            "reader_options": {"encoding": "utf-16"},
            "writer_options": {"partitionOverwriteMode": "dynamic"},
        },
    )

    assert adapter.protocol == "s3"
    assert adapter.storage_options["key"] == "AKIA123"
    assert adapter.storage_options["secret"] == "super-secret"
    assert adapter.storage_options["client_kwargs"]["endpoint_url"] == "https://example.com"
    assert adapter.reader_options["encoding"] == "utf-16"
    assert adapter.writer_options["compression"] == "gzip"
    assert adapter.writer_options["partitionOverwriteMode"] == "dynamic"


def test_storage_options_from_env_rejects_disable_ssl(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("S3A_DISABLE_SSL", "true")

    with pytest.raises(ValueError):
        storage_options_from_env("s3://bucket/path", {})
