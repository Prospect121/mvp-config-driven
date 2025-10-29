from datacore.utils.paths import normalize_uri


def test_normalize_uri_local():
    assert normalize_uri("/tmp/data") == "file:///tmp/data"


def test_normalize_uri_s3():
    assert normalize_uri("s3://bucket/path") == "s3a://bucket/path"


def test_normalize_uri_abfs():
    assert normalize_uri("abfs://container@account/path") == "abfss://container@account/path"
