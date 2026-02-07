import importlib
from pyspark.sql import functions as F
from datacore.core import ops


def _has_crypto():
    return importlib.util.find_spec("cryptography") is not None


def test_encrypt_decrypt_fields_roundtrip_or_error(spark):
    df = spark.createDataFrame([("alice@example.com",)], ["email"])
    cfg = {"cols": ["email"], "algo": "aes-gcm", "key": "test-key"}
    if not _has_crypto():
        try:
            ops.op_encrypt_fields(df, cfg)
        except RuntimeError as exc:
            assert "cryptography" in str(exc).lower()
        else:
            assert False, "Se esperaba RuntimeError sin cryptography"
        return
    enc = ops.op_encrypt_fields(df, cfg)
    assert enc.select(F.col("email")).first()[0] is not None
    dec = ops.op_decrypt_fields(enc, cfg)
    assert dec.select(F.col("email")).first()[0] == "alice@example.com"
