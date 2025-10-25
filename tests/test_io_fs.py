import glob
import os
import shutil
from pathlib import Path
from typing import Dict, List

import pytest

pytest.importorskip("fsspec")
pd = pytest.importorskip("pandas")

from datacore.io import fs as fs_mod
from datacore.io.fs import read_df, write_df, storage_options_from_env


class DummyFS:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)

    def _normalize(self, path: str) -> Path:
        cleaned = (path or "").lstrip("/")
        if not cleaned:
            return self.base_dir
        return self.base_dir / cleaned.replace("/", os.sep)

    def open(self, path: str, mode: str = "r", newline=None):
        full = self._normalize(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        return open(full, mode, newline=newline)

    def glob(self, pattern: str) -> List[str]:
        full_pattern = self.base_dir / pattern.replace("/", os.sep)
        matches = glob.glob(str(full_pattern), recursive=True)
        return [str(Path(m).relative_to(self.base_dir)).replace(os.sep, "/") for m in matches]

    def isdir(self, path: str) -> bool:
        return self._normalize(path).is_dir()

    def exists(self, path: str) -> bool:
        return self._normalize(path).exists()

    def get(self, path: str, dest: str, recursive: bool = False):
        src = self._normalize(path)
        dest_path = Path(dest)
        if src.is_dir():
            if recursive:
                if dest_path.exists():
                    shutil.rmtree(dest_path, ignore_errors=True)
                shutil.copytree(src, dest_path)
            else:
                shutil.copy(src, dest_path)
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest_path)

    def put(self, local: str, dest: str, recursive: bool = False):
        src_path = Path(local)
        dest_path = self._normalize(dest)
        if recursive and src_path.is_dir():
            if dest_path.exists():
                shutil.rmtree(dest_path, ignore_errors=True)
            shutil.copytree(src_path, dest_path)
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest_path)

    def rm(self, path: str, recursive: bool = False):
        target = self._normalize(path)
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        elif target.exists():
            target.unlink()

    def makedirs(self, path: str, exist_ok: bool = False):
        self._normalize(path).mkdir(parents=True, exist_ok=exist_ok)


class DummySparkDataFrame:
    def __init__(self, pdf: pd.DataFrame):
        self._pdf = pdf

    def repartition(self, _parts: int):
        return self

    def coalesce(self, _parts: int):
        return self

    @property
    def write(self):
        return DummySparkWriter(self._pdf)


class DummySparkWriter:
    def __init__(self, pdf: pd.DataFrame):
        self.pdf = pdf

    def mode(self, _mode: str):
        return self

    def options(self, **_opts: Dict[str, str]):
        return self

    def partitionBy(self, *_cols: str):
        return self

    def parquet(self, path: str):
        os.makedirs(path, exist_ok=True)
        self.pdf.to_csv(os.path.join(path, "part-0000.parquet"), index=False)

    def csv(self, path: str):
        os.makedirs(path, exist_ok=True)
        self.pdf.to_csv(os.path.join(path, "part-0000.csv"), index=False)

    def json(self, path: str):
        os.makedirs(path, exist_ok=True)
        self.pdf.to_json(os.path.join(path, "part-0000.json"), orient="records", lines=True)


class DummySparkReader:
    def __init__(self):
        self.options_dict: Dict[str, str] = {}

    def options(self, **opts: Dict[str, str]):
        self.options_dict.update(opts)
        return self

    def _resolve_files(self, path: str) -> List[str]:
        if os.path.isdir(path):
            pattern = os.path.join(path, "**", "*")
            return [p for p in glob.glob(pattern, recursive=True) if os.path.isfile(p)]
        return [path]

    def csv(self, path: str):
        header_opt = self.options_dict.get("header")
        use_header = str(header_opt).lower() not in {"false", "0"}
        encoding = self.options_dict.get("encoding")
        sep = self.options_dict.get("sep")
        frames = [
            pd.read_csv(
                f,
                header=0 if use_header else None,
                encoding=encoding,
                sep=sep,
                engine="python",
            )
            for f in self._resolve_files(path)
        ]
        return DummySparkDataFrame(pd.concat(frames, ignore_index=True))

    def json(self, path: str):
        multiline = self.options_dict.get("multiline")
        lines = not (str(multiline).lower() in {"true", "1"})
        encoding = self.options_dict.get("encoding")
        frames = [
            pd.read_json(f, lines=lines, encoding=encoding)
            for f in self._resolve_files(path)
        ]
        return DummySparkDataFrame(pd.concat(frames, ignore_index=True))

    def parquet(self, path: str):
        frames = [pd.read_csv(f) for f in self._resolve_files(path)]
        return DummySparkDataFrame(pd.concat(frames, ignore_index=True))


class DummySparkSession:
    def __init__(self):
        self._reader = DummySparkReader()

    @property
    def read(self):
        self._reader = DummySparkReader()
        return self._reader

    def createDataFrame(self, pdf: pd.DataFrame):
        return DummySparkDataFrame(pdf)


def _strip_protocol(uri: str) -> str:
    return uri.split("://", 1)[1]


@pytest.mark.parametrize(
    "uri, protocol",
    [
        ("s3://bucket/data/sample.csv", "s3"),
        ("abfss://container/data/sample.csv", "abfss"),
        ("gs://bucket/data/sample.csv", "gs"),
    ],
)
def test_read_and_write_roundtrip(monkeypatch, tmp_path, uri, protocol):
    base_dir = tmp_path / protocol
    base_dir.mkdir()
    fs_stub = DummyFS(base_dir)

    def fake_filesystem(requested: str, **_opts):
        assert requested == protocol
        return fs_stub

    monkeypatch.setattr(fs_mod.fsspec, "filesystem", fake_filesystem)

    remote_path = _strip_protocol(uri)
    with fs_stub.open(remote_path, "w") as handle:
        handle.write("id,value\n1,foo\n2,bar\n")

    spark = DummySparkSession()

    df, metadata = read_df(uri, "csv", spark=spark, storage_options={})
    assert metadata == {}
    assert isinstance(df, DummySparkDataFrame)
    assert df._pdf.shape == (2, 2)

    out_uri = uri.replace("sample.csv", "output/")
    write_df(df, out_uri, "parquet", storage_options={})

    out_path = _strip_protocol(out_uri)
    parquet_files = [p for p in fs_stub.glob(f"{out_path}**") if p.endswith(".parquet")]
    assert parquet_files

    df_back, metadata_back = read_df(out_uri, "parquet", spark=spark, storage_options={})
    assert metadata_back == {}
    assert df_back._pdf.equals(df._pdf)


def test_read_with_wildcard(monkeypatch, tmp_path):
    base_dir = tmp_path / "s3"
    base_dir.mkdir()
    fs_stub = DummyFS(base_dir)

    def fake_filesystem(requested: str, **_opts):
        assert requested == "s3"
        return fs_stub

    monkeypatch.setattr(fs_mod.fsspec, "filesystem", fake_filesystem)

    with fs_stub.open("bucket/data/part1.csv", "w") as handle:
        handle.write("id,value\n1,a\n")
    with fs_stub.open("bucket/data/part2.csv", "w") as handle:
        handle.write("id,value\n2,b\n")

    spark = DummySparkSession()
    df, metadata = read_df("s3://bucket/data/*.csv", "csv", spark=spark, storage_options={})
    assert metadata == {}
    assert df._pdf.shape == (2, 2)


def test_storage_options_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA123")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://example.com")
    monkeypatch.setenv("S3A_DISABLE_SSL", "false")

    opts = storage_options_from_env("s3://bucket/data", {})
    assert opts["key"] == "AKIA123"
    assert opts["secret"] == "secret"
    assert opts["token"] == "token"
    assert opts["client_kwargs"]["endpoint_url"] == "https://example.com"
    assert "use_ssl" in opts["client_kwargs"]
