from __future__ import annotations

import sys
import types

from types import SimpleNamespace

import pytest

if "pydantic" not in sys.modules:
    fake_pydantic = types.ModuleType("pydantic")

    class _BaseModel:
        model_config = {}

        @classmethod
        def model_validate(cls, value):
            return value

        def model_dump(self, **_: object) -> dict:
            return {}

    def _field(*, default=None, default_factory=None, **__: object):
        if default_factory is not None:
            return default_factory()
        return default

    def _config_dict(**kwargs: object) -> dict:
        return dict(kwargs)

    fake_pydantic.BaseModel = _BaseModel
    fake_pydantic.Field = _field
    fake_pydantic.ConfigDict = _config_dict
    sys.modules["pydantic"] = fake_pydantic

if "fsspec" not in sys.modules:
    fake_fsspec = types.ModuleType("fsspec")

    def _filesystem(protocol: str, **_: object) -> object:  # pragma: no cover - stub
        return object()

    def _open(*args: object, **kwargs: object):  # pragma: no cover - stub
        raise FileNotFoundError()

    fake_fsspec.filesystem = _filesystem
    fake_fsspec.open = _open

    core_module = types.ModuleType("fsspec.core")

    def _url_to_fs(uri: str, **_: object):  # pragma: no cover - stub
        class _DummyFS:
            def exists(self, path: str) -> bool:
                return False

        return _DummyFS(), uri

    core_module.url_to_fs = _url_to_fs
    fake_fsspec.core = core_module
    sys.modules["fsspec"] = fake_fsspec
    sys.modules["fsspec.core"] = core_module

from datacore.layers.raw import main as raw_main


class FakeMergeBuilder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def whenMatchedUpdateAll(self, condition: str | None = None):
        self.calls.append(("whenMatchedUpdateAll", (condition,)))
        return self

    def whenMatchedUpdate(self, *, set: dict[str, str], condition: str | None = None):
        self.calls.append(("whenMatchedUpdate", (set, condition)))
        return self

    def whenNotMatchedInsertAll(self, condition: str | None = None):
        self.calls.append(("whenNotMatchedInsertAll", (condition,)))
        return self

    def whenNotMatchedInsert(self, *, values: dict[str, str], condition: str | None = None):
        self.calls.append(("whenNotMatchedInsert", (values, condition)))
        return self

    def whenNotMatchedBySourceDelete(self, condition: str | None = None):
        self.calls.append(("whenNotMatchedBySourceDelete", (condition,)))
        return self

    def whenMatchedDelete(self, condition: str | None = None):
        self.calls.append(("whenMatchedDelete", (condition,)))
        return self

    def execute(self) -> None:
        self.calls.append(("execute", ()))


class FakeDeltaTable:
    existing_paths: set[str] = set()
    last_instance: "FakeDeltaTable" | None = None

    def __init__(self) -> None:
        self.builder = FakeMergeBuilder()
        self.alias_value: str | None = None
        self.merge_condition: str | None = None
        self.source_alias: str | None = None

    @classmethod
    def reset(cls) -> None:
        cls.existing_paths.clear()
        cls.last_instance = None

    @classmethod
    def isDeltaTable(cls, _spark: object, path: str) -> bool:
        return path in cls.existing_paths

    @classmethod
    def forPath(cls, _spark: object, path: str) -> "FakeDeltaTable":
        cls.existing_paths.add(path)
        instance = cls()
        instance.path = path
        cls.last_instance = instance
        return instance

    @classmethod
    def forName(cls, _spark: object, name: str) -> "FakeDeltaTable":
        instance = cls()
        instance.table = name
        cls.last_instance = instance
        return instance

    def alias(self, alias: str) -> "FakeDeltaTable":
        self.alias_value = alias
        return self

    def merge(self, df_alias: SimpleNamespace, condition: str) -> FakeMergeBuilder:
        self.source_alias = df_alias.alias
        self.merge_condition = condition
        return self.builder


class FakeWriter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def format(self, fmt: str) -> "FakeWriter":
        self.calls.append(("format", (fmt,)))
        return self

    def options(self, **opts: str) -> "FakeWriter":
        self.calls.append(("options", (opts,)))
        return self

    def mode(self, mode: str) -> "FakeWriter":
        self.calls.append(("mode", (mode,)))
        return self

    def save(self, path: str) -> None:
        self.calls.append(("save", (path,)))

    def saveAsTable(self, table: str) -> None:
        self.calls.append(("saveAsTable", (table,)))


class FakeDataFrame:
    def __init__(self, writer: FakeWriter | None = None) -> None:
        self._writer = writer or FakeWriter()

    @property
    def write(self) -> FakeWriter:
        return self._writer

    def alias(self, alias: str) -> SimpleNamespace:
        return SimpleNamespace(alias=alias)


class FakeAdapter:
    def __init__(self, uri: str, writer_options: dict[str, str] | None = None) -> None:
        self.uri = uri
        self._writer_options = writer_options or {}
        self.storage_options: dict[str, str] = {}

    def merge_writer_options(self, overrides: dict[str, str]) -> dict[str, str]:
        return dict(self._writer_options | overrides)


@pytest.fixture(autouse=True)
def patch_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeDeltaTable.reset()
    monkeypatch.setattr(raw_main, "DeltaTable", FakeDeltaTable)


def test_perform_delta_merge_generates_condition() -> None:
    df = FakeDataFrame()
    adapter = FakeAdapter("abfss://container@acct.dfs.core.windows.net/path")
    FakeDeltaTable.existing_paths.add(adapter.uri)

    raw_main._perform_delta_merge(
        SimpleNamespace(),
        df,
        {"key_columns": ["id"]},
        mode="append",
        writer_options={},
        adapter=adapter,
    )

    instance = FakeDeltaTable.last_instance
    assert instance is not None
    assert instance.alias_value == "target"
    assert instance.source_alias == "source"
    assert instance.merge_condition == "target.id = source.id"
    assert ("whenMatchedUpdateAll", (None,)) in instance.builder.calls
    assert ("whenNotMatchedInsertAll", (None,)) in instance.builder.calls
    assert instance.builder.calls[-1][0] == "execute"


def test_perform_delta_merge_creates_table_when_missing() -> None:
    writer = FakeWriter()
    df = FakeDataFrame(writer)
    adapter = FakeAdapter("abfss://container@acct.dfs.core.windows.net/path")

    raw_main._perform_delta_merge(
        SimpleNamespace(),
        df,
        {"key_columns": ["id"]},
        mode="overwrite",
        writer_options={"checkpointLocation": "abfss://chk"},
        adapter=adapter,
    )

    instance = FakeDeltaTable.last_instance
    assert instance is not None
    assert ("format", ("delta",)) in writer.calls
    assert ("mode", ("overwrite",)) in writer.calls
    assert ("save", ("abfss://container@acct.dfs.core.windows.net/path",)) in writer.calls
    # Initial creation skips merge execution
    assert instance.builder.calls == []


def test_perform_delta_merge_requires_condition_or_keys() -> None:
    df = FakeDataFrame()
    adapter = FakeAdapter("abfss://container@acct.dfs.core.windows.net/path")

    with pytest.raises(ValueError):
        raw_main._perform_delta_merge(
            SimpleNamespace(),
            df,
            {},
            mode="append",
            writer_options={},
            adapter=adapter,
        )
