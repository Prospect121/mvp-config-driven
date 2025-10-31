import logging
import sys
import types

from datacore.utils import observability


def test_new_run_id_unique():
    run_a = observability.new_run_id()
    run_b = observability.new_run_id()
    assert run_a != run_b


def test_record_dataset_logs(monkeypatch, caplog):
    monkeypatch.setattr(observability, "DATASET_RUNS", None)
    monkeypatch.setattr(observability, "DATASET_DURATION", None)
    with caplog.at_level(logging.DEBUG):
        observability.record_dataset("demo", "completed", 0.5, {"rows": 10})
    assert "demo" in caplog.text


def test_emit_openlineage_skip(caplog):
    with caplog.at_level(logging.DEBUG):
        observability.emit_openlineage({}, {"enabled": True})
    assert "OpenLineage" in caplog.text


def test_record_dataset_with_metrics(monkeypatch):
    class _Metric:
        def __init__(self):
            self.calls: list[dict[str, str]] = []
            self.count = 0

        def labels(self, **labels):  # type: ignore[override]
            self.calls.append(labels)
            return self

        def inc(self):
            self.count += 1

        def observe(self, value):  # type: ignore[override]
            self.count += 1
            self.value = value

    counter = _Metric()
    histogram = _Metric()
    monkeypatch.setattr(observability, "DATASET_RUNS", counter)
    monkeypatch.setattr(observability, "DATASET_DURATION", histogram)

    observability.record_dataset("customers", "completed", 1.2, {"rows": 5})

    assert counter.calls[0]["dataset"] == "customers"
    assert histogram.calls[0]["dataset"] == "customers"
    assert histogram.value == 1.2


def test_emit_openlineage_happy_path(monkeypatch):
    emitted: dict[str, object] = {}

    class _Client:
        def __init__(self, url, api_key=None):  # type: ignore[no-untyped-def]
            self.url = url
            self.api_key = api_key

        def emit(self, event):  # type: ignore[no-untyped-def]
            emitted["event"] = event

    class _RunState:
        COMPLETE = "COMPLETE"
        FAILED = "FAILED"

        @classmethod
        def from_string(cls, value):  # type: ignore[no-untyped-def]
            if value == "FAILED":
                return cls.FAILED
            return cls.COMPLETE

    class _Run:
        def __init__(self, runId):  # type: ignore[no-untyped-def]
            self.runId = runId

    class _Job:
        def __init__(self, namespace, name):  # type: ignore[no-untyped-def]
            self.namespace = namespace
            self.name = name

    class _RunEvent:
        def __init__(self, eventType, eventTime, run, job, inputs, outputs):  # type: ignore[no-untyped-def]
            self.eventType = eventType
            self.eventTime = eventTime
            self.run = run
            self.job = job
            self.inputs = inputs
            self.outputs = outputs

    client_mod = types.SimpleNamespace(OpenLineageClient=_Client)
    run_mod = types.SimpleNamespace(Job=_Job, Run=_Run, RunEvent=_RunEvent, RunState=_RunState)
    monkeypatch.setitem(sys.modules, "openlineage", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "openlineage.client", client_mod)
    monkeypatch.setitem(sys.modules, "openlineage.client.run", run_mod)

    observability.emit_openlineage(
        {"run_id": "abc", "dataset": "customers", "layer": "silver", "status": "failed"},
        {"enabled": True, "url": "https://ol", "namespace": "demo", "api_key": "token"},
    )

    event = emitted["event"]
    assert event.eventType == _RunState.FAILED
    assert event.job.name == "silver::customers"
    assert event.run.runId == "abc"
