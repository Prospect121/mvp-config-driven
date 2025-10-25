from pathlib import Path

from datacore.catalog.state import load_dataset_state, read_watermark, update_watermark


def test_state_roundtrip(tmp_path: Path):
    dataset = "finance.transactions"
    state_dir = tmp_path / "state"

    initial = load_dataset_state(dataset, state_dir=state_dir)
    assert initial == {}

    update_watermark(dataset, "raw_http", "2024-01-02T00:00:00Z", field="updated_at", state_dir=state_dir)

    loaded = load_dataset_state(dataset, state_dir=state_dir)
    assert "raw_http" in loaded

    watermark = read_watermark(dataset, "raw_http", state_dir=state_dir)
    assert watermark.value == "2024-01-02T00:00:00Z"
    assert watermark.column == "updated_at"
