from pathlib import Path

from datacore.catalog.state import get_watermark, load_state, set_watermark


def test_state_roundtrip(tmp_path: Path):
    dataset = "finance.transactions"
    state_dir = tmp_path / "state"

    initial = load_state(dataset, state_dir=state_dir)
    assert initial == {}

    set_watermark(dataset, "2024-01-02T00:00:00Z", field="updated_at", state_dir=state_dir)

    loaded = load_state(dataset, state_dir=state_dir)
    assert loaded["value"] == "2024-01-02T00:00:00Z"

    watermark = get_watermark(dataset, state_dir=state_dir)
    assert watermark.value == "2024-01-02T00:00:00Z"
    assert watermark.field == "updated_at"
