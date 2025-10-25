from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(
    os.getenv("LEGACY_CANARY") != "1",
    reason="Legacy canary disabled; set LEGACY_CANARY=1 to exercise the quarantine guard",
)
def test_audit_cleanup_flags_legacy_reference() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sentinel = repo_root / "tests" / "_legacy_canary.txt"
    legacy_parts = ["legacy", "datasets", "example.csv"]
    sentinel.write_text(
        "Do not touch " + "/".join(legacy_parts),
        encoding="utf-8",
    )

    try:
        result = subprocess.run(
            ["python", "tools/audit_cleanup.py", "--check"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0, "audit_cleanup should fail when legacy references exist"
        assert "legacy path" in result.stderr
    finally:
        try:
            sentinel.unlink()
        except FileNotFoundError:  # pragma: no cover - cleanup safeguard
            pass
