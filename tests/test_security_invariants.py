from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET_DIRS = ("src", "scripts")


def _iter_candidate_files(extensions: Iterable[str]) -> Iterable[Path]:
    for directory in TARGET_DIRS:
        base = REPO_ROOT / directory
        for ext in extensions:
            yield from base.rglob(f"*{ext}")


def _collect_lines(path: Path) -> List[Tuple[int, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:  # pragma: no cover - skip binaries
        return []
    return [(idx + 1, line) for idx, line in enumerate(text.splitlines())]


def test_tls_never_disabled() -> None:
    insecure_hits: List[str] = []
    for path in _iter_candidate_files([".py", ".sh", ".yml", ".yaml", ".cfg"]):
        for lineno, line in _collect_lines(path):
            normalized = line.replace(" ", "")
            if "fs.s3a.connection.ssl.enabled=false" in normalized.lower():
                insecure_hits.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not insecure_hits, "TLS must remain enabled (found overrides in " + ", ".join(insecure_hits)


def test_secret_never_logged() -> None:
    suspicious: List[str] = []
    keywords = ("AWS_SECRET_ACCESS_KEY", "fs.s3a.secret.key")
    log_tokens = ("print", "logger", "logging", "log.info", "log.debug")

    for path in _iter_candidate_files([".py", ".sh"]):
        for lineno, line in _collect_lines(path):
            if not any(keyword in line for keyword in keywords):
                continue
            lowered = line.lower()
            if any(token in lowered for token in log_tokens):
                suspicious.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")
    assert not suspicious, "Secret-bearing keys must not be written to logs: " + ", ".join(suspicious)
