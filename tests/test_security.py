import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_rg(pattern: str, *relative_paths: str):
    paths = [str(PROJECT_ROOT / rel) for rel in relative_paths]
    result = subprocess.run(
        ["rg", "--files-with-matches", pattern, *paths],
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout.strip()


def test_no_scripts_disable_tls():
    code, output = _run_rg(r"ssl\\.enabled=false", "scripts")
    assert code == 1, f"Found insecure TLS disable flags:\n{output}"


def test_no_scripts_log_aws_credentials():
    patterns = [
        r"echo[^\n]*AWS_(?:ACCESS_KEY_ID|SECRET_ACCESS_KEY)",
        r"logger\.(?:info|warning|error|debug)[^\n]*AWS_(?:ACCESS_KEY_ID|SECRET_ACCESS_KEY)",
    ]
    for pattern in patterns:
        code, output = _run_rg(pattern, "scripts")
        assert code == 1, f"Found credential leakage pattern '{pattern}':\n{output}"
