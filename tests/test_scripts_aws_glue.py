import json
import os
import stat
import subprocess


def _prepare_stub(tmp_path):
    output_file = tmp_path / "aws_args.json"
    script_path = tmp_path / "aws"
    script_path.write_text(
        "#!/usr/bin/env bash\n"
        f"echo \"$6\" > '{output_file}'\n"
    )
    script_path.chmod(stat.S_IRWXU)
    return script_path, output_file


def test_aws_glue_script_uses_prodi_force(monkeypatch, tmp_path):
    stub, output_file = _prepare_stub(tmp_path)

    env = os.environ.copy()
    env["PATH"] = f"{tmp_path}:{env.get('PATH', '')}"
    env["PRODI_FORCE_DRY_RUN"] = "1"

    subprocess.run(
        ["bash", "scripts/aws_glue_submit.sh", "raw"],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert payload.get("--env.PRODI_FORCE_DRY_RUN") == "true"


def test_aws_glue_script_accepts_legacy_force(monkeypatch, tmp_path):
    stub, output_file = _prepare_stub(tmp_path)

    env = os.environ.copy()
    env["PATH"] = f"{tmp_path}:{env.get('PATH', '')}"
    env.pop("PRODI_FORCE_DRY_RUN", None)
    env["FORCE_DRY_RUN"] = "yes"

    subprocess.run(
        ["bash", "scripts/aws_glue_submit.sh", "silver"],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert payload.get("--layer") == "silver"
    assert payload.get("--env.PRODI_FORCE_DRY_RUN") == "true"
