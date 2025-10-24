from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

import sys
import typer
import yaml

from datacore.context import build_context, ensure_spark, stop_spark
from datacore.layers.raw import main as raw_main
from datacore.layers.bronze import main as bronze_main
from datacore.layers.silver import main as silver_main
from datacore.layers.gold import main as gold_main

app = typer.Typer(help="DataCore orchestration commands")


def _load_config(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise typer.BadParameter(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _stage_sequence(target: str) -> Iterable[str]:
    stages = ["raw", "bronze", "silver", "gold"]
    if target not in stages:
        raise typer.BadParameter(f"Unsupported layer '{target}'. Expected one of {', '.join(stages)}")
    for stage in stages:
        yield stage
        if stage == target:
            break


@app.command()
def run_layer(
    layer: str = typer.Argument(..., help="Layer to execute"),
    config: Path = typer.Option(..., "-c", "--config", help="Path to layer configuration"),
) -> None:
    raw_config = _load_config(config)
    layer = layer.lower()
    context = build_context(raw_config)

    if context.dry_run:
        typer.echo(f"[{layer}] Dry run requested - skipping execution")
        return

    df = None
    try:
        spark = ensure_spark(context)
        for stage in _stage_sequence(layer):
            if stage == "raw":
                df = raw_main.execute(context, spark)
            elif stage == "bronze":
                df = bronze_main.execute(context, spark, df)
            elif stage == "silver":
                df = silver_main.execute(context, spark, df)
            elif stage == "gold":
                df = gold_main.execute(context, spark, df)
    finally:
        stop_spark(context)


def main() -> None:
    args = sys.argv[1:]
    if args and args[0] == "run-layer":
        sys.argv = [sys.argv[0]] + args[1:]
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
