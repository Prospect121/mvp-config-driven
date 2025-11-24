"""Entry point para Dataproc."""
import argparse

from datacore.cli.main import main


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--platform", default="gcp")
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()
    main(
        [
            "run",
            "--layer",
            args.layer,
            "--config",
            args.config,
            "--platform",
            args.platform,
            "--env",
            args.env,
        ]
    )
