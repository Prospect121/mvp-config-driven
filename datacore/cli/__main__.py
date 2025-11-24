"""Module entrypoint so ``python -m datacore.cli`` works in CI and local runs."""

from datacore.cli.main import main


if __name__ == "__main__":  # pragma: no cover
    main()
