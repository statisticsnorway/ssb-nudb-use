"""Command-line interface."""

import click


@click.command()
@click.version_option()
def main() -> None:
    """SSB Nudb Use."""


if __name__ == "__main__":
    main(prog_name="ssb-nudb-use")  # pragma: no cover
