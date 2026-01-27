"""Location utilities for discovering the latest shared NUDB datasets."""

from pathlib import Path

from fagfunksjoner.paths.versions import get_latest_fileversions
from nudb_config import settings

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

UTDANNING_SHARED_EXTERNAL = settings.paths["local_daplalab"].get(
    "delt_utdanning", "/buckets/shared/utd-nudb/utdanning/"
)
UTDANNING_SHARED_LOCAL = "/buckets/delt-utdanning/nudb-data"
NUDB_PRODUCT = "/buckets/produkt/nudb-data/"

POSSIBLE_PATHS = [
    Path(UTDANNING_SHARED_EXTERNAL),
    Path(UTDANNING_SHARED_LOCAL),
    Path(NUDB_PRODUCT),
]


def _add_delt_path(path: str | Path) -> None:
    global POSSIBLE_PATHS

    if not isinstance(path, Path):
        path = Path(path)

    if not path.is_dir():
        raise OSError(
            f"'{path}' is not a directory!"
        )  # OSError might not be the right choice

    POSSIBLE_PATHS.append(path)


def _get_available_files(filename: str = "", filetype: str = "parquet") -> list[Path]:
    global POSSIBLE_PATHS

    # For custom paths we don't know if there is a klargjorte-data
    # directory, so we search in the directory directly as well
    # We could perhaps rework this logic into _add_delt_path()
    # and add the /klargjorte-data to the paths in POSSIBLE_PATHS
    filepattern = f"{filename}*" if filename else "*"

    globs = [
        f"klargjorte-data/**/{filepattern}.{filetype}",
        f"**/{filepattern}.{filetype}",
        f"{filepattern}.{filetype}",
    ]

    logger.debug(f"globs = {globs}")
    files = []

    for path in POSSIBLE_PATHS:
        if not path.is_dir():
            continue

        for glob in globs:
            files += list(path.glob(glob))

    return files


def filter_out_periods_paths(p: Path) -> str:
    """Filter the versions and periods out of a path.

    Args:
        p: Path that potentially includes period/version information.

    Returns:
        str: File stem without period and version fragments.
    """
    p = Path(p)  # In case someone sends a str...
    parts_left = [
        part
        for part in p.stem.split("_")
        if not (
            (
                part.startswith("v") and len(part) >= 2 and part[1:].isdigit()
            )  # This means its a version part
            or (
                part.startswith("p")
                and len(part) >= 5
                and part[1:].strip("-").isdigit()
            )  # This should mean it is a period part only checking for year and dash-seperated dates
        )
    ]
    return "_".join(parts_left)


def latest_shared_paths(dataset_name: str = "") -> dict[str, Path] | Path:
    """Find the last shared version and period of each stem in the shared folder.

    Args:
        dataset_name: Optional dataset identifier. When provided only the path
            for that dataset is returned.

    Returns:
        dict[str, Path] | Path: Mapping of dataset stems to their newest paths,
        or a single `Path` when `dataset_name` is supplied.
    """
    with LoggerStack("Finding all the latest shared paths for NUDB."):
        # Filter to only the last versions of each period
        latest_parquets = sorted(
            get_latest_fileversions(_get_available_files(dataset_name))
        )
        logger.info(latest_parquets)
        # Filtering out earlier periods of the same files

        paths_dict: dict[str, Path] = {}
        for p in latest_parquets:
            paths_dict[filter_out_periods_paths(p)] = p

        # We should probably log what we found as latest files to disk?

        if dataset_name and dataset_name in paths_dict:
            logger.info(
                f"Found {dataset_name} in the paths_dict, returning single Path."
            )
            return paths_dict[dataset_name]
        logger.info(f"Did not find {dataset_name} in the paths_dict, all found paths.")
        return paths_dict
