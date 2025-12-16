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


def find_delt_path() -> Path:
    """Figure out where you might have the shared NUDB-data mounted locally.

    Returns:
        Path: Path to the shared NUDB data folder.

    Raises:
        OSError: If neither of the expected shared data locations exists.
    """
    utdata_path = Path(UTDANNING_SHARED_EXTERNAL) / "nudb-data"
    if not utdata_path.is_dir():
        utdata_path = Path(UTDANNING_SHARED_LOCAL)
    if not utdata_path.is_dir():
        raise OSError("Cant find the folder for the shared data...")

    return utdata_path


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
        delt_path = find_delt_path() / "klargjort-data"

        # Filter to only the last versions of each period
        latest_parquets = sorted(
            get_latest_fileversions(list(delt_path.glob("**/*.parquet")))
        )
        logger.info(latest_parquets)
        # Filtering out earlier periods of the same files

        paths_dict: dict[str, Path] = {}
        for p in latest_parquets:
            paths_dict[filter_out_periods_paths(p)] = p

        # We should probably log what we found as latest files to disk?

        if dataset_name and dataset_name in paths_dict:
            return paths_dict[dataset_name]
        return paths_dict
