from pathlib import Path

from fagfunksjoner.paths.versions import get_latest_fileversions

from nudb_use import LoggerStack
from nudb_use import logger


def find_delt_path() -> Path:
    """Figure out where you might have the shared NUDB-data mounted locally.

    Returns:
        Path: the path of the folder that we found out of these.
    """
    utdata_path = Path("/buckets/shared/utd-nudb/utdanning/nudb-data")
    if not utdata_path.is_dir():
        utdata_path = Path("/buckets/delt-utdanning/nudb-data")
    if not utdata_path.is_dir():
        raise OSError("Cant find the folder for the shared data...")

    return utdata_path


def filter_out_periods_paths(p: Path) -> str:
    """Filter the versions and periods out of a path.

    Args:
        p: The path we should find the stem name from.

    Returns:
        str: The part of the filestem we deem to not be versions or periods.
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


def latest_shared_paths(dataset_name: str = "") -> list[Path] | Path:
    """Find the last shared version and period of each stem in the shared folder.

    Args:
        dataset_name: If you want to filter down already here, you can. But it just indexes into the resulting dict, so... yeah.

    Returns:
        list[Path] | Path: If you used dataset_name, only a single path is returned, otherwise all the keys and files.
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

        if dataset_name:
            return paths_dict[dataset_name]
        return paths_dict
