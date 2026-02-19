"""Location utilities for discovering the latest shared NUDB datasets."""

from pathlib import Path

from fagfunksjoner.paths.versions import get_latest_fileversions
from nudb_config import settings

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

UTDANNING_SHARED_EXTERNAL = settings.paths["local_daplalab"].get(
    "delt_utdanning", "/buckets/shared/utd-nudb/utdanning/"
)

SHARED_ROOT = "/buckets/shared"
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
        f"klargjorte-data/*/{filepattern}.{filetype}",
        f"*/{filepattern}.{filetype}",
        f"{filepattern}.{filetype}",
    ]

    logger.debug(f"globs = {globs}")
    files = []

    for path in POSSIBLE_PATHS:
        if not path.is_dir():
            continue

        for glob in globs:
            files += list(path.glob(glob))

    if files:
        logger.info(
            f"Found relevant files locally in POSSIBLE_PATHS, not using glob from config: {files}"
        )
        return files

    # Fall back to the dataset config for external datasets that live in other teams
    if (
        filename in settings.datasets
        and settings.datasets[filename].team != settings.dapla_team
    ):
        datameta = settings.datasets[filename]
        if datameta.team and datameta.bucket and datameta.path_glob:
            local_path = Path(f"{SHARED_ROOT}/{datameta.team}/{datameta.bucket}/")
            found_files = list(local_path.glob(datameta.path_glob))
            if found_files:
                return found_files
        # If we are here, the file looks external, but we couldnt find it locally
        msg = f"Either you need to get access to and mount locally the bucket {datameta.bucket} from the team {datameta.team}.\n"
        msg += f"Or the config is missing an important value for the dataset `{filename}`, the team name: `{datameta.team}`,"
        msg += f"the bucket name: `{datameta.bucket}` or full path glob: `/buckets/shared/{datameta.team}/{datameta.bucket}/{datameta.path_glob}`"
        raise FileNotFoundError(msg)

    return files


def filter_out_periods_paths(p: Path) -> str:
    """Filter the versions and periods out of a path.

    Args:
        p: Path that potentially includes period/version information.

    Returns:
        str: File stem without period and version fragments.
    """
    p = Path(p)  # In case someone sends a str...
    current_stem = p.stem

    # Removing version part
    last_part = current_stem.rsplit("_", 1)[-1]
    if last_part[0] == "v" and last_part[1:].isdigit():
        current_stem = current_stem.rsplit("_", 1)[0]

    # Removing up to two period parts, starts with p followed by 4 digits
    for _ in range(2):
        last_part = current_stem.rsplit("_", 1)[-1]
        if last_part[0] == "p" and last_part[1:].strip("-").isdigit():
            current_stem = current_stem.rsplit("_", 1)[0]

    return current_stem


def latest_shared_path(dataset_name: str = "") -> tuple[str, Path]:
    """Return the newest shared dataset path.

    This is a convenience wrapper around `latest_shared_paths` that returns
    the most recent dataset entry (by sorted key).

    Args:
        dataset_name: Optional dataset identifier to filter available paths
            before selecting the newest entry.

    Returns:
        tuple[str, Path]: The dataset key and its latest path.
    """
    paths = latest_shared_paths(dataset_name)
    if isinstance(paths, Path):
        return dataset_name, paths
    paths_dict: dict[str, Path] = paths
    last_key = sorted(paths_dict.keys())[-1]
    last_path = paths_dict[last_key]
    logger.info(f"{dataset_name} name: {last_key} at: {last_path}.")
    return last_key, last_path


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
            logger.info(f"Selected path: '{paths_dict[dataset_name]}'.'")

            return paths_dict[dataset_name]

        logger.info(
            f"Did not find {dataset_name} in the paths_dict, all found path keys: {list(paths_dict.keys())}"
        )
        return paths_dict
