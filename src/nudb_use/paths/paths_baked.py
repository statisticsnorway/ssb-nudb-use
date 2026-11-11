from pathlib import Path

from dapla_metadata.datasets.dapla_dataset_path_info import DaplaDatasetPathInfo
from fagfunksjoner.paths.versions import get_latest_fileversions

from nudb_use.nudb_logger import logger

AVAILABLE_BAKED_DATASETS: list[str] | None = None

# HARD CODED VARIABLES WHICH SHOULD BE REPLACED BY THE CONFIG...
ALL_BAKED_PATHS = [
    Path("/buckets/shared/utd-nudb/utdanning/nudb-data/klargjorte-data/baked/"),
    Path("/buckets/delt-utdanning/nudb-data/klargjorte-data/baked/"),
]


BAKED_DATASETS_WANT = ["utd_hoeyeste", "utd_foreldres_utdnivaa"]
BAKED_PATHS = [path for path in ALL_BAKED_PATHS if path.is_dir()]


def _add_baked_path(path: str | Path) -> None:
    global BAKED_PATHS

    if not isinstance(path, Path):
        path = Path(path)

    if not path.is_dir():
        raise OSError(
            f"'{path}' is not a directory!"
        )  # OSError might not be the right choice

    BAKED_PATHS.append(path)


def _get_available_baked_datasets() -> list[str]:
    global AVAILABLE_BAKED_DATASETS

    # already initialized?
    if AVAILABLE_BAKED_DATASETS is not None:
        return AVAILABLE_BAKED_DATASETS

    available: set[str] = set()
    for path in BAKED_PATHS:
        datasets = list(path.glob("*.parquet"))

        for data in datasets:
            try:
                info = DaplaDatasetPathInfo(data)
                if isinstance(info.dataset_short_name, str):
                    available.add(info.dataset_short_name)
                else:
                    # could also just raise an error, sending us to except Expection: ...
                    logger.warning(f"Unable to parse name of parquet file: {data}")
            except Exception:
                logger.warning(f"Unable to parse name of parquet file: {data}")

    if not available:
        logger.warning("There are no available baked datasets!")
        return []

    # When initializing we should also check if the results
    # match our expectations
    want = set(BAKED_DATASETS_WANT)
    not_defined = list(available - want)
    not_available = list(want - available)

    if not_defined:
        logger.warning(
            f"Some baked datasets in the baked directory are not defined in the config: {not_defined}"
        )

    if not_available:
        logger.warning(
            f"Some baked datasets defined in the config are not available: {not_available}"
        )

    AVAILABLE_BAKED_DATASETS = list(available)
    return AVAILABLE_BAKED_DATASETS


def _get_latest_path_baked_data(name: str) -> Path:
    # We do implement this as a part of get_latest_shared_path,
    # because we don't want these baked datasets to be mixed
    # with other datasets, where we by accident end up returning
    # a baked dataset path, for a dataset which is not a baked dataset
    if name not in _get_available_baked_datasets():
        raise ValueError(f"{name} is not avaiable as a baked dataset!")

    paths = []
    for directory in BAKED_PATHS:
        paths += list(
            directory.glob(
                f"{name}_p[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_v*[0-9].parquet"
            )
        )

    if not paths:
        raise ValueError(f"Unable to find a path for {name}!")

    latest = sorted(get_latest_fileversions(paths))
    return latest[-1]
