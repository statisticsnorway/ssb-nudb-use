from pathlib import Path

from dapla_metadata.datasets.dapla_dataset_path_info import DaplaDatasetPathInfo
from fagfunksjoner.paths.versions import get_latest_fileversions

from nudb_use.nudb_logger import logger

USE_BAKT_DATASETS = True
AVAILABLE_BAKT_DATASETS: list[str] | None = None

# HARD CODED VARIABLES WHICH SHOULD BE REPLACED BY THE CONFIG...
ALL_BAKT_PATHS = [
    Path("/buckets/shared/utd-nudb/utdanning/nudb-data/klargjorte-data/bakt/"),
    Path("/buckets/delt-utdanning/nudb-data/klargjorte-data/bakt/"),
]


BAKT_DATASETS_WANT = ["utd_hoeyeste", "utd_foreldres_utdnivaa"]
BAKT_PATHS = [path for path in ALL_BAKT_PATHS if path.is_dir()]


def _set_use_bakt_datasets() -> None:
    global USE_BAKT_DATASETS
    USE_BAKT_DATASETS = True


def _set_ignore_bakt_datasets() -> None:
    global USE_BAKT_DATASETS
    USE_BAKT_DATASETS = False


def _use_bakt_datasets() -> bool:
    global USE_BAKT_DATASETS
    return USE_BAKT_DATASETS


def _add_bakt_path(path: str | Path) -> None:
    global BAKT_PATHS

    if not isinstance(path, Path):
        path = Path(path)

    if not path.is_dir():
        raise OSError(
            f"'{path}' is not a directory!"
        )  # OSError might not be the right choice

    BAKT_PATHS.append(path)


def _get_available_bakt_datasets() -> list[str]:
    global AVAILABLE_BAKT_DATASETS

    # already initialized?
    if AVAILABLE_BAKT_DATASETS is not None:
        return AVAILABLE_BAKT_DATASETS

    available = set()
    for path in BAKT_PATHS:
        datasets = list(path.glob("*.parquet"))

        for data in datasets:
            info = DaplaDatasetPathInfo(data)
            available.add(info.dataset_short_name)

    # When initializing we should also check if the results
    # match our expectations
    want = set(BAKT_DATASETS_WANT)
    not_defined = list(available - want)
    not_available = list(want - available)

    if not_defined:
        logger.warning(
            f"Some baked datasets in the bakt directory are not defined in the config: {not_defined}"
        )

    if not_available:
        logger.warning(
            f"Some baked datasets defined in the config are not available: {not_available}"
        )

    AVAILABLE_BAKT_DATASETS = list(available)
    return AVAILABLE_BAKT_DATASETS


def _get_latest_path_bakt_data(name: str) -> Path:
    # We do implement this as a part of get_latest_shared_path,
    # because we don't want these baked datasets to be mixed
    # with other datasets, where we by accident end up returning
    # a bakt dataset path, for a dataset which is not a bakt dataset
    if name not in AVAILABLE_BAKT_DATASETS:
        raise ValueError(f"{name} is not avaiable as a baked dataset!")

    paths = []
    for directory in BAKT_PATHS:
        paths += list(
            directory.glob(
                f"{name}_p[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_v*[0-9].parquet"
            )
        )

    if not paths:
        raise ValueError(f"Unable to find a path for {name}!")

    latest = sorted(get_latest_fileversions(paths))
    return latest[-1]
