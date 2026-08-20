from pathlib import Path

from nudb_use.nudb_logger import logger


def _nudb_read_parquet(path: str | Path, alias: str) -> str:
    from nudb_use.datasets.nudb_database import nudb_database  # avoid circular import

    if not isinstance(path, Path):
        path = Path(path)

    if not alias:
        raise ValueError(f"Invalid value of alias: '{alias}'")

    # check if file exists
    if not path.is_file():
        raise ValueError(f"'{path}' is not a file, or does not exist!")

    # log and attach
    logger.info(f"Reading parquet from path: '{path}'")

    if alias not in nudb_database._dataset_paths.keys():
        nudb_database._dataset_paths[alias] = [path]
    else:
        nudb_database._dataset_paths[alias].append(path)

    # return duckdb expression
    return f"read_parquet('{path}')"
