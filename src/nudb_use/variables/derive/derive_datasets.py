from nudb_use.paths.latest import latest_shared_paths
from nudb_use.variables.checks import pyarrow_columns_from_metadata


con = duckdb.connect(':memory:')

NATIVE_NUDB_DATASETS = ["avslutta", "eksamen", "igang"]
NUDB_DATA_


def _get_path_dataset_name(dataset: str) -> str | None:
    dataset = dataset.lower()
    is_native = dataset in NATIVE_NUDB_DATASETS

    return str(latest_shared_paths(dataset)) if is_native else None


class NudbData:

    def __init__(self, name: str, cols: None, initfunc: None | Callable = None):
        is_native: bool = name in NATIVE_NUDB_DATASETS
        path: str | None = _get_path_dataset_name()
        duckdb_alias: str = f"read_parquet({path})" if is_native else f"NUDB_DATA_{name.upper()}"

