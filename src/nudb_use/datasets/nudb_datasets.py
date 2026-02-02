import duckdb as db

from nudb_use.paths.latest import latest_shared_paths
from nudb_use.variables.checks import pyarrow_columns_from_metadata


NUDB_DATABASE_CONNECTION = db.connect(':memory:')
NATIVE_NUDB_DATASETS = ["avslutta", "eksamen", "igang"]


def _get_path_dataset_name(dataset: str) -> str | None:
    dataset = dataset.lower()
    is_native = dataset in NATIVE_NUDB_DATASETS

    return str(latest_shared_paths(dataset)) if is_native else None


def _dataset_in_database(dataset: str) -> bool:
    if dataset in NATIVE_NUDB_DATASETS:
        return True

    existing = NUDB_DATABASE_CONNECTION.sql("SHOW TABLES").df()["name"]
    return dataset in existing


class NudbDataSet:

    def __init__(self, name: str, initfunc: None | Callable = None):
        is_native: bool = name in NATIVE_NUDB_DATASETS

        if is_native:
            path  = _get_path_dataset_name(name)
            alias = f"read_parquet({path})"
        else:
            path  = None
            alias = f"NUDB_DATA_{name.upper()}"

        self.name      = name
        self.path      = path
        self.alias     = alias
        self.is_native = is_native
        self.exists    = _dataset_in_database(name)

    def _attach(self) -> None:
        self.exists = _dataset_in_database(name)

        if (
            self.is_native or # Exists as a parquet file
            self.exists       # Already initialized
        ):
            return None
        elif callable(self.initfunc):
            df = self.initfunc()
        else:
            raise ValueError(f"Missing initfunction for {self.name}!")

    def _available_cols(self):
        if self.is_native:
            return pyarrow_columns_from_metadata(self.path)
        elif self.exists:
            NUDB_DATABASE_CONNECTION.sql(f"DESCRIBE {self.alias}")
        else:
            raise ValueError(f"{self.name} is not available in duckdb database!")


AVSLUTTA = NudbDataSet("avslutta")
IGANG    = NudbDataSet("igang")
EKSAMEN  = NudbDataSet("eksamen")
