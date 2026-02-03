import duckdb as db
import pandas as pd

from nudb_use.datasets.eksamen import init_eksamen_aggregated
from nudb_use.nudb_logger import logger
from nudb_use.paths.latest import latest_shared_paths
from nudb_use.variables.checks import pyarrow_columns_from_metadata

NUDB_DATABASE_CONNECTION = db.connect(":memory:")

NATIVE_NUDB_DATASETS = ["avslutta", "eksamen", "igang"]

DYNAMIC_NUDB_DATASET_GENERATORS = {
    "eksamen_aggregated": lambda: init_eksamen_aggregated(NUDB_DATABASE_CONNECTION)
}

DYNAMIC_NUDB_DATASETS = list(DYNAMIC_NUDB_DATASET_GENERATORS)

DATASETS = {}


def reset_nudb_database():
    global NUDB_DATABASE_CONNECTION, DATASETS
    NUDB_DATABASE_CONNECTION.close()
    NUDB_DATABASE_CONNECTION = db.connect(":memory:")
    DATASETS = {}


def _get_path_dataset_name(dataset: str) -> str | None:
    dataset = dataset.lower()
    is_native = dataset in NATIVE_NUDB_DATASETS

    return str(latest_shared_paths(dataset)) if is_native else None


def _dataset_in_database(name: str, alias: str) -> bool:
    if name in NATIVE_NUDB_DATASETS:
        return True

    existing = NUDB_DATABASE_CONNECTION.sql("SHOW TABLES").df()["name"]
    return alias in list(existing.astype("string[pyarrow]"))


class NudbDataSet:

    def __init__(self, name: str, attach_on_init: bool = True):
        name = name.lower()

        if name in DATASETS.keys():
            self._copy_attributes_from_existing(DATASETS[name])
            return None

        is_native: bool = name in NATIVE_NUDB_DATASETS
        is_dynamic: bool = name in DYNAMIC_NUDB_DATASETS

        if is_native:
            path = _get_path_dataset_name(name)
            alias = f"read_parquet('{path}')"
            initfunc = None
        elif is_dynamic:
            path = None
            alias = f"NUDB_DATA_{name.upper()}"
            initfunc = DYNAMIC_NUDB_DATASET_GENERATORS[name]
        else:
            raise ValueError("Unrecognized NUDB dataset!")

        self.name = name
        self.path = path
        self.alias = alias
        self.is_native = is_native
        self.is_dynamic = is_dynamic
        self.exists = _dataset_in_database(name=self.name, alias=self.alias)
        self.initfunc = initfunc

        if attach_on_init:  # Setting the default to `True` may be a bad idea...
            self._attach()

    def _attach(self) -> None:
        global DATASETS

        self.exists = _dataset_in_database(name=self.name, alias=self.alias)

        if self.is_native or self.exists:
            if (
                self.name not in DATASETS.keys()
            ):  # Datasets can exist without having been indexed
                DATASETS[self.name] = self

            return None

        elif callable(self.initfunc):
            nudb_pandas_df = self.initfunc()
            NUDB_DATABASE_CONNECTION.sql(
                f"CREATE TABLE {self.alias} AS SELECT * FROM nudb_pandas_df"
            )
            self.exists = _dataset_in_database(
                name=self.name, alias=self.alias
            )  # check again
            DATASETS[self.name] = self

            if not self.exists:
                logger.critical(f"Failed to attach {self.name} to NUDB_DATABASE!")
        else:
            raise ValueError(f"Missing initfunction for {self.name}!")

    def _available_cols(self):
        if self.is_native:
            return pyarrow_columns_from_metadata(self.path)
        elif self.exists:
            NUDB_DATABASE_CONNECTION.sql(f"DESCRIBE {self.alias}")
        else:
            raise ValueError(f"{self.name} is not available in duckdb database!")

    def __str__(self):
        return f"""
        NUDB DATASET:
            name:   {self.name}
            alias:  {self.alias}
            path:   {self.path}
            native: {self.is_native}
            exists: {self.exists}
        """

    def __repr__(self):
        return self.__str__()

    def _copy_attributes_from_existing(self, other):
        self.name = other.name
        self.path = other.path
        self.alias = other.alias
        self.is_native = other.is_native
        self.is_dynamic = other.is_dynamic
        self.exists = other.exists
        self.initfunc = other.initfunc

    def df(self) -> pd.DataFrame:
        return NUDB_DATABASE_CONNECTION.sql(f"""
            SELECT * FROM {self.alias}
        """).df()
