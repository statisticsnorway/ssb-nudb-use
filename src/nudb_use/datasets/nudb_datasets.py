from collections.abc import Callable
from functools import partial

import duckdb as db
import pandas as pd

from nudb_use.datasets.avslutta import _generate_avslutta_view
from nudb_use.datasets.eksamen import _generate_eksamen_aggregated_view
from nudb_use.datasets.eksamen import _generate_eksamen_avslutta_hoyeste_view
from nudb_use.datasets.eksamen import _generate_eksamen_hoyeste_table
from nudb_use.datasets.eksamen import _generate_eksamen_view
from nudb_use.datasets.igang import _generate_igang_view
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

_DATABASE_CONNECTION: db.DuckDBPyConnection = db.connect(":memory:")
_DATASET_GENERATORS: dict[str, Callable[[str], None]] = {
    "eksamen_aggregated": partial(
        _generate_eksamen_aggregated_view, connection=_DATABASE_CONNECTION
    ),
    "eksamen": partial(_generate_eksamen_view, connection=_DATABASE_CONNECTION),
    "avslutta": partial(_generate_avslutta_view, connection=_DATABASE_CONNECTION),
    "igang": partial(_generate_igang_view, connection=_DATABASE_CONNECTION),
    "eksamen_hoyeste": partial(
        _generate_eksamen_hoyeste_table, connection=_DATABASE_CONNECTION
    ),
    "eksamen_avslutta_hoyeste": partial(
        _generate_eksamen_avslutta_hoyeste_view, connection=_DATABASE_CONNECTION
    ),
}

_DATASET_NAMES = list(_DATASET_GENERATORS.keys())
_DATASETS: dict[str, "NudbData"] = {}


def reset_nudb_database() -> None:
    """Reset (I.e., clear) the internal database."""
    global _DATABASE_CONNECTION, _DATASETS
    _DATABASE_CONNECTION.close()
    _DATABASE_CONNECTION = db.connect(":memory:")
    _DATASETS = {}


def _is_view(alias: str) -> bool:
    views = list(
        _DATABASE_CONNECTION.sql("SELECT view_name FROM duckdb_views()")
        .df()["view_name"]
        .astype("string[pyarrow]")
    )

    return alias in views


def _is_table(alias: str) -> bool:
    tables = list(
        _DATABASE_CONNECTION.sql("SHOW TABLES").df()["name"].astype("string[pyarrow]")
    )

    return alias in tables


def _is_in_database(alias: str) -> bool:
    return _is_table(alias) or _is_view(alias)


class NudbData:
    """Lazy representation of a NUDB dataset.

    Args:
        name: Name of the dataset.
        attach_on_init: Should dataset be attached to the internal database?

    Attributes:
        name: Name of the dataset.
        alias: Database alias for the dataset.
        exists: Does the dataset exist in the internal database?
        is_view: Is the dataset stored (lazily) as a view, in the internal database?
        generator: Function for generating and attaching dataset to the internal database.
    """

    def __init__(self, name: str, attach_on_init: bool = True) -> None:
        with LoggerStack(f"Getting NUDB dataset ({name.upper()})"):
            name = name.lower()

            if name in _DATASETS.keys():
                logger.info("Dataset is already initialized!")
                self._copy_attributes_from_existing(_DATASETS[name])
                return None

            elif name not in _DATASET_GENERATORS.keys():
                raise ValueError("Unrecognized NUDB dataset!")

            generator = _DATASET_GENERATORS[name]
            alias = f"NUDB_DATA_{name.upper()}"

            self.name: str = name
            self.alias: str = alias
            self.exists: bool = False
            self.is_view: bool = False
            self.generator: Callable[[str], None] = generator

            if attach_on_init:  # Setting the default to `True` may be a bad idea...
                logger.info("Initializing dataset!")
                self._attach()

    def _attach(self) -> None:
        global _DATASETS

        self.generator(self.alias)
        self.is_view = _is_view(self.alias)
        self.exists = _is_in_database(self.alias)

        if self.exists:
            _DATASETS[self.name] = self
        else:
            logger.critical(f"Failed to attach {self.name} to database!")

    def get_available_cols(self) -> list[str]:
        """Get available columns in dataset."""
        if self.exists:
            return list(
                _DATABASE_CONNECTION.sql(f"DESCRIBE {self.alias}")
                .df()["column_name"]
                .astype("string[pyarrow]")
            )
        else:
            logger.warning(f"{self.name} is not available in duckdb database!")
            return []

    def __str__(self) -> str:
        """Get string representation of NUDB dataset."""
        return f"""
        NUDB DATASET:
            name:     {self.name}
            alias:    {self.alias}
            exists:   {self.exists}
            is_view:  {self.is_view}
        """

    def __repr__(self) -> str:
        """Get string representation of NUDB dataset."""
        return self.__str__()

    def _copy_attributes_from_existing(self, other: "NudbData") -> None:
        self.name = other.name
        self.alias = other.alias
        self.is_view = other.is_view
        self.exists = other.exists
        self.generator = other.generator

    def df(self) -> pd.DataFrame:
        """Return dataset as a pandas DataFrame."""
        return _DATABASE_CONNECTION.sql(f"SELECT * FROM {self.alias}").df()
