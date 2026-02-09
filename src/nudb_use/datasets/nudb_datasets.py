from collections.abc import Callable

import duckdb as db
import pandas as pd

from nudb_use.datasets.avslutta import _generate_avslutta_fullfoert_table
from nudb_use.datasets.avslutta import _generate_avslutta_view
from nudb_use.datasets.eksamen import _generate_eksamen_aggregated_view
from nudb_use.datasets.eksamen import _generate_eksamen_avslutta_hoyeste_view
from nudb_use.datasets.eksamen import _generate_eksamen_hoyeste_table
from nudb_use.datasets.eksamen import _generate_eksamen_view
from nudb_use.datasets.igang import _generate_igang_view
from nudb_use.metadata.nudb_config.map_get_dtypes import STRING_DTYPE_NAME
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

# Create a mutable singleton for the database, so it can be safely
# passed around to other modules, without being immutable


class NudbDatabase:
    """Singleton for internal NUDB database."""

    def __init__(self) -> None:
        self._connection: db.DuckDBPyConnection = db.connect(":memory:")

        self._dataset_generators: dict[
            str, Callable[[str, db.DuckDBPyConnection], None]
        ] = {
            "eksamen_aggregated": _generate_eksamen_aggregated_view,
            "eksamen": _generate_eksamen_view,
            "avslutta": _generate_avslutta_view,
            "avslutta_fullfoert": _generate_avslutta_fullfoert_table,
            "igang": _generate_igang_view,
            "eksamen_hoyeste": _generate_eksamen_hoyeste_table,
            "eksamen_avslutta_hoyeste": _generate_eksamen_avslutta_hoyeste_view,
        }

        self._dataset_names = list(self._dataset_generators.keys())
        self._datasets: dict[str, NudbData] = {}

    def _reset(self) -> None:
        self._connection.close()
        self._connection = db.connect(":memory:")
        self._datasets = {}

    def __del__(self) -> None:
        """Destructor for NudbDatabase."""
        self._connection.close()  # close before deleting

    def get_connection(self) -> db.DuckDBPyConnection:
        """Get database connection."""
        return self._connection


_NUDB_DATABASE = NudbDatabase()


def reset_nudb_database() -> None:
    """Reset (I.e., clear) the internal database."""
    _NUDB_DATABASE._reset()


def _is_view(alias: str) -> bool:
    views = list(
        _NUDB_DATABASE.get_connection()
        .sql("SELECT view_name FROM duckdb_views()")
        .df()["view_name"]
        .astype(STRING_DTYPE_NAME)
    )

    return alias in views


def _is_table(alias: str) -> bool:
    tables = list(
        _NUDB_DATABASE.get_connection()
        .sql("SHOW TABLES")
        .df()["name"]
        .astype(STRING_DTYPE_NAME)
    )

    return alias in tables


def _is_in_database(alias: str) -> bool:
    return _is_table(alias) or _is_view(alias)


class NudbData:
    """Lazy representation of a NUDB dataset.

    Args:
        name: Name of the dataset.
        attach_on_init: Should dataset be attached to the internal database?

    Raises:
        ValueError: If the dataset name isn't recognized.
    """

    def __init__(self, name: str, attach_on_init: bool = True) -> None:
        with LoggerStack(f"Getting NUDB dataset ({name.upper()})"):
            name = name.lower()

            if name in _NUDB_DATABASE._datasets.keys():
                logger.info("Dataset is already initialized!")
                self._copy_attributes_from_existing(_NUDB_DATABASE._datasets[name])
                return None

            elif name not in _NUDB_DATABASE._dataset_generators.keys():
                raise ValueError("Unrecognized NUDB dataset!")

            generator = _NUDB_DATABASE._dataset_generators[name]
            alias = f"NUDB_DATA_{name.upper()}"

            self.name: str = name
            self.alias: str = alias
            self.exists: bool = False
            self.is_view: bool = False
            self.generator: Callable[[str, db.DuckDBPyConnection], None] = generator

            if attach_on_init:  # Setting the default to `True` may be a bad idea...
                logger.info("Initializing dataset!")
                self._attach()

    def _attach(self) -> None:
        self.generator(self.alias, _NUDB_DATABASE.get_connection())
        self.is_view = _is_view(self.alias)
        self.exists = _is_in_database(self.alias)

        if self.exists:
            _NUDB_DATABASE._datasets[self.name] = self
        else:
            logger.critical(f"Failed to attach {self.name} to database!")

    def get_available_cols(self) -> list[str]:
        """Get available columns in dataset."""
        if self.exists:
            return list(
                _NUDB_DATABASE.get_connection()
                .sql(f"DESCRIBE {self.alias}")
                .df()["column_name"]
                .astype(STRING_DTYPE_NAME)
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
        return _NUDB_DATABASE.get_connection().sql(f"SELECT * FROM {self.alias}").df()
