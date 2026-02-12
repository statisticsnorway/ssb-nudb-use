import copy
from collections.abc import Callable
from functools import partial
from typing import Any

import pandas as pd

from nudb_use.datasets.nudb_database import _NUDB_DATABASE
from nudb_use.datasets.nudb_database import STRING_DTYPE
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


class NudbData:
    """Lazy representation of a NUDB dataset.

    Args:
        name: Name of the dataset.
        attach_on_init: Should dataset be attached to the internal database?
        *args: Unnamed arguments passed on to the dataset generator.
        **kwargs: Named arguments passed on to the dataset generator.

    Raises:
        ValueError: If the dataset name isn't recognized.
    """

    def __init__(
        self, name: str, attach_on_init: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        with LoggerStack(f"Getting NUDB dataset ({name.upper()})"):
            name = name.lower()

            if name in _NUDB_DATABASE._datasets.keys():
                logger.info("Dataset is already initialized!")
                self._copy_attributes_from_existing(_NUDB_DATABASE._datasets[name])
                return None

            elif name not in _NUDB_DATABASE._dataset_generators.keys():
                raise ValueError("Unrecognized NUDB dataset!")

            self.name: str = name
            if "alias" in kwargs:
                self.alias : str = kwargs["alias"]
            else:
                self.alias  = _default_alias_from_name(name)
            self.exists: bool = False
            self.is_view: bool = False

            self.generator: Callable[..., None] = partial(
                _NUDB_DATABASE._dataset_generators[name], *args, **kwargs
            )

            self._select = "*"
            self._where = ""

            if attach_on_init:  # Setting the default to `True` may be a bad idea...
                logger.info("Initializing dataset!")
                self._attach()

    def _attach(self) -> None:
        self.generator(alias=self.alias, connection=_NUDB_DATABASE.get_connection())
        self.is_view = _is_view(self.alias)
        self.exists = _is_in_database(self.alias)

        if self.exists:
            _NUDB_DATABASE._datasets[self.name] = self
        else:
            logger.critical(f"Failed to attach {self.name} to database!")

    def get_available_cols(
        self,
    ) -> list[
        str | Any
    ]:  # always returns list[str] but mypy struggles with STRING_DTYPE
        """Get available columns in dataset."""
        if self.exists:
            return list(
                _NUDB_DATABASE.get_connection()
                .sql(f"DESCRIBE {self.alias}")
                .df()["column_name"]
                .astype(STRING_DTYPE)
            )
        else:
            logger.warning(f"{self.name} is not available in duckdb database!")
            return []

    def _copy_attributes_from_existing(self, other: "NudbData") -> None:
        self.name = other.name
        self.alias = other.alias
        self.is_view = other.is_view
        self.exists = other.exists
        self.generator = other.generator
        self._select = other._select
        self._where = other._where

    def _get_query(self) -> str:
        query = f"SELECT\n\t{self._select}\nFROM\n\t{self.alias}"

        if self._where:
            query += f"\nWHERE\n\t{self._where}"

        return query

    def __str__(self) -> str:
        """Get string representation of NUDB dataset."""
        return f"""
        NUDB DATASET:
            name:     {self.name}
            alias:    {self.alias}
            exists:   {self.exists}
            is_view:  {self.is_view}
            select:   {self._select}
            where:    {self._where}
        """

    def where(self, expr: str) -> "NudbData":
        """Specify (inner part) of the WHERE statement in SQL query."""
        out = copy.copy(self)
        out._where = expr
        out._select = self._select
        return out

    def select(self, expr: str) -> "NudbData":
        """Specify (inner part) of the SELECT statement in SQL query."""
        out = copy.copy(self)
        out._select = expr
        out._where = self._where
        return out

    def __repr__(self) -> str:
        """Get string representation of NUDB dataset."""
        return self.__str__()

    def df(self) -> pd.DataFrame:
        """Return dataset as a pandas DataFrame."""
        query = self._get_query()
        return _NUDB_DATABASE.get_connection().sql(query).df()

    def sql(self, expr: str) -> Any:
        """Use sql method of database connection."""
        return _NUDB_DATABASE.get_connection().sql(expr)

    def execute(self, expr: str) -> Any:
        """Use execute method of database connection."""
        return _NUDB_DATABASE.get_connection().execute(expr)


def _is_view(alias: str) -> bool:
    views = list(
        _NUDB_DATABASE.get_connection()
        .sql("SELECT view_name FROM duckdb_views()")
        .df()["view_name"]
        .astype(STRING_DTYPE)
    )

    return alias in views


def _is_in_database(alias: str) -> bool:
    return _is_table(alias) or _is_view(alias)


def _is_table(alias: str) -> bool:
    tables = list(
        _NUDB_DATABASE.get_connection()
        .sql("SHOW TABLES")
        .df()["name"]
        .astype(STRING_DTYPE)
    )

    return alias in tables
