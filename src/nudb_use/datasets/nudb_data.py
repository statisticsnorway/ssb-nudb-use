import copy
from collections.abc import Callable
from functools import partial
from typing import Any
from typing import cast

import pandas as pd
import polars as pl

from nudb_use.datasets.nudb_database import STRING_DTYPE
from nudb_use.datasets.nudb_database import nudb_database
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

            if name in nudb_database._datasets.keys():
                logger.info("Dataset is already initialized!")
                self._copy_attributes_from_existing(nudb_database._datasets[name])
                return None

            elif name not in nudb_database._dataset_generators.keys():
                raise ValueError("Unrecognized NUDB dataset!")

            self.name: str = name
            self.exists: bool = False
            self.is_view: bool = False

            self.generator: Callable[..., None] = partial(
                nudb_database._dataset_generators[name], *args, **kwargs
            )

            self._select = "*"
            self._where = ""
            self.data = None

            if attach_on_init:  # Setting the default to `True` may be a bad idea...
                logger.info("Initializing dataset!")
                self._attach()

    def _attach(self) -> None:
        self.data = self.generator()

        nudb_database._datasets[self.name] = self
        self.exists = _is_in_database(self.name)
        
        if not self.exists:
            logger.critical(f"Failed to attach {self.name} to database!")

    def get_available_cols(
        self,
    ) -> list[
        str | Any
    ]:  # always returns list[str] but mypy struggles with STRING_DTYPE
        """Get available columns in dataset."""
        return self.data.collect_schema().columns

    def _copy_attributes_from_existing(self, other: "NudbData") -> None:
        self.name = other.name
        self.exists = other.exists
        self.generator = other.generator
        self._select = other._select
        self._where = other._where
        self.data = other.data

    def _get_query(self) -> str:
        query = f"SELECT\n\t{self._select}\nFROM\n\tSELF"

        if self._where:
            query += f"\nWHERE\n\t{self._where}"

        return query

    def __str__(self) -> str:
        """Get string representation of NUDB dataset."""
        return f"""
        NUDB DATASET:
            name:     {self.name}
            exists:   {self.exists}
            select:   {self._select}
            where:    {self._where}
        """

    def where(self, expr: str) -> "NudbData":
        """Specify (inner part) of the WHERE statement in SQL query."""
        out = copy.copy(self)
        out._where = expr
        
        out.data = pl.SQLContext(SELF = self.data).execute(
            f"""SELECT * FROM SELF
                WHERE {out._where}
            """
        )
        
        return out

    def select(self, expr: str) -> "NudbData":
        """Specify (inner part) of the SELECT statement in SQL query."""
        out = copy.copy(self)
        out._select = expr

        out.data = pl.SQLContext(SELF = self.data).execute(
            f"SELECT {out._select} FROM SELF"
        )
        
        return out

    def __repr__(self) -> str:
        """Get string representation of NUDB dataset."""
        return self.__str__()

    def lf(self) -> pl.LazyFrame:
        """Return dataset as a polars LazyFrame."""
        return self.data
        
    def df(self) -> pd.DataFrame:
        """Return dataset as a pandas DataFrame."""
        return self.data.collect().to_pandas()

    def sql(self, expr: str) -> Any:
        """Use sql method of database connection."""
        return pl.SQLContext(SELF = self.data).execute(expr)


def _is_in_database(name: str) -> bool:
    return name in nudb_database._datasets.keys()
