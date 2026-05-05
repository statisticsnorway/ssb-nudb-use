import copy
from collections.abc import Callable
from functools import partial
from typing import Any
from typing import Literal
from typing import cast

import pandas as pd

from nudb_use.datasets.nudb_database import STRING_DTYPE
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

JOIN_TYPES = {"left", "right", "inner", "cross", "full", "outer", "self"}


class NudbData:
    """Lazy representation of a NUDB dataset.

    Args:
        name: Name of the dataset.
        attach_using_init: Should dataset be attached to the internal database?
        *args: Unnamed arguments passed on to the dataset generator.
        **kwargs: Named arguments passed on to the dataset generator.

    Raises:
        ValueError: If the dataset name isn't recognized.
    """

    def __init__(
        self, name: str, attach_using_init: bool = True, *args: Any, **kwargs: Any
    ) -> None:
        with LoggerStack(f"Getting NUDB dataset ({name.upper()})"):
            name = name.lower()

            if name in nudb_database._datasets.keys():
                logger.info("Dataset is already initialized!")
                self._copy_attributes_from_existing(nudb_database._datasets[name])
                return None

            elif name not in nudb_database._dataset_generators.keys():
                available = ",\n\t".join(
                    sorted(nudb_database._dataset_generators.keys())
                )
                raise ValueError(
                    f"Unrecognized NUDB dataset!\nAvailable datasets:\n\t{available}"
                )

            self.name: str = name
            if "alias" in kwargs:
                self.alias: str = kwargs["alias"]
            else:
                self.alias = _default_alias_from_name(name)
            self.exists: bool = False
            self.is_view: bool = False

            self.generator: Callable[..., None] = partial(
                nudb_database._dataset_generators[name], *args, **kwargs
            )

            self._select = "*"
            self._where = ""
            self._limit = ""
            self._join = ""
            self._join_type = ""
            self._join_as = ""
            self._using = ""
            self._as = ""
            self._on = ""

            if attach_using_init:  # Setting the default to `True` may be a bad idea...
                logger.info("Initializing dataset!")
                self._attach()

    def _attach(self) -> None:
        self.generator(alias=self.alias, connection=nudb_database.get_connection())
        self.is_view = _is_view(self.alias)
        self.exists = _is_in_database(self.alias)

        if self.exists:
            nudb_database._datasets[self.name] = self
        else:
            logger.critical(f"Failed to attach {self.name} to database!")

    def get_available_cols(
        self,
    ) -> list[
        str | Any
    ]:  # always returns list[str] but mypy struggles with STRING_DTYPE
        """Get available columns in dataset."""
        if self.exists:
            return _fetch_string_column(
                f"DESCRIBE {self.alias}",
                "column_name",
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
        self._limit = other._limit
        self._join = other._join
        self._join_type = other._join_type
        self._join_as = other._join_as
        self._using = other._using
        self._as = other._as
        self._on = other._on

    def _get_query(self, check_validity: bool = False) -> str:
        # SELECT ... FROM ...
        query = f"SELECT\n    {self._select}\nFROM\n    {self.alias}"

        # AS
        if self._as:
            query += " AS " + self._as

        # <TYPE> JOIN ...
        if self._join and self._join_type:
            query += (
                f"\n{self._join_type} JOIN \n    {self._join.replace("\n", "\n    ")}"
            )

            if self._join_as:
                query += self._join_as

            query += "\n"

            if check_validity and not self._using and not self._on:
                raise ValueError(
                    f"Missing USING/ON statement for {self._join_type} JOIN!"
                )

        # USING ...
        if self._using:
            query += f"\nUSING (\n    {self._using.replace("\n", "\n    ")}\n)"

            if check_validity and not self._join:
                raise ValueError("Missing JOIN statement for USING statement!")
            elif check_validity and self._on:
                raise ValueError(
                    "The USING statement cannot be used at the same time as the ON statement!"
                )

        if self._on:
            query += f"\nON\n    {self._on.replace("\n", "\n    ")}"

        # WHERE ...
        if self._where:
            query += f"\nWHERE\n\t{self._where.replace("\n", "\n    ")}"

        # LIMIT ...
        if self._limit:
            query += f"\nLIMIT\n\t{self._limit}"

        # SELECT
        #   ...
        # FROM
        #   ... AS ...
        # <TYPE> JOIN
        #   ...
        # USING (
        #   <keys>
        # )
        # WHERE
        #   ...
        # LIMIT ...;
        return query

    def __str__(self) -> str:
        """Get string representation of NUDB dataset."""
        query = self._get_query(check_validity=False)

        return f"""
NUDB DATASET:
    name:     {self.name}
    alias:    {self.alias}
    exists:   {self.exists}
    is_view:  {self.is_view}

QUERY:
    {query.replace("\n", "\n    ")};
        """

    def where(self, expr: str) -> "NudbData":
        """Specify (inner part) of the WHERE statement in SQL query."""
        out = copy.copy(self)
        out._where = expr
        return out

    def select(self, expr: str) -> "NudbData":
        """Specify (inner part) of the SELECT statement in SQL query."""
        out = copy.copy(self)
        out._select = expr
        return out

    def select_distinct(self, expr: str) -> "NudbData":
        """Specify (inner part) of the SELECT DISTINCT statement in SQL query."""
        return self.select("DISTINCT " + expr)

    def limit(self, expr: str) -> "NudbData":
        """Specify (inner part) of the LIMIT statement in SQL query."""
        out = copy.copy(self)
        out._limit = expr
        return out

    def join(
        self,
        data: str | pd.DataFrame | Literal["NudbData"],
        how: str = "inner",
        as_name: str | None = None,
    ) -> "NudbData":
        """Specify (inner part) of the JOIN statement in SQL query."""
        if isinstance(data, str):
            try:
                logger.debug("Checking if string is the name of an NUDB datasett")
                nudb_data = NudbData(data)

                logger.debug("Getting alias...")
                # Since it's the name of an NUDB datasett we can get the alias from the
                # NudbData object. When the user passes an NudbData object directly
                # we must get the query, but here the query should be default/empty
                expr = nudb_data.alias
                _as = nudb_data._as

            except Exception:
                logger.debug("Using raw string...")
                expr = data
                _as = ""

        elif isinstance(data, NudbData):
            # If the user passes an NudbData object, it might have some
            # query attributes, so we cannot use the raw alias
            logger.debug("Getting query from NudbData")
            expr = f"(\n{data._get_query(check_validity = True)}\n)"
            _as = data._as

        elif isinstance(data, pd.DataFrame):
            logger.debug("Registering pandas DataFrame in Database...")
            connection = nudb_database.get_connection()
            expr = "_TMP_DATAFRAME_INPUT"
            _as = ""
            connection.register(expr, data.copy())

        if how.lower() not in JOIN_TYPES:
            raise ValueError(f"how must be one of: {list(JOIN_TYPES)}")

        out = copy.copy(self)
        out._join = expr
        out._join_type = how.upper()
        out._join_as = as_name if as_name is not None else _as

        return out

    def left_join(self, data: str | pd.DataFrame | Literal["NudbData"]) -> "NudbData":
        """Specify (inner part) of the LEFT JOIN statement in SQL query."""
        return self.join(data, how="left")

    def right_join(self, data: str | pd.DataFrame | Literal["NudbData"]) -> "NudbData":
        """Specify (inner part) of the RIGHT JOIN statement in SQL query."""
        return self.join(data, how="right")

    def inner_join(self, data: str | pd.DataFrame | Literal["NudbData"]) -> "NudbData":
        """Specify (inner part) of the INNER JOIN statement in SQL query."""
        return self.join(data, how="inner")

    def full_join(self, data: str | pd.DataFrame | Literal["NudbData"]) -> "NudbData":
        """Specify (inner part) of the FULL JOIN statement in SQL query."""
        return self.join(data, how="full")

    def cross_join(self, data: str | pd.DataFrame | Literal["NudbData"]) -> "NudbData":
        """Specify (inner part) of the CROSS JOIN statement in SQL query."""
        return self.join(data, how="cross")

    def self_join(self, data: str | pd.DataFrame | Literal["NudbData"]) -> "NudbData":
        """Specify (inner part) of the CROSS JOIN statement in SQL query."""
        return self.join(data, how="self")

    def using(self, expr: str) -> "NudbData":
        """Specify (inner part) of the USING statement in SQL query."""
        out = copy.copy(self)
        out._using = expr
        return out

    def on(self, expr: str) -> "NudbData":
        """Specify (inner part) of the ON statement in SQL query."""
        out = copy.copy(self)
        out._on = expr
        return out

    def as_name(self, expr: str) -> "NudbData":
        """Specify (inner part) of the AS statement in SQL query."""
        out = copy.copy(self)
        out._as = expr
        return out

    def __repr__(self) -> str:
        """Get string representation of NUDB dataset."""
        return self.__str__()

    def df(self) -> pd.DataFrame:
        """Return dataset as a pandas DataFrame."""
        query = self._get_query(check_validity=True)
        return nudb_database.get_connection().sql(query).df()

    def sql(self, expr: str | None = None) -> Any:
        """Use sql method of database connection."""
        if expr is None:
            expr = self._get_query(check_validity=True)

        return nudb_database.get_connection().sql(expr)

    def execute(self, expr: str) -> Any:
        """Use execute method of database connection."""
        return nudb_database.get_connection().execute(expr)


def _is_view(alias: str) -> bool:
    views = _fetch_string_column(
        "SELECT view_name FROM duckdb_views()",
        "view_name",
    )

    return alias in views


def _is_in_database(alias: str) -> bool:
    return _is_table(alias) or _is_view(alias)


def _is_table(alias: str) -> bool:
    tables = _fetch_string_column(
        "SHOW TABLES",
        "name",
    )

    return alias in tables


def _fetch_string_column(sql: str, column_name: str) -> list[str]:
    series = (
        nudb_database.get_connection().sql(sql).df()[column_name].astype(STRING_DTYPE)
    )
    return list(cast("pd.Series[str]", series))
