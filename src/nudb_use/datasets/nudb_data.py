import copy
import re
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any
from typing import cast

import duckdb as db
import pandas as pd

from nudb_use.datasets.nudb_database import STRING_DTYPE
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.datasets.nudb_read_parquet import _nudb_read_parquet
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from fagfunksjoner.paths.shared_files import check_shared_files
from fagfunksjoner.paths.shared_files import FileStatusReport

JOIN_TYPES = {"left", "right", "inner", "cross", "full", "outer", "self"}

# Matcher SSBs navnekonvensjon: <navn>_p<år1>[_p<år2>]_v<versjon>.parquet
# år2 (om den finnes) er perioden check_shared_files forventer som "year",
# mens år1 (om den finnes) må bli en del av "name" for at glob-mønsteret
# <name>_p<year>_v*.parquet i check_shared_files skal matche.
_FILE_PATTERN = re.compile(
    r"^(?P<base>.+?)_p(?P<year1>\d{4})(?:_p(?P<year2>\d{4}))?_v(?P<version>\d+)\.parquet$"
)


def _indent(
    x: str,
    indent_first: bool = True,
    indent_last: bool = True,
    pad: str = 4 * " ",
) -> str:
    if indent_first:
        x = pad + x

    if not indent_last:
        split = x.split("\n")
        prev = split[: (len(split) - 1)]
        last = split[-1]
        return ("\n" + pad).join(prev) + "\n" + last
    else:
        return x.replace("\n", "\n" + pad)


def _extract_root_source_path(dataset_id: str) -> str | None:
    """Find the first (deepest/original) parquet path in a lineage string.

    ``nudb_dataset_id`` values are chains of segments separated by ``>``,
    optionally prefixed with a tag (e.g. ``ssbresultatv>``). The first
    segment that looks like a parquet path (contains ``/`` and ends in
    ``.parquet``) is the original source file; later segments are
    intermediate/merged files.
    """
    for segment in dataset_id.split(">"):
        segment = segment.strip()
        if "/" in segment and segment.lower().endswith(".parquet"):
            return segment
    return None


def _parse_source_file(raw_path: str) -> dict[str, Any] | None:
    """Parse a source-file path into the pieces needed for grouping and lookup.

    Returns a dict with:
        base:        source name without any period/version (grouping key,
                     used to find the currently active vintage among many
                     historical ones).
        check_name:  the ``name`` to pass to ``check_shared_files`` (includes
                     the first period if the file uses a two-period name).
        check_year:  the ``year`` to pass to ``check_shared_files``.
        path:        parent directory of the file.
        sort_key:    (year, version) tuple used to pick the latest vintage.
    """
    p = Path(raw_path)
    match = _FILE_PATTERN.match(p.name)
    if not match:
        return None

    base = match.group("base")
    year1 = int(match.group("year1"))
    year2 = match.group("year2")
    version = int(match.group("version"))

    if year2 is not None:
        year2 = int(year2)
        check_name = f"{base}_p{year1}"
        check_year = year2
        sort_key = (year2, version)
    else:
        check_name = base
        check_year = year1
        sort_key = (year1, version)

    return {
        "base": base,
        "check_name": check_name,
        "check_year": check_year,
        "path": str(p.parent),
        "sort_key": sort_key,
    }


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
                    f"Unrecognized NUDB dataset ({name})!\nAvailable datasets:\n\t{available}"
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

            # Cache for auto-derived source-file specs (see get_source_files)
            self._source_files: list[dict[str, Any]] | None = None

            if attach_using_init:  # Setting the default to `True` may be a bad idea...
                logger.info("Initializing dataset!")
                self._attach()

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
    {_indent(query, indent_first = False)};
"""

    def __repr__(self) -> str:
        """Get string representation of NUDB dataset."""
        return self.__str__()

    @classmethod
    def from_parquet(
        cls,
        path: str | Path,
        name: str | None = None,
        force: bool = False,
        **kwargs: Any,
    ) -> "NudbData":
        """Create NudbData object from a path of a parquet file."""
        if isinstance(path, str):
            path = Path(path)
        elif not isinstance(path, Path):
            raise TypeError("path must be either a string or a pathlib.Path object!")

        if not path.is_file() or path.suffix.lower() != ".parquet":
            raise ValueError("path must point to a parquet file!")

        if not name:
            name = path.name.replace(".", "_").replace("-", "_").lower()

        alias = _default_alias_from_name(name)
        logger.info(f"Creating NudbData object with name='{name}'...")
        logger.debug(f"Creating NudbData object with alias={alias}...")

        if name in nudb_database._dataset_generators:
            if not force:
                raise ValueError(
                    f"Name {name} is already defined, if you really want to overwrite it pass `force=True`"
                )

            logger.warning(f"Overwriting existing dataset: {name}")
            del nudb_database._datasets[name]

            if alias in nudb_database._dataset_paths.keys():
                previous_paths = nudb_database._dataset_paths[alias]
                logger.warning(
                    f"Overwriting existing dataset input paths:\n{previous_paths}"
                )
                del nudb_database._dataset_paths[alias]

        def generator(alias: str, connection: db.DuckDBPyConnection) -> None:
            query = f"""
            CREATE OR REPLACE VIEW
                {alias} AS
            SELECT * FROM
                {_nudb_read_parquet(path, alias)}
            """

            connection.execute(query)

        logger.debug("Attaching generator...")
        nudb_database._dataset_generators[name] = generator
        nudb_database._dataset_names = list(nudb_database._dataset_generators.keys())

        return cls(name=name, alias=alias, **kwargs)

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

    def get_source_files(
        self, id_col: str = "nudb_dataset_id", refresh: bool = False
    ) -> list[dict[str, Any]]:
        """Derive the currently active source-file spec for each underlying source.

        The lineage column (``nudb_dataset_id`` by default) typically holds
        many historical vintages of the same underlying source (e.g. old
        panel years mixed in with the current data). This parses out the
        original source path for every distinct lineage value, groups them
        by source name (ignoring period/version), and keeps only the entry
        with the highest (year, version) per source — i.e. the vintage
        that is actually in use today.

        Args:
            id_col: Name of the lineage column to inspect.
            refresh: Force re-derivation instead of using the cached result.

        Returns:
            A list of dicts with ``name``, ``path`` and ``year`` keys. The
            ``name``/``path`` pair is ready to pass into ``check_shared_files``
            (together with the matching ``year``).
        """
        if self._source_files is not None and not refresh:
            return self._source_files

        if not self.exists:
            logger.warning(f"{self.name} is not attached; cannot inspect source files.")
            return []

        if id_col not in self.get_available_cols():
            logger.warning(
                f"Column '{id_col}' not found in {self.name}; "
                "cannot derive source files."
            )
            return []

        raw_values = _fetch_string_column(
            f"SELECT DISTINCT {id_col} FROM {self.alias}",
            id_col,
        )

        parsed: list[dict[str, Any]] = []
        for value in raw_values:
            raw_path = _extract_root_source_path(value)
            if raw_path is None:
                continue

            spec = _parse_source_file(raw_path)
            if spec:
                parsed.append(spec)

        latest_per_base: dict[str, dict[str, Any]] = {}
        for spec in parsed:
            key = spec["base"]
            current_best = latest_per_base.get(key)
            if current_best is None or spec["sort_key"] > current_best["sort_key"]:
                latest_per_base[key] = spec

        self._source_files = [
            {
                "name": s["check_name"],
                "path": s["path"],
                "year": s["check_year"],
            }
            for s in latest_per_base.values()
        ]

        logger.debug(
            f"Derived {len(self._source_files)} active source file(s) for "
            f"'{self.name}' out of {len(raw_values)} distinct '{id_col}' values."
        )

        return self._source_files

    def check_source_versions(
        self, id_col: str = "nudb_dataset_id", refresh: bool = False
    ) -> FileStatusReport:
        """Check the latest available version for each currently used source file.

        Only the source files actually in use in the loaded data are
        checked (older vintages found in the lineage column are ignored).
        Since sources can be at different vintages (e.g. videregående
        updated before universitet), files are grouped by their own
        reference year and checked in separate batches, then merged into
        a single report.

        Args:
            id_col: Name of the lineage column (default "nudb_dataset_id").
            refresh: Force re-derivation of source files instead of using the cache.

        Returns:
            FileStatusReport with status (green/yellow/red) per source file.

        Raises:
            ValueError: If no source files could be derived for the dataset.
        """
        files = self.get_source_files(id_col=id_col, refresh=refresh)
        if not files:
            raise ValueError(
                f"No source files could be derived for dataset '{self.name}'. "
                f"Check that '{id_col}' exists and follows the SSB naming convention."
            )

        by_year: dict[int, list[dict[str, str]]] = {}
        for f in files:
            by_year.setdefault(f["year"], []).append(
                {"name": f["name"], "path": f["path"]}
            )

        all_statuses = []
        for year in sorted(by_year):
            sub_report = check_shared_files(by_year[year], year=year)
            all_statuses.extend(sub_report.files)

        return FileStatusReport(year=max(by_year), files=all_statuses)

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
        self._source_files = other._source_files

    def _check_query_validity(self) -> None:
        if self._join and not self._using and not self._on:
            raise ValueError(f"Missing USING/ON statement for {self._join_type} JOIN!")

        if self._using and not self._join:
            raise ValueError("Missing JOIN statement for USING statement!")

        if self._on and not self._join:
            raise ValueError("Missing JOIN statement for ON statement!")

        if self._using and self._on:
            raise ValueError(
                "The USING statement cannot be used at the same time as the ON statement!"
            )

    def _get_query(self, check_validity: bool = False) -> str:
        if check_validity:
            self._check_query_validity()

        # SELECT ... FROM ...
        query = f"SELECT\n    {self._select}\nFROM\n    {self.alias}"

        # AS
        if self._as:
            query += " AS " + self._as

        # <TYPE> JOIN ...
        if self._join and self._join_type:
            query += f"\n{self._join_type} JOIN {_indent(self._join, indent_first=False, indent_last=False)}"

            if self._join_as:
                query += " AS " + self._join_as

        # USING ...
        if self._using:
            query += f"\nUSING (\n{_indent(self._using)}\n)"

        if self._on:
            query += f"\nON\n{_indent(self._on)}"

        # WHERE ...
        if self._where:
            query += f"\nWHERE\n{_indent(self._where)}"

        # LIMIT ...
        if self._limit:
            query += f"\nLIMIT\n{_indent(self._limit)}"

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

    def where(self, *exprs: str) -> "NudbData":
        """Specify (inner part) of the WHERE statement in SQL query."""
        expr = " AND ".join(exprs)

        out = copy.copy(self)
        out._where = expr

        return out

    def select(self, *exprs: str) -> "NudbData":
        """Specify (inner part) of the SELECT statement in SQL query."""
        expr = ", ".join(exprs)

        out = copy.copy(self)
        out._select = expr

        return out

    def select_distinct(self, *exprs: str) -> "NudbData":
        """Specify (inner part) of the SELECT DISTINCT statement in SQL query."""
        expr = ", ".join(exprs)
        return self.select("DISTINCT " + expr)

    def limit(self, expr: str | int) -> "NudbData":
        """Specify (inner part) of the LIMIT statement in SQL query."""
        out = copy.copy(self)
        out._limit = str(expr)
        return out

    def join(
        self,
        data: "str | pd.DataFrame | NudbData",
        how: str = "inner",
        as_name: str | None = None,
    ) -> "NudbData":
        """Specify (inner part) of the JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            how: A string indicator the join type.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.

        Raises:
            ValueError: If `how` is not a supported join type.
        """
        if isinstance(data, str):
            try:
                logger.debug("Checking if string is the name of an NUDB datasett")
                nudb_data = NudbData(data)

                logger.debug("Getting alias...")
                # Since it's the name of an NUDB datasett we can get the alias from the
                # NudbData object. When the user passes an NudbData object directly
                # we must get the query, but here the query should be default/empty
                expr = _indent(nudb_data.alias)
                _as = nudb_data._as

            except Exception:
                logger.debug("Using raw string...")
                expr = _indent(data)
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
            name = "_TMP_DATAFRAME_INPUT"
            _as = ""
            connection.register(name, data.copy())
            expr = _indent(name)

        if how.lower() not in JOIN_TYPES:
            raise ValueError(f"how must be one of: {list(JOIN_TYPES)}")

        out = copy.copy(self)
        out._join = expr
        out._join_type = how.upper()
        out._join_as = as_name if as_name is not None else _as

        return out

    def left_join(
        self, data: "str | pd.DataFrame | NudbData", as_name: str = ""
    ) -> "NudbData":
        """Specify (inner part) of the LEFT JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.
        """
        return self.join(data, how="left", as_name=as_name)

    def right_join(
        self, data: "str | pd.DataFrame | NudbData", as_name: str = ""
    ) -> "NudbData":
        """Specify (inner part) of the RIGHT JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.
        """
        return self.join(data, how="right", as_name=as_name)

    def inner_join(
        self, data: "str | pd.DataFrame | NudbData", as_name: str = ""
    ) -> "NudbData":
        """Specify (inner part) of the INNER JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.
        """
        return self.join(data, how="inner", as_name=as_name)

    def full_join(
        self, data: "str | pd.DataFrame | NudbData", as_name: str = ""
    ) -> "NudbData":
        """Specify (inner part) of the FULL JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.
        """
        return self.join(data, how="full", as_name=as_name)

    def cross_join(
        self, data: "str | pd.DataFrame | NudbData", as_name: str = ""
    ) -> "NudbData":
        """Specify (inner part) of the CROSS JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.
        """
        return self.join(data, how="cross", as_name=as_name)

    def self_join(
        self, data: "str | pd.DataFrame | NudbData", as_name: str = ""
    ) -> "NudbData":
        """Specify (inner part) of the SELF JOIN statement in SQL query.

        Args:
            data: Input data. Either an NudbData object, a string indicating the name
                  of the NudbData-datasett (e.g., "avslutta"), or a pandas DataFrame.
            as_name: Should the dataset be given an alias in the join (e.g., "T2")?

        Returns:
            NudbData: An NudbData object.
        """
        return self.join(data, how="self", as_name=as_name)

    def using(self, *exprs: str) -> "NudbData":
        """Specify (inner part) of the USING statement in SQL query."""
        expr = ", ".join(exprs)

        out = copy.copy(self)
        out._using = expr

        return out

    def on(self, *exprs: str) -> "NudbData":
        """Specify (inner part) of the ON statement in SQL query."""
        expr = " AND ".join(exprs)

        out = copy.copy(self)
        out._on = expr

        return out

    def as_name(self, expr: str) -> "NudbData":
        """Specify (inner part) of the AS statement in SQL query."""
        out = copy.copy(self)
        out._as = expr

        return out

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

    @property
    def input_paths(self) -> list[Path] | None:
        """Get input paths used to create data. Returned in the order they were read."""
        if self.alias in nudb_database._dataset_paths.keys():
            return nudb_database._dataset_paths[self.alias]
        else:
            return None


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
