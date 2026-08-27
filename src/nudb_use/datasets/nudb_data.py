import copy
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

JOIN_TYPES = {"left", "right", "inner", "cross", "full", "outer", "self"}


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
            if name in nudb_database._datasets:
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
            if self.name in ("igang", "avslutta"):
                try:
                    report = self.check_shared_files()
                    if report is not None:
                        print(report)
                except Exception as e:
                    logger.debug(f"Failed to automatically check shared files: {e}")
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

    def check_shared_files(self, year: int | None = None) -> Any:
        """Check status of shared input files that constitute this dataset."""
        import re
        from datetime import datetime
        from pathlib import Path

        from fagfunksjoner.paths.shared_files import FileState
        from fagfunksjoner.paths.shared_files import FileStatus
        from fagfunksjoner.paths.shared_files import FileStatusReport
        from fagfunksjoner.paths.shared_files import SharedFileSpec

        # 1. Check if column exists
        if "nudb_dataset_id" not in self.get_available_cols():
            return None

        # 2. Query distinct nudb_dataset_id values
        query = f"SELECT DISTINCT nudb_dataset_id FROM ({self._get_query(check_validity=True)})"
        try:
            rows = self.execute(query).fetchall()
            unique_ids = [row[0] for row in rows if row[0] is not None]
        except Exception as e:
            logger.debug(f"Failed to fetch nudb_dataset_id column: {e}")
            return None

        # 3. Extract absolute parquet file paths using regex
        path_regex = re.compile(r"/[^()>\+,\s\'\"]+\.parquet")
        loaded_paths = set()
        for uid in unique_ids:
            for match in path_regex.findall(uid):
                loaded_paths.add(Path(match))

        # 4. Helper function to map filenames to the 4 main source categories
        def get_source_category(filename: str) -> str | None:
            filename_lower = filename.lower()
            if "videregaaende" in filename_lower:
                return "videregående"
            elif "resultat" in filename_lower or "grunnskole" in filename_lower:
                return "grunnskole"
            elif "fagskole" in filename_lower:
                return "fagskole"
            elif "hoeyereutdanning" in filename_lower or "uh" in filename_lower:
                return "høyere utdanning"
            return None

        # 5. Parse each found path
        parsed_files = []
        for path in sorted(loaded_paths):
            filename = path.name
            source_cat = get_source_category(filename)
            if source_cat is None:
                continue

            years = [int(y) for y in re.findall(r"_p(\d{4})", filename)]
            file_year = years[-1] if years else None

            version_match = re.search(r"_v(\d+)", filename)
            version = int(version_match.group(1)) if version_match else None

            parsed_files.append({
                "path": path,
                "filename": filename,
                "year": file_year,
                "version": version,
                "source": source_cat,
            })

        if not parsed_files:
            return None

        # 6. Determine target year as maximum year found among the matched files
        target_year = year if year is not None else max((f["year"] for f in parsed_files if f["year"] is not None), default=datetime.now().year)

        expected_sources = ["videregående", "grunnskole", "høyere utdanning", "fagskole"]
        statuses = []

        # 7. Check each source for target_year and target_year - 1
        for src in expected_sources:
            file_for_target = next((f for f in parsed_files if f["source"] == src and f["year"] == target_year), None)

            spec_path = Path("/buckets/shared")
            modified_at = None

            if file_for_target:
                spec_path = file_for_target["path"].parent
                try:
                    if file_for_target["path"].exists():
                        modified_at = datetime.fromtimestamp(file_for_target["path"].stat().st_mtime)
                except Exception:
                    pass

                spec = SharedFileSpec(
                    name=src,
                    path=spec_path,
                    description=f"Kildedata for {src}",
                )

                v = file_for_target["version"]
                state = FileState.DRAFT if v == 0 else FileState.READY
                statuses.append(FileStatus(
                    spec=spec,
                    year=target_year,
                    state=state,
                    version=v,
                    file_path=file_for_target["path"],
                    modified_at=modified_at,
                ))
            else:
                # Target year file is missing, check the previous year
                prev_year = target_year - 1
                file_for_prev = next((f for f in parsed_files if f["source"] == src and f["year"] == prev_year), None)

                if file_for_prev:
                    spec_path = file_for_prev["path"].parent
                    v_str = f"v{file_for_prev['version']}" if file_for_prev["version"] is not None else "ukjent versjon"
                    warnings = (f"Fant ikke kildedata for '{src}' for {target_year}, men fant forrige årgang: {prev_year} ({file_for_prev['filename']} {v_str})",)
                else:
                    warnings = (f"Fant ikke kildedata for '{src}' i verken {target_year} eller {prev_year}",)

                spec = SharedFileSpec(
                    name=src,
                    path=spec_path,
                    description=f"Kildedata for {src}",
                )

                statuses.append(FileStatus(
                    spec=spec,
                    year=target_year,
                    state=FileState.MISSING,
                    warnings=warnings,
                ))

        # 8. Check for year mismatches among loaded sources
        loaded_sources_info = [f for f in parsed_files if f["source"] in expected_sources]
        loaded_years = {f["year"] for f in loaded_sources_info if f["year"] is not None}

        mismatch_warning = None
        if len(loaded_years) > 1:
            mismatch_warning = f"OBS: Kildefilene i denne {self.name}-årgangen kommer fra forskjellige årganger: {', '.join(map(str, sorted(loaded_years)))}!"

        if mismatch_warning and statuses:
            first_status = statuses[0]
            new_warnings = (mismatch_warning, *first_status.warnings)
            statuses[0] = FileStatus(
                spec=first_status.spec,
                year=first_status.year,
                state=first_status.state,
                version=first_status.version,
                file_path=first_status.file_path,
                modified_at=first_status.modified_at,
                warnings=new_warnings,
            )

        return FileStatusReport(year=target_year, files=statuses)


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
