from __future__ import annotations

import duckdb
import pandas as pd
from nudb_config import settings

from nudb_use.nudb_logger import function_logger_context
from nudb_use.nudb_logger import logger
from nudb_use.paths.latest import latest_shared_paths
from nudb_use.variables.checks import pyarrow_columns_from_metadata


@function_logger_context(level="debug")
def _get_baselevel_derived_from_variables_single(
    variable: str, visited: set[str] | None = None
) -> set[str]:
    visited = visited or set()

    logger.debug(f"Visiting {variable}")
    if variable not in settings.variables.keys():
        logger.debug(f"{variable} is irrelevant!")
        return set()

    metadata = settings.variables[variable]

    logger.debug(f"derived_from = {metadata.derived_from}")
    if not metadata.derived_from:
        logger.debug(f"{variable} is a baselevel variable!")
        return {
            variable,
        }  # variable is a baselevel variable (i.e., underivable)
    elif variable in visited:
        logger.debug(f"{variable} has already been checked out!")
        return set()

    visit = set(metadata.derived_from) - visited
    baselevel = set()

    logger.debug(f"{variable} is NOT a baselevel variable! Checking out:\n    {visit}")

    for derived_from in visit:
        baselevel |= _get_baselevel_derived_from_variables_single(
            derived_from, visited=visited
        )
        visited.add(derived_from)

    return baselevel


def _get_baselevel_derived_from_variables(variables: list[str]) -> list[str]:
    visited: set[str] = set()
    baselevel: set[str] = set()

    for variable in variables:
        baselevel |= _get_baselevel_derived_from_variables_single(
            variable, visited=visited
        )  # visited is mutated in _get_baselevel_derived_from_single

    return list(baselevel)


def _get_column_aliases(columns: list[str], available: list[str]) -> str:
    available_set = set(available)

    return ", ".join(
        [
            f"NULL AS {column}" if column not in available_set else column
            for column in columns
        ]
    )


def get_source_data(
    variable_name: str,
    df_left: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Load and prepare source data for deriving a variable.

    This function reads one or more parquet datasets defined in config, selects only
    the columns needed for derivation, unions all datasets, and (optionally) filters
    rows down to only those whose join-key values overlap with `df_left`.

    Filtering is performed inside DuckDB by registering a temporary in-memory
    key table derived from `df_left[derived_join_keys]` and performing an
    INNER JOIN on the derived join keys.

    Args:
        variable_name: Name of the variable being derived (used for config lookup).
        df_left: If provided, limits source rows to those matching the join-key
            combinations present in this dataframe.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the unioned (and possibly filtered) source data.

    Raises:
        ValueError: If required config fields are missing or invalid.
        KeyError: If `df_left` is missing required join key columns.
    """
    cfg = settings.variables[variable_name]

    derived_from = cfg.derived_from
    derived_uses_datasets = cfg.derived_uses_datasets
    derived_join_keys = cfg.derived_join_keys

    if not derived_from:
        raise ValueError(
            f"{variable_name}: settings.variables[{variable_name}].derived_from must be defined"
        )
    if not derived_uses_datasets:
        raise ValueError(
            f"{variable_name}: settings.variables[{variable_name}].derived_uses_datasets must be defined"
        )
    if not derived_join_keys:
        raise ValueError(
            f"{variable_name}: settings.variables[{variable_name}].derived_join_keys must be defined"
        )

    # Ensure we always read join keys + all columns required for derivation.
    baselevel_derived_from = _get_baselevel_derived_from_variables(derived_from)
    cols_to_read = list(
        set(dict.fromkeys([*derived_join_keys, *baselevel_derived_from]))
    )

    dataset_paths = [
        str(latest_shared_paths(ds_name)) for ds_name in derived_uses_datasets
    ]

    available_cols = [pyarrow_columns_from_metadata(path) for path in dataset_paths]

    col_aliases = [
        _get_column_aliases(cols_to_read, available) for available in available_cols
    ]

    logger.info(f"Paths used to form `source_data`:\n{dataset_paths}")

    # Build a UNION ALL over all datasets, selecting only needed columns.
    union_sql = "\nUNION ALL\n".join(
        [
            f"SELECT {cols} FROM read_parquet('{path}')"
            for cols, path in zip(col_aliases, dataset_paths, strict=True)
        ]
    )

    logger.notice(f"SQL query:\n{union_sql}")  # type: ignore[attr-defined]

    con_factory = duckdb.connect
    with con_factory() as con:
        if df_left is None:
            return con.execute(union_sql).df()

        # Validate presence of join keys in df_left
        missing = [k for k in derived_join_keys if k not in df_left.columns]
        if missing:
            raise KeyError(
                f"{variable_name}: df_left is missing join keys {missing}. "
                f"Expected columns: {list(derived_join_keys)}"
            )

        # Build a distinct key table from the incoming data, dropping rows where any join key is NA.
        key_df = (
            df_left.loc[:, list(derived_join_keys)]
            .dropna(subset=list(derived_join_keys), how="any")
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # If there are no keys to filter on, return empty source (nothing overlaps).
        if key_df.empty:
            return pd.DataFrame(columns=cols_to_read)

        con.register("key_filter", key_df)

        # Filter source rows to overlap join-key combinations using an INNER JOIN.
        using_keys = ", ".join(derived_join_keys)
        filtered_sql = f"""
        SELECT DISTINCT u.*
        FROM ({union_sql}) AS u
        INNER JOIN key_filter AS k
        USING ({using_keys})
        """
        result: pd.DataFrame = con.execute(filtered_sql).df()

        return result


def join_variable_data(
    variable_name: str,
    df_right: pd.DataFrame,
    df_left: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join a derived variable dataframe onto an input dataframe using config keys.

    Args:
        variable_name: Name of the variable being derived (used for config lookup).
        df_right: Derived data containing at least the join keys and derived columns.
        df_left: Base dataframe to enrich.

    Returns:
        pd.DataFrame: `df_left` with `df_right` merged in using a left join on the config keys.

    Raises:
        ValueError: If derived join keys are missing in config.
        KeyError: If either dataframe is missing required join key columns.
    """
    cfg = settings.variables[variable_name]
    derived_join_keys = list(cfg.derived_join_keys or [])

    if not derived_join_keys:
        raise ValueError(
            f"{variable_name}: settings.variables[{variable_name}].derived_join_keys must be defined"
        )

    missing_left = [k for k in derived_join_keys if k not in df_left.columns]
    missing_right = [k for k in derived_join_keys if k not in df_right.columns]
    if missing_left or missing_right:
        raise KeyError(
            f"{variable_name}: missing join keys. "
            f"df_left missing: {missing_left}; df_right missing: {missing_right}"
        )

    if variable_name not in df_right.columns:
        raise KeyError(f"Missing '{variable_name}' in `df_right`!")

    # Remove (possibly) conflicting variables which aren't keys or the variable
    # which we wan't to add to df_left
    df_right = df_right[[*derived_join_keys, variable_name]]

    # For now we just remove the values in variable_name if it exists in
    # df_left. In the future we should consider doing a fillna. For now
    # we just log a warning
    if variable_name in df_left:
        logger.warning(
            f"{variable_name} already exists in the dataset! Replacing existing values!"
        )
        df_left = df_left.drop(columns=variable_name)

    # Check and handle duplicated merge keys
    dupes = df_right.duplicated(subset=derived_join_keys)
    if dupes.any():
        dupes_all = df_right.duplicated(subset=derived_join_keys, keep=False)
        df_dup = df_right[dupes_all].sort_values(by=derived_join_keys)

        logger.warning(
            f"Found duplicated merge keys! Showing first 50 rows:\n{df_dup.head(50)}"
        )
        logger.warning("Keeping first valid row for duplicates...")

        k = dupes.sum()
        n = df_right.shape[0]
        pct = 100 * k / n

        logger.warning(f"Dropping {k}/{n} rows ({pct:.2f}%)")

        df_right = df_right[~dupes]

    return df_left.merge(
        df_right,
        on=derived_join_keys,
        how="left",
        validate="m:1",
    )


def enforce_datetime_s(series: pd.Series) -> pd.Series:
    """Enforce the datetime dtype to datetime64[s]."""
    return pd.to_datetime(series, errors="coerce", unit="s").astype("datetime64[s]")
