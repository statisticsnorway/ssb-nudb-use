from __future__ import annotations

import duckdb
import pandas as pd
from nudb_config import settings

from nudb_use.paths.latest import latest_shared_paths


def get_source_data(
    variable_name: str,
    data_to_merge: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Load and prepare source data for deriving a variable.

    This function reads one or more parquet datasets defined in config, selects only
    the columns needed for derivation, unions all datasets, and (optionally) filters
    rows down to only those whose join-key values overlap with `data_to_merge`.

    Filtering is performed inside DuckDB by registering a temporary in-memory
    key table derived from `data_to_merge[derived_join_keys]` and performing an
    INNER JOIN on the derived join keys.

    Args:
        variable_name: Name of the variable being derived (used for config lookup).
        data_to_merge: If provided, limits source rows to those matching the join-key
            combinations present in this dataframe.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the unioned (and possibly filtered) source data.

    Raises:
        ValueError: If required config fields are missing or invalid.
        KeyError: If `data_to_merge` is missing required join key columns.
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
    cols_to_read = list(set(dict.fromkeys([*derived_join_keys, *derived_from])))

    dataset_paths = [
        str(latest_shared_paths(ds_name)) for ds_name in derived_uses_datasets
    ]

    # Build a UNION ALL over all datasets, selecting only needed columns.
    select_cols = ", ".join(cols_to_read)
    union_sql = "\nUNION ALL\n".join(
        f"SELECT {select_cols} FROM read_parquet('{path}')" for path in dataset_paths
    )
    con_factory = duckdb.connect
    with con_factory() as con:
        if data_to_merge is None:
            return con.execute(union_sql).df()

        # Validate presence of join keys in data_to_merge
        missing = [k for k in derived_join_keys if k not in data_to_merge.columns]
        if missing:
            raise KeyError(
                f"{variable_name}: data_to_merge is missing join keys {missing}. "
                f"Expected columns: {list(derived_join_keys)}"
            )

        # Build a distinct key table from the incoming data, dropping rows where any join key is NA.
        key_df = (
            data_to_merge.loc[:, list(derived_join_keys)]
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
        SELECT u.*
        FROM ({union_sql}) AS u
        INNER JOIN key_filter AS k
        USING ({using_keys})
        """
        result: pd.DataFrame = con.execute(filtered_sql).df()
        return result


def join_variable_data(
    variable_name: str,
    df_to_join: pd.DataFrame,
    data_to_merge: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join a derived variable dataframe onto an input dataframe using config keys.

    Args:
        variable_name: Name of the variable being derived (used for config lookup).
        df_to_join: Derived data containing at least the join keys and derived columns.
        data_to_merge: Base dataframe to enrich.

    Returns:
        pd.DataFrame: `data_to_merge` with `df_to_join` merged in using a left join on the config keys.

    Raises:
        ValueError: If derived join keys are missing in config.
        KeyError: If either dataframe is missing required join key columns.
    """
    cfg = settings.variables[variable_name]
    derived_join_keys = cfg.derived_join_keys
    if not derived_join_keys:
        raise ValueError(
            f"{variable_name}: settings.variables[{variable_name}].derived_join_keys must be defined"
        )

    missing_left = [k for k in derived_join_keys if k not in data_to_merge.columns]
    missing_right = [k for k in derived_join_keys if k not in df_to_join.columns]
    if missing_left or missing_right:
        raise KeyError(
            f"{variable_name}: missing join keys. "
            f"data_to_merge missing: {missing_left}; df_to_join missing: {missing_right}"
        )

    return data_to_merge.merge(
        df_to_join,
        on=list(derived_join_keys),
        how="left",
        validate="m:1",
    )


def enforce_datetime_s(series: pd.Series) -> pd.Series:
    """Enforce the datetime dtype to datetime64[s]."""
    return pd.to_datetime(series, errors="coerce", unit="s").astype("datetime64[s]")
