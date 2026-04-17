import importlib
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from typing import cast

import pandas as pd

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.metadata.external_apis.brreg_api import orgnr_is_underenhet
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


# Typing helper for mypy
def _progress(iterable: Iterable[str]) -> Iterable[str]:
    tqdm_module = importlib.import_module("tqdm")
    tqdm = cast(Callable[[Iterable[str]], Iterator[str]], tqdm_module.tqdm)
    return tqdm(iterable)


def cleanup_orgnr_bedrift_foretak(
    df: pd.DataFrame,
    time_col_name: str = "utd_skoleaar_start",
    extra_orgnr_cols_split_prio: list[str] | None = None,
) -> pd.DataFrame:
    """Cleanup into the columns orgnrbed and orgnr_foretak using datasets from BOF.

    Args:
        df: The data we should fix.
        time_col_name: The name of the column that has time we will use to date the BOF-join-connections.
        extra_orgnr_cols_split_prio: If there are extra columns containing orgnr in your dataset, not in the default list:
            orgnr, utd_orgnr, orgnrbed, bof_orgnrbed, orgnr_foretak

    Returns:
        pd.DataFrame: The modified dataframe.

    Raises:
        TypeError: If we are struggeling to determine the time-columns formatting or dtype.
    """
    with LoggerStack("Cleaning up orgnr columns"):
        # Type check on time_col so we can exit early if its not something we recognize
        # Might be a year column, encoded as a string
        if pd.api.types.is_string_dtype(df[time_col_name]):
            time_col = pd.to_datetime(df[time_col_name], format="%Y")
        elif not pd.api.types.is_datetime64_any_dtype(df[time_col_name]):
            raise TypeError(
                f"Unrecognized datatype on column {time_col_name}, should be a year-string or a datetime64."
            )
        else:
            time_col = df[time_col_name]

        cols_split_priority_order: list[str] = [
            "orgnr",
            "utd_orgnr",
            "orgnrbed",
            "bof_orgnrbed",
            "orgnr_foretak",
        ]
        if extra_orgnr_cols_split_prio is not None:
            cols_split_priority_order += extra_orgnr_cols_split_prio

        overlap: list[str] = [c for c in cols_split_priority_order if c in df.columns]

        # Early exits to save processing
        if not overlap:
            logger.info("Found no orgnr columns to clean up. Aborting")
            return df
        
        if df[overlap].isna().all().all():
            logger.info(f"Orgnr cols {overlap} - contain only empty values. Nothing to clean up. Aborting.")
        
        # init empty series to contain results
        orgnrbed_combine: pd.Series = pd.Series(pd.NA, index=df.index)
        orgnr_foretak_combine: pd.Series = pd.Series(pd.NA, index=df.index)

        # Split up existing columns
        found_old_cols: list[str] = []
        for col in overlap:
            logger.info(
                f"Found {col} in dataframe, and splitting it and filling it into new orgnrbed and orgnr_foretak cols (first is prio)."
            )
            found_old_cols.append(col)
            with LoggerStack(f"Splitting {col} into orgnrbed, orgnr_foretak"):
                orgnr_foretak_temp, orgnrbed_temp = _split_orgnr_col(df[col])
                logger.info(
                    f"{orgnr_foretak_temp.notna().sum()} values placed in orgnr_foretak"
                )
                logger.info(
                    f"{orgnrbed_temp.notna().sum()} values placed in orgnrbed"
                )
                orgnrbed_combine = orgnrbed_combine.fillna(orgnrbed_temp)
                orgnr_foretak_combine = orgnr_foretak_combine.fillna(
                    orgnr_foretak_temp
                )

        # We need to do this first, because we are passing it into the join
        orgnrbed_combine = _empty_orgnr_sentinel_values(orgnrbed_combine)

        # Join new orgnr_foretak_bof from BOF from orgnrbed - also back in time?
        # Ifølge AM stoler vi mer på det "som ligger der fra før" - enn det vi kobler på
        orgnr_foretak_combine = _empty_orgnr_sentinel_values(orgnr_foretak_combine)
        orgnr_foretak_combine_joined = orgnr_foretak_combine.fillna(
            _find_orgnr_foretak_bof(orgnrbed_combine, time_col)
        )
        orgnr_foretak_combine_joined = _empty_orgnr_sentinel_values(
            orgnr_foretak_combine_joined
        )

        # Create orgnrbed_bof from cleaned orgnr_foretak where foretak is "enkeltbedriftsforetak"
        # We need to do this after fixing orgnr_foretak because we are joining back
        orgnrbed_combine_joined = orgnrbed_combine.fillna(
            _find_orgnrbed_enkelbedforetak_bof(orgnr_foretak_combine_joined, time_col)
        )
        orgnrbed_combine_joined = _empty_orgnr_sentinel_values(orgnrbed_combine_joined)

        # Report the changes we have made
        # Filling degrees (non-NA)
        for col in found_old_cols:
            logger.info(
                f"Old filling degree for {col}: {_percent_filled_orgnr(df[col])}% (remember that orgnr might have contained orgnrbed)"
            )
        logger.info(
            f"New filling degree for orgnrbed (before join): {_percent_filled_orgnr(orgnrbed_combine)}%"
        )
        logger.info(
            f"New filling degree for orgnrbed (after join): {_percent_filled_orgnr(orgnrbed_combine_joined)}%"
        )
        logger.info(
            f"New filling degree for orgnr_foretak (before join): {_percent_filled_orgnr(orgnr_foretak_combine)}%"
        )
        logger.info(
            f"New filling degree for orgnr_foretak (after join): {_percent_filled_orgnr(orgnr_foretak_combine_joined)}%"
        )

        # Remove old columns
        df = df.drop(columns=cols_split_priority_order, errors="ignore")

        # Insert new columns
        df["orgnrbed"] = orgnrbed_combine_joined
        df["orgnr_foretak"] = orgnr_foretak_combine_joined

        return df


def _percent_filled_orgnr(s: pd.Series) -> float:
    return (
        0.0
        if not len(s)
        else round(s.copy().replace("000000000", pd.NA).notna().sum() / len(s) * 100, 2)
    )


def _empty_orgnr_sentinel_values(s: pd.Series) -> pd.Series:
    s.loc[s == "000000000"] = pd.NA
    return s


def _split_orgnr_col(orgnr_col: pd.Series) -> tuple[pd.Series, pd.Series]:
    bof_orgnrbed = NudbData("_bof_unique_orgnrbed").df()["orgnrbed"]
    bof_orgnr_foretak = NudbData("_bof_unique_orgnr_foretak").df()["orgnr"]
    is_bed = orgnr_col.isin(bof_orgnrbed)
    is_foretak = orgnr_col.isin(bof_orgnr_foretak)
    missing_from_bof = orgnr_col[~is_bed & ~is_foretak].dropna().unique()
    missing_orgnr_er_orgnrbed: dict[str, bool] = {}
    if len(missing_from_bof):
        logger.info(
            f"Looking for {len(missing_from_bof)} orgnr in brregs API because the orgnr(s) are missing from the BOF-sittuttak."
        )
        for nr in _progress(missing_from_bof):
            missing_orgnr_er_orgnrbed[nr] = orgnr_is_underenhet(nr)
    orgnr_is_orgnrbed = (
        missing_orgnr_er_orgnrbed
        | dict.fromkeys(orgnr_col[is_bed].dropna().unique(), True)
        | dict.fromkeys(orgnr_col[is_foretak].dropna().unique(), False)
    )
    mask_orgnrbed = orgnr_col.map(orgnr_is_orgnrbed).astype("bool[pyarrow]")
    orgnrbed_out = pd.Series(pd.NA, index=orgnr_col.index, dtype="string[pyarrow]")
    orgnrbed_out[mask_orgnrbed] = orgnr_col
    orgnr_foretak_out = pd.Series(pd.NA, index=orgnr_col.index, dtype="string[pyarrow]")
    orgnr_foretak_out.loc[~mask_orgnrbed] = orgnr_col
    return _empty_orgnr_sentinel_values(
        orgnr_foretak_out
    ), _empty_orgnr_sentinel_values(orgnrbed_out)


def _find_orgnr_foretak_bof(
    orgnrbed_col: pd.Series,
    time_col: pd.Series,
) -> pd.Series:
    """Map orgnrbed to orgnr using dated BOF connections in DuckDB.

    The function first finds the latest known orgnr connection on or before the
    input date for each orgnrbed. If no such connection exists, it falls back
    to the earliest known connection after that date.

    All matching is performed in DuckDB before collecting the final result to
    pandas.

    Args:
        orgnrbed_col: Series containing orgnrbed values.
        time_col: Series containing data's time.

    Returns:
       pd.Series: A Series of orgnr values aligned to the input index.
    """
    with LoggerStack("Joining orgnr_foretak on orgnrbed"):
        logger.info("Making pandas dataframe from sent columns.")
        input_df = pd.DataFrame(
            {
                "orgnrbed": orgnrbed_col.astype("string"),
                "_row_id": range(len(orgnrbed_col)),
            },
            index=orgnrbed_col.index,
        )

        if pd.api.types.is_string_dtype(time_col):
            input_df["join_date"] = pd.to_datetime(
                time_col,
                format="%Y",
                errors="coerce",
            )
        else:
            input_df["join_date"] = pd.to_datetime(
                time_col,
                errors="coerce",
            )

        original_index = orgnrbed_col.index

        logger.info("Initialize the dataset for the bof connections")
        bof_rel = NudbData("_bof_dated_orgnr_connections")

        logger.info("Attach created pandas dataframe to the nudb_database connection.")
        con = nudb_database.get_connection()
        con.register("input_df", input_df)

        logger.info(
            "Executing join on dates for data and orgnrbed -> orgnr_foretak connections"
        )
        result_df = con.sql(f"""
            WITH input_clean AS (
                SELECT
                    _row_id,
                    TRIM(orgnrbed) AS orgnrbed,
                    CAST(join_date AS DATE) AS join_date
                FROM input_df
                WHERE orgnrbed IS NOT NULL
                  AND TRIM(orgnrbed) <> ''
                  AND orgnrbed <> '000000000'
                  AND join_date IS NOT NULL
            ),

            input_keys AS (
                SELECT DISTINCT
                    orgnrbed,
                    join_date
                FROM input_clean
            ),

            relevant_orgnrbed AS (
                SELECT DISTINCT
                    orgnrbed
                FROM input_keys
            ),

            conn_base AS (
                SELECT
                    CAST(conn.orgnrbed AS VARCHAR) AS orgnrbed,
                    CAST(conn.orgnr AS VARCHAR) AS orgnr,
                    CAST(conn.bof_period_date AS DATE) AS bof_period_date
                FROM {bof_rel.alias} AS conn
                JOIN relevant_orgnrbed AS r
                    ON CAST(conn.orgnrbed AS VARCHAR) = r.orgnrbed
            ),

            conn_changes AS (
                SELECT
                    orgnrbed,
                    orgnr,
                    bof_period_date
                FROM (
                    SELECT
                        orgnrbed,
                        orgnr,
                        bof_period_date,
                        LAG(orgnr) OVER (
                            PARTITION BY orgnrbed
                            ORDER BY bof_period_date
                        ) AS prev_orgnr
                    FROM conn_base
                )
                WHERE prev_orgnr IS NULL
                   OR orgnr IS DISTINCT FROM prev_orgnr
            ),

            resolved_keys AS (
                SELECT
                    k.orgnrbed,
                    k.join_date,
                    COALESCE(
                        (
                            SELECT c.orgnr
                            FROM conn_changes AS c
                            WHERE c.orgnrbed = k.orgnrbed
                              AND c.bof_period_date <= k.join_date
                            ORDER BY c.bof_period_date DESC
                            LIMIT 1
                        ),
                        (
                            SELECT c.orgnr
                            FROM conn_changes AS c
                            WHERE c.orgnrbed = k.orgnrbed
                              AND c.bof_period_date > k.join_date
                            ORDER BY c.bof_period_date ASC
                            LIMIT 1
                        )
                    ) AS orgnr
                FROM input_keys AS k
            ),

            resolved AS (
                SELECT
                    inp._row_id,
                    rk.orgnr
                FROM input_clean AS inp
                LEFT JOIN resolved_keys AS rk
                    ON inp.orgnrbed = rk.orgnrbed
                   AND inp.join_date = rk.join_date
            )

            SELECT
                raw._row_id,
                resolved.orgnr
            FROM input_df AS raw
            LEFT JOIN resolved
                ON raw._row_id = resolved._row_id
            ORDER BY raw._row_id
            """).df()

        result = result_df["orgnr"].astype("string").set_axis(original_index)
        return result


def _find_orgnrbed_enkelbedforetak_bof(
    orgnr_foretak_col: pd.Series,
    time_col: pd.Series,
) -> pd.Series:
    """Map orgnr_foretak to orgnrbed using dated BOF connections in DuckDB.

    For each input row, the function first finds the applicable BOF period using
    these rules:

    1. Prefer the latest known period on or before the input date.
    2. If none exists, fall back to the earliest known period after the input
       date.

    The function only returns an orgnrbed when the chosen period contains
    exactly one distinct orgnrbed for the orgnr_foretak. If the chosen period
    contains multiple orgnrbed values, the result is null.

    Args:
        orgnr_foretak_col: Series containing orgnr_foretak / orgnr values.
        time_col: Series containing data's time.

    Returns:
        pd.Series: A Series of orgnrbed values aligned to the input index.
    """
    with LoggerStack("Joining orgnrbed on orgnr_foretak when first period is 1:1"):
        logger.info("Making pandas dataframe from sent columns.")
        input_df = pd.DataFrame(
            {
                "orgnr": orgnr_foretak_col.astype("string"),
                "_row_id": range(len(orgnr_foretak_col)),
            },
            index=orgnr_foretak_col.index,
        )

        if pd.api.types.is_string_dtype(time_col):
            input_df["join_date"] = pd.to_datetime(
                time_col,
                format="%Y",
                errors="coerce",
            )
        else:
            input_df["join_date"] = pd.to_datetime(
                time_col,
                errors="coerce",
            )

        original_index = orgnr_foretak_col.index

        logger.info("Initialize the dataset for the bof connections")
        bof_rel = NudbData("_bof_dated_orgnr_connections")

        logger.info("Attach created pandas dataframe to the nudb_database connection.")
        con = nudb_database.get_connection()
        con.register("input_df", input_df)

        logger.info("Executing join from orgnr_foretak to orgnrbed")
        result_df = con.sql(f"""
            WITH input_clean AS (
                SELECT
                    _row_id,
                    TRIM(orgnr) AS orgnr,
                    CAST(join_date AS DATE) AS join_date
                FROM input_df
                WHERE orgnr IS NOT NULL
                  AND TRIM(orgnr) <> ''
                  AND orgnr <> '000000000'
                  AND join_date IS NOT NULL
            ),

            input_keys AS (
                SELECT DISTINCT
                    orgnr,
                    join_date
                FROM input_clean
            ),

            relevant_orgnr AS (
                SELECT DISTINCT
                    orgnr
                FROM input_keys
            ),

            -- One row per orgnr + period, summarizing whether that period is 1:1 or 1:m.
            conn_periods AS (
                SELECT
                    CAST(conn.orgnr AS VARCHAR) AS orgnr,
                    CAST(conn.bof_period_date AS DATE) AS bof_period_date,
                    COUNT(DISTINCT CAST(conn.orgnrbed AS VARCHAR)) AS orgnrbed_count,
                    MIN(CAST(conn.orgnrbed AS VARCHAR)) AS single_orgnrbed
                FROM {bof_rel.alias} AS conn
                JOIN relevant_orgnr AS r
                    ON CAST(conn.orgnr AS VARCHAR) = r.orgnr
                GROUP BY
                    CAST(conn.orgnr AS VARCHAR),
                    CAST(conn.bof_period_date AS DATE)
            ),

            chosen_periods AS (
                SELECT
                    k.orgnr,
                    k.join_date,
                    COALESCE(
                        (
                            SELECT p.bof_period_date
                            FROM conn_periods AS p
                            WHERE p.orgnr = k.orgnr
                              AND p.bof_period_date <= k.join_date
                            ORDER BY p.bof_period_date DESC
                            LIMIT 1
                        ),
                        (
                            SELECT p.bof_period_date
                            FROM conn_periods AS p
                            WHERE p.orgnr = k.orgnr
                              AND p.bof_period_date > k.join_date
                            ORDER BY p.bof_period_date ASC
                            LIMIT 1
                        )
                    ) AS chosen_period
                FROM input_keys AS k
            ),

            resolved_keys AS (
                SELECT
                    cp.orgnr,
                    cp.join_date,
                    CASE
                        WHEN p.orgnrbed_count = 1 THEN p.single_orgnrbed
                        ELSE NULL
                    END AS orgnrbed
                FROM chosen_periods AS cp
                LEFT JOIN conn_periods AS p
                    ON p.orgnr = cp.orgnr
                   AND p.bof_period_date = cp.chosen_period
            ),

            resolved AS (
                SELECT
                    inp._row_id,
                    rk.orgnrbed
                FROM input_clean AS inp
                LEFT JOIN resolved_keys AS rk
                    ON inp.orgnr = rk.orgnr
                   AND inp.join_date = rk.join_date
            )

            SELECT
                raw._row_id,
                resolved.orgnrbed
            FROM input_df AS raw
            LEFT JOIN resolved
                ON raw._row_id = resolved._row_id
            ORDER BY raw._row_id
            """).df()

        result = result_df["orgnrbed"].astype("string").set_axis(original_index)
        return result
