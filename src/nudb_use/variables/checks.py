"""Validation utilities for ensuring variable schemas match expectations."""

from pathlib import Path
from typing import Any
from typing import cast

import klass
import pandas as pd
import pyarrow.parquet as pq
from nudb_config import settings

from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.metadata.nudb_klass.klass_utils import (
    find_earliest_latest_klass_version_date,
)
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def pyarrow_columns_from_metadata(path: str | Path) -> list[str]:
    """Read column names from a Parquet file via metadata only."""
    file_path = Path(path)
    metadata = pq.ParquetFile(file_path).metadata
    return metadata.schema.names


def identify_cols_not_in_keep_drop_in_paths(
    paths: list[Path],
    cols_keep: list[str],
    cols_drop: list[str],
    raise_error_found: bool = False,
) -> set[str]:
    """Identify columns present in data files that are missing from keep/drop lists."""
    extra_cols: set[str] = set()
    for path in paths:
        columns_in_data = pyarrow_columns_from_metadata(path)
        extra_cols |= {
            col
            for col in columns_in_data
            if col not in cols_keep and col not in cols_drop
        }
    if extra_cols and raise_error_found:
        raise KeyError(
            f"These columns are missing from keep / drop, please define them in your config or keep+drop lists: {extra_cols}"
        )

    return extra_cols


def check_column_presence(
    df: pd.DataFrame,
    dataset_name: str | None = None,
    check_for: None | list[str] = None,
    raise_errors: bool = True,
) -> list[Exception]:
    """Validate columns against config or a supplied list."""
    with LoggerStack(
        f"Checking for column presence in dataframe for dataset: {dataset_name}"
    ):
        datasets = list(settings["datasets"].keys())
        errors: list[Exception] = []

        columns_to_check = _derive_columns_to_check(
            datasets=datasets,
            dataset_name=dataset_name,
            check_for=check_for,
            errors=errors,
        )

        _log_integer_like_floats(df)

        col_in_df_but_not_defined, col_defined_but_not_in_data = (
            _find_column_mismatches(df.columns, columns_to_check)
        )

        _append_if_missing(
            errors,
            col_in_df_but_not_defined,
            "Cant find columns among defined columns, but they're in the data: {}",
        )
        _append_if_missing(
            errors,
            col_defined_but_not_in_data,
            "Cant find colums in the data, which are defined as if we want them in the dataset: {}",
        )

        if raise_errors and errors:
            raise_exception_group(errors)
        if not errors:
            logger.info(
                "All columns exist in the datasets config, or in the provided check_for list."
            )
        return errors


def _derive_columns_to_check(
    datasets: list[str],
    dataset_name: str | None,
    check_for: list[str] | None,
    errors: list[Exception],
) -> list[str]:
    if dataset_name is None and check_for is None:
        errors.append(
            ValueError(
                f"""
                `check_column_presence()` needs either `check_for` or `name`.
                `name` can be one of the following:
                    {datasets}
            """
            )
        )
        return []

    if check_for is None:
        if dataset_name not in datasets:
            errors.append(
                KeyError(
                    f"""
                    `name` must be one of the following:
                        {datasets}
                    got '{dataset_name}'
                """
                )
            )
            return []
        return cast(list[str], settings["datasets"][dataset_name]["variables"])

    return check_for


def _log_integer_like_floats(df: pd.DataFrame) -> None:
    # Sjekk om naavarende flyttall trenger vaere flyttall?
    for col in df.select_dtypes("float").columns:
        mask_whole_number = df[col].mod(1.0) == 0
        if mask_whole_number.all():
            logger.info(f"{col} ser ut til aa kunne vaere ett heltall.")


def _find_column_mismatches(
    df_columns: pd.Index, expected_columns: list[str]
) -> tuple[list[str], list[str]]:
    col_in_df_but_not_defined = [
        column for column in df_columns if column not in expected_columns
    ]
    col_defined_but_not_in_data = [
        column for column in expected_columns if column not in df_columns
    ]
    return col_in_df_but_not_defined, col_defined_but_not_in_data


def _append_if_missing(errors: list[Exception], missing: list[str], msg: str) -> None:
    if missing:
        err_msg = msg.format(missing)
        logger.warning(err_msg)
        errors.append(KeyError(err_msg))


def _get_klass_codelist(
    df: pd.DataFrame,
    col_codelist: dict[str, list[str] | dict[str, str]] | None = None,
    full_timeline: bool = False,
) -> dict[str, list[str] | dict[str, str]]:
    """Retrieve or validate KLASS codelists for DataFrame columns."""
    if _col_codelist_is_valid(col_codelist):
        assert col_codelist is not None
        return col_codelist

    return _build_codelists_from_config(df, full_timeline)


def _col_codelist_is_valid(
    col_codelist: dict[str, list[str] | dict[str, str]] | None,
) -> bool:
    return bool(
        isinstance(col_codelist, dict)
        and all(isinstance(k, str) for k in col_codelist)
        and all(isinstance(v, (list, str)) for v in col_codelist.values())
    )


def _build_codelists_from_config(
    df: pd.DataFrame, full_timeline: bool
) -> dict[str, list[str] | dict[str, str]]:
    variables = settings.variables
    col_codelist: dict[str, list[str] | dict[str, str]] = {}

    for col in df.columns:
        variable = variables.get(col)
        logger.debug(col)
        logger.debug(f"col in variables: {col in variables}")

        if variable is not None:
            logger.debug(f"variables[col]:\n{variable}")

        if variable is None:
            continue

        meta = _normalize_variable(variable)
        if meta.get("klass_codelist"):
            col_codelist |= _build_codelist_entry(col, meta, full_timeline)
        elif meta.get("klass_variant"):
            col_codelist |= {
                col: klass.KlassVariant(meta["klass_variant"]).data["code"].to_list()
            }

    return col_codelist


def _build_codelist_entry(
    col: str, meta: dict[str, Any], full_timeline: bool
) -> dict[str, list[str]]:
    klass_id = int(meta["klass_codelist"])
    earliest_version_date, latest_version_date = (
        find_earliest_latest_klass_version_date(klass_id)
    )
    if full_timeline and earliest_version_date != latest_version_date:
        codes = klass.KlassClassification(klass_id).get_codes(
            from_date=earliest_version_date, to_date=latest_version_date
        )
    else:
        codes = klass.KlassClassification(klass_id).get_codes()

    return {col: list(codes.to_dict().keys())}


def _normalize_variable(variable: Any) -> dict[str, Any]:
    if isinstance(variable, dict):
        return dict(variable)
    if hasattr(variable, "model_dump"):
        return dict(variable.model_dump())
    if hasattr(variable, "dict"):
        return dict(variable.dict())
    return dict(vars(variable))


def check_cols_against_klass_codelists(
    df: pd.DataFrame, col_codelist: dict[str, list[str] | dict[str, str]] | None = None
) -> None:
    """Validate DataFrame values against KLASS codelists."""
    col_codelist = _get_klass_codelist(df, col_codelist)
    col_codelist_earliest: dict[str, list[str] | dict[str, str]] = _get_klass_codelist(
        df, None, full_timeline=True
    )

    for col, codes in col_codelist.items():
        logger.info(col)

        earliest_codes = col_codelist_earliest[col]

        if isinstance(codes, list) and isinstance(earliest_codes, list):
            codelist = codes
            earliest_codelist: list[str] = list(earliest_codes)
        elif isinstance(codes, dict) and isinstance(earliest_codes, dict):
            codelist = list(codes.keys())
            earliest_codelist = list(earliest_codes.keys())
        else:
            raise TypeError(
                f"Codes does not seem to have correct datatype, or they dont match. codes={type(codes)}, col_codelist_earliest[col]={type(col_codelist_earliest[col])}"
            )

        outside_df = df[(~df[col].isin(codelist)) & (df[col].notna())]

        if outside_df.shape[0]:
            logger.warning(
                f"Codes in {col} outside codelist: {outside_df[col].unique()} - first 40 valid values: {list(codelist)[:40]}..."
            )
            outside_df_earliest = df[
                (~df[col].isin(earliest_codelist)) & (df[col].notna())
            ]

            if not outside_df_earliest.shape[0]:
                logger.info(
                    "BUT Codes are not outside the list if we check all the way back the first version."
                )
            else:
                logger.warning(
                    f"Some codes are even outside the codelist in {col} if we check all the way back the first version: {outside_df_earliest[col].unique()} - first 40 valid values: {list(earliest_codelist)[:40]}..."
                )
