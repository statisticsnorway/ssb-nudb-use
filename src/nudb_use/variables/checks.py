"""Validation utilities for ensuring variable schemas match expectations."""

from pathlib import Path

import klass
import pandas as pd
import pyarrow.parquet as pq
from nudb_config import settings

from nudb_use import LoggerStack
from nudb_use import logger
from nudb_use.exceptions.groups import raise_exception_group


def pyarrow_columns_from_metadata(path: str | Path) -> list[str]:
    """Read the metadata and column names from a Parquet file using PyArrow without loading the data.

    Args:
        path: The file path to the Parquet file.

    Returns:
        list: A list of column names in the Parquet file.
    """
    file_path = Path(path)  # Convert to pathlib.Path
    metadata = pq.ParquetFile(file_path).metadata  # Read only the metadata
    return metadata.schema.names  # Return the column names


def identify_cols_not_in_keep_drop_in_paths(
    paths: list[Path],
    cols_keep: list[str],
    cols_drop: list[str],
    raise_error_found: bool = False,
) -> set[str]:
    """Identifies columns in data files that are not in keep or drop lists.

    Args:
        paths: List of file paths to scan for column metadata.
        cols_keep: List of column names that should be kept in the data.
        cols_drop: List of column names that should be dropped from the data.
        raise_error_found: If True, raises a KeyError when columns are found
            that are not in either list. If False, only returns the columns.
            Defaults to False.

    Returns:
        extra_cols: Set of column names found in the data files that are not present in
        either `cols_keep` or `cols_drop`.

    Raises:
        KeyError: If `raise_error_found` is True and unconfigured columns are
            found in the data files.
    """
    extra_cols = set()
    for path in paths:
        columns_in_data = pyarrow_columns_from_metadata(path)

        extra_cols = extra_cols | set(
            [
                col
                for col in columns_in_data
                if col not in cols_keep and col not in cols_drop
            ]
        )
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
    """Validates that DataFrame columns match expected schema configuration.

    Checks that all columns in the DataFrame are defined in the schema and that
    all required columns from the schema are present in the DataFrame. Optionally
    logs information about columns that appear to be integer-typed but are stored
    as floats.

    Args:
        df: DataFrame to check.
        dataset_name: Name of dataset layout in config. Used to look up expected
            columns if `check_for` is not provided.
        check_for: List of column names to validate against. If provided, takes
            precedence over `dataset_name`. If neither is provided, raises an error.
        raise_errors: If True, raises an ExceptionGroup containing all validation
            errors. If False, errors are only returned. Defaults to True.

    Returns:
        list[Exception]: Validation errors, including ValueError/KeyError instances
        describing missing or unexpected columns.
    """
    with LoggerStack(
        f"Checking for column presence in dataframe for dataset: {dataset_name}"
    ):
        datasets = list(settings["datasets"].keys())
        errors = []

        if dataset_name is None and check_for is None:
            errors += [
                ValueError(
                    f"""
                `check_column_presence()` needs either `check_for` or `name`.
                `name` can be one of the following:
                    {datasets}
            """
                )
            ]

        elif check_for is None:
            if dataset_name not in datasets:
                errors += [
                    KeyError(
                        f"""
                    `name` must be one of the following:
                        {datasets}
                    got '{dataset_name}'
                """
                    )
                ]
            check_for = settings["datasets"][dataset_name]["variables"]

        # Sjekk om nåværende flyttall trenger være flyttall?
        for col in df.select_dtypes("float").columns:
            maske_heltall = df[col].mod(1.0) == 0
            if maske_heltall.all():
                logger.info(f"{col} ser ut til å kunne være ett heltall.")

        col_in_df_but_not_defined = [c for c in df.columns if c not in check_for]
        col_defined_but_not_in_data = [c for c in check_for if c not in df.columns]

        if col_in_df_but_not_defined:
            err_msg = f"Cant find columns among defined columns, but they're in the data: {col_in_df_but_not_defined}"
            logger.warning(err_msg)
            errors.append(KeyError(err_msg))

        if col_defined_but_not_in_data:
            err_msg = f"Cant find colums in the data, which are defined as if we want them in the dataset: {col_defined_but_not_in_data}"
            logger.warning(err_msg)
            errors.append(KeyError(err_msg))

        if raise_errors and errors:
            raise_exception_group(errors)
        if not errors:
            logger.info(
                "All columns exist in the datasets config, or in the provided check_for list."
            )
        return errors


def _find_earliest_latest_klass_version_date(
    klass_classification_id: int,
) -> tuple[str, str]:
    """Finds the earliest and latest version dates for a KLASS classification.

    Retrieves all versions of a given KLASS classification and identifies the
    earliest and latest dates when the classification was valid. Used to
    determine the full historical range of a classification's validity.

    Args:
        klass_classification_id: The numeric ID of the KLASS classification
            to query.

    Returns:
        tuple[str, str]: A `(min_date, max_date)` tuple representing the earliest
        and latest valid dates for the classification versions.
    """
    min_date: str = ""
    max_date: str = ""
    for version in klass.KlassClassification(klass_classification_id).versions:
        valid_from = version["validFrom"]
        if not min_date:
            min_date = valid_from
        else:
            min_date = sorted([min_date, valid_from])[0]
        if not max_date:
            max_date = valid_from
        else:
            max_date = sorted([max_date, valid_from])[-1]
    return min_date, max_date


def _get_klass_codelist(
    df: pd.DataFrame,
    col_codelist: dict[str, list[str] | dict[str, str]] | None = None,
    full_timeline: bool = False,
) -> dict[str, list[str] | dict[str, str]]:
    """Retrieves or validates KLASS codelists for DataFrame columns.

    Args:
        df: DataFrame to check.
        col_codelist: Dictionary mapping column names to their codelists. Values
            can be lists or dicts of valid codes. If None or invalid format,
            codelists are retrieved from configuration instead. Defaults to None.
        full_timeline: If True, retrieves all valid codes across the entire
            historical timeline for KLASS classifications that have changed.
            If False, retrieves only current codes. Defaults to False.

    Returns:
        col_codelist: Dictionary mapping column names to their codelists, where each codelist
        is either a list of code strings or a dict with codes as keys.
    """
    # If col_codelist does not match types, get data from config
    if not (
        isinstance(col_codelist, dict)
        and all(isinstance(k, str) for k in col_codelist)
        and all(isinstance(v, list | str) for v in col_codelist.values())
    ):

        variables = settings.variables

        col_codelist = {}
        for col in df.columns:
            logger.debug(col)
            logger.debug(f"col in variables: {col in variables}")

            if col in variables:
                logger.debug(f"variables[col]:\n{variables[col]}")

            if col in variables and variables[col]["klass_codelist"]:

                earliest_version_date, latest_version_date = (
                    _find_earliest_latest_klass_version_date(
                        variables[col]["klass_codelist"]
                    )
                )

                if full_timeline and earliest_version_date != latest_version_date:
                    col_codelist |= {
                        col: list(
                            klass.KlassClassification(variables[col]["klass_codelist"])
                            .get_codes(
                                from_date=earliest_version_date,
                                to_date=latest_version_date,
                            )
                            .to_dict()
                            .keys()
                        )
                    }
                else:
                    col_codelist |= {
                        col: list(
                            klass.KlassClassification(variables[col]["klass_codelist"])
                            .get_codes()
                            .to_dict()
                            .keys()
                        )
                    }

            elif col in variables and variables[col]["klass_variant"]:
                col_codelist |= {
                    col: klass.KlassVariant(variables[col]["klass_variant"])
                    .data["code"]
                    .to_list()
                }

    return col_codelist


def check_cols_against_klass_codelists(
    df: pd.DataFrame, col_codelist: dict[str, list[str] | dict[str, str]] | None = None
) -> None:
    """Validates DataFrame column values against defined KLASS codelists.

    Args:
        df: DataFrame to check.
        col_codelist: Dictionary mapping column names to their valid codes.
            If None, codelists are retrieved from KLASS
            configuration. Defaults to None.

    Raises:
        TypeError: If a codelist value is neither a list nor a dict.
    """
    col_codelist = _get_klass_codelist(df, col_codelist)
    col_codelist_earliest = _get_klass_codelist(df, None, full_timeline=True)

    for col, codes in col_codelist.items():
        logger.info(col)

        if isinstance(codes, list):
            codelist = codes
            earliest_codelist = col_codelist_earliest[col]
        elif isinstance(codes, dict):
            codelist = codes.keys()
            earliest_codelist = col_codelist_earliest[col].keys()
        else:
            raise TypeError(
                f"Codes does not seem to have correct datatype. {type(codes)}"
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
