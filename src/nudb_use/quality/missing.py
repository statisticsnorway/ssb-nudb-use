"""Checks ensuring NUDB datasets respect missing-value thresholds."""

import pandas as pd

from nudb_use import settings
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def check_non_missing(
    df: pd.DataFrame, cols_not_empty: list[str], raise_errors: bool = True
) -> list[NudbQualityError]:
    """Ensure the provided columns never contain missing values.

    Args:
        df: DataFrame to inspect.
        cols_not_empty: Column names that must be fully populated.
        raise_errors: When True, raise grouped errors if violations are found.

    Returns:
        list[NudbQualityError]: Errors describing columns that contain missing
        values, or an empty list if all columns are complete.
    """
    with LoggerStack("Checking that {cols_not_empty} have no missing values."):
        errors: list[NudbQualityError] = []
        missing_cols = [col for col in cols_not_empty if col not in list(df.columns)]
        if missing_cols:
            errors.append(
                NudbQualityError(
                    f"Missing cols that we want to test for no missing values: {missing_cols}"
                )
            )
        for col in cols_not_empty:
            if df[col].isna().any():
                errors.append(
                    NudbQualityError(f"{col} contains empty values, it shouldnt.")
                )
        if raise_errors:
            raise_exception_group(errors)
        return errors


def check_columns_only_missing(
    df: pd.DataFrame, raise_errors: bool = True
) -> list[NudbQualityError]:
    """Identify columns that consist entirely of missing values.

    Args:
        df: DataFrame to inspect.
        raise_errors: When True, raise grouped errors if violations are found.

    Returns:
        list[NudbQualityError]: Errors describing columns that contain only
        missing values, or an empty list when every column has data.
    """
    with LoggerStack("Looking for columns in the dataset that are only empty"):
        errors: list[NudbQualityError] = []
        empty_mask: pd.Series[bool] = df.isna().all(axis=0)
        empty_cols: list[str] = list(empty_mask[empty_mask].index)
        for col in empty_cols:
            err_msg = f"Column {col} only contains empty values. Why is it in the dataset if it contains nothing?"
            logger.warning(err_msg)
            errors.append(NudbQualityError(err_msg))
        if raise_errors:
            raise_exception_group(errors)
        return errors


def empty_percents_over_columns(
    df: pd.DataFrame, group_cols: str | list[str] | None = None
) -> pd.DataFrame:
    """Check the percentage of empty values in specified columns in a DataFrame.

    Args:
        df: DataFrame to check columns in.
        group_cols: List of columns to check for percentage of empty values.

    Returns:
        pd.DataFrame: DataFrame with percentage values for empty values for each column.
    """
    if isinstance(group_cols, str):
        group_cols = [group_cols]

    def percent_empty(series: pd.Series) -> float:
        return series.isna().sum() / len(series) * 100

    if group_cols is not None:
        return df.groupby(group_cols).agg(percent_empty)
    return df.apply(percent_empty).to_frame(name="percent_empty").T


def last_period_within_thresholds(
    df: pd.DataFrame,
    period_col: str,
    thresholds: dict[str, float] | None = None,
    raise_errors: bool = True,
) -> list[NudbQualityError]:
    """Validate that the latest period satisfies missing-value thresholds.

    Args:
        df: DataFrame containing the data to evaluate.
        period_col: Column identifying the period dimension.
        thresholds: Mapping of column names to allowed missing-value percentages.
        raise_errors: When True, raise grouped errors if violations are found.

    Returns:
        list[NudbQualityError]: Errors describing columns that exceed thresholds
        in the most recent period, or an empty list if all pass.
    """
    last_period = sorted(df[period_col].unique())[-1]
    return df_within_missing_thresholds(
        df[df[period_col] == last_period], thresholds, raise_errors
    )


def df_within_missing_thresholds(
    df: pd.DataFrame,
    thresholds: dict[str, float] | None = None,
    raise_errors: bool = True,
) -> list[NudbQualityError]:
    """Check whether each column respects its configured missing-value threshold.

    Args:
        df: DataFrame providing the values to inspect.
        thresholds: Mapping of column names to allowed missing-value percentages.
        raise_errors: When True, raise grouped errors if violations are found.

    Returns:
        list[NudbQualityError]: Errors describing columns that exceed their
        thresholds, or an empty list when all limits are met.
    """
    emptiness = empty_percents_over_columns(df)
    errors: list[NudbQualityError] = []
    if thresholds is None:
        logger.warning(
            "No thresholds were sent into df_within_missing_thresholds, why?"
        )
        return errors
    thresholds_not_none: dict[str, float] = thresholds
    for col, threshold in thresholds_not_none.items():
        if col not in emptiness.columns:
            errors += [
                NudbQualityError(
                    f"Cant find {col} that has defined emptiness {threshold} in the config. It should probably be in the dataset?"
                )
            ]
            continue
        if emptiness[col].iloc[0] > threshold:
            errors += [
                NudbQualityError(
                    f"{col} has above the accepted threshold {threshold} amount of empty cells: {emptiness[col].iloc[0]} percent"
                )
            ]
    if errors and raise_errors:
        raise_exception_group(errors)
    if not errors:
        logger.info("All columns seem to be within missing-percentage thresholds.")
    return errors


def check_missing_thresholds_dataset_name(
    df: pd.DataFrame, dataset_name: str, raise_errors: bool = True
) -> list[NudbQualityError]:
    """Validate a dataset against the configured missing-value thresholds.

    Args:
        df: DataFrame to validate.
        dataset_name: Name of the dataset whose threshold config should be used.
        raise_errors: When True, raise grouped errors if violations are found.

    Returns:
        list[NudbQualityError]: Errors describing columns that exceed their
        thresholds, or an empty list when all limits are met.
    """
    with LoggerStack(
        "Checking amount of missing against defined thresholds in the config from dataset-level."
    ):
        thresholds = get_thresholds_from_config(dataset_name)
        if not thresholds:
            logger.warning(
                f"Found no registered thresholds for empty for {dataset_name}, check the config to define."
            )
            return []
        return df_within_missing_thresholds(df, thresholds, raise_errors=raise_errors)


def get_thresholds_from_config(dataset_name: str) -> dict[str, float]:
    """Retrieve percentage completion threshold values for a given dataset from config.

    Args:
        dataset_name: Name of the dataset to retrieve threshold values for.

    Returns:
        dict[str, float]: Dictionary mapping variable names to specified
        percentage completion threshold values.
    """
    thresholds_empty: dict[str, float] = settings.datasets[dataset_name][
        "thresholds_empty"
    ]
    return thresholds_empty
