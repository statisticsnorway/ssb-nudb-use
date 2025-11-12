import pandas as pd
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use import logger, LoggerStack
from nudb_use.config import settings

def check_non_missing(df: pd.DataFrame, cols_not_empty: list[str], raise_errors: bool = True) -> list[NudbQualityError]:
    """Check that specified DataFrame columns do not contain any missing values.
    
    Args:
        df: Dataframe to check columns,
        cols_not_empty: List of column names expected to have no missing (NaN) values. 
        raise_errors: If True, raise a grouped NudbQualityError if any issues are found;
                      if False, return a list of errors without raising.

    Returns:
        list[NudbQualityError]: List of NudbQualityError instances for columns that
                               are missing or contain empty values. Empty list if no errors.

    Raises: 
        NudbQualityError: If any specified columns contains missing (NaN) values and `raise_errors` is True.

    """
    with LoggerStack("Checking that {cols_not_empty} have no missing values."):
        errors: list[NudbQualityError] = []
        missing_cols = [col for col in cols_not_empty if col not in list(df.columns)]
        if missing_cols:
            errors.append(NudbQualityError(f"Missing cols that we want to test for no missing values: {missing_cols}"))
        for col in cols_not_empty:
            if df[col].isna().any():
                errors.append(NudbQualityError(f"{col} contains empty values, it shouldnt."))
        if raise_errors:
            raise_exception_group(errors)
        return errors

def check_columns_only_missing(df: pd.DataFrame, raise_errors: bool = True) -> list[NudbQualityError]:
    """Check the dataframe for the columns only containing empty values.
    
    Args:
        df: The dataframe we will check.
        raise_errors: If True, raise a grouped NudbQualityError if any issues are found;
                      if False, return a list of errors without raising.

    Returns:
         list[NudbQualityError]: List of NudbQualityError instances for columns that
                               only contain missing values. Empty list if all columns have some content.

    Raises: 
        NudbQualityError: If any specified columns contains missing (NaN) values and `raise_errors` is True.
    """
    with LoggerStack("Looking for columns in the dataset that are only empty"):
        errors: list[NudbQualityError] = []
        is_empty_cols = df.isna().all(axis=1)
        empty_cols = list(is_empty_cols[is_empty_cols].index)
        for col in empty_cols:
            err_msg = f"Column {col} only contains empty values. Why is it in the dataset if it contains nothing?"
            logger.warning(err_msg)
            errors.append(NudbQualityError(err_msg))
        if raise_errors:
            raise_exception_group(errors)
        return errors
    

def empty_percents_over_columns(df: pd.DataFrame, group_cols: str | list[str] | None = None) -> pd.DataFrame:
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
    return df.apply(percent_empty).to_frame(name='percent_empty').T

def last_period_within_thresholds(df: pd.DataFrame,
                                  period_col: str,
                                  thresholds: dict[str, float] | None = None,
                                  raise_errors: bool = True) -> list[NudbQualityError]:
    """Finds the last period within a dataframe that has precentage value completion above a defined threshold for a given column. 
    
    Args:
        df: DataFrame to check for completion. 
        period_col: Name of column specifying period. 
        thresholds: Dictionary of percentage completion thresholds for a specific column. 
        raise_errors: If True, raise a grouped NudbQualityError if any issues are found;
                      if False, return a list of errors without raising. 
        
    Returns:
        list[NudbQualityError]: List of quality errors indicating threshold violations in the last period found.
    Raises:
        NudbQualityError: Raised if threshold violations exist and `raise_errors` is True.
    """
    last_period = sorted(list(df[period_col].unique()))[-1]
    return df_within_missing_thresholds(df[df[period_col] == last_period], thresholds, raise_errors)

def df_within_missing_thresholds(df: pd.DataFrame,
                         thresholds: dict[str, float] | None = None,
                         raise_errors: bool = True) -> list[NudbQualityError]:
    """Check if columns in a dataframe are within a percentage completion threshold.  

    Args:
        df: DataFrame to check for completion. 
        thresholds: Dictionary of percentage completion thresholds for a specific column. 
        raise_errors: If True, raise a grouped NudbQualityError if any issues are found;
                      if False, return a list of errors without raising.
        
    Returns:
        list[NudbQualityError]: 

    Raises: 
        NudbQualityError: If a column has a higher amount of empty values than the threshold and `raise_errors` is True.

    """
    emptiness = empty_percents_over_columns(df)
    errors = []
    for col, threshold in thresholds.items():
        if col not in emptiness.columns:
            errors += [NudbQualityError(f"Cant find {col} that has defined emptiness {threshold} in the config. It should probably be in the dataset?")]
            continue
        if emptiness[col].iloc[0] > threshold:
            errors += [NudbQualityError(f"{col} has above the accepted threshold {threshold} amount of empty cells: {emptiness[col].iloc[0]} percent")]
    if errors and raise_errors:
        raise_exception_group(errors)
    if not errors:
        logger.info("All columns seem to be within missing-percentage thresholds.")
    return errors

def check_missing_thresholds_dataset_name(df: pd.DataFrame, dataset_name: str, raise_errors: bool = True) -> list[NudbQualityError]:
    """Check whether the provided DataFrame meets the missing value thresholds defined for a specific dataset.
    
    Args:
        df: Input DataFrame to be validated against the missing value thresholds.
        dataset_name : Name of the dataset whose threshold configuration is used for comparison.
        raise_errors: If True, raise a grouped NudbQualityError if any issues are found;
                      if False, return a list of errors without raising.
        
    Returns:
        list[NudbQualityError]: List of threshold validation errors for variables exceeding the allowed missing value limits.

    Raises:
        NudbQualityError: and `raise_errors` is True.
    """
    with LoggerStack("Checking amount of missing against defined thresholds in the config from dataset-level."):
        thresholds = get_thresholds_from_config(dataset_name)
        if not thresholds:
            logger.warning(f"Found no registered thresholds for empty for {dataset_name}, check the config to define.")
            return []
        return df_within_missing_thresholds(df, thresholds, raise_errors=raise_errors)

def get_thresholds_from_config(dataset_name) -> dict[str, float]:
    """Retrieve percentage completion threshold values for a given dataset from config. 
    
    Args:
        dataset_name: Name of the dataset to retrieve threshold values for. 
        
    Returns:
        dict[str,float]: Dictionary mapping variable names to specified percentage completion threshold values.
    """
    return settings.datasets[dataset_name]["thresholds_empty"]
