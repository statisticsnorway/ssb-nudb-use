import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use import logger
from nudb_use.quality.widths import check_column_widths
from nudb_use.variables.checks import check_column_presence
from nudb_use.quality.specific_variables import run_all_specific_variable_tests
from nudb_use.quality.duplicated_columns import check_duplicated_columns
from nudb_use.quality.missing import check_missing_thresholds_dataset_name, check_columns_only_missing
from nudb_use.quality.outdated_variables import check_outdated_variables
from nudb_use.metadata.nudb_klass import check_klass_codes


def run_quality_suite(df: pd.DataFrame,
                      dataset_name: str,
                      data_time_start: str | None = None,
                      data_time_end: str | None = None,
                      raise_errors: bool = True,
                      **kwargs) -> list[NudbQualityError]:
    """Run all quality checks for a dataset. 

    Nested function combining check_column_presence, check_column_widths
    run_all_specific_variable_tests, check_klass_codes, and check_missing_thresholds_dataset_name.
    
    Args:
        df: Dataframe to check.
        dataset_name: Name of dataset to validate from defined in config. 
        data_time_start:
        data_time_end:
        raise_errors: If True, raises an exception group on validation errors;
                      otherwise, only logs warnings.

    Returns:
        list[NudbQualityError]: List of quality errors detected during validation, empty if none.
    
    Raises:
        NudbQualityError: Raised if any quality errors are found and `raise_errors` is True.
    """
    # Check if dataset name is in config
    
    errors = []
    errors += check_column_presence(df, dataset_name=dataset_name, raise_errors=False)
    errors += check_outdated_variables(df)
    errors += check_duplicated_columns(df)
    errors += check_column_widths(df, ignore_na=True, raise_errors=False)
    errors += run_all_specific_variable_tests(df, dataset_name=dataset_name, raise_errors=False, **kwargs)
    errors += check_klass_codes(df, data_time_start, data_time_end, raise_errors=False)
    errors += check_columns_only_missing(df, raise_errors = False)
    errors += check_missing_thresholds_dataset_name(df, dataset_name=dataset_name, raise_errors=False)

    if errors and raise_errors:
        raise_exception_group(errors)
    if not errors:
        logger.info(f"No quality errors for dataset {dataset_name}.")
    return errors
