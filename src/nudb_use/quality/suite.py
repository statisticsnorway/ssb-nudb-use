"""High-level orchestration for NUDB quality checks."""

from collections.abc import Sequence

import pandas as pd

from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.metadata.nudb_klass import check_klass_codes
from nudb_use.nudb_logger import logger
from nudb_use.quality.duplicated_columns import check_duplicated_columns
from nudb_use.quality.missing import check_columns_only_missing
from nudb_use.quality.missing import check_missing_thresholds_dataset_name
from nudb_use.quality.outdated_variables import check_outdated_variables
from nudb_use.quality.specific_variables import run_all_specific_variable_tests
from nudb_use.quality.widths import check_column_widths
from nudb_use.variables.checks import check_column_presence


def run_quality_suite(
    df: pd.DataFrame,
    dataset_name: str,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
    raise_errors: bool = True,
    **kwargs: object,
) -> Sequence[Exception]:
    """Run the full NUDB quality suite over a dataset.

    Args:
        df: DataFrame to validate.
        dataset_name: Name of the dataset in config; controls which part of the config to choose for values used in the valiadations.
        data_time_start: Optional start date used by codelist validations.
        data_time_end: Optional end date used by codelist validations.
        raise_errors: When True, raise grouped exceptions if any check fails.
        **kwargs: Additional keyword arguments forwarded to specific checks.

    Returns:
        Sequence[Exception]: All collected quality errors, or an empty sequence
        when every check passes.

    Raises:
        TypeError: If the first parameter df is not a pandas dataframe.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"First parameter (df) into `run_quality_suite` must be a pandas dataframe, not a {type(df)}."
        )

    errors = []
    errors += check_column_presence(df, dataset_name=dataset_name, raise_errors=False)
    errors += check_outdated_variables(df)
    errors += check_duplicated_columns(df)
    errors += check_column_widths(df, raise_errors=False)
    errors += run_all_specific_variable_tests(
        df, dataset_name=dataset_name, raise_errors=False, **kwargs
    )
    errors += check_klass_codes(df, data_time_start, data_time_end, raise_errors=False)
    errors += check_columns_only_missing(df, raise_errors=False)
    errors += check_missing_thresholds_dataset_name(
        df, dataset_name=dataset_name, raise_errors=False
    )

    if errors and raise_errors:
        raise_exception_group(errors)
    if not errors:
        logger.info(f"No quality errors for dataset {dataset_name}.")
    return errors
