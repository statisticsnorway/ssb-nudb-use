"""Checks that flag columns marked as outdated are absent from datasets."""

import pandas as pd
from nudb_config.pydantic.variables import Variable

from nudb_use import settings as settings_use
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack


def check_outdated_variables(df: pd.DataFrame) -> list[NudbQualityError]:
    """Return errors for columns marked as outdated in config.

    Args:
        df: DataFrame to inspect.

    Returns:
        list[NudbQualityError]: Errors describing each outdated column present.
    """
    with LoggerStack("Checking for outdated variables in the dataset."):
        outdated_vars_in_df = find_outdated_variables_in_df(df)

        errors = []
        for var_name, var_details in outdated_vars_in_df.items():
            errors.append(
                NudbQualityError(
                    f"Outdated column: {var_name} in dataset: {var_details.get('outdated_comment')}"
                )
            )
        return errors


def find_outdated_variables_in_df(df: pd.DataFrame) -> dict[str, Variable]:
    """Return metadata for outdated variables present in a DataFrame.

    Args:
        df: DataFrame to inspect.

    Returns:
        dict[str, Variable]: Mapping from variable name to metadata entries.
    """
    return {
        k: v
        for k, v in settings_use.variables.items()
        if v["unit"] == "utdatert" and k in df.columns.str.lower()
    }
