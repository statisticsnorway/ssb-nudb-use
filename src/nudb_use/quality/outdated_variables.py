import pandas as pd
from nudb_config.pydantic.variables import Variable

from nudb_use import LoggerStack
from nudb_use import settings as settings_use
from nudb_use.exceptions.exception_classes import NudbQualityError


def check_outdated_variables(df: pd.DataFrame) -> list[NudbQualityError]:
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
    return {
        k: v
        for k, v in settings_use.variables.items()
        if v["unit"] == "utdatert" and k in df.columns.str.lower()
    }
