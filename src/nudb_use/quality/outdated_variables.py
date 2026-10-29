"""Checks that flag columns marked as outdated are absent from datasets."""

import pandas as pd

from nudb_use import settings as settings_use
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.metadata.nudb_config._renames import _rename_candidates
from nudb_use.metadata.nudb_config._renames import _value
from nudb_use.metadata.nudb_config.find_var_missing import VariableMetadata
from nudb_use.metadata.nudb_config.find_var_missing import _normalize_variable
from nudb_use.nudb_logger import LoggerStack


def check_outdated_variables(
    df: pd.DataFrame, dataset_name: str | None = None
) -> list[NudbQualityError]:
    """Return errors for outdated columns and historical variable names.

    Args:
        df: DataFrame to inspect.
        dataset_name: Dataset used to select rename overrides, or None for all mappings.

    Returns:
        list[NudbQualityError]: Errors describing each outdated column present.
    """
    with LoggerStack("Checking for outdated variables in the dataset."):
        outdated_vars_in_df = find_outdated_variables_in_df(df, dataset_name)

        errors = []
        for var_name, var_details in outdated_vars_in_df.items():
            message = f"Outdated column: {var_name}"
            if comment := var_details.get("outdated_comment"):
                message += f": {comment}"
            if mappings := var_details.get("renamed_to"):
                details = "; ".join(
                    f"{target!r} ({', '.join(sources)})"
                    for target, sources in mappings.items()
                )
                message += f". Rename mappings: {details}"
            errors.append(NudbQualityError(message))
        return errors


def find_outdated_variables_in_df(
    df: pd.DataFrame, dataset_name: str | None = None
) -> dict[str, VariableMetadata]:
    """Return metadata for outdated columns and historical variable names.

    Args:
        df: DataFrame to inspect.
        dataset_name: Dataset used to select rename overrides, or None for all mappings.

    Returns:
        dict[str, VariableMetadata]: Lowercase column names mapped to metadata,
        including renamed_to targets and their configuration sources when applicable.
    """
    variables = {
        str(name): variable for name, variable in settings_use.variables.items()
    }
    candidates = _rename_candidates(
        variables, getattr(settings_use, "datasets", {}), dataset_name
    )
    result: dict[str, VariableMetadata] = {}
    for name in df.columns.str.lower():
        variable = variables.get(name)
        mappings = {
            target: sources
            for target, sources in candidates.get(name, {}).items()
            if target.lower() != name
        }
        if _value(variable, "unit") != "utdatert" and not mappings:
            continue
        metadata: VariableMetadata = (
            _normalize_variable(variable, name)
            if variable is not None
            else {"name": name}
        )
        if mappings:
            metadata["renamed_to"] = mappings
        result[name] = metadata
    return result
