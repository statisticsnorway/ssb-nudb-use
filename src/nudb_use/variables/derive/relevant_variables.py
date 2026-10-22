"""Compose derivations using dataset relevance from NUDB config."""

import pandas as pd
from nudb_config import settings

from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.derive_decorator import get_derive_function


def get_relevant_variable_names_dataset(dataset_name: str) -> list[str]:
    """Return variable names configured as relevant for a dataset.

    Args:
        dataset_name: Dataset name to match in ``derived_relevant_datasets``.

    Returns:
        list[str]: Matching variable names in configuration order, including
        variables without a registered derive function. Empty if none match.
    """
    return [
        variable_name
        for variable_name, metadata in settings.variables.items()
        if dataset_name in (metadata.derived_relevant_datasets or [])
    ]


def pipe_all_relevant_variables_dataset(
    df: pd.DataFrame,
    dataset_name: str,
) -> pd.DataFrame:
    """Derive a dataset's relevant variables on the supplied dataframe.

    Variables are selected from ``derived_relevant_datasets`` in config order.
    Each matching derive function receives the previous function's dataframe.
    The existing derive wrappers handle prerequisites and missing source columns.
    Variables without a registered derive function are logged and skipped.
    If no functions match, the dataframe is returned unchanged.

    Args:
        df: Dataframe to derive relevant variables on.
        dataset_name: Dataset name to match in ``derived_relevant_datasets``.

    Returns:
        pd.DataFrame: Dataframe with the relevant variables derived. Usable with
        ``df.pipe(derive.pipe_all_relevant_variables_dataset, "avslutta")``.
    """
    for variable_name in get_relevant_variable_names_dataset(dataset_name):
        function = get_derive_function(variable_name)
        if function is None:
            logger.warning(
                f"Skipping relevant variable {variable_name} for {dataset_name}: "
                "no derive function found."
            )
            continue
        df = df.pipe(function)

    return df
