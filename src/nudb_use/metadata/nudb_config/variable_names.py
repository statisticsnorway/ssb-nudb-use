"""Tools for sorting and renaming NUDB variables based on config metadata."""

import pandas as pd

from nudb_use import settings as settings_use
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.variables.var_utils.duped_columns import find_duplicated_columns

from ._renames import _rename_candidates
from ._renames import _resolve_rename
from .get_variable_info import get_var_metadata


def _collapse(x: list[str] | str) -> list[str] | str:
    return x[0] if isinstance(x, list) and len(x) == 1 else x


def sort_cols_after_config_order_and_unit(data: pd.DataFrame) -> pd.DataFrame:
    """Sort columns after order defined in the config and by unit.

    Nested function that combined the functionality of the sort_cols_by_unit
    and sort_cols_after_config_order functions.

    Args:
        data: Dataframe to sort.

    Returns:
        pd.DataFrame: Dataframe with columns reordered by the column and unit order defined in the config.
    """
    return sort_cols_by_unit(sort_cols_after_config_order(data))


def sort_cols_after_config_order(data: pd.DataFrame) -> pd.DataFrame:
    """Sort DataFrame columns according to the config-defined order.

    Args:
        data: DataFrame whose columns should be reordered.

    Returns:
        pd.DataFrame: DataFrame with columns reordered per config definition.
    """
    data.columns = data.columns.str.lower()
    sorted_cols = [
        col for col in settings_use.variables.keys() if col in data.columns
    ] + [col for col in data.columns if col not in settings_use.variables.keys()]
    return data[sorted_cols]


def sort_cols_by_unit(data: pd.DataFrame) -> pd.DataFrame:
    """Sort DataFrame columns based on the unit of each variable.

    Args:
        data: Input DataFrame with columns representing variables to sort.

    Returns:
        pd.DataFrame: DataFrame with columns reordered by their units' sort
        order.

    Raises:
        ValueError: If no config is found for the sort unit.
    """
    # Guarding for mypy
    if settings_use.variables_sort_unit is None:
        raise ValueError("Missing config for variables_sort_unit.")
    sorting: list[str] = settings_use.variables_sort_unit
    sort_order = {k: v for v, k in enumerate(sorting)}

    # Raise error if column in data is not in the settings_use?
    order = (
        get_var_metadata(variables=list(data.columns))
        .assign(sort_unit=lambda df: df["unit"].map(sort_order))
        .sort_values(by="unit")
        .index
    )
    logger.info(order)
    return data[order]


def get_cols_in_config(name: str | None) -> list[str]:
    """Retrieve column (variable) names from settings_use.

    Args:
        name: Name of a dataset. If None, returns all variable names across datasets.

    Returns:
        list[str]: List of column or variable names defined in settings_use.

    Raises:
        KeyError: If the provided dataset name is not defined in settings_use.
    """
    if name is None:
        cols_in_config: list[str] = list(settings_use["variables"].keys())
    else:
        datasets = list(settings_use["datasets"].keys())
        if name not in datasets:
            raise KeyError(f"""
                `name` must be one of the following:
                    {datasets}
                got '{name}'
            """)

        cols_in_config = list(settings_use["datasets"][name]["variables"])

    return cols_in_config


def get_cols2keep(data: pd.DataFrame, name: str | None = None) -> pd.Index:
    """Get column names to keep in a dataset based on settings_use.

    Args:
        data: DataFrame to check.
        name: Name of the dataset to compare against settings_use.

    Returns:
        pd.Index: Columns present in the dataset that are defined in settings_use.
    """
    cols_in_config = get_cols_in_config(name)
    return data.columns[data.columns.isin(cols_in_config)]


def get_cols2drop(data: pd.DataFrame, name: str | None = None) -> pd.Index:
    """Return column names to drop from a dataset based on settings_use.

    Args:
        data: DataFrame to check.
        name: Name of the dataset to compare against settings_use.

    Returns:
        pd.Index: Columns present in the DataFrame but not defined in settings_use.
    """
    cols_in_config = get_cols_in_config(name)
    return data.columns[~data.columns.isin(cols_in_config)]


def update_colnames(
    data: pd.DataFrame, dataset_name: str = "", lowercase: bool = True
) -> pd.DataFrame:
    """Rename columns in a DataFrame based on metadata mappings.

    Without a dataset, historical names follow avslutta > igang > eksamen priority.
    Conflicting targets at the same priority raise ValueError with mapping details.

    Args:
        data: Input DataFrame whose column names should be updated.
        dataset_name: Dataset identifier for applying dataset-specific overrides.
        lowercase: Whether to lowercase column names before renaming.

    Returns:
        pd.DataFrame: Copy of the DataFrame with columns renamed according to
        metadata.

    Raises:
        KeyError: If the renaming results in duplicate column names.
    """
    with LoggerStack("Updating Colnames"):
        data = data.copy()

        # Lowercase all colnames usually?
        if lowercase:
            data.columns = data.columns.str.lower()

        metadata = get_var_metadata()
        datasets = getattr(settings_use, "datasets", {})
        candidates = _rename_candidates(
            {
                str(name): variable
                for name, variable in metadata.to_dict("index").items()
            },
            datasets,
            dataset_name or None,
        )
        # Resolve before applying overrides so equally ranked conflicts fail first.
        resolved = {
            col: _resolve_rename(col, candidates)
            for col in data.columns
            if col not in metadata.index
        }

        data = handle_dataset_specific_renames(data, dataset_name)

        renames_completed = {}
        for oldname, newname in resolved.items():
            if newname is None or oldname not in data.columns:
                continue
            if newname in data.columns:
                logger.warning(
                    f"Skipping renaming {oldname} to {newname} in dataset, as {newname} already exists!"
                )
                continue
            logger.debug(f"renaming {oldname} to {newname}!")
            renames_completed[oldname] = newname
            data = data.rename({oldname: newname}, axis=1)

        # There might be overrides for certain variables in the datasets
        if not dataset_name:
            logger.warning(
                "No dataset_name specified; dataset renames follow avslutta > igang > eksamen priority."
            )

        logger.info(f"Renamed {len(renames_completed)} columns to new names.")

    duped_columns = find_duplicated_columns(data)
    if duped_columns:
        err_msg = f"The renaming of columns resulted in duplicated column-names: {duped_columns}"
        logger.error(err_msg)
        raise KeyError(err_msg)

    return data


def handle_dataset_specific_renames(
    df: pd.DataFrame,
    dataset_name: str = "",
) -> pd.DataFrame:
    """Apply dataset-specific rename overrides defined in configuration.

    Args:
        df: DataFrame whose columns should be updated in place.
        dataset_name: Dataset key, inheriting its family's rules. An empty name
            selects mappings using avslutta > igang > eksamen priority.

    Returns:
        pd.DataFrame: DataFrame with overrides applied if they exist in the config.
    """
    candidates = _rename_candidates(
        {},
        {str(name): dataset for name, dataset in settings_use.datasets.items()},
        dataset_name or None,
    )
    renames = {
        old_name: target
        for old_name in candidates
        if old_name in df.columns
        and (dataset_name or old_name not in getattr(settings_use, "variables", {}))
        and (target := _resolve_rename(old_name, candidates)) is not None
    }
    if not renames:
        logger.info(
            "Found specified dataset in config, but the 'dataset_specific_renames' is empty. So no such extra renaming will occur."
        )
        return df

    renames_flip_list = _flip_dict_to_list(renames)

    for new_name, old_names in renames_flip_list.items():
        df = _apply_dataset_specific_rename(df, new_name, old_names)

    return df


def _apply_dataset_specific_rename(
    df: pd.DataFrame, new_name: str, old_names: str | list[str]
) -> pd.DataFrame:
    """Apply a single dataset-specific rename or fillna merge."""
    old_names_list = [old_names] if isinstance(old_names, str) else old_names
    candidates = [c for c in [new_name, *old_names_list] if c in df.columns]

    if len(candidates) > 1:
        _warn_fillna(candidates)
        df = _ensure_target_column(df, new_name, candidates[0])
        df = _fill_and_drop_sources(df, new_name, candidates)
    elif len(candidates) == 1 and candidates[0] != new_name:
        logger.info(
            f"Single value found for dataset_specific_rename, just renaming: {candidates[0]} to {new_name}"
        )
        return df.rename(columns={candidates[0]: new_name})
    else:
        logger.debug(
            "Dont know if anything needs to be done, if the dataset only contains the correct new name?"
        )

    return df


def _warn_fillna(candidates: list[str]) -> None:
    candidate_lines = "\n".join(candidates)
    logger.warning(
        f"Multiple columns map to the same, doing a fillna, instead of a pure renames:\n {candidate_lines}"
    )


def _ensure_target_column(
    df: pd.DataFrame, new_name: str, source_name: str
) -> pd.DataFrame:
    if new_name not in df.columns:
        df[new_name] = df[source_name].copy()
    return df


def _fill_and_drop_sources(
    df: pd.DataFrame, new_name: str, candidates: list[str]
) -> pd.DataFrame:
    col: str
    for col in [c for c in candidates if c != new_name]:
        if df[col].isna().all():
            logger.debug(
                f"{col} is all empty, no need to do dataset_specific_rename_fillna"
            )
        else:
            logger.warning(
                f"Found multiple columns mapping to the same, doing a fillna into {new_name} from {col}, deleting {col} after."
            )
            df[new_name] = df[new_name].fillna(df[col])
        df = df.drop(columns=[col])
    return df


def _flip_dict_to_list(d: dict[str, str]) -> dict[str, str | list[str]]:
    # Here we just invert the dictionary, so values become keys,
    # and keys become values. Multiple keys have the same value,
    # the keys are gathered in a list for where the value is the new key
    flipped: dict[str, list[str]] = {}

    for k, v in d.items():
        flipped[v] = flipped[v] + [k] if v in flipped else [k]

    # Flatten single value lists
    # Do it in a seperate step, to make type checker happy
    # Doing it directly in the main loop, confuses it

    return {k: _collapse(v) for k, v in flipped.items()}
