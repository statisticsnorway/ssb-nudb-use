"""Ensure requested drop columns do not collide with configured variables."""

from collections.abc import Iterable

import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.metadata.nudb_config import get_var_metadata
from nudb_use.metadata.nudb_config.find_var_missing import VariableMetadata
from nudb_use.metadata.nudb_config.find_var_missing import find_vars
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def _drop_vars_derivable_from_valid(var_meta_valid: pd.DataFrame) -> pd.DataFrame:
    """Remove variables that are derivable from other valid variables.

    A variable is considered derivable when all its `derived_from` dependencies
    exist among the valid variables, including through transitive dependencies.
    Such variables are dropped from `var_meta_valid` so they do not trigger
    warnings when listed in drop columns.

    Args:
        var_meta_valid: Variable metadata filtered to valid variables.

    Returns:
        pd.DataFrame: A filtered metadata frame with derivable variables removed.
    """
    if "derived_from" not in var_meta_valid.columns:
        return var_meta_valid

    import math

    def _is_missing(value: object) -> bool:
        if value is None or value is pd.NA or value is pd.NaT:
            return True
        if isinstance(value, float) and math.isnan(value):
            return True
        return False

    def _normalize_var(value: object) -> str | None:
        if _is_missing(value):
            return None
        if not isinstance(value, str):
            return None
        name = value.strip().lower()
        return name or None

    def _to_list(value: object) -> list[str]:
        if _is_missing(value):
            return []
        if isinstance(value, str):
            norm = _normalize_var(value)
            return [norm] if norm else []
        if isinstance(value, Iterable):
            items: list[str] = []
            for item in value:
                norm = _normalize_var(item)
                if norm:
                    items.append(norm)
            return items
        norm = _normalize_var(value)
        return [norm] if norm else []

    want_set = {_normalize_var(idx) for idx in var_meta_valid.index}
    want_set.discard(None)

    deps_map: dict[str, list[str]] = {}
    base_vars: set[str] = set()
    for idx, derived_from in var_meta_valid["derived_from"].items():
        key = _normalize_var(idx)
        if key is None:
            continue
        deps = _to_list(derived_from)
        deps_map[key] = deps
        if not deps:
            base_vars.add(key)

    available = set(base_vars)
    pending = set(deps_map.keys()) - available
    changed = True
    while changed:
        changed = False
        for var in list(pending):
            deps = deps_map.get(var, [])
            if deps and all(dep in available for dep in deps):
                available.add(var)
                pending.remove(var)
                changed = True

    derivable = available - base_vars
    drop_indices = [
        idx
        for idx in var_meta_valid.index
        if _normalize_var(idx) in derivable and _normalize_var(idx) in want_set
    ]
    if not drop_indices:
        return var_meta_valid
    return var_meta_valid.drop(index=drop_indices)


def check_drop_cols_for_valid_cols(
    drop_cols: list[str],
    ignores: list[str] | str | None = None,
    raise_errors: bool = False,
) -> NudbQualityError | None:
    """Warn when requested drop columns overlap with defined valid columns.

    Args:
        drop_cols: Column names that are about to be dropped.
        ignores: Optional iterable or single column name that should be ignored
            when computing the overlap.
        raise_errors: When True, raise a NudbQualityError instead of returning
            it. Defaults to False.

    Returns:
        NudbQualityError | None: Error describing the overlapping columns, or
        None when no problematic columns are found.

    Raises:
        NudbQualityError: Raised when overlaps exist and `raise_errors` is True.
    """
    with LoggerStack("Looking for columns in your drop that you might want to keep."):
        var_meta_valid = get_var_metadata().query("unit != 'utdatert'")
        var_meta_valid = _drop_vars_derivable_from_valid(var_meta_valid)

        renamed_list = [
            x
            for y in (
                var_meta_valid[
                    (var_meta_valid["renamed_from"].notna())
                    & (var_meta_valid["renamed_from"].apply(bool))
                ]["renamed_from"].to_list()
            )
            for x in y
        ]
        drops_old_names = [c for c in drop_cols if c in renamed_list]
        if drops_old_names:
            logger.warning(
                f"You are trying to drop the old names of columns (should have been renamed?), these may not be handled correctly: {drops_old_names}"
            )

        want_list = var_meta_valid.index.to_list()

        drop_cols_lower = {c.lower() for c in drop_cols}
        overlap = [col for col in want_list if col in drop_cols_lower]

        if not overlap:  # Lets do less work
            return None

        if ignores is not None:
            if isinstance(ignores, str):
                ignore_list: list[str] = [ignores]
            elif isinstance(ignores, list):
                ignore_list = ignores
            ignore_list_lower = {col.lower() for col in ignore_list}
            overlap = [c for c in overlap if c not in ignore_list_lower]

        if overlap:
            found_vars: dict[str, VariableMetadata] = {
                k: v for k, v in find_vars(overlap).items() if v is not None
            }
            # Lets be nice and find the mappings to show what the current renaming is.
            overlap_dict = {k: v["name"] for k, v in found_vars.items()}
            err_msg = (
                "There are columns in your drop that you should consider keeping, "
                "because they are part of valid columns or their renames: "
                f"{overlap_dict}"
            )
            logger.warning(err_msg)
            if raise_errors:
                raise NudbQualityError(err_msg)
            return NudbQualityError(err_msg)

        # Found no overlaps
        return None
