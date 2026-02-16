"""Validation helpers that ensure column values follow expected widths."""

import pandas as pd

from nudb_use import settings
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def _get_length(var_info: object) -> list[int] | None:
    if isinstance(var_info, dict):
        return var_info.get("length")
    return getattr(var_info, "length", None)


def _is_valid_widths(widths: object) -> bool:
    if not isinstance(widths, dict):
        return False
    if not all(isinstance(k, str) for k in widths):
        return False
    if not all(isinstance(v, list) for v in widths.values()):
        return False
    return all(isinstance(i, int) for items in widths.values() for i in items)


def _get_widths_definition(
    df: pd.DataFrame, widths: dict[str, list[int]] | None
) -> dict[str, list[int]]:
    if _is_valid_widths(widths):
        return widths  # type: ignore[return-value]
    logger.info("widths does not match datatype, getting widths from config.")
    return {
        col: length
        for col, var_info in settings.variables.items()
        if col in df.columns and (length := _get_length(var_info)) is not None
    }


def _ensure_string_column(series: pd.Series, col: str) -> None:
    if pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
        return
    raise TypeError(
        f"Checking char widths using config: {col} should be string, but its a {series.dtype}."
    )


def _find_width_errors(
    df: pd.DataFrame, widths_def: dict[str, list[int]], maxprint: int = 50
) -> list[NudbQualityError]:
    errors: list[NudbQualityError] = []
    for col, widths_conf in widths_def.items():
        if not widths_conf:
            continue

        logger.debug(col)
        _ensure_string_column(df[col], col)

        try:
            len_mask_diff = (~df[col].str.len().isin(widths_conf)) & (~df[col].isna())
            if len_mask_diff.sum():
                first_values = pd.Series(df[len_mask_diff][col].unique()).head(
                    maxprint
                )  # pd.Series.unique() doesn't return a Series object if dtype is a pyarrow type
                unique_mismatch_vals = ",\n".join(list(first_values))
                too_many_message = (
                    f"first {maxprint}" if len(unique_mismatch_vals) > maxprint else ""
                )
                errors.append(
                    NudbQualityError(
                        f"In {col} found values not of the defined widths: {widths_conf}, the {too_many_message} mismatched codes:\n{unique_mismatch_vals}"
                    )
                )
        except AttributeError as e:
            raise TypeError(
                f"Column {col} has dtype {df[col].dtype}, might not work when looking at char width from config?"
            ) from e

    return errors


def check_column_widths(
    df: pd.DataFrame,
    widths: dict[str, list[int]] | None = None,
    raise_errors: bool = True,
) -> list[NudbQualityError]:
    """Validate that string lengths in each column match expected widths.

    Note: `ignore_na` is currently unused.

    Args:
        df: DataFrame to inspect.
        widths: Optional mapping of column names to allowed string lengths.
            When omitted or malformed, definitions are loaded from config.
        raise_errors: When True, raise grouped errors if mismatches are found.

    Returns:
        list[NudbQualityError]: Errors describing columns whose values are outside
        the allowed width definitions, or an empty list when all pass.
    """
    with LoggerStack(
        "Checking the widths of values in columns according to a dict sent in or gotten from the config."
    ):
        widths_def = _get_widths_definition(df, widths)

        widths_def_str = str(widths_def).replace(",", ",\n")
        logger.debug(f"widths_def:\n{widths_def_str}")

        errors = _find_width_errors(df, widths_def)

        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)

        return errors
