"""Validation helpers that ensure column values follow expected widths."""

import pandas as pd

from nudb_use import LoggerStack
from nudb_use import config
from nudb_use import logger
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group

from nudb_use.exceptions.exception_classes import NudbQualityError

class WidthMismatch(NudbQualityError):
    """Error raised when column values violate defined width constraints."""

    ...


def check_column_widths(
    df: pd.DataFrame,
    widths: dict[str, list[int]] | None = None,
    raise_errors: bool = True,
) -> list[WidthMismatch]:
    """Validate that string lengths in each column match expected widths.

    Note: `ignore_na` is currently unused.

    Args:
        df: DataFrame to inspect.
        widths: Optional mapping of column names to allowed string lengths.
            When omitted or malformed, definitions are loaded from config.
        raise_errors: When True, raise grouped errors if mismatches are found.

    Returns:
        list[WidthMismatch]: Errors describing columns whose values are outside
        the allowed width definitions, or an empty list when all pass.
    """
    with LoggerStack(
        "Checking the widths of values in columns according to a dict sent in or gotten from the config."
    ):
        if (
            isinstance(widths, dict)
            and all(isinstance(k, str) for k in widths)
            and all(isinstance(v, list) for v in widths.values())
            and all(
                isinstance(i, int) for int_list in widths.values() for i in int_list
            )
        ):
            widths_def: dict[str, list[int]] = widths

        else:
            logger.info("widths does not match datatype, getting widths from config.")
            widths_def: dict[str, int] = {
                col: var_info.length
                for col, var_info in config.settings.variables.items()
                if col in df.columns and "length" in var_info
            }

        widths_def_str = str(widths_def).replace(",", ",\n")
        logger.debug(f"widths_def:\n{widths_def_str}")

        # Check for variables in the dataframe, that are not defined in the config?
        errors = []
        maxprint = 50
        for col, widths in widths_def.items():
            if not widths:
                continue

            logger.debug(col)
            # display(~df[col])
            len_mask_diff = (~df[col].str.len().isin(widths)) & (~df[col].isna())
            if len_mask_diff.sum():
                first_values = pd.Series(df[len_mask_diff][col].unique()).head(
                    maxprint
                )  # pd.Series.unique() doesn't return a Series object if dtype is a pyarrow type
                unique_mismatch_vals = ",\n".join(list(first_values))
                too_many_message = (
                    f"first {maxprint}" if len(unique_mismatch_vals) > maxprint else ""
                )
                errors.append(
                    WidthMismatch(
                        f"In {col} found values not of the defined widths: {widths}, the {too_many_message} mismatched codes:\n{unique_mismatch_vals}"
                    )
                )

        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)

        return errors
