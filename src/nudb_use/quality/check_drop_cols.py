from nudb_use import LoggerStack
from nudb_use import logger
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.metadata.nudb_config import get_var_metadata
from nudb_use.variables.var_utils.find_var import find_vars


def check_drop_cols_for_valid_cols(
    drop_cols: list[str],
    ignores: list[str] | str | None = None,
    raise_errors: bool = False,
):
    """Check if there are any columns in drop that is contained in the valid variables or their renames (old variables that are renamed to valid ones).

    Args:
        drop_cols: The columns we are considering dropping.
        raise_errors: If we shoud raise the errors found, or return them (if any).

    Returns:
        None | NudbQualityError: If nothing is found, returns None, otherwise returns the error.

    Raises:
        NudbQualityError: If raise_errors is set to True, and
    """
    with LoggerStack("Looking for columns in your drop that you might want to keep."):
        var_meta_valid = get_var_metadata().query("unit != 'utdatert'")
        renamed_list = [
            x
            for y in (
                var_meta_valid.query("~renamed_from.isna()")["renamed_from"].to_list()
            )
            for x in y
        ]
        want_list = var_meta_valid.index.to_list()

        overlap = [col for col in want_list if col in [c.lower() for c in drop_cols]]

        if not overlap:  # Lets do less work
            return None

        if ignores is not None:
            if isinstance(ignores, str):
                ignore_list: list[str] = [ignores]
            elif isinstance(ignores, list):
                ignore_list = ignores
            overlap = [
                c for c in overlap if c not in [col.lower() for col in ignore_list]
            ]

        if overlap:
            # Lets be nice and find the mappings to show what the current renaming is.
            overlap_dict = {k: v["name"] for k, v in find_vars(overlap).items()}
            err_msg = f"There are columns in your drop, that you should consider keeping, because they are part of valid columns or their renames: {overlap_dict}"
            logger.warning(err_msg)
            error = NudbQualityError(err_msg)
            if raise_errors:
                raise error
            return error

        # Found no overlaps
        return None
