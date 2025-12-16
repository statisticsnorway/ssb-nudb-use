"""Ensure requested drop columns do not collide with configured variables."""

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.metadata.nudb_config import get_var_metadata
from nudb_use.metadata.nudb_config.find_var_missing import VariableMetadata
from nudb_use.metadata.nudb_config.find_var_missing import find_vars
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


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
