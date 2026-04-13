import inspect
from collections.abc import Callable
from typing import Concatenate
from typing import Literal
from typing import ParamSpec

import pandas as pd
from nudb_config import settings

import nudb_use.variables.derive as derive
from nudb_use.exceptions.exception_classes import NudbDerivedFromNotFoundError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.all_data_helpers import get_source_data
from nudb_use.variables.derive.all_data_helpers import join_variable_data
from nudb_use.variables.derive.derive_decorator_utils import fillna_by_priority
from nudb_use.variables.derive.derive_decorator_utils import (
    swap_temp_colnames_from_temp,
)
from nudb_use.variables.derive.derive_decorator_utils import swap_temp_colnames_to_temp

P = ParamSpec("P")


class DeriveError(Exception):
    """For errors that occur during deriving variables."""

    ...


def get_derive_function(varname: str) -> Callable[..., pd.DataFrame] | None:
    """Return the derive function for a variable if it exists.

    Args:
        varname: The name of the variable to get the derive function for.

    Returns:
        Callable[..., pd.DataFrame] | None: The derive function, or None if no
        function was found.
    """
    if varname not in derive.__all__:
        return None

    if hasattr(derive, varname):
        found_func: Callable[..., pd.DataFrame] = getattr(derive, varname)
        return found_func
    logger.warning(f"Found no derive function for {varname}")
    return None


def wrap_derive(
    basefunc: Callable[Concatenate[pd.DataFrame, P], pd.Series | pd.DataFrame],
) -> Callable[..., pd.DataFrame]:
    """Decorator for derive functions that enforces config metadata and logging.

    Notes:
        - Validates that the variable exists in config and has a `derived_from` definition.
        - Recursively derives missing prerequisites before calling the decorated function.
        - Logs fill percentages and merges existing data with derived data based on priority.

    Args:
        basefunc: Function that derives a single variable from an input dataframe.

    Returns:
        Callable[..., pd.DataFrame]: Wrapped derive function that
        writes/updates the derived column.

    Raises:
        NudbDerivedFromNotFoundError: No matching entry can be found in the config for the function name.
    """

    def get_filling_pct(x: pd.Series) -> float:
        return 100 * x.notna().sum() / len(x) if len(x) else 0.0

    def get_pct_string(p: float) -> str:
        return f"{p:.2f}%"

    name = basefunc.__name__
    derived_from = settings.variables[name].derived_from

    # This check runs at runtime, since that is when the function gets decorated?
    if not derived_from:
        raise NudbDerivedFromNotFoundError(
            f"No `derived_from` entries for variable {name}!"
        )

    def wrapper(
        df: pd.DataFrame,
        priority: Literal["old", "new"] = "old",
        temp_col_renames: dict[str, str] | None = None,
        raise_errors: bool = False,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> pd.DataFrame:

        with LoggerStack(f"Deriving {name} from {', '.join(derived_from)}..."):
            logger.debug(f"Source code for {name}:\n{inspect.getsource(basefunc)}")

            df, rename_state = swap_temp_colnames_to_temp(
                df, derived_from, temp_col_renames
            )
            out_df = df

            try:
                available = set(df.columns)
                need = set(derived_from)

                missing = need - available
                have = [x for x in need if x in available]
                if have:
                    logger.info(
                        f"Already have these columns, not deriving them again (if they are derivable): {have}"
                    )

                priority_literal: Literal["old", "new"] = (
                    "new" if priority == "new" else "old"
                )

                for missing_var in missing.copy():
                    if missing_var not in missing:
                        continue  # missing is mutable, and may change due to derivations

                    derive_func = get_derive_function(missing_var)

                    if derive_func:
                        df = derive_func(
                            df,
                            *args,
                            priority=priority_literal,
                            temp_col_renames=temp_col_renames,
                            **kwargs,
                        )

                    if missing_var in df.columns:
                        missing -= {missing_var}

                if missing:
                    msg = f"Unable to derive {name}, missing: {', '.join(list(need))}! You might want to rename columns you have with the temp_col_renames parameter."
                    if raise_errors:
                        raise KeyError(msg)
                    logger.warning(msg)
                    out_df = df

                else:
                    exists = name in df.columns
                    if exists:
                        oldvals = df[name]
                        fill_pct0 = get_filling_pct(oldvals)
                        logger.info(
                            f"Filling degree before deriving variable: {get_pct_string(fill_pct0)}"
                        )
                    else:
                        oldvals = None
                        fill_pct0 = None

                    logger.debug(
                        "All `derived_from` variables are available, running basefunc"
                    )
                    result = basefunc(df, *args, **kwargs)

                    if isinstance(result, pd.DataFrame):
                        logger.notice(  # type: ignore[attr-defined]
                            "Basefunc returned a dataframe! Ignoring `priority` argument..."
                        )

                        # clean up
                        df = result
                        newvals = df[name]
                        exists = False

                    elif isinstance(result, pd.Series):
                        newvals = result

                    else:
                        raise TypeError(
                            f"`basefunc` ({name}) returned an unexpected type: '{type(result)}'"
                        )

                    should_write = True
                    if exists:
                        newvals_filled = fillna_by_priority(
                            oldvals=oldvals, newvals=newvals, priority=priority_literal
                        )
                        if newvals_filled is None:
                            should_write = False
                            out_df = df
                        else:
                            newvals = newvals_filled

                    if should_write:
                        fill_pct1 = get_filling_pct(newvals)
                        logger.info(
                            f"Filling degree after deriving variable: {get_pct_string(fill_pct1)}"
                        )
                        if fill_pct0 and fill_pct1 < fill_pct0:
                            logger.warning(
                                f"Filling degree for {name} went down by {get_pct_string(fill_pct0 - fill_pct1)} after deriving it againg"
                            )

                        df[name] = newvals
                        out_df = df

            except Exception as err:
                if raise_errors:
                    raise DeriveError(err) from err
                logger.warning(
                    f"Derivation of {name} failed, returning data as is!\n{type(err).__name__}: {err}"
                )
                out_df = df
            finally:
                out_df = swap_temp_colnames_from_temp(out_df, rename_state)

            return out_df

    wrapper.__name__ = basefunc.__name__
    docstring = basefunc.__doc__ or ""
    wrapper.__doc__ = f"""{docstring}

            Args:
                df: Dataframe that should contain prerequisites listed in {derived_from}.
                priority: 'old' keeps existing {name} values when present, 'new' prefers freshly derived values.

            Returns:
                pd.DataFrame: The dataframe with {name} added/updated when all prerequisites are available.
        """
    return wrapper


def wrap_derive_join_all_data(
    basefunc: Callable[Concatenate[pd.DataFrame, P], pd.DataFrame],
) -> Callable[..., pd.DataFrame]:
    """Decorator for derive functions that need specific data as a base, specified in the config.

    Args:
        basefunc: Function that derives a single variable from an input dataframe.

    Returns:
        Callable[..., pd.DataFrame]: Wrapped derive function that
        writes/updates the derived column using whole NUDB-datasets.
    """
    name = basefunc.__name__

    def subfunc(
        df: pd.DataFrame | None = None,
        priority: Literal["old", "new"] = "old",
        temp_col_renames: dict[str, str] | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> pd.DataFrame:
        with LoggerStack(f"Deriving variable {name}, using whole NUDB-datasets."):
            source_data = get_source_data(name, df)

            basefunc_wrapped = wrap_derive(basefunc)
            derived_source = basefunc_wrapped(
                source_data,
                *args,
                priority=priority,
                temp_col_renames=temp_col_renames,
                **kwargs,
            )

            if df is None:
                logger.warning("data is None, why u do this?")
                return derived_source

            try:
                return join_variable_data(name, derived_source, df)
            except Exception:
                logger.warning(f"Unable to join {name} onto data! Returning as is...")
                return df

    subfunc.__name__ = basefunc.__name__
    docstring = basefunc.__doc__ or ""
    subfunc.__doc__ = f"""{docstring}

            Args:
                df: Dataframe that we should merge the variable data onto.
                priority: 'old' keeps existing {name} values when present, 'new' prefers freshly derived values.
                temp_col_renames: Temporary source-to-prerequisite rename mapping passed through to `wrap_derive`.

            Returns:
                pd.DataFrame: The dataframe with {name} added/updated.
        """

    return subfunc
