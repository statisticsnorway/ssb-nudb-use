import inspect
from collections.abc import Callable
from typing import Concatenate
from typing import Literal
from typing import ParamSpec
from typing import Protocol

import pandas as pd
import polars as pl
from nudb_config import settings

import nudb_use.variables.derive as derive
from nudb_use.exceptions.exception_classes import NudbDerivedFromNotFoundError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.all_data_helpers import get_source_data
from nudb_use.variables.derive.all_data_helpers import join_variable_data

P = ParamSpec("P")
TMP_COL = "_TMP_VALUES"

class WrappedDerive(Protocol[P]):
    """Arg types for the wrap_derive decorator."""

    def __call__(
        self,
        df: pd.DataFrame,
        priority: Literal["old", "new"] = "old",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> pd.DataFrame:
        """Typing as a call to the class."""
        ...


class WrappedDeriveJoinAllData(Protocol[P]):
    """Arg types for the wrap_derive_join_all_data decorator."""

    def __call__(
        self,
        df: pd.DataFrame | None = None,
        priority: Literal["old", "new"] = "old",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> pd.DataFrame:
        """Typing as a call to the class."""
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


def fillna_by_priority(
    name: str,
    lf: pl.LazyFrame,
    priority: Literal["old", "new"] = "old",
) -> pl.LazyFrame:
    """Fill missing values in prioritized order when a column already exists.

    Args:
        newvals: A pandas series with the newly added values.
        oldvals: A pandas series with the old values.
        priority: "old" if we should prioritze the old values, "new" if we should prioritize the new.

    Returns:
        pd.Series | None: The resulting merged columns using fillna-methods. Returns None if both newvals and oldvals is None.

    Raises:
        ValueError: If you are sending in a non-specific Literal for the priority-arg.
    """

    if priority == "old":
        logger.info("Filling missing values in existing variable...")
        x = pl.col(TMP_COL)
        y = pl.col(name)
    else: # priority == "new":
        logger.info("Filling missing values in derived variable with existing ones...")
        x = pl.col(name)
        y = pl.col(TMP_COL)
        
    return lf.with_columns(x.fill_null(y).alias(name))


def wrap_derive(
    basefunc: Callable[Concatenate[pl.LazyFrame, P], pd.Series | pl.LazyFrame],
) -> WrappedDerive[P]:
    """Decorator for derive functions that enforces config metadata and logging.

    Notes:
        - Validates that the variable exists in config and has a `derived_from` definition.
        - Recursively derives missing prerequisites before calling the decorated function.
        - Logs fill percentages and merges existing data with derived data based on priority.

    Args:
        basefunc: Function that derives a single variable from an input dataframe.

    Returns:
        WrappedDerive[P]: Wrapped derive function that
        writes/updates the derived column.

    Raises:
        NudbDerivedFromNotFoundError: No matching entry can be found in the config for the function name.
    """

    def get_filling_pct(x: pd.Series | pl.Series) -> float:
        if isinstance(x, pl.Series):
            x = x.to_pandas()

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
        df: pd.DataFrame | pl.LazyFrame,
        priority: Literal["old", "new"] = "old",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> pd.DataFrame:


        with LoggerStack(f"Deriving {name} from {', '.join(derived_from)}..."):
            logger.debug(f"Source code for {name}:\n{inspect.getsource(basefunc)}")

            if isinstance(df, pl.LazyFrame):
                eager = False
                lf = df
            elif isinstance(df, pd.DataFrame):
                eager = True
                lf = pl.from_pandas(df).lazy()

            available = set(lf.collect_schema().names())
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
                    lf = derive_func(lf, *args, priority=priority_literal, **kwargs)

                if missing_var in df.columns:
                    missing -= {missing_var}

            if missing:
                logger.warning(
                    f"Unable to derive {name}, missing: {', '.join(list(need))}!"
                )
                return df if eager else lf

            exists = name in lf.collect_schema().names()

            fill_pct0 = None
            if exists:
                lf = lf.rename({name: TMP_COL})
                
                if eager:
                    fill_pct0 = get_filling_pct(df[name])
                    logger.info(
                        f"Filling degree before deriving variable: {get_pct_string(fill_pct0)}"
                    )
            
            try:
                logger.debug(
                    "All `derived_from` variables are available, running basefunc"
                )
                result = basefunc(df, *args, **kwargs)

                if isinstance(result, pl.LazyFrame):
                    logger.notice(  # type: ignore[attr-defined]
                        "Basefunc returned a frame! Ignoring `priority` argument..."
                    )

                    # clean up
                    lf = result

                    columns = result.collect_schema().names()

                    if TMP_COL in columns:
                        lf = lf.drop(TMP_COL)

                    exists = False

                elif isinstance(result, pl.Expr):
                    lf = lf.with_columns(result.alias(name))

                else:
                    raise TypeError(
                        f"`basefunc` ({name}) returned an unexpected type: '{type(result)}'"
                    )

                if exists:
                    lf = fillna_by_priority(
                        lf=lf, name=name, priority=priority_literal
                    )

                if eager:
                    df = lf.collect().to_pandas()
                    
                    fill_pct1 = get_filling_pct(df[name])
                    logger.info(
                        f"Filling degree after deriving variable: {get_pct_string(fill_pct1)}"
                    )
                    if fill_pct0 and fill_pct1 < fill_pct0:
                        logger.warning(
                            f"Filling degree for {name} went down by {get_pct_string(fill_pct0 - fill_pct1)} after deriving it againg"
                        )
    
                    return df

                else:
                    return lf

            except Exception as err:
                logger.warning(
                    f"Derivation of {name} failed, returning data as is!\nMessage: {err}"
                )
                return df if eager else lf

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
) -> WrappedDeriveJoinAllData[P]:
    """Decorator for derive functions that need specific data as a base, specified in the config.

    Args:
        basefunc: Function that derives a single variable from an input dataframe.

    Returns:
        WrappedDeriveJoinAllData[P]: Wrapped derive function that
        writes/updates the derived column using whole NUDB-datasets.
    """
    name = basefunc.__name__

    def subfunc(
        df: pd.DataFrame | None = None,
        priority: Literal["old", "new"] = "old",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> pd.DataFrame:
        with LoggerStack(f"Deriving variable {name}, using whole NUDB-datasets."):
            source_data = get_source_data(name, df)

            basefunc_wrapped = wrap_derive(basefunc)
            derived_source = basefunc_wrapped(source_data, priority, *args, **kwargs)

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

            Returns:
                pd.DataFrame: The dataframe with {name} added/updated.
        """

    return subfunc
