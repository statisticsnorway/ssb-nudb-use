from collections.abc import Callable
from typing import Any
from typing import Literal

import pandas as pd
from nudb_config import settings

import nudb_use.variables.derive as derive
from nudb_use.exceptions.exception_classes import NudbDerivedFromNotFoundError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def get_derive_function(varname: str) -> Callable | None:
    """Return the derive function for a variable if it exists.

    Args:
        varname: The name of the variable to get the derive function for.

    Returns:
        None | Callable: The derive function, or None if no function was found.
    """
    if varname not in derive.__all__:
        return None

    try:
        return getattr(derive, varname)
    except Exception:
        return None


def fillna_by_priority(
    newvals: pd.Series | None,
    oldvals: pd.Series | None,
    priority: Literal["old"] | Literal["new"] = "old",
) -> pd.Series | None:
    """Fill missing values in prioritized order when a column already exists.

    Args:
        newvals: A pandas series with the newly added values.
        oldvals: A pandas series with the old values.
        priority: "old" if we should prioritze the old values, "new" if we should prioritize the new.

    Returns:
        pd.Series | None: The resulting merged columns using fillna-methods. Returns None if both newvals and oldvals is None.

    Raises:
        ValueError: If you are sending in a non-specifiec Literal for the priority-arg.
    """
    if newvals is None:
        logger.info("Newvals is None, just returning oldvals.")
        return oldvals
    if oldvals is None:
        logger.info("Oldvals is None, just returning newvals.")
        return newvals

    priority = priority.lower()

    if priority == "old":
        logger.info("Filling missing values in existing variable...")
        return oldvals.fillna(newvals)

    elif priority == "new":
        logger.info("Filling missing values in derived variable with existing ones...")
        out = newvals.fillna(oldvals)

        ok = oldvals.notna()
        nchanged = (oldvals[ok] != out[ok]).sum()
        if ok.sum():
            pchanged = 100 * nchanged / ok.sum()
            logger.info(
                f"{nchanged} ({pchanged:.2f}%) values were overwritten with new (derived) values"
            )
        else:
            logger.info("No existing values in oldvals, so nothing was overwritten.")

        return out
    else:
        raise ValueError("priority must be either 'old' or 'new'!")


def wrap_derive(basefunc: Callable) -> Callable:
    """Decorator for derive functions that enforces config metadata and logging.

    Notes:
        - Validates that the variable exists in config and has a `derived_from` definition.
        - Recursively derives missing prerequisites before calling the decorated function.
        - Logs fill percentages and merges existing data with derived data based on priority.

    Args:
        basefunc: Function that derives a single variable from an input dataframe.

    Returns:
        Callable: Wrapped derive function that writes/updates the derived column.

    Raises:
        NudbDerivedFromNotFoundError: No matching entry can be found in the config for the function name.
    """

    def get_filling_pct(x: pd.Series) -> float:
        return 100 * x.notna().sum() / len(x)

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
        df: pd.DataFrame, priority: str = "old", *args: Any, **kwargs: Any
    ) -> pd.DataFrame:

        with LoggerStack(f"Deriving {name} from {derived_from}..."):
            available = set(df.columns)
            need = set(derived_from)

            missing = need - available

            for need_var in need:
                derive_func = get_derive_function(need_var)

                if derive_func:
                    df = derive_func(df, *args, priority=priority, **kwargs)

            if missing:
                logger.warning(f"Unable to derive {name}, missing: {list(need)}!")
                return df

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
            try:
                logger.debug(
                    "All `derived_from` variables are available, running basefunc"
                )
                newvals = basefunc(df, *args, **kwargs)
                if exists:
                    newvals = fillna_by_priority(
                        oldvals=oldvals, newvals=newvals, priority=priority
                    )

                fill_pct1 = get_filling_pct(newvals)
                logger.info(
                    f"Filling degree after deriving variable: {get_pct_string(fill_pct1)}"
                )
                if fill_pct0 and fill_pct1 < fill_pct0:
                    logger.warning(
                        f"Filling degree for {name} went down by {get_pct_string(fill_pct0 - fill_pct1)} after deriving it againg"
                    )
                df[name] = newvals
                return df
            except Exception as err:
                logger.warning(
                    f"Derivation of {name} failed, returning data as is!\nMessage: {err}"
                )
                return df

    wrapper.__name__ = basefunc.__name__
    wrapper.__doc__ = f"""{basefunc.__doc__}

            Args:
                df: Dataframe that should contain prerequisites listed in {derived_from}.
                priority: 'old' keeps existing {name} values when present, 'new' prefers freshly derived values.

            Returns:
                pd.DataFrame: The dataframe with {name} added/updated when all prerequisites are available.
        """
    return wrapper
