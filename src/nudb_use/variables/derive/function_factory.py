import pandas as pd

from typing import Callable, Any

from nudb_use.nudb_logger import logger, LoggerStack
from nudb_config import settings
from nudb_use.exceptions.exception_classes import NudbDerivedFromNotFoundError


def get_derive_function(varname: str) -> Callable | None:
    """Return function for deriving a variable, returns None if it doens't exist"""
    import nudb_build.variables.derive as derive

    if varname not in derive.__all__:
        return None

    try:
        return getattr(derive, varname)
    except Exception:
        return None


def fillna_by_priority(newvals: pd.Series, oldvals: pd.Series, priority: str = "old") -> pd.Series:
    priority = priority.lower()

    if priority == "old":
        logger.info("Filling missing values in existing variable...")
        return oldvals.fillna(newvals)

    elif priority == "new":
        logger.info("Filling missing values in derived variable with existing ones...")
        out = newvals.fillna(oldvals)

        ok = oldvals.notna()
        nchanged = (oldvals[ok] != out[ok]).sum()
        pchanged = 100 * nchanged / ok.sum()
        logger.info(f"{nchanged} ({pchanged:.2f}%) values were overwritten with new (derived) values")

        return oldvals
    else:
        raise ValueError("priority must be either 'old' or 'new'!")


def wrap_derive_function(basefunc) -> Callable:
    get_filling_pct = lambda x: 100 * x.notna().sum() / x.shape[0]
    get_pct_string  = lambda p: f"{p:.2f}%"

    name = basefunc.__name__
    derived_from = settings.variables[name].derived_from

    if not derived_from:
        raise NudbDerivedFromNotFoundError(f"No `derived_from` entries for variable {name}!")

    def wrapper(df: pd.DataFrame, priority: str = "old", *args, **kwargs) -> pd.DataFrame:

        with LoggerStack(f"Deriving {name} from {derived_from}..."):
            available = set(df.columns)
            need      = set(derived_from)

            missing = need - available

            for need_var in need:
                derive_func = get_derive_func(need_var)

                if derive_func:
                    df = derive_func(df, priority = priority, *args, **kwargs)

            if missing:
                logger.warning(f"Unable to derive {name}, missing: {list(need)}!")
                return df

            exists = name in df.columns
            if exists:
                oldvals   = df[name]
                fill_pct0 = get_filling_pct(oldvals)
                logger.info(f"Filling degree before deriving variable: {get_pct_string(fill_pct0)}")
            else:
                olvals = None
                fill_pct0 = None
            try:
                logger.debug("All `derived_from` variables are available, running basefunc")
                newvals = basefunc(df, *args, **kwargs)
                if exists:
                    newvals = fillna_by_priority(oldvals = oldvals, newvals = newvals, priority = priority)

                fill_pct1 = get_filling_pct(newvals)
                logger.info(f"Filling degree after deriving variable: {get_pct_string(fill_pct1)}")
                if fill_pct0 and fill_pct1 < fill_pct0:
                    logger.warning(f"Filling degree for {name} went down by {get_pct_string(fill_pct0 - fill_pct1)} after deriving it againg")
                df[name] = newvals
                return df
            except Exception as err:
                logger.warning(f"Derivation of {name} failed, returning data as is!\nMessage: {err}")
                return df

    wrapper.__name__ = basefunc.__name__
    wrapper.__doc__ = (
        f"""{basefunc.__doc__}

            Args:
                df: The dataframe to attempt to insert {name} into.
                priority: String, either 'old' or 'new'. Indicates what values to keep, if {name} already exists.

            Returns:
                pd.DataFrame: The dataframe with {name} if we found {derived_from}.
        """
    )

    return wrapper
