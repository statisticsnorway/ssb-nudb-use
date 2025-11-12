import pandas as pd
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use import logger, LoggerStack
from .utils import add_err2list, get_column, args_have_None


def check_sn07(df: pd.DataFrame, **kwargs) -> list[NudbQualityError]:
    with LoggerStack("Validating for specific variable: sn07"):
        sn07 = get_column(df, col = "sn07")
    
        errors = []
        add_err2list(errors, subcheck_sn07_bad_value(sn07))
        
        return errors


def subcheck_sn07_bad_value(sn07: pd.Series) -> NudbQualityError | None:
    if args_have_None(sn07_col = sn07):
        return None
    
    # Find the unique codes in the column
    sn07_unique = [v for v in sn07.unique() if not pd.isna(v) and v]
    if not sn07_unique:
        logger.debug("No values found in sn07-column, exiting check early.")
        return None
    
    wrong_vals = {"992580": "Ukjent næring i Utlandet?"}
    wrong_used = {k: v for k, v in wrong_vals.items() if k in sn07_unique}
    
    if not wrong_used:
        return None

    err_msg = f'Weird SN07 values used in data: {wrong_used}'
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
    
