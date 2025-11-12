import pandas as pd
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use import logger, LoggerStack
from .utils import add_err2list, get_column, args_have_None


def check_gro_elevstatus(df: pd.DataFrame, **kwargs) -> list[NudbQualityError]:
    with LoggerStack("Validating specific variable: gro_elevstatus"):
        utd_utdanningstype = get_column(df, col = "utd_utdanningstype")
        gro_elevstatus = get_column(df, col = "gro_elevstatus")
    
        errors = []
        add_err2list(errors, subcheck_elevstatus_utd_211(utd_utdanningstype, gro_elevstatus))
        
        return errors
    

def subcheck_elevstatus_utd_211(utd_utdanningstype: pd.Series, gro_elevstatus: pd.Series) -> NudbQualityError | None:
    if args_have_None(utd_utdanningstype = utd_utdanningstype, gro_elevstatus = gro_elevstatus):
        return None

    wrong = (utd_utdanningstype == "211") & (gro_elevstatus == "M")

    if not wrong.sum():
        return None

    err_msg = f'Where `utd_utdanningstype`is 211, `gro_elevstatus` should be E, not M.'
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
    
