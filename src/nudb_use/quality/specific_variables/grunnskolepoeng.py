import pandas as pd

from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use import logger, LoggerStack

from .utils import add_err2list, get_column, args_have_None


def check_grunnskolepoeng(df: pd.DataFrame, **kwargs) -> list[NudbQualityError]:
    with LoggerStack("Validating specific variable: gr_grunnskolepoeng"):
        grunnskolepoeng = get_column(df, "gr_grunnskolepoeng")
        pers_id = get_column(df, "pers_id")
        
        errors = []
        
        add_err2list(errors, subcheck_grunnskolepoeng_maxval(grunnskolepoeng))
        
        return errors


def subcheck_grunnskolepoeng_maxval(grunnskolepoeng: pd.Series, max_poeng: int = 70) -> NudbQualityError | None:
    if args_have_None(grunnskolepoeng = grunnskolepoeng):
        return None
    
    ok = (grunnskolepoeng.astype("Int64") <= max_poeng).all()
    if not ok:
        err_msg = f"Found values in grunnskolepoeng larger than {max_poeng}!"
        logger.warning(err_msg)
        return NudbQualityError(err_msg)

    return None