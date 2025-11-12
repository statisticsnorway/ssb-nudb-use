import pandas as pd
from nudb_config import settings
from nudb_use.exceptions.exception_classes import NudbQualityError
from .utils import add_err2list, get_column, args_have_None
from nudb_use import logger, LoggerStack

LAND_VARS = [var_name for var_name, var_details in settings.variables.items() if var_details.get("klass_codelist") == 91]


def check_land(df: pd.DataFrame, **kwargs) -> list[NudbQualityError]:
    errors: list[NudbQualityError] = []
    
    land_vars_in_df = [col for col in df.columns.str.lower() if col in LAND_VARS]
    if not land_vars_in_df:
        return errors

    with LoggerStack(f"Validating country (land) variables that link to klass codelist 91 {LAND_VARS}"):
        for land_col_name in land_vars_in_df:
            land_col = get_column(df, col = land_col_name)
            add_err2list(errors, subcheck_landkode_000(land_col, land_col_name))
        return errors

def subcheck_landkode_000(land_col: pd.Series, col_name: str) -> NudbQualityError | None:
    if args_have_None(land_col=land_col, col_name=col_name):
        return None
        
    illegal_vals = ["000"]
    err_results = [val for val in land_col.unique() if 
                   pd.isna(val) and 
                   isinstance(val, str) and 
                   val in illegal_vals]
    if not err_results:
        return None
    return NudbQualityError(f'Found landkode for Norway in {col_name}, are you sure it should be there? (Is it a valid "studieland" if that relates to the column?')

