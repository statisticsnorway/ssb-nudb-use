import pandas as pd
from nudb_use import config
from nudb_use import logger, LoggerStack
from nudb_use.exceptions.groups import raise_exception_group, warn_exception_group

class WidthMismatch(Exception):
    ...

def check_column_widths(df: pd.DataFrame,
                        widths: dict[str, list[int]] | None = None,
                        ignore_na: bool = False,
                        raise_errors: bool = True) -> list[WidthMismatch]:
    """Validate that the string lengths in specified DataFrame columns match expected widths.

    Note: ignore_na is not implemented in the function. 
    
    Args:
        df: DataFrame to check.
        widths (dict[str, list[int]]: A dictionary mapping column names to 
            lists of acceptable string lengths. If None or invalid, falls back to
            values from config..
        ignore_na: If True, NaN or missing values are ignored in validation.
        raise_errors: If True, raises a grouped exception for all mismatches
                      found; if False, returns the list of mismatches and logs warnings.

    Returns:
        list[WidthMismatch]: A list of `WidthMismatch` errors, one for each column that
            contains values not conforming to the expected widths. Empty if all values pass.

    Raises:
        WidthMismatch: Raised as an exception group if any column contains string lengths
            outside of the allowed range and `raise_errors` is True.
    """
    with LoggerStack("Checking the widths of values in columns according to a dict sent in or gotten from the config."):
        if (isinstance(widths, dict) and 
            all(isinstance(k, str) for k in widths) and
            all(isinstance(v, list) for v in widths.values()) and
            all(isinstance(i, int) for int_list in widths.values() for i in int_list)
           ):
            widths_def: dict[str, list[int]] = widths
            
        else:
            logger.info("widths does not match datatype, getting widths from config.")
            widths_def: dict[str, int] = {col: var_info.length 
                           for col, var_info 
                           in config.settings.variables.items() 
                           if col in df.columns and "length" in var_info}

        widths_def_str = str(widths_def).replace(",", ",\n")
        logger.debug(f"widths_def:\n{widths_def_str}")
        
        # Check for variables in the dataframe, that are not defined in the config?
        errors = []
        maxprint = 50
        for col, widths in widths_def.items():
            if not widths:
                continue

            logger.debug(col)
            #display(~df[col])
            len_mask_diff = (~df[col].str.len().isin(widths)) & (~df[col].isna())
            if len_mask_diff.sum():
                first_values = pd.Series(df[len_mask_diff][col].unique()).head(maxprint) # pd.Series.unique() doesn't return a Series object if dtype is a pyarrow type
                unique_mismatch_vals = ",\n".join(list(first_values))
                too_many_message = f"first {maxprint}" if len(unique_mismatch_vals) > maxprint else ""
                errors.append(WidthMismatch(f"In {col} found values not of the defined widths: {widths}, the {too_many_message} mismatched codes:\n{unique_mismatch_vals}"))

        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)
            
        return errors
