import pandas as pd

from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use import logger, LoggerStack

from .grunnskolepoeng import check_grunnskolepoeng
from .nus2000         import check_nus2000
from .land            import check_land
from .kommune         import check_kommune
from .gro_elevstatus import check_gro_elevstatus
from .sn07 import check_sn07
from .unique_per_person import check_unique_per_person
from .pers_id_fnr import check_has_personal_ids

VARIABLE_CHECKS = [
    check_nus2000,
    check_grunnskolepoeng,
    check_land,
    check_kommune,
    check_gro_elevstatus,
    check_sn07,
    check_unique_per_person,
    check_has_personal_ids
]

# variable check functions should all take a dataframe as an argument
# They should follow a specific naming format.
#
# The main check function should follow this signature:
#   check_<variable>(df: pd.DataFrame, raise_errors: bool = False) -> list[NudbQualityError]
#
# The main check function [check_<variable>()] should call a set of individual check functions, with the signature:
#   subcheck_<variable>_<subname>(<individual pd.Series args>) -> NudbQualityError
#
# The individual pd.Series args should be fetched in the main function using the get_column function, which
# returns None, if the column is not present. If any of the individual pd.Series arguments are None
# the subcheck_<variable>_<subname>() should early return, not running any checks.
#
# Errors should be added to the error list in check_<variable> using the add_err2list() function, to ensure
# None values aren't added to the error lists


def run_all_specific_variable_tests(df: pd.DataFrame, raise_errors: bool = False, **kwargs) -> list[NudbQualityError]:
    """Check data for any erroneous 'nuskodes' and 'nuskode'/'hskode' combinations. 

    Combines the check_valid_nus and check_hskode_against_nus functions. 
    
    Args:
        df: Dataframe to check.
        raise_errors: If True, raises an exception group on validation errors;
                      otherwise, only logs warnings.
    Returns:
        err[NudbQualityError]: List of erroneous 'hskode' and 'nuskode' combinations, empty if none.
    
    Raises:
        NudbQualityError: Raised if invalid 'nuskode' or 'hskode' on 'nuscode' is found and `raise_errors` is True.
    """
    with LoggerStack("Running tests for specific variables"):
        errors = []
    
        for check in VARIABLE_CHECKS:
            errors += check(df, **kwargs)
        
        if errors and raise_errors:
            raise_exception_group(errors)
        elif not errors:
            logger.info("All tests for specific variables passed.")
    
        return errors
