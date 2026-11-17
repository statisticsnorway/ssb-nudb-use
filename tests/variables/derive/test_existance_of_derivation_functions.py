from nudb_config import settings

from nudb_use.nudb_logger import logger
from nudb_use.variables.derive import __all__ as derivation_functions

# These variables have dependency metadata but are derived by SQL dataset
# generators, rather than individual dataframe derivation functions.
SQL_DERIVED_VARIABLES = {
    # datasets/utd_hoeyeste.py
    "utd_foerste_aar",
    "utd_hoeyeste_dato",
    "utd_hoeyeste_aar",
    "utd_hoeyeste_nus2000_ujustert",
    # datasets/person.py
    "pers_aar_16",
    "far_snr",
    "mor_snr",
    # datasets/utd_foreldres_utdnivaa.py
    "far_utd_hoeyeste_rangering",
    "mor_utd_hoeyeste_rangering",
}


class MissingDerivationFunctions(Exception): ...


def test_existence_of_derivation_functions() -> None:
    confvars = settings.variables.keys()
    missing = []

    for confvar in confvars:
        has_func = confvar in derivation_functions
        derived_from = settings.variables[confvar].derived_from

        if derived_from and not has_func and confvar not in SQL_DERIVED_VARIABLES:
            missing.append(confvar)

    if missing:
        n = len(missing)
        message = f"{n} derived variables in the config are missing derivation functions!: {missing}"
        logger.error(message)
        raise MissingDerivationFunctions(message)
