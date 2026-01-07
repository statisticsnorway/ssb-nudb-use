from nudb_config import settings

from nudb_use.nudb_logger import logger
from nudb_use.variables.derive import __all__ as derivation_functions


class MissingDerivationFunctions(Exception): ...


def test_existence_of_derivation_functions() -> None:
    confvars = settings.variables.keys()
    missing = []

    for confvar in confvars:
        has_func = confvar in derivation_functions
        derived_from = settings.variables[confvar].derived_from

        if derived_from and not has_func:
            missing.append(confvar)

    if missing:
        n = len(missing)
        message = f"{n} derived variables in the config are missing derivation functions!\n{confvars}"
        logger.error(message)
        raise MissingDerivationFunctions(message)
