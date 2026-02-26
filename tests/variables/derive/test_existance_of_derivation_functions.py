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

        uses_external_dataset = bool(settings.variables[confvar].derived_uses_datasets)
        if uses_external_dataset:
            logger.error("At some point we want to remove this from the test yes?")

        if derived_from and not uses_external_dataset and not has_func:
            missing.append(confvar)

    if missing:
        n = len(missing)
        message = f"{n} derived variables in the config are missing derivation functions!: {missing}"
        logger.error(message)
        raise MissingDerivationFunctions(message)
