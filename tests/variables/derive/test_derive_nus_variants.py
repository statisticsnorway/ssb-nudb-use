import pandas as pd

import nudb_use.variables.derive as derive
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.derive_decorator import get_derive_function


class DerivationFailures(Exception): ...


def test_derive_nus_variants(avslutta: pd.DataFrame) -> None:
    cols_orig = avslutta.columns
    failures = []

    for func_name in derive.__all__:
        if not func_name.endswith("_nus") and not func_name.endswith("_nus_label"):
            continue

        logger.info(f"Testing derive function: '{func_name}'...")
        func = get_derive_function(func_name)

        if not func:
            failures.append(f"Could not find derive function: '{func_name}'!")
            continue

        # This is ugly, since avslutta is mutable we cannot simply do
        # something like: `avslutta_new` = func(avslutta)', and use `avslutta`
        # in the next loop. For now we just keep the columns we started with
        # discarding derived columns
        avslutta = func(avslutta)

        if func_name not in avslutta.columns:
            failures.append(f"Derivation of '{func_name}' failed!")

        avslutta = avslutta[cols_orig]

    if failures:
        sep = "\n\t"
        logger.error(f"{len(failures)} derivations failed: \n\t{sep.join(failures)}")
        raise DerivationFailures(f"{len(failures)} derivations failed")
