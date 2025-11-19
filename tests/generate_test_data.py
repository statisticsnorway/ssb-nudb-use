import pandas as pd
import numpy as np
import klass
import pytest
import string

from nudb_config import settings
from nudb_use.metadata.nudb_config.get_dtypes import DTYPE_MAPPINGS
from nudb_use.nudb_logger import logger


LETTERS = pd.Series(list(string.ascii_lowercase) + list(string.ascii_uppercase))
DEFAULT_SEED = 2384972


def generate_test_variable(
        name,
        n                 = 1_000_000,
        add_klass_errors  = True,
        add_old_cols      = True,
        seed              = DEFAULT_SEED
    ) -> tuple[str | pd.Series]: # (newname, values)

    rng      = np.random.default_rng(seed=seed)
    metadata = settings.variables[name]
    codelist = metadata.klass_codelist
    length   = metadata.length
    dtype    = metadata.dtype

    renamed_from = metadata.renamed_from
    has_codelist = codelist is not None and codelist != 0
    has_length   = length is not None
    has_rename   = renamed_from is not None

    if has_codelist:
        codes = (
            klass
            .get_classification(codelist)
            .get_codes().data["code"]
        )

        if has_length:
            codes = codes[codes.str.len().isin(length)]

    else:

        match dtype:
            case "STRING":
                codes = LETTERS*length[0] if has_length else LETTERS
            case "INTEGER":
                codes = pd.Series(np.arange(n))
            case "FLOAT":
                codes = pd.Series(np.arange(n)) + 0.28372
            case "BOOLEAN":
                codes = pd.Series([True, False])
            case "DATETIME":
                ky     = 30
                km     = 12
                endy   = 2024
                starty = endy - ky
                years  = np.repeat(np.arange(starty, endy), km).astype("U")
                months = np.tile(np.arange(1, km + 1), ky).astype("U")
                codes  = pd.to_datetime(pd.Series(years + "-" + months + "-01"))
            case _:
                raise TypeError(f"Unknown dtype: {dtype}!")


    pdtype = DTYPE_MAPPINGS["pandas"][dtype]
    values = codes.sample(n=n, random_state=rng, replace = True).astype(pdtype)
    values = values.reset_index(drop=True)
    newname = renamed_from[0] if has_rename else name

    return newname, values


def generate_test_data(
        dataset,
        n                 = 1_000_000,
        add_klass_errors  = True,
        add_old_cols      = True,
        add_non_nudb_vars = True,
        add_bad_widths    = True,
        seed              = DEFAULT_SEED
    ) -> pd.DataFrame:

    rng = np.random.default_rng(seed=seed)

    variables = settings.datasets[dataset].variables

    cols = {}
    for var in variables:
        try:
            newname, values = generate_test_variable(
                    name = var,
                    n=n,
                    add_klass_errors=add_klass_errors,
                    add_old_cols=add_old_cols,
                    seed=seed)

            cols[newname] = values
        except Exception as err:
            logger.warning(f"Generation of {var} failed, with message: '{err}'...")

    return pd.DataFrame(cols)


@pytest.fixture
def test_avslutta():
    test_avslutta = generate_test_data("avslutta")
