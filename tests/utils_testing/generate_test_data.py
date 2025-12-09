import datetime
import string
from collections.abc import Generator
from functools import lru_cache

import klass
import numpy as np
import pandas as pd
import pytest
from nudb_config import settings

from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.nudb_logger import logger

from .mutate_codelist import mutated_extra_codes

YieldDataFrame = Generator[pd.DataFrame, None, None]


LETTERS = pd.Series(list(string.ascii_lowercase) + list(string.ascii_uppercase))
DEFAULT_SEED = 2384972


PREDEFINED_CODES_NEWNAME = {
    "utd_skoleaar_start": [str(yr) for yr in range(1950, datetime.datetime.now().year)],
}


def generate_test_variable(
    name: str,
    n: int = 100_000,
    add_klass_errors: bool = False,
    add_old_cols: bool = True,
    seed: int = DEFAULT_SEED,
) -> tuple[str, pd.Series]:  # (newname, values)

    if name not in settings.variables.keys():
        raise ValueError(f"Unable to find '{name}' in config!")

    rng = np.random.default_rng(seed=seed)
    metadata = settings.variables[name]
    codelist = metadata.klass_codelist
    length = metadata.length
    dtype = metadata.dtype

    renamed_from = metadata.renamed_from
    has_codelist = codelist is not None and codelist != 0
    has_length = length is not None
    has_rename = renamed_from is not None

    if has_codelist:
        codes = klass.get_classification(codelist).get_codes().data["code"]

        if has_length:
            codes = codes[codes.str.len().isin(length)]

        if add_klass_errors:
            codes = pd.concat([codes, mutated_extra_codes(codes, coverage_pct=0.2)])
    else:

        match dtype:
            case "STRING":
                if name in PREDEFINED_CODES_NEWNAME:
                    codes = pd.Series(PREDEFINED_CODES_NEWNAME[name]).astype(
                        "string[pyarrow]"
                    )
                else:
                    codes = pd.Series(np.repeat([""], n)).astype("string[pyarrow]")
                    for _i in range(
                        length[0] if has_length else 2
                    ):  # Mange koder fra kodelister er 2 brei?
                        rletters = LETTERS.sample(n=n, random_state=rng, replace=True)
                        codes += rletters.reset_index(drop=True)
            case "INTEGER":
                codes = pd.Series(np.arange(n)).astype("Int64")
            case "FLOAT":
                codes = (pd.Series(np.arange(n)) + 0.28372).astype("Float64")
            case "BOOLEAN":
                codes = pd.Series([True, False]).astype("bool[pyarrow]")
            case "DATETIME":
                ky = 30
                km = 12
                endy = 2024
                starty = endy - ky
                years = np.repeat(np.arange(starty, endy), km).astype("U")
                months = np.tile(np.arange(1, km + 1), ky).astype("U")
                codes = pd.to_datetime(pd.Series(years + "-" + months + "-01")).astype(
                    "datetime64[s]"
                )
            case _:
                raise TypeError(f"Unknown dtype: {dtype}!")

    newname: str = renamed_from[0] if has_rename and add_old_cols else name
    pdtype: str = DTYPE_MAPPINGS["pandas"][dtype]
    values: pd.Series = codes.sample(n=n, random_state=rng, replace=True).astype(pdtype)  # type: ignore
    values = values.reset_index(drop=True)

    return newname, values


# This is costly, and should be a pure function, so lets try caching the results
@lru_cache(maxsize=50)
def generate_test_data(
    dataset: str,
    n: int = 100_000,
    add_klass_errors: bool = True,
    add_old_cols: bool = True,
    add_non_nudb_vars: bool = True,
    add_bad_widths: bool = True,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:

    variables = settings.datasets[dataset].variables

    cols = {}
    for var in variables:
        newname, values = generate_test_variable(
            name=var,
            n=n,
            add_klass_errors=add_klass_errors,
            add_old_cols=add_old_cols,
            seed=seed,
        )

        cols[newname] = values
        logger.info(f"Generation of {var} worked!")

    return pd.DataFrame(cols)


@pytest.fixture
def avslutta() -> YieldDataFrame:
    yield generate_test_data("avslutta").copy(deep=True)


@pytest.fixture
def igang() -> YieldDataFrame:
    yield generate_test_data("igang").copy(deep=True)


@pytest.fixture
def eksamen() -> YieldDataFrame:
    yield generate_test_data("eksamen").copy(deep=True)
