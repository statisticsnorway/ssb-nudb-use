from __future__ import annotations

import string
from typing import Any

import numpy as np
import pandas as pd

from nudb_use import logger

LETTERS = pd.Series(list(string.ascii_lowercase) + list(string.ascii_uppercase))
NUMBERS = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
DEFAULT_SEED = 2384972


def mutated_extra_codes(
    codes: pd.Series, coverage_pct: float = 0.2, r: int | None = None
) -> pd.Series[Any]:
    letters = pd.Series(list(set(codes.str.cat())), dtype="string[pyarrow]")
    if letters.empty:
        logger.warning(
            "No characters available to generate extra codes; returning original codes."
        )
        return codes
    width = codes.str.len().max()

    if not r:
        r = 10 * len(codes)
    valid_extra_codes = pd.Series([], dtype="string[pyarrow]")

    if coverage_pct >= 1:
        coverage_pct = 0.99
    elif coverage_pct <= 0:
        coverage_pct = 0.01

    total_n = int(codes.shape[0] / (1 - coverage_pct))
    extra_n = max(1, int(total_n * coverage_pct))  # pct of total number of codes

    for i in range(100):
        if valid_extra_codes.shape[0] >= extra_n:
            break

        all_codes = pd.concat([codes, valid_extra_codes])
        extra_codes = pd.Series(np.repeat("", r))

        for _ in range(width):
            extra_codes += letters.sample(r, replace=True).reset_index(drop=True)

        valid = ~extra_codes.isin(all_codes)
        valid_extra_codes = pd.concat(
            [
                valid_extra_codes.astype("string[pyarrow]"),
                extra_codes[valid].astype("string[pyarrow]"),
            ]
        )

        available = list(set(LETTERS) | set(NUMBERS) - set(letters))
        if i >= 99:
            logger.warning("max iter reached!")
            break
        elif i % 2 == 0 and available:
            letters = pd.concat(
                [letters, pd.Series([available[0]], dtype="string[pyarrow]")],
                ignore_index=True,
            )
        elif i % 5 == 0:
            width += 1

    return valid_extra_codes.iloc[0:extra_n]
