"""Helpers for detecting which identifier column is in use."""

from typing import Literal

import pandas as pd


def detect_pers_id_fnr_used(
    pers_id_fnr: pd.Series,
) -> Literal["pers_id"] | Literal["fnr"]:
    """Detect if the user sent in a column of pers_id (snr) or fnr.

    Args:
        pers_id_fnr: The column to check.

    Returns:
        "pers_id" | "fnr": Returns a literal string of one of these dependant on what we detected.

    Raises:
        ValueError: If the lengths dont match any of our expectations.
    """
    mode_values = pers_id_fnr.str.strip().str.len().mode()
    if mode_values.empty:
        raise ValueError("Unable to determine a common identifier width.")
    common_width = int(mode_values.iloc[0])
    if common_width == 7:
        return "pers_id"
    elif common_width == 11:
        return "fnr"
    else:
        raise ValueError(
            f"What sort of personal id has {common_width} as the most common width? We only support pers_id (snr, 7 chars) or fnr (11 chars)."
        )
