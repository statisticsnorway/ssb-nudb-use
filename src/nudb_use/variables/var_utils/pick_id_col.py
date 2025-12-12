"""Helpers for detecting which identifier column is in use."""

from typing import Literal

import pandas as pd


def detect_snr_fnr_used(
    snr_fnr: pd.Series,
) -> Literal["snr"] | Literal["fnr"]:
    """Detect if the user sent in a column of snr (snr) or fnr.

    Args:
        snr_fnr: The column to check.

    Returns:
        Literal["snr"] | Literal["fnr"]: Literal string indicating which id
        was detected.

    Raises:
        ValueError: If the lengths dont match any of our expectations.
    """
    mode_values = snr_fnr.str.strip().str.len().mode()
    if mode_values.empty:
        raise ValueError("Unable to determine a common identifier width.")
    common_width = int(mode_values.iloc[0])
    if common_width == 7:
        return "snr"
    elif common_width == 11:
        return "fnr"
    else:
        raise ValueError(
            f"What sort of personal id has {common_width} as the most common width? We only support snr (snr, 7 chars) or fnr (11 chars)."
        )
