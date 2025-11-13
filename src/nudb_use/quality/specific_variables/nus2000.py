from typing import Any

import pandas as pd

from nudb_use import LoggerStack
from nudb_use import logger
from nudb_use import settings as settings_use
from nudb_use.exceptions.exception_classes import NudbQualityError

from .utils import add_err2list
from .utils import args_have_None
from .utils import get_column


def check_nus2000(df: pd.DataFrame, **kwargs) -> list[NudbQualityError]:

    with LoggerStack("Validating for specific variable: nus2000"):
        # Get needed variables
        nus2000 = get_column(df, col="nus2000")
        uh_institusjon_id = get_column(df, col="uh_institusjon_id")
        utd_skoleaar_start = get_column(df, col="utd_skoleaar_start")

        errors = []
        add_err2list(errors, subcheck_nus2000_valid_nus(nus2000))
        add_err2list(errors, subcheck_nus2000_valid_range(nus2000, **kwargs))
        add_err2list(
            errors,
            subcheck_nus2000_uh_institusjon_id_against_nus(
                uh_institusjon_id_col=uh_institusjon_id,
                nus_col=nus2000,
                utd_skoleaar_start_col=utd_skoleaar_start,
            ),
        )

        return errors


def subcheck_nus2000_valid_nus(col: pd.Series | None) -> NudbQualityError | None:
    """Check if a 'nuskode' is invalid.

    Codes should only contain 6 integers, and only start with either
    ‘1','2','3','4','5','6','7', or '8'.

    Args:
        col:
        raise_errors: If True, raises an exception group on validation errors;
                      otherwise, only logs warnings.

    Returns:
        err[NudbQualityError]: List of erroneous 'nuskode's, empty if none.
    """
    if args_have_None(nus2000=col):
        return None

    # nus2000: første siffer skal kun være ‘1','2','3','4','5','6','7','8'
    # nus2000 skal være 6 siffer lang, og kun tallsiffer
    nus_maske_ok = (col.str[0].isin([str(x) for x in range(1, 9)])) & (
        col.str.len() == 6
    )
    non_valid_nus = ", ".join(list(col[~nus_maske_ok].unique()))

    if len(non_valid_nus):
        err = NudbQualityError(f"Nonvalid nuscodes: {non_valid_nus}")
        return err

    return None


def subcheck_nus2000_valid_range(
    nus_col: pd.Series,
    range_valid_nus: range | None = None,
    **kwargs: Any,
) -> NudbQualityError | None:
    lowest_nus: int = int("099901")  # 6-digit lower clamp
    highest_nus: int = int("999999")  # 6-digit upper clamp

    # Prefer explicit kwarg
    if range_valid_nus is None and "range_valid_nus" in kwargs:
        range_valid_nus = kwargs.get("range_valid_nus")
        range_valid_nus = range(range_valid_nus.start, range_valid_nus.stop + 1)

    # Try settings if dataset_name given
    if range_valid_nus is None and "dataset_name" in kwargs:
        logger.info(
            f"Trying to fetch nus2000 valid range from config for dataset {kwargs['dataset_name']}."
        )
        min_vals = settings_use.datasets[kwargs["dataset_name"]].get("min_values")
        max_vals = settings_use.datasets[kwargs["dataset_name"]].get("max_values")
        min_val: int | None = int(min_vals.get("nus2000")) if min_vals else None
        max_val: int | None = int(max_vals.get("nus2000")) if max_vals else None
        if min_val is not None and max_val is not None:
            # Interpret config bounds as inclusive
            range_valid_nus = range(min_val, max_val + 1)

    # Reasonable default if still None: digit range 0..9 (inclusive)
    if range_valid_nus is None:
        range_valid_nus = range(0, 10)

    logger.info(f"Nus2000 digit range defined as {range_valid_nus!r}")

    # Derive 6-digit bounds from the *first digit* range (e.g., 5..7 -> 500000..799999)
    first_digit: int = range_valid_nus.start
    last_digit_inclusive: int = range_valid_nus.stop - 1
    range_level5_min: int = int(str(first_digit).ljust(6, "0"))
    range_level5_max: int = int(str(last_digit_inclusive).ljust(6, "9"))

    # Clamp to global bounds
    range_level5_min = max(range_level5_min, lowest_nus)
    range_level5_max = min(range_level5_max, highest_nus)

    logger.info(
        f"Looking for nus2000 outside the defined valid range of "
        f"{range_level5_min:06d} - {range_level5_max:06d}"
    )

    # Clean to nullable integers (no NumPy)
    vals: pd.Series = pd.to_numeric(nus_col, errors="coerce").astype("Int64")

    # Correct mask: outside the range; handle NA in comparisons
    mask: pd.Series = ((vals < range_level5_min) | (vals > range_level5_max)).fillna(
        False
    )

    invalid_vals: pd.Series = pd.Series(vals[mask].dropna().astype(int).unique())
    invalid_nus: list[str] = invalid_vals.map(lambda v: f"{v:06d}").tolist()

    if not invalid_nus:
        return None

    err_msg: str = (
        f"Found nus2000 outside specified range of "
        f"{range_level5_min:06d}-{range_level5_max:06d}, "
        f"here are the invalid codes: {invalid_nus}"
    )
    logger.warning(err_msg)
    return NudbQualityError(err_msg)


def subcheck_nus2000_uh_institusjon_id_against_nus(
    uh_institusjon_id_col: pd.Series,
    nus_col: pd.Series,
    utd_skoleaar_start_col: pd.Series,
) -> NudbQualityError | None:
    """Check if the value of 'uh_institusjon_id' is either "NaN" or "999" if the value of 'nuskode' is either "6", "7", or "8".

    Args:
        uh_institusjon_id_col: Columndata containing 'uh_institusjon_id'
        nus_col: Columndata containing 'nuskode'.
        raise_errors: If True, raises an exception group on validation errors;
                      otherwise, only logs warnings.

    Returns:
        err[NudbQualityError]: List of erroneous 'hskode' and 'nuskode' combinations, empty if none.
    """
    if args_have_None(
        hskode=uh_institusjon_id_col,
        nus2000=nus_col,
        utd_skoleaar_start=utd_skoleaar_start_col,
    ):
        return None

    # hskode: Når første siffer i nus er 6, 7, eller 8, så skal ikke hskode være blankt eller ‘999'.
    # hskode ble ikke inført før i 1994, så testen er ikke valid før det.
    invalid_hskode_mask = (
        nus_col.str[0].isin(["6", "7", "8"])
        & ((uh_institusjon_id_col.isna()) | (uh_institusjon_id_col == "999"))
        & (utd_skoleaar_start_col.astype("Int64") >= 1994)
    )

    nuscodes_nonvalid_hskode = nus_col[invalid_hskode_mask].unique()
    if len(nuscodes_nonvalid_hskode):
        err = NudbQualityError(
            f"Found invalid {uh_institusjon_id_col.name} on nuscodes (when first digit of nus2000 is 6, 7 or 8, {uh_institusjon_id_col.name} should not be empty or 999). Take a look at these nuscodes: {nuscodes_nonvalid_hskode}"
        )

        return err

    return None
