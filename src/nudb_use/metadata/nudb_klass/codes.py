"""Fetch and validate KLASS classification codes."""

from typing import Any
from typing import cast

import klass
import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group
from nudb_use.metadata.nudb_config.get_variable_info import get_var_metadata
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

from .klass_utils import _include_codelist_extras
from .klass_utils import _outside_codes_handeling
from .klass_utils import _resolve_date_range
from .variants import _check_klass_variant_column_id
from .variants import _check_klass_variant_column_search_term


def get_klass_codes(
    klassid: int,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
) -> list[str]:
    """Fetch code dictionaries for a classification scheme within a given time range.

    Args:
        klassid: Identifier from Klass.
        data_time_start: Start date (YYYY-MM-DD) for code filtering.
        data_time_end: End date (YYYY-MM-DD) for code filtering.

    Returns:
        list[str]: List of codes for the classification.

    Raises:
        ValueError: If data_time_end is specified, but not data_time_start.
    """
    logger.info(
        f"Getting klass-codes for date-range: {data_time_start} -> {data_time_end}"
    )
    if data_time_start is None and data_time_end is None:
        code_obj = klass.KlassClassification(klassid).get_codes()
    elif data_time_end is None:
        code_obj = klass.KlassClassification(klassid).get_codes(
            from_date=data_time_start
        )
    elif data_time_start is not None and data_time_end is not None:
        code_obj = klass.KlassClassification(klassid).get_codes(
            from_date=data_time_start, to_date=data_time_end
        )
    else:
        raise ValueError(
            "If you specify the end, you MUST also specify the start date (or just the start)."
        )

    last_level = sorted(code_obj.data["level"].unique())[-1]
    filtered_to_last_level = code_obj.data[code_obj.data["level"] == last_level].copy()

    return list(filtered_to_last_level["code"].str.strip().unique())


def check_klass_codes(
    df: pd.DataFrame,
    data_time_start: str | None = None,
    data_time_end: str | None = None,
    raise_errors: bool = True,
) -> list[NudbQualityError]:
    """Validate DataFrame columns against KLASS codelists from metadata.

    Args:
        df: DataFrame containing columns to be validated.
        data_time_start: Start date for restricting code validation, overrides metadata.
        data_time_end: End date for restricting code validation, overrides metadata.
        raise_errors: If True, raises an exception group on validation errors;
                      otherwise, only logs warnings.

    Returns:
        list[NudbQualityError]: List of quality errors detected during
        validation, empty if none.

    """
    with LoggerStack("Checking if column-content matches codelists in KLASS"):
        metadata = get_var_metadata()
        errors: list[NudbQualityError] = []
        for col in df.columns:
            if pd.api.types.is_bool_dtype(df[col]):
                logger.info(
                    f"Column {col} is a BOOLEAN, and should not have an associated codelist in KLASS. Skipping checking the column against klass."
                )
            else:
                errors.extend(
                    _check_column_against_klass(
                        df[col],
                        col,
                        metadata,
                        data_time_start=data_time_start,
                        data_time_end=data_time_end,
                    )
                )
        if raise_errors:
            raise_exception_group(errors)
        else:
            warn_exception_group(errors)

        return errors


def _check_column_against_klass(
    series: pd.Series,
    col: str,
    metadata: pd.DataFrame,
    data_time_start: str | None,
    data_time_end: str | None,
) -> list[NudbQualityError]:
    """Validate a single column against its configured KLASS codes."""
    if col not in metadata.index:
        logger.info(f"Not checking `{col}`, not in nudb_config!")
        return []

    # Because we are storing metadata in a pandas dataframe, we gotta be stupid?
    def _type_narrow_meta(x: Any) -> int | str | None:
        if x is None or pd.isna(x):
            return None
        if isinstance(x, float | int):  # There are no floats in these fields mah dude
            return int(x)
        if isinstance(x, str):
            return x
        raise TypeError(
            f"Cant coerce variable metadata field to type expected: {type(x)}"
        )

    def _type_narrow_dict_meta(x: Any) -> dict[str, str] | None:
        if x is None or pd.isna(x):
            return None
        if (
            isinstance(x, dict)
            and all(isinstance(k, str) for k in x)
            and all(isinstance(v, str) for v in x.values())
        ):
            return x
        raise TypeError(
            f"Cant coerce variable metadata dict-field to type expected: {type(x)}"
        )

    klass_codelist = cast(
        int | None, _type_narrow_meta(metadata.loc[col, "klass_codelist"])
    )
    klass_codelist_from_date = cast(
        str | None, _type_narrow_meta(metadata.loc[col, "klass_codelist_from_date"])
    )
    klass_variant = cast(
        int | None, _type_narrow_meta(metadata.loc[col, "klass_variant"])
    )
    klass_variant_search_term = _type_narrow_meta(
        metadata.loc[col, "klass_variant_search_term"]
    )
    codelist_extras = _type_narrow_dict_meta(metadata.loc[col, "codelist_extras"])

    result: list[NudbQualityError]
    # If variant fields are filled, we want to get the codelits to check from the variant instead
    # Prioritize a variant ID if filled
    if klass_variant is not None:
        result = _check_klass_variant_column_id(
            series=series, col=col, klass_variant=klass_variant
        )
    # If we have a search-term, we also need the classification to search under
    elif (
        klass_variant_search_term is not None
        and isinstance(klass_variant_search_term, str)
        and isinstance(klass_codelist, int)
        and klass_codelist
    ):
        result = _check_klass_variant_column_search_term(
            series=series,
            col=col,
            klass_codelist=klass_codelist,
            klass_variant_search_term=klass_variant_search_term,
            klass_codelist_from_date=klass_codelist_from_date,
            data_time_start=data_time_start,
            data_time_end=data_time_end,
        )
    elif klass_variant_search_term:
        raise ValueError(
            f"For variable `{col}`: If klass_variant_search_term is filled, we need valid content in klass_codelist (int above 0), otherwise there is no classification to search for the variant under."
        )
    elif klass_codelist and isinstance(klass_codelist, int):
        result = _check_klass_codelist_codes(
            series=series,
            col=col,
            klass_codelist=klass_codelist,
            klass_codelist_from_date=klass_codelist_from_date,
            data_time_start=data_time_start,
            data_time_end=data_time_end,
            codelist_extras=codelist_extras,
        )
    else:
        logger.debug(
            f"Not checking `{col}`, its not supposed to have a codelist, the codelist-int is 0 or non-valid: {klass_codelist}, or there is no klass_variant_id {klass_variant}. (For a klass-variant-search term, we need both klass_codelist and klass_variant_search_term)."
        )
        result = []
    return result


def _check_klass_codelist_codes(
    series: pd.Series,
    col: str,
    klass_codelist: int,
    klass_codelist_from_date: str | None,
    data_time_start: str | None,
    data_time_end: str | None,
    codelist_extras: dict[str, str] | None,
) -> list[NudbQualityError]:
    logger.info(f"Checking `{col}`, found codelist ID: {klass_codelist}!")
    from_date, to_date = _resolve_date_range(
        klass_codelist,
        klass_codelist_from_date,
        data_time_start,
        data_time_end,
    )
    codes = get_klass_codes(
        klass_codelist, data_time_start=from_date, data_time_end=to_date
    )
    codes = _include_codelist_extras(codes, codelist_extras)
    return _outside_codes_handeling(series=series, codes=set(codes), col=col)
