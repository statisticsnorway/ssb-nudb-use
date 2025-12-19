import datetime

import dateutil.parser
import klass
import pandas as pd
from klass.requests.klass_types import VersionPartType
from nudb_config.pydantic.variables import Variable

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import logger

from .klass_utils import _outside_codes_handeling
from .klass_utils import find_earliest_latest_klass_version_date


def klass_variant_search_term_mapping(
    var_meta: Variable,
    key: str = "code",
    value: str = "parentCode",
    select_level: int | None = None,
) -> dict[str, str]:
    """Subfunction to get the mapping from klass for the variable.

    Args:
        var_meta: The variable metadata from the settings relating to the derived variable.
        key: key argument passed to 'klass.KlassVariant.to_dict'
        value: value argument passed to 'klass.KlassVariant.to_dict'
        select_level: select_level argument passed to 'klass.KlassVariant.to_dict'

    Returns:
        dict[str, str]: The mapping dict from klass.

    Raises:
        TypeError: If the klass_codelist in the config for the variable is not an int, or klass_variant_search_term is not a str.
        ValueError: If no variant or multiple variants match the search term.
    """
    klass_codelist_maybe_none: int | None = var_meta.klass_codelist
    if not isinstance(klass_codelist_maybe_none, int):
        raise TypeError(
            "klass_codelist on variable sent to klass_variant_search_term_mapping must be an int in the config."
        )
    else:
        klass_codelist: int = klass_codelist_maybe_none

    search_term_maybe_none: str | None = var_meta.klass_variant_search_term
    if not isinstance(search_term_maybe_none, str):
        raise TypeError(
            "search_term on variable sent to klass_variant_search_term_mapping must be a str in the config."
        )
    else:
        search_term: str = search_term_maybe_none

    version = klass.KlassClassification(
        klass_codelist
    ).get_version()  # Future development: Could we support "refdate" in the klass package on this to get the version by date?

    found_variants = {
        k: v
        for k, v in version.variants_simple().items()
        if v.lower().startswith(
            search_term.lower()
        )  # klass-package does an "in" here, while we need a startswith because of "Varighet" is defined twice as a variant in nus2000
    }
    if len(found_variants) != 1:
        err_msg = f"When searching for a variant that matches your search, we did not find a single match. If you got multiple matches, be more specific in your search term: {list(found_variants.values())}"
        raise ValueError(err_msg)

    variant = version.get_variant(next(iter(found_variants.keys())))
    # Should we log the amount of codes that do not map to a grouping in the variant?

    data = variant.data

    if select_level is not None:
        data = data[data["level"].astype("Int64") == select_level]

    keys = data[key]
    values = data[value]

    return dict(zip(keys, values, strict=True))


def _check_klass_variant_column_id(
    series: pd.Series, col: str, klass_variant: int
) -> list[NudbQualityError]:
    codes = set(
        x.strip() for x in klass.KlassVariant(variant_id=klass_variant).to_dict().keys()
    )
    return _outside_codes_handeling(series=series, codes=codes, col=col)


def _check_klass_variant_column_search_term(
    series: pd.Series,
    col: str,
    klass_codelist: int,
    klass_variant_search_term: str,
    klass_codelist_from_date: str | None,
    data_time_start: str | None,
    data_time_end: str | None,
) -> list[NudbQualityError]:

    # Lets figure out what our refdate for the version should be
    refdate: str
    if klass_codelist_from_date:
        refdate = klass_codelist_from_date
    elif data_time_end:
        refdate = data_time_end
    elif data_time_start:
        refdate = data_time_start
    else:
        _first_date, refdate = find_earliest_latest_klass_version_date(klass_codelist)
    refdate_datetime = dateutil.parser.parse(refdate)

    # Go backwards from the future until we find an earlier date
    classification = klass.KlassClassification(klass_codelist)
    date_keyed: dict[datetime.datetime, VersionPartType] = {
        dateutil.parser.parse(version_part["validFrom"]): version_part
        for version_part in classification.versions
    }
    date_keyed_sorted_reversed = {k: date_keyed[k] for k in sorted(date_keyed)[::-1]}

    ver_final: None | VersionPartType = None
    ver_date: datetime.datetime
    for ver_date, ver in date_keyed_sorted_reversed.items():
        if ver_date <= refdate_datetime:
            ver_final = ver
            break

    if ver_final is None:
        raise KeyError(
            f"Couldnt find a version for classification {klass_codelist}, that matches refdate {refdate}."
        )
    ver_id: int = ver_final["version_id"]
    version = klass.KlassVersion(ver_id)
    variant = version.get_variant(search_term=klass_variant_search_term)
    logger.info(
        f"For `{col}` found a klass-variant with id {variant.variant_id}, dated {variant.validFrom}, with variant-name {variant.name}, based on search-term {klass_variant_search_term}."
    )

    codes = set(x.strip() for x in variant.to_dict().keys())
    return _outside_codes_handeling(series=series, codes=codes, col=col)
