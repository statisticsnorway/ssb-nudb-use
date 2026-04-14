import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

__all__ = [
    "pers_bokommune_16aar",
    "pers_bokommune_nr",
    "pers_bydel_nr",
    "pers_foedeland",
    "pers_foedselsdato",
    "pers_foerste_bosattdato",
    "pers_fra_land",
    "pers_gkrets_nr",
    "pers_innflyttingsdato",
    "pers_inngrunn1",
    "pers_invkat",
    "pers_kjoenn",
    "pers_landbak3gen",
    "pers_statsborgerskap",
]


def _apply_pers320_mapping(
    left: pd.DataFrame, name360: str, name320: str = ""
) -> pd.DataFrame:
    name320 = name320 or name360

    metadata = settings.variables[name360]
    join_keys = metadata.derived_join_keys or []
    datasets = metadata.derived_uses_datasets

    if not datasets or len(datasets) > 1:
        raise ValueError(
            f"Expected a single dataset, got: {metadata.derived_uses_datasets}"
        )

    dataset = datasets[0]
    keys_str = ", ".join(join_keys)

    right = (
        NudbData(dataset).select(f"DISTINCT {keys_str}, {name320} AS {name360}").df()
    )

    original_index = left.index
    merged = left.merge(right, how="left", on=join_keys, validate="m:1")
    if len(merged) != len(original_index):
        logger.warning(
            f"{name360}: row count changed during merge, unable to preserve original index."
        )
    else:
        merged = merged.set_axis(original_index)

    _x = name360 + "_x"
    _y = name360 + "_y"

    if _x in merged.columns and _y in merged.columns:
        logger.warning(f"{name360} already exists in data! Prioritizing new values...")
        merged[name360] = merged[_y].fillna(merged[_x])
        merged = merged.drop(columns=[_x, _y])

    merged[name360] = merged[name360].fillna(pd.NA)

    return merged


@wrap_derive
def pers_kjoenn(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_kjoenn."""
    return _apply_pers320_mapping(left=df, name360="pers_kjoenn", name320="kjoenn")


@wrap_derive
def pers_foedselsdato(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_foedselsdato."""
    return _apply_pers320_mapping(
        left=df, name360="pers_foedselsdato", name320="foedselsdato"
    )


@wrap_derive
def pers_bokommune_16aar(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive bokommune_16aar."""
    return _apply_pers320_mapping(
        left=df, name360="pers_bokommune_16aar", name320="komm_nr"
    )


@wrap_derive
def pers_bokommune_nr(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_bokommune_nr."""
    return _apply_pers320_mapping(
        left=df, name360="pers_bokommune_nr", name320="komm_nr"
    )


@wrap_derive
def pers_gkrets_nr(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_gkrets_nr."""
    return _apply_pers320_mapping(
        left=df, name360="pers_gkrets_nr", name320="gkrets_nr"
    )


@wrap_derive
def pers_bydel_nr(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_bydel_nr."""
    return _apply_pers320_mapping(left=df, name360="pers_bydel_nr", name320="bydel_nr")


@wrap_derive
def pers_invkat(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_invkat."""
    return _apply_pers320_mapping(left=df, name360="pers_invkat", name320="invkat")


@wrap_derive
def pers_inngrunn1(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_inngrunn1."""
    return _apply_pers320_mapping(
        left=df, name360="pers_inngrunn1", name320="inngrunn1"
    )


@wrap_derive
def pers_foerste_bosattdato(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_foerste_bosattdato."""
    return _apply_pers320_mapping(
        left=df, name360="pers_foerste_bosattdato", name320="foerste_bosattdato"
    )


@wrap_derive
def pers_innflyttingsdato(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_innflyttingsdato."""
    return _apply_pers320_mapping(
        left=df, name360="pers_innflyttingsdato", name320="innflyttingsdato"
    )


@wrap_derive
def pers_statsborgerskap(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_statsborgerskap."""
    return _apply_pers320_mapping(
        left=df, name360="pers_statsborgerskap", name320="statsborgerskap"
    )


@wrap_derive
def pers_foedeland(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_foedeland."""
    return _apply_pers320_mapping(
        left=df, name360="pers_foedeland", name320="foedeland"
    )


@wrap_derive
def pers_fra_land(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_fra_land."""
    return _apply_pers320_mapping(left=df, name360="pers_fra_land", name320="fra_land")


@wrap_derive
def pers_landbak3gen(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_landbak3gen."""
    return _apply_pers320_mapping(
        left=df, name360="pers_landbak3gen", name320="landbak3gen"
    )
