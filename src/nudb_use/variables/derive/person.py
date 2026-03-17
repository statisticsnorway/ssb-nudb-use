import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

__all__ = [
    "pers_bokommune",
    "pers_bokommune_16aar",
    "pers_bydel",
    "pers_foedeland",
    "pers_foedselsdato",
    "pers_foerste_bosattdato",
    "pers_fra_land",
    "pers_grunnkrets",
    "pers_innflyttingsdato",
    "pers_innvandringsgrunn",
    "pers_innvandringskategori",
    "pers_kjoenn",
    "pers_landbakgrunn_land",
    "pers_statsborgerskap_land",
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

    merged = left.merge(right, how="left", on=join_keys, validate="m:1")

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
def pers_bokommune(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_bokommune."""
    return _apply_pers320_mapping(left=df, name360="pers_bokommune", name320="komm_nr")


@wrap_derive
def pers_grunnkrets(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_grunnkrets."""
    return _apply_pers320_mapping(
        left=df, name360="pers_grunnkrets", name320="gkrets_nr"
    )


@wrap_derive
def pers_bydel(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_bydel."""
    return _apply_pers320_mapping(left=df, name360="pers_bydel", name320="bydel_nr")


@wrap_derive
def pers_innvandringskategori(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_innvandringskategori."""
    return _apply_pers320_mapping(
        left=df, name360="pers_innvandringskategori", name320="invkat"
    )


@wrap_derive
def pers_innvandringsgrunn(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_innvandringsgrunn."""
    return _apply_pers320_mapping(
        left=df, name360="pers_innvandringsgrunn", name320="inngrunn1"
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
def pers_statsborgerskap_land(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_statsborgerskap_land."""
    return _apply_pers320_mapping(
        left=df, name360="pers_statsborgerskap_land", name320="statsborgerskap"
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
def pers_landbakgrunn_land(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_landbakgrunn_land."""
    return _apply_pers320_mapping(
        left=df, name360="pers_landbakgrunn_land", name320="landbak3gen"
    )
