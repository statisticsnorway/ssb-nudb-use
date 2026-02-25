import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.nudb_logger import logger

from .derive_decorator import wrap_derive

__all__ = ["pers_foedselsdato", "pers_invkat", "pers_kjoenn"]


def _apply_pers320_mapping(
    left: pd.DataFrame, name360: str, name320: str = ""
) -> pd.DataFrame:
    name320 = name320 or name360

    metadata = settings.variables[name360]
    join_keys = metadata.derived_join_keys
    dataset = metadata.derived_uses_datasets[0]

    if len(metadata.derived_uses_datasets) > 1:
        raise ValueError(
            f"Expected a single dataset, got: {metadata.derived_uses_datasets}"
        )

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
def pers_invkat(  # noqa:DOC201
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive pers_invkat."""
    return _apply_pers320_mapping(left=df, name360="pers_invkat", name320="invkat")


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
