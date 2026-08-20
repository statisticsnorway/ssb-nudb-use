import hashlib
from typing import Any

import pandas as pd

from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.variables.derive.derive_decorator import wrap_derive

__all__ = ["utd_hendelse_id"]
_STRING_DTYPE = DTYPE_MAPPINGS["pandas"]["STRING"]


def _safe_sha256_hash(val: Any) -> str | None:
    if pd.isna(val):
        return None
    return hashlib.sha256(str(val).encode("utf-8")).hexdigest()


def _row_number(df: pd.DataFrame) -> pd.Series:
    return pd.Series(range(df.shape[0]), index=df.index, dtype="Int64")


@wrap_derive
def utd_hendelse_id(df: pd.DataFrame, hashed: bool = True) -> pd.Series:
    """Derive `utd_hendelse_id`."""
    hendelse_id = (  # mypy doesn't understand this
        df["nudb_dataset_id"].astype(_STRING_DTYPE)  # type: ignore
        + "["  # type: ignore
        + _row_number(df).astype(_STRING_DTYPE)
        + "]"  # type: ignore
    )

    if hashed:
        hendelse_id = hendelse_id.apply(_safe_sha256_hash)  # really slow?

    return hendelse_id.astype(_STRING_DTYPE)
