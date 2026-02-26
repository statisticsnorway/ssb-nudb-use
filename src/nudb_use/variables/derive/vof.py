import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.variables.derive.derive_decorator import wrap_derive

__all__ = ["vof_eierforhold", "vof_orgnr_foretak"]


@wrap_derive
def vof_orgnr_foretak(df: pd.DataFrame) -> pd.Series:
    """Derive vof_orgnr_foretak."""
    metadata = settings.variables["vof_orgnr_foretak"]
    datasets = metadata.derived_uses_dataset

    if not datasets:
        raise ValueError(f"Expected a single dataset name, got: {datasets}.")

    dataset = datasets[0]
    catalogue = NudbData(dataset).df()
    orgnrbed_mapping = dict(
        zip(catalogue["vof_orgnrbed"], catalogue["vof_orgnr_foretak"], strict=True)
    )

    utd_orgnr = df["utd_orgnr"]
    utd_orgnrbed = df["utd_orgnrbed"]

    is_foretak = utd_orgnr.notna() & ((utd_orgnr != utd_orgnrbed) | utd_orgnrbed.isna())

    utd_orgnr_foretak = utd_orgnr.copy()
    utd_orgnr_foretak[~is_foretak] = pd.NA

    utd_orgnr_foretak = utd_orgnr_foretak.fillna(utd_orgnrbed.map(orgnrbed_mapping))

    return utd_orgnr_foretak


@wrap_derive
def vof_eierforhold(df: pd.DataFrame) -> pd.Series:
    """Derive vof_eierforhold."""
    metadata = settings.variables["vof_eierforhold"]
    datasets = metadata.derived_uses_dataset

    if not datasets:
        raise ValueError(f"Expected a single dataset name, got: {datasets}.")

    dataset = datasets[0]
    catalogue = NudbData(dataset).df()
    orgnrf_mapping = dict(
        zip(catalogue["vof_orgnr_foretak"], catalogue["vof_eierforhold"], strict=True)
    )

    return df["vof_orgnr_foretak"].map(orgnrf_mapping)
