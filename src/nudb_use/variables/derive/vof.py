import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.variables.derive.derive_decorator import wrap_derive

__all__ = ["vof_eierforhold"]

@wrap_derive
def vof_eierforhold(df: pd.DataFrame) -> pd.Series:
    """Derive vof_eierforhold."""
    metadata = settings.variables["vof_eierforhold"]
    datasets = metadata.derived_uses_datasets

    if not datasets:
        raise ValueError(f"Expected a single dataset name, got: {datasets}.")

    catalogue = NudbData(datasets[0]).select("orgnr_foretak, orgnrbed, vof_eierforhold").df()

    eierf = pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]")



    # Join first on orgnrbed
    if "orgnrbed" in df.columns:

        eierf = eierf.fillna(df.merge(catalogue, on="orgnrbed", how="left"))
    # Then on 

    orgnrf_mapping = dict(
        zip(catalogue["orgnr_foretak"], catalogue["vof_eierforhold"], strict=True)
    )

    return df["vof_orgnr_foretak"].map(orgnrf_mapping).fillna(pd.NA)
