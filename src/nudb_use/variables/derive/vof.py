import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.nudb_logger import logger
from nudb_use.variables.derive.derive_decorator import wrap_derive

__all__ = ["vof_eierforhold"]


def _percent_notna(s: pd.Series) -> float:
    return 0.0 if not len(s) else float(round(s.notna().sum() / len(s) * 100, 2))


@wrap_derive
def vof_eierforhold(df: pd.DataFrame) -> pd.Series:
    """Derive vof_eierforhold."""
    metadata = settings.variables["vof_eierforhold"]
    datasets = metadata.derived_uses_datasets

    if datasets is None or len(datasets) != 1:
        raise ValueError(f"Expected a single dataset name, got: {datasets}.")
    else:
        dataset: str = datasets[0]

    unique_orgnr_foretak = "', '".join(
        df["orgnr_foretak"].replace("000000000", pd.NA).dropna().unique()
    )
    where = f"orgnr_foretak in ('{unique_orgnr_foretak}')"
    if "orgnrbed" in df.columns:
        unique_orgnrbed = "', '".join(
            df["orgnrbed"].replace("000000000", pd.NA).dropna().unique()
        )
        where += f" or orgnrbed in ('{unique_orgnrbed}')"

    logger.info(
        "Getting vof-catalogue for vof_eierforhold (combination of vof-situttak)."
    )
    catalogue = (
        NudbData(dataset)
        .select("orgnr_foretak, orgnrbed, vof_eierforhold")
        .where(where)
        .df()
    )

    eierf = df.merge(
        (
            catalogue[["orgnr_foretak", "vof_eierforhold"]].drop_duplicates(
                subset=["orgnr_foretak"], keep="last"
            )
        ),  # This assumes that "the last eierforhold is the correct one"...
        on="orgnr_foretak",
        how="left",
        validate="m:1",
    )["vof_eierforhold"].astype("string[pyarrow]")
    logger.info(
        f"Joining `vof_eierforhold` first on orgnr_fortak (preferred by UH). Filled on {_percent_notna(eierf)}%"
    )

    if "orgnrbed" in df.columns:
        orgnr_bed_missing_value = catalogue[eierf.isna()]["orgnrbed"].unique()
        filtered_catalogue = catalogue[
            catalogue["orgnrbed"].isin(orgnr_bed_missing_value).astype("bool[pyarrow]")
        ][["orgnrbed", "vof_eierforhold"]].drop_duplicates(
            subset="orgnrbed", keep="last"
        )  # This assumes that "the last eierforhold is the correct one"...
        eierf = eierf.fillna(
            df.merge(filtered_catalogue, on="orgnrbed", how="left", validate="m:1")[
                "vof_eierforhold"
            ]
        )
        logger.info(
            f"Joining `vof_eierforhold` second on orgnrbed. After both joins, eierforhold filled on {_percent_notna(eierf)}%"
        )
    else:
        logger.info("Did not find orgnrbed to join `vof_eierforhold` on.")

    return eierf
