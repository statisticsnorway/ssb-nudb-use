from __future__ import annotations

import pandas as pd

from .derive_decorator import wrap_derive_join_all_data

__all__ = [
    "utd_hoyeste_nus2000",
]


@wrap_derive_join_all_data
def utd_hoyeste_nus2000(df: pd.DataFrame) -> pd.Series:
    """Derive utd_hoyeste_nus2000 / old BU. The persons highest education according to nus2000.

    Args:
        df: Source dataset containing at least snr, nus2000, utd_fullfoertkode.

    Returns:
        pd.Series: A column suitable for adding as a new column to the df.

    Raises:
        NotImplementedError: Raises this error because the function is not finished yet.
    """
    raise NotImplementedError(
        "This is not finished BU-programming, we need to decide if we should be producing a dataset like F_UTD_DEMOGRAFI or similar."
    )

    variable_name = "utd_hoyeste_nus2000"  # type: ignore[unreachable]

    #################################################
    # QUALIFICATION - rows that are eligible for BU #
    #################################################

    # We are already in the context of KODE = 0 and RECTYPE != 3, because we only use "avslutta" dataset for this
    completed = df[df["utd_fullfoertkode"] == "8"]

    #####################
    # SORTING ALGORITHM #
    #####################

    # Changes startingcode from 9 -> 0, all others get bumped one up
    sorter_col = (
        (completed["nus2000"].str[0].astype("Int64").fillna(9) + 1)
        .astype("string[pyarrow]")
        .str[-1]
    )
    # Adds kltrinn zfilled 2 to the right of the sorter col
    sorter_col += (
        completed["kltrinn2000"].fillna(0).astype("string[pyarrow]").str.zfill(2)
    )

    # Downprio nus2000 2nd digit if it is 0
    second_nus = completed["nus2000"].str[2]
    second_nus.loc[second_nus != "0"] = "1"
    sorter_col += second_nus

    # Downprio UHGRUPPE 01, 23
    uhgruppe_prio = pd.Series("9", index=sorter_col.index)
    uhgruppe_prio.loc[completed["uh_gruppering_nus"] == "01"] = "0"
    uhgruppe_prio.loc[completed["uh_gruppering_nus"] == "23"] = "1"
    sorter_col += uhgruppe_prio

    # Prio by newer date - since our records are all "avslutta" -> the old regdato was created from "sluttd"?
    sorter_col += (
        completed["utd_aktivitet_start"]
        .fillna(completed["utd_aktivitet_slutt"])
        .dt.strftime(r"%Y%m")
        .astype("string[pyarrow]")
    )

    # Full nus2000 into prio
    sorter_col += completed["nus2000"].str.zfill(6)

    # Keep highest sortorder nus2000-value per person
    completed["sort_order"] = sorter_col
    df_agg = (
        sorter_col.sort_values(["sort_order"], ascending=False)
        .groupby("snr", as_index=False)
        .first()
        .rename(columns={"nus2000": variable_name})
    )
    return df.merge(
        df_agg[["snr", variable_name]], on="snr", how="left", validate="m:1"
    )[variable_name]
