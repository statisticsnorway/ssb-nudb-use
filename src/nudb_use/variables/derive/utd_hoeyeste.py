import datetime as dt

import pandas as pd
from nudb_config import settings

from nudb_use.datasets import NudbData
from nudb_use.variables.derive.derive_decorator import wrap_derive

# column renames
nus2000 = "nus2000"
kltrinn2000 = "utd_klassetrinn"
uhgruppe = "uh_gruppering_nus"
regdato = "utd_skoleaar_start"  # Potensielt skummelt med tanke på likhet?
VENSTRESENSUR = "1970"


@wrap_derive
def utd_hoeyeste_rangering(df: pd.DataFrame) -> pd.Series:
    """Derive `utd_hoyeste_rangering`."""
    # Før vi kommer hit bør "df" være en sammenslåing av eksamen og avslutta, hvor vi kun har beholdt fullføringer og sammenslåtte eksamen-ikke-gjentak-rader med studiepoeng?
    # Det gjøres spesielle ting med at grunnskole blir satt til født_dato + 16 år, istedenfor at det er venstrejustert rundt omkring...

    # immutability guard will not be needed in pandas 3 - variables we depend on
    df = df[
        [
            nus2000,
            kltrinn2000,
            uhgruppe,
            regdato,
            "uh_eksamen_studpoeng",
            "uh_eksamen_dato",
            "utd_aktivitet_slutt",
        ]
    ].copy()

    # Sett sentinellverdier
    df[nus2000] = df[nus2000].fillna("999999")
    df[kltrinn2000] = (
        df[kltrinn2000].astype("string[pyarrow]").fillna("00")
    )  # er klassetrinn ett heltall?
    df[uhgruppe] = (
        df[uhgruppe].astype("string[pyarrow]").fillna("00")
    )  # 99 er en dårlig default fordi det betyr bachelor i kodelisten, men det er 99 som er bruk i oracle-koden
    df[regdato] = (
        df[regdato].astype("string[pyarrow]").fillna(VENSTRESENSUR)
    )  # Vi bør kanskje benytt venstresensur-dato ved manglende dato her?

    ######################################
    # Bygge rangeringstall - høyt er bra #
    ######################################

    eksamener_maske = (df["uh_eksamen_dato"].notna()) | (
        df["uh_eksamen_studpoeng"] > 0
    )  # Trenger en måte å skille eksamensrader fra avslutta rader

    trinn_plassering = pd.Series("2", index=df.index)
    # Om det er en avslutning på høyere nivå (uten studiepoeng) - så ansees det alltid som noe som skal erstattes av det som er nytt
    trinn_plassering.loc[
        df[nus2000].str[0].isin(["6", "7", "8"]) & (~eksamener_maske)
    ] = "4"
    # Om det er en rad med studiepoeng
    trinn_plassering.loc[
        df[nus2000].str[0].isin(["6", "7", "8"]) & (eksamener_maske)
    ] = "3"

    # Fullføringer på VGS på lavere nivå, nus starter på 3, ansees som DÅRLIGERE enn alt, inkludert grunnskole?
    # Hardkoding på disse er potensielt allerede gjort til nuskode 201199?
    trinn_plassering.loc[
        (
            (df[nus2000].str[0] == "3")
            & (df[kltrinn2000].isin([10, 11]))
            & (df["utd_aktivitet_slutt"] >= dt.datetime(year=1975, month=10, day=1))
        )
        & (eksamener_maske)
    ] = "1"

    rangering = trinn_plassering

    # Om det IKKE er å anse som en fullføring på UH, så venter vi med å tie-breake på dato
    dato_kanskje = (
        df["utd_aktivitet_slutt"]
        .fillna(df["uh_eksamen_dato"])
        .fillna(df["utd_aktivitet_slutt"])
        .dt.strftime("%Y%m")
        .copy()
    )

    # Innenfor "vanlig sammenligning" - sorterer vi ikke med dato, slik vi gjør på de andre prioritetene
    dato_kanskje.loc[trinn_plassering == "2"] = pd.NA
    rangering += dato_kanskje

    # Første siffer nus
    rangering += (
        (df[nus2000].str[0].astype("Int64") + 1).astype("string[pyarrow]").str[-1]
    )  # Cycles nus2000 starts with 9 to 0

    # Klassetrinn tilsier ett høyere nivå
    rangering += df[kltrinn2000].astype("string[pyarrow]").str.zfill(2).str[-2:]

    # Allmenfag, generelle saker ansees som dårligere - ting med uoppgitte fagfelt kan ende her ifølge NUDB-team
    generelle = pd.Series("1", index=rangering.index)
    generelle.loc[df[nus2000].str[1] == "0"] = "0"
    rangering += generelle

    # Annet enn PPU og forberedende prøver er bra
    uhgruppe_tall = pd.Series("9", index=rangering.index)
    uhgruppe_tall.loc[df[uhgruppe] == "01"] = "0"  # Forberedende prøver er dårlig
    uhgruppe_tall.loc[df[uhgruppe] == "23"] = "1"  # PPU er nesten like dårlig
    rangering += uhgruppe_tall

    # Vi tar med dato som en tie-breaker, dvs. høyere dato er bedre? Selv om man tar sånn ca. den samme utdanningen igjen???
    # rangering += df[regdato].dt.strftime("%Y%m").fillna(VENSTRESENSUR)
    rangering += df[regdato].astype("string[pyarrow]")

    # Hele nuskoden som tiebreaker, litt rart å ta med 1. siffer, når denne allerede er del av rangeringstallet fra før...
    # Og dette betyr at vi rangerer opp "99"-koder, som egentlig er litt feil sånn faglig, men akk vel, la oss rekreere fra Oracle...
    rangering += df[nus2000]

    nchar = rangering.dropna().str.len().unique()

    if len(nchar) > 1:
        raise ValueError(f"Rank numbers have different numbers of digits!\n{nchar}")
    elif (rangering.str.slice(0, 1) == "0").any():
        raise ValueError("Rank numbers have leading zeros!")
    elif not rangering.str.isdigit().all():
        raise ValueError("Rank numbers are not all digits!")

    return rangering


@wrap_derive
def utd_hoeyeste_nus2000(df: pd.DataFrame, year_col: str | None = None) -> pd.DataFrame:
    """Derive `utd_hoyeste_nus2000`."""
    merge_keys = settings.variables.utd_hoeyeste_nus2000.merge_keys
    if year_col:
        merge_keys += ["utd_hoeyeste_aar"]
        first_year = int(df["year_col"].min())
        last_year = int(df["year_col"].max())
    else:
        first_year = dt.now().year
        last_year = first_year

    utd_hoeyeste_df = NudbData(
        "utd_hoeyeste", first_year=first_year, last_year=last_year
    )

    return df.rename({year_col: "utd_hoeyeste_aar"}).merge(
        utd_hoeyeste_df, on=merge_keys, how="left", validate="m:1"
    )
