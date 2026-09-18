"""Genererer microdata-datasett for enheten FAGVURDERING.

Støtter tre separate kilder (vgs, gs, nasjprov) via felles caching i DuckDB.
"""

from typing import Literal
import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import logger

VurderingsformKode = Literal[
    "STANDPUNKT_GS",
    "STANDPUNKT_VGS",
    "EKSAMEN_SKRIFTLIG",
    "EKSAMEN_MUNTLIG",
    "NASJONAL_PROVE",
    "EKSAMEN",
]

# Kolonner som alle kilde-datasett harmoniseres til (lopenr_kurs er fjernet)
_HARMONISED_COLUMNS: list[str] = [
    "snr",
    "fagkode",
    "karakter",
    "vurderingsform",
    "orgnr",
    "start",
    "stop",
    "kilde",  # Ny kolonne for å kunne skille filgrunnlagene
]


def _ensure_harmonised_columns(df: pd.DataFrame, kilde: str) -> pd.DataFrame:
    """Sikrer at alle påkrevde kolonner eksisterer og setter kilde."""
    out = df.copy()
    out["kilde"] = kilde
    for col in _HARMONISED_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out[_HARMONISED_COLUMNS]


def _harmonise_standpunkt_vgs(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser standpunktkarakterer VGS fra `avslutta_videregaaende`."""
    out = df.rename(
        columns={
            "nus2000": "fagkode",
            "vg_karakterpoeng": "karakter",
            "utd_skoleaar_start": "start",
            "orgnrbed": "orgnr",
        }
    ).copy()

    # Formater start/stop som standard skoleår-datoer (YYYY-MM-DD)
    out["start"] = out["start"].astype(str) + "-08-01"
    out["stop"] = out["start"].astype(str).str[:4] + "-06-30"
    out["vurderingsform"] = "STANDPUNKT_VGS"

    return _ensure_harmonised_columns(out, kilde="vgs")


def _harmonise_standpunkt_gs(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser standpunktkarakterer grunnskole fra `grunnskolekarakterer`."""
    out = df.rename(
        columns={
            "grsk_gro_fagkode_vigo": "fagkode",
            "gro_fagkode_vigo": "fagkode",
            "grsk_gro_karakter_standpunkt": "karakter",
            "gro_karakter_standpunkt": "karakter",
            "grsk_utd_skolekom": "orgnr",
            "utd_orgnr": "orgnr",
        }
    ).copy()

    # Hvis 'start' og 'stop' allerede finnes i kilden (som er tilfelle med den nye direkte viewen), beholder vi dem
    if "start" in out.columns and "stop" in out.columns:
        pass
    # Hvis grsk_utd_aktivitet_slutt inneholder en dato som f.eks. '2025-06-20', utleder vi start og stop
    elif "grsk_utd_aktivitet_slutt" in df.columns:
        out["stop"] = df["grsk_utd_aktivitet_slutt"].astype(str)
        # Ekstraher årstallet og sett start til august året før
        out["start"] = df["grsk_utd_aktivitet_slutt"].astype(str).str[:4].apply(
            lambda y: f"{int(y) - 1}-08-01" if y.isdigit() else pd.NA
        )
    else:
        out["start"] = pd.NA
        out["stop"] = pd.NA

    out["vurderingsform"] = "STANDPUNKT_GS"

    return _ensure_harmonised_columns(out, kilde="gs")


def _harmonise_nasjonale_proever(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser nasjonale prøver fra `nasjprov`."""
    out = df.rename(
        columns={
            "provekode": "fagkode",
            "skalapoeng": "karakter",
            "utd_skoleaar_start": "start",
            "orgnrbed": "orgnr",
        }
    ).copy()

    # Formater start/stop (siden nasjonale prøver tas på høsten, setter vi faste dager)
    out["start"] = out["start"].astype(str) + "-09-15"
    out["stop"] = out["start"]
    out["vurderingsform"] = "NASJONAL_PROVE"

    return _ensure_harmonised_columns(out, kilde="nasjprov")


def _harmonise_eksamen(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser eksamenskarakterer fra `eksamen`."""
    out = df.rename(
        columns={
            "uh_emnekode": "fagkode",
            "uh_eksamen_karakter": "karakter",
            "uh_eksamen_dato": "start",
            "orgnrbed": "orgnr",
        }
    ).copy()

    out["stop"] = out["start"]
    out["vurderingsform"] = "EKSAMEN"

    return _ensure_harmonised_columns(out, kilde="vgs")


def _build_fagvurdering_id(df: pd.DataFrame) -> pd.Series:
    """Konstruer forenklet kompositt-ID: snr + fagkode + vurderingsform."""
    from nudb_use.datasets.nudb_database import STRING_DTYPE

    return (
        df["snr"].astype(STRING_DTYPE)
        + "_"
        + df["fagkode"].astype(STRING_DTYPE)
        + "_"
        + df["vurderingsform"].astype(STRING_DTYPE)
    )


def _build_fagvurdering_long() -> pd.DataFrame:
    """Bygg en samlet, lang dataframe med alle FAGVURDERING-records."""
    from nudb_use.datasets.nudb_data import NudbData

    logger.info("Bygger samlet FAGVURDERING-datasett...")

    standpunkt_vgs = _harmonise_standpunkt_vgs(NudbData("avslutta_videregaaende").df())
    standpunkt_gs = _harmonise_standpunkt_gs(NudbData("_microdata_gs_fagvurdering").df())
    nasjonale_proever = _harmonise_nasjonale_proever(NudbData("nasjprov").df())
    eksamen = _harmonise_eksamen(NudbData("eksamen").df())

    long_df = pd.concat(
        [standpunkt_vgs, standpunkt_gs, nasjonale_proever, eksamen],
        ignore_index=True,
    )

    long_df = long_df.dropna(subset=["snr", "fagkode", "karakter", "vurderingsform"])
    long_df["fagvurdering_id"] = _build_fagvurdering_id(long_df)

    n_dupes = long_df.duplicated(
        subset=["fagvurdering_id", "start", "stop"]
    ).sum()
    if n_dupes:
        logger.warning(
            f"Fant {n_dupes} duplikate FAGVURDERING-ID'er innenfor samme "
            "periode. Disse vil feile i microdata sin validering."
        )

    return long_df


def _generate_fagvurdering_base_table_if_needed(connection: db.DuckDBPyConnection) -> None:
    """Materialiserer den lange dataframen i DuckDB dersom den ikke finnes."""
    existing_tables = [row[0] for row in connection.execute("SHOW TABLES").fetchall()]
    if "_fagvurdering_long_cached" not in existing_tables:
        long_df = _build_fagvurdering_long()
        connection.register("_temp_fagvurdering_long_df", long_df)
        connection.execute(
            "CREATE TABLE _fagvurdering_long_cached AS SELECT * FROM _temp_fagvurdering_long_df"
        )
        connection.unregister("_temp_fagvurdering_long_df")


def _generate_microdata_vgs_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Genererer samlet microdata-datasett for videregående skole (vgs)."""
    _generate_fagvurdering_base_table_if_needed(connection)
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT
            fagvurdering_id AS id,
            fagkode AS fagvurdering_vgs_fagkode,
            karakter AS fagvurdering_vgs_karakter,
            orgnr AS fagvurdering_vgs_skole,
            vurderingsform AS fagvurdering_vgs_vurderingsform,
            start,
            stop
        FROM
            _fagvurdering_long_cached
        WHERE
            kilde = 'vgs'
        """
    )

# Trenger å legges i riktig format, dette henter bare variablene direkte for øyeblikket. 
# Kompositt-id må lages f.eks, istedet for å hente snr som ID
def _generate_microdata_gs_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Genererer samlet microdata-datasett for grunnskole (gs) direkte fra source."""
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.datasets.nudb_database import nudb_database

    # Identifiser om vi skal bruke mock-navnet fra testene eller produksjonsnavnet
    if "_microdata_grunnskolekarakterer" in nudb_database._dataset_generators and "_microdata_grunnskole_karakterer" not in nudb_database._dataset_generators:
        gs_source = NudbData("_microdata_grunnskolekarakterer")
    else:
        gs_source = NudbData("_microdata_grunnskole_karakterer")

    # Hent kolonnenavnene som faktisk finnes i kildetabellen/viewet i DuckDB
    columns = [row[0] for row in connection.execute(f"DESCRIBE {gs_source.alias}").fetchall()]

    # Map kolonnenavn basert på hva som finnes i kilden
    fagkode_col = "grsk_gro_fagkode_vigo" if "grsk_gro_fagkode_vigo" in columns else "gro_fagkode_vigo"
    karakter_stp_col = "grsk_gro_karakter_standpunkt" if "grsk_gro_karakter_standpunkt" in columns else "gro_karakter_standpunkt"
    karakter_skr_col = "grsk_gro_karakter_skriftlig" if "grsk_gro_karakter_skriftlig" in columns else "gro_karakter_skriftlig"
    karakter_mun_col = "grsk_gro_karakter_muntlig" if "grsk_gro_karakter_muntlig" in columns else "gro_karakter_muntlig"
    orgnr_col = "grsk_utd_skolekom" if "grsk_utd_skolekom" in columns else ("utd_orgnr" if "utd_orgnr" in columns else "orgnr")

    # Beregn start- og stop-datoer basert på tilgjengelige kolonner
    if "utd_skoleaar_start" in columns:
        start_expr = "CAST(utd_skoleaar_start AS VARCHAR) || '-08-01'"
        stop_expr = "CAST(CAST(utd_skoleaar_start AS INTEGER) + 1 AS VARCHAR) || '-06-30'"
    elif "grsk_utd_aktivitet_slutt" in columns:
        # Legacy/Test mock format: grsk_utd_aktivitet_slutt inneholder en full dato som f.eks. '2025-06-20'
        # Vi trekker fra 1 år for start (august året før) og beholder stopp-datoen as-is
        start_expr = "CAST(CAST(SUBSTR(grsk_utd_aktivitet_slutt, 1, 4) AS INTEGER) - 1 AS VARCHAR) || '-08-01'"
        stop_expr = "grsk_utd_aktivitet_slutt"
    else:
        start_expr = "NULL"
        stop_expr = "NULL"

    # Håndter snr vs andre mulige ID-kolonner
    snr_col = "snr" if "snr" in columns else "fnr"

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT
            CONCAT({snr_col}, '_', {fagkode_col}) AS id,
            {fagkode_col} AS gro_fagkode_vigo,
            {karakter_stp_col} AS gro_karakter_standpunkt,
            {"NULL" if "grsk_gro_karakter_skriftlig" not in columns and "gro_karakter_skriftlig" not in columns else karakter_skr_col} AS gro_karakter_skriftlig,
            {"NULL" if "grsk_gro_karakter_muntlig" not in columns and "gro_karakter_muntlig" not in columns else karakter_mun_col} AS gro_karakter_muntlig,
            {orgnr_col} AS utd_orgnr,
            {start_expr} AS start,
            {stop_expr} AS stop
        FROM
            {gs_source.alias}
        """
    )


def _generate_microdata_nasjprov_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Genererer samlet microdata-datasett for nasjonale prøver (nasjprov)."""
    _generate_fagvurdering_base_table_if_needed(connection)
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW {alias} AS
        SELECT
            fagvurdering_id AS id,
            fagkode AS fagvurdering_nasjprov_fagkode,
            karakter AS fagvurdering_nasjprov_karakter,
            orgnr AS fagvurdering_nasjprov_skole,
            vurderingsform AS fagvurdering_nasjprov_vurderingsform,
            start,
            stop
        FROM
            _fagvurdering_long_cached
        WHERE
            kilde = 'nasjprov'
        """
    )
