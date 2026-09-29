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

# Kolonner som alle kilde-datasett harmoniseres til
_HARMONISED_COLUMNS: list[str] = [
    "snr",
    "fagkode",
    "karakter",
    "vurderingsform",
    "orgnr",
    "start",
    "stop",
    "kilde",  # For å kunne skille filgrunnlagene
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
    """Harmoniser standpunkt- og eksamenskarakterer grunnskole fra `_microdata_gs_fagvurdering`."""
    out = df.rename(
        columns={
            "fagvurdering_grsk_fagkode": "fagkode",
            "fagvurdering_grsk_karakter": "karakter",
            "fagvurdering_grsk_skole": "orgnr",
            "fagvurdering_grsk_vurderingsform": "vurderingsform",
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

    # Map compact vurderingsform back to global pipeline expectations
    form_map = {
        "stp": "STANDPUNKT_GS",
        "skr": "EKSAMEN_SKRIFTLIG",
        "mun": "EKSAMEN_MUNTLIG"
    }
    if "vurderingsform" in out.columns:
        out["vurderingsform"] = out["vurderingsform"].map(form_map).fillna(out["vurderingsform"])
    else:
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
    """Genererer samlet microdata-datasett for videregående skole (vgs) i langformat."""
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.datasets.nudb_database import nudb_database

    # Identifiser om vi skal bruke mock-navnet fra testene eller produksjonsnavnet
    if "_microdata_videregaaendekarakterer" in nudb_database._dataset_generators and "_microdata_videregaaende_karakterer" not in nudb_database._dataset_generators:
        vgs_source = NudbData("_microdata_videregaaendekarakterer")
    else:
        vgs_source = NudbData("_microdata_videregaaende_karakterer")

    # Hent kolonnenavnene som faktisk finnes i kildetabellen/viewet i DuckDB
    columns = [row[0] for row in connection.execute(f"DESCRIBE {vgs_source.alias}").fetchall()]

    fagkode_col = "fagkode" if "fagkode" in columns else "vgs_fagkode"
    stp_col = "stp" if "stp" in columns else "vg_karakter_standpunkt"
    skr_col = "skr" if "skr" in columns else "vg_karakter_skriftlig"
    mun_col = "mun" if "mun" in columns else "vg_karakter_muntlig"
    snr_col = "snr" if "snr" in columns else "fnr"

    parts = []

    # 1. Standpunkt (stp)
    if stp_col in columns:
        parts.append(f"""
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_stp') AS id,
                {fagkode_col} AS fagvurdering_vgs_fagkode,
                {stp_col} AS fagvurdering_vgs_karakter,
                'stp' AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            FROM
                {vgs_source.alias}
            WHERE
                {stp_col} IS NOT NULL AND {stp_col} != ''
        """)

    # 2. Skriftlig eksamen (skr)
    if skr_col in columns:
        parts.append(f"""
            {"UNION ALL" if parts else ""}
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_skr') AS id,
                {fagkode_col} AS fagvurdering_vgs_fagkode,
                {skr_col} AS fagvurdering_vgs_karakter,
                'skr' AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            FROM
                {vgs_source.alias}
            WHERE
                {skr_col} IS NOT NULL AND {skr_col} != ''
        """)

    # 3. Muntlig eksamen (mun)
    if mun_col in columns:
        parts.append(f"""
            {"UNION ALL" if parts else ""}
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_mun') AS id,
                {fagkode_col} AS fagvurdering_vgs_fagkode,
                {mun_col} AS fagvurdering_vgs_karakter,
                'mun' AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            FROM
                {vgs_source.alias}
            WHERE
                {mun_col} IS NOT NULL AND {mun_col} != ''
        """)

    if not parts:
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {alias} AS
            SELECT
                CAST(NULL AS VARCHAR) AS id,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_fagkode,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_karakter,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_vurderingsform,
                CAST(NULL AS VARCHAR) AS fagvurdering_vgs_skole,
                CAST(NULL AS VARCHAR) AS start,
                CAST(NULL AS VARCHAR) AS stop
            WHERE FALSE
            """
        )
    else:
        union_query = "\n".join(parts)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS {union_query}")

# Trenger å legges i riktig format, dette henter bare variablene direkte for øyeblikket. 
# Kompositt-id må lages f.eks, istedet for å hente snr som ID
def _generate_microdata_gs_fagvurdering_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Genererer samlet microdata-datasett for grunnskole (gs) i langformat."""
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

    parts = []

    # 1. Standpunkt (stp)
    parts.append(f"""
        SELECT
            CONCAT({snr_col}, '_', {fagkode_col}, '_stp') AS id,
            {snr_col} AS snr,
            {fagkode_col} AS fagvurdering_grsk_fagkode,
            {karakter_stp_col} AS fagvurdering_grsk_karakter,
            'stp' AS fagvurdering_grsk_vurderingsform,
            {orgnr_col} AS fagvurdering_grsk_skole,
            {start_expr} AS start,
            {stop_expr} AS stop
        FROM
            {gs_source.alias}
        WHERE
            {karakter_stp_col} IS NOT NULL AND {karakter_stp_col} != ''
    """)

    # 2. Skriftlig eksamen (skr)
    has_skr = "grsk_gro_karakter_skriftlig" in columns or "gro_karakter_skriftlig" in columns
    if has_skr:
        parts.append(f"""
            UNION ALL
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_skr') AS id,
                {snr_col} AS snr,
                {fagkode_col} AS fagvurdering_grsk_fagkode,
                {karakter_skr_col} AS fagvurdering_grsk_karakter,
                'skr' AS fagvurdering_grsk_vurderingsform,
                {orgnr_col} AS fagvurdering_grsk_skole,
                {start_expr} AS start,
                {stop_expr} AS stop
            FROM
                {gs_source.alias}
            WHERE
                {karakter_skr_col} IS NOT NULL AND {karakter_skr_col} != ''
        """)

    # 3. Muntlig eksamen (mun)
    has_mun = "grsk_gro_karakter_muntlig" in columns or "gro_karakter_muntlig" in columns
    if has_mun:
        parts.append(f"""
            UNION ALL
            SELECT
                CONCAT({snr_col}, '_', {fagkode_col}, '_mun') AS id,
                {snr_col} AS snr,
                {fagkode_col} AS fagvurdering_grsk_fagkode,
                {karakter_mun_col} AS fagvurdering_grsk_karakter,
                'mun' AS fagvurdering_grsk_vurderingsform,
                {orgnr_col} AS fagvurdering_grsk_skole,
                {start_expr} AS start,
                {stop_expr} AS stop
            FROM
                {gs_source.alias}
            WHERE
                {karakter_mun_col} IS NOT NULL AND {karakter_mun_col} != ''
        """)

    union_query = "\n".join(parts)
    connection.execute(f"CREATE OR REPLACE VIEW {alias} AS {union_query}")


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
