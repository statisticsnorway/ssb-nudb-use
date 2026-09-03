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
    """Harmoniser standpunktkarakterer VGS til fellesskjema."""
    out = df.rename(
        columns={
            "karakter_standpunkt": "karakter",
            "skoleaar_start": "start",
            "skoleaar_stopp": "stop",
        }
    ).copy()

    out["vurderingsform"] = "STANDPUNKT_VGS"
    return _ensure_harmonised_columns(out, kilde="vgs")


def _harmonise_standpunkt_gs(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser standpunktkarakterer grunnskole til fellesskjema."""
    out = df.rename(
        columns={
            "karakter_standpunkt": "karakter",
            "skoleaar_start": "start",
            "skoleaar_stopp": "stop",
        }
    ).copy()

    out["vurderingsform"] = "STANDPUNKT_GS"
    return _ensure_harmonised_columns(out, kilde="gs")


def _harmonise_nasjonale_proever(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser nasjonale prøver til fellesskjema."""
    out = df.rename(
        columns={
            "karakter_np": "karakter",
            "gjennomforingsdato": "start",
        }
    ).copy()

    out["stop"] = out["start"]
    out["vurderingsform"] = "NASJONAL_PROVE"
    return _ensure_harmonised_columns(out, kilde="nasjprov")


def _harmonise_eksamen(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser eksamenskarakterer (skriftlig/muntlig) til fellesskjema.
    
    Her antar vi at eksamen per nå tilhører videregående (vgs).
    """
    out = df.rename(
        columns={
            "karakter_eksamen": "karakter",
            "eksamensdato": "start",
        }
    ).copy()

    out["stop"] = out["start"]
    out["vurderingsform"] = out["eksamensform"].map(
        {
            "SKRIFTLIG": "EKSAMEN_SKRIFTLIG",
            "MUNTLIG": "EKSAMEN_MUNTLIG",
        }
    )
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

    standpunkt_vgs = _harmonise_standpunkt_vgs(NudbData("standpunkt_vgs").df())
    standpunkt_gs = _harmonise_standpunkt_gs(NudbData("standpunkt_gs").df())
    nasjonale_proever = _harmonise_nasjonale_proever(
        NudbData("nasjonale_proever").df()
    )
    eksamen = _harmonise_eksamen(NudbData("eksamen_karakterer").df())

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


def _make_variable_view_generator(value_column: str, kilde_filter: str):
    """Lag en generator-funksjon for en enkelt microdata-variabel filtrert på kilde."""

    def _generator(alias: str, connection: db.DuckDBPyConnection) -> None:
        _generate_fagvurdering_base_table_if_needed(connection)

        # Genererer det tynne viewet direkte i DuckDB filtrert på kilde
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW {alias} AS
            SELECT
                fagvurdering_id AS id,
                {value_column} AS verdi,
                start,
                stop
            FROM
                _fagvurdering_long_cached
            WHERE
                kilde = '{kilde_filter}'
            """
        )

    return _generator


# === Genererte views for VIDEREGÅENDE (VGS) ===

def _generate_microdata_fagvurdering_vgs_karakter_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("karakter", "vgs")(alias, connection)


def _generate_microdata_fagvurdering_vgs_fagkode_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("fagkode", "vgs")(alias, connection)


def _generate_microdata_fagvurdering_vgs_vurderingsform_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("vurderingsform", "vgs")(alias, connection)


def _generate_microdata_fagvurdering_vgs_skole_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("orgnr", "vgs")(alias, connection)


# === Genererte views for GRUNNSKOLE (GS) ===

def _generate_microdata_fagvurdering_gs_karakter_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("karakter", "gs")(alias, connection)


def _generate_microdata_fagvurdering_gs_fagkode_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("fagkode", "gs")(alias, connection)


def _generate_microdata_fagvurdering_gs_vurderingsform_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("vurderingsform", "gs")(alias, connection)


def _generate_microdata_fagvurdering_gs_skole_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("orgnr", "gs")(alias, connection)


# === Genererte views for NASJONALE PRØVER (nasjprov) ===

def _generate_microdata_fagvurdering_nasjprov_karakter_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("karakter", "nasjprov")(alias, connection)


def _generate_microdata_fagvurdering_nasjprov_fagkode_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("fagkode", "nasjprov")(alias, connection)


def _generate_microdata_fagvurdering_nasjprov_vurderingsform_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("vurderingsform", "nasjprov")(alias, connection)


def _generate_microdata_fagvurdering_nasjprov_skole_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    _make_variable_view_generator("orgnr", "nasjprov")(alias, connection)
