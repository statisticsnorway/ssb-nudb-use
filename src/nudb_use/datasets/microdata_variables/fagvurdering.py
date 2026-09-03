"""Genererer microdata-datasett for enheten FAGVURDERING.

FAGVURDERING samler standpunktkarakterer (grunnskole og VGS), karakterer fra
nasjonale prover, og eksamenskarakterer (skriftlig/muntlig) i en felles
statistisk enhet. ID-en er en komposittnokkel av snr + fagkode + vurderingsform
(+ lopenr_kurs for VGS, der det finnes), slik at hver record blir unik innenfor
riktig tidsperiode, jf. reglene for STATUS-variabler i microdata.no.

Hver "variabel"-view under (karakter, fagkode, vurderingsform, skole) deler
samme ID og kan derfor kobles sammen av brukere pa microdata.no.
"""

from typing import Literal

import duckdb as db
import pandas as pd

from nudb_use.datasets import NudbData
from nudb_use.datasets.nudb_database import STRING_DTYPE
from nudb_use.nudb_logger import logger

VurderingsformKode = Literal[
    "STANDPUNKT_GS",
    "STANDPUNKT_VGS",
    "EKSAMEN_SKRIFTLIG",
    "EKSAMEN_MUNTLIG",
    "NASJONAL_PROVE",
]

# Kolonner som alle kilde-datasett harmoniseres til, for enkel union.
_HARMONISED_COLUMNS: list[str] = [
    "snr",
    "fagkode",
    "karakter",
    "vurderingsform",
    "lopenr_kurs",
    "orgnr",
    "start",
    "stop",
]


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

    if "lopenr_kurs" not in out.columns:
        out["lopenr_kurs"] = pd.NA

    return out[_HARMONISED_COLUMNS]


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
    out["lopenr_kurs"] = pd.NA

    return out[_HARMONISED_COLUMNS]


def _harmonise_nasjonale_proever(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser nasjonale prover til fellesskjema.

    Nasjonale prover gjennomfores pa en gitt dato, sa start == stop
    (STATUS-variabel).
    """
    out = df.rename(
        columns={
            "karakter_np": "karakter",
            "gjennomforingsdato": "start",
        }
    ).copy()

    out["stop"] = out["start"]
    out["vurderingsform"] = "NASJONAL_PROVE"
    out["lopenr_kurs"] = pd.NA

    return out[_HARMONISED_COLUMNS]


def _harmonise_eksamen(df: pd.DataFrame) -> pd.DataFrame:
    """Harmoniser eksamenskarakterer (skriftlig/muntlig) til fellesskjema.

    Forventer at kildedatasettet allerede har en kolonne `eksamensform`
    med verdiene "SKRIFTLIG" eller "MUNTLIG".
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

    if "lopenr_kurs" not in out.columns:
        out["lopenr_kurs"] = pd.NA

    return out[_HARMONISED_COLUMNS]


def _build_fagvurdering_id(df: pd.DataFrame) -> pd.Series:
    """Konstruer kompositt-ID: snr + fagkode + vurderingsform (+ lopenr_kurs)."""
    lopenr = df["lopenr_kurs"].astype(STRING_DTYPE).fillna("")

    return (
        df["snr"].astype(STRING_DTYPE)
        + "_"
        + df["fagkode"].astype(STRING_DTYPE)
        + "_"
        + df["vurderingsform"].astype(STRING_DTYPE)
        + lopenr.where(lopenr == "", "_" + lopenr)
    )


def _build_fagvurdering_long() -> pd.DataFrame:
    """Bygg en samlet, lang dataframe med alle FAGVURDERING-records.

    Henter fra de fire kildedatasettene, harmoniserer til felles skjema,
    slar dem sammen, og konstruerer kompositt-ID-en. Dette er det eneste
    stedet der kildelogikken for de fire vurderingsformene bor endres.
    """
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


def _make_variable_view_generator(value_column: str) -> "db.CFunction":  # type: ignore[name-defined]
    """Lag en generator-funksjon for en enkelt microdata-variabel.

    Args:
        value_column: Navnet pa kolonnen i den lange dataframen som skal
            brukes som "Verdi" i microdata-leveransen.

    Returns:
        En funksjon som kan registreres i `nudb_database._dataset_generators`.
    """

    def _generator(alias: str, connection: db.DuckDBPyConnection) -> None:
        long_df = _build_fagvurdering_long()

        out = long_df.rename(
            columns={
                "fagvurdering_id": "id",
                value_column: "verdi",
            }
        )[["id", "verdi", "start", "stop"]]

        connection.register("_temp_fagvurdering_df", out)
        connection.execute(
            f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_fagvurdering_df"
        )

    return _generator


def _generate_microdata_fagvurdering_karakter_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generer microdata-view for karakterverdien i FAGVURDERING."""
    _make_variable_view_generator("karakter")(alias, connection)


def _generate_microdata_fagvurdering_fagkode_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generer microdata-view for fagkoden i FAGVURDERING."""
    _make_variable_view_generator("fagkode")(alias, connection)


def _generate_microdata_fagvurdering_vurderingsform_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generer microdata-view for vurderingsformen i FAGVURDERING."""
    _make_variable_view_generator("vurderingsform")(alias, connection)


def _generate_microdata_fagvurdering_skole_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    """Generer microdata-view for skole (orgnr) i FAGVURDERING."""
    _make_variable_view_generator("orgnr")(alias, connection)
