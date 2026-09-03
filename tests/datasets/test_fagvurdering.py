import pandas as pd
import pytest

from nudb_use.datasets import reset_nudb_database
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.datasets.microdata import MicroData


def test_fagvurdering_pipeline(monkeypatch) -> None:
    # 1. Mock the four source datasets in the test database using production schemas
    def _gen_avslutta_videregaaende(alias, connection):
        df = pd.DataFrame({
            "snr": ["1", "2"],
            "vg_karakterpoeng": ["5", "4"],
            "utd_skoleaar_start": ["2025", "2025"],
            "nus2000": ["REA3022", "NOR1211"],
            "orgnrbed": ["974760673", "974760673"],
        })
        connection.register("_temp_avslutta_videregaaende", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_avslutta_videregaaende")

    def _gen_grunnskolekarakterer(alias, connection):
        df = pd.DataFrame({
            "snr": ["3"],
            "grsk_gro_karakter_standpunkt": ["6"],
            "grsk_utd_aktivitet_slutt": ["2025-06-20"],
            "grsk_gro_fagkode_vigo": ["MAT0010"],
            "grsk_utd_skolekom": ["987654321"],
        })
        connection.register("_temp_grunnskolekarakterer", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_grunnskolekarakterer")

    def _gen_nasjprov(alias, connection):
        df = pd.DataFrame({
            "snr": ["4"],
            "skalapoeng": ["3"],
            "utd_skoleaar_start": ["2025"],
            "provekode": ["ENG05"],
            "orgnrbed": ["123456789"],
        })
        connection.register("_temp_nasjprov", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_nasjprov")

    def _gen_eksamen(alias, connection):
        df = pd.DataFrame({
            "snr": ["5"],
            "uh_eksamen_karakter": ["4"],
            "uh_eksamen_dato": ["2026-05-25"],
            "uh_emnekode": ["NOR1211"],
            "orgnrbed": ["974760673"],
        })
        connection.register("_temp_eksamen", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_eksamen")

    # Clean existing registered datasets or cached tables if any
    reset_nudb_database()
    connection = nudb_database.get_connection()

    # Register the mock source generators with production names
    nudb_database._dataset_generators["avslutta_videregaaende"] = _gen_avslutta_videregaaende
    nudb_database._dataset_generators["grunnskolekarakterer"] = _gen_grunnskolekarakterer
    nudb_database._dataset_generators["nasjprov"] = _gen_nasjprov
    nudb_database._dataset_generators["eksamen"] = _gen_eksamen

    # 2. Test VGS views
    vgs_karakter = MicroData("fagvurdering_vgs_karakter")
    vgs_karakter_df = vgs_karakter.df()

    # Verify structure and data
    assert list(vgs_karakter_df.columns) == ["id", "verdi", "start", "stop"]
    # Should include standpunkt_vgs (snr 1, 2) and eksamen (snr 5)
    assert len(vgs_karakter_df) == 3
    
    # Check composite ID construction
    expected_ids = ["1_REA3022_STANDPUNKT_VGS", "2_NOR1211_STANDPUNKT_VGS", "5_NOR1211_EKSAMEN"]
    assert sorted(vgs_karakter_df["id"].tolist()) == sorted(expected_ids)

    # Test GS views
    gs_karakter = MicroData("fagvurdering_gs_karakter")
    gs_karakter_df = gs_karakter.df()
    assert len(gs_karakter_df) == 1
    assert gs_karakter_df["id"].iloc[0] == "3_MAT0010_STANDPUNKT_GS"
    assert gs_karakter_df["verdi"].iloc[0] == "6"
    assert gs_karakter_df["start"].iloc[0] == "2024-08-01"
    assert gs_karakter_df["stop"].iloc[0] == "2025-06-20"

    # Test Nasjonale Prøver views
    np_karakter = MicroData("fagvurdering_nasjprov_karakter")
    np_karakter_df = np_karakter.df()
    assert len(np_karakter_df) == 1
    assert np_karakter_df["id"].iloc[0] == "4_ENG05_NASJONAL_PROVE"
    assert np_karakter_df["verdi"].iloc[0] == "3"
    assert np_karakter_df["start"].iloc[0] == "2025-09-15"
    assert np_karakter_df["stop"].iloc[0] == "2025-09-15"

    # 3. Test other variable views for VGS
    vgs_fagkode_df = MicroData("fagvurdering_vgs_fagkode").df()
    assert vgs_fagkode_df[vgs_fagkode_df["id"] == "1_REA3022_STANDPUNKT_VGS"]["verdi"].iloc[0] == "REA3022"

    vgs_vurderingsform_df = MicroData("fagvurdering_vgs_vurderingsform").df()
    assert vgs_vurderingsform_df[vgs_vurderingsform_df["id"] == "5_NOR1211_EKSAMEN"]["verdi"].iloc[0] == "EKSAMEN"

    vgs_skole_df = MicroData("fagvurdering_vgs_skole").df()
    assert vgs_skole_df[vgs_skole_df["id"] == "1_REA3022_STANDPUNKT_VGS"]["verdi"].iloc[0] == "974760673"
