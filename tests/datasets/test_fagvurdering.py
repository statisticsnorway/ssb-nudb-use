import pandas as pd
import pytest

from nudb_use.datasets import reset_nudb_database
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.datasets.microdata import MicroData


def test_fagvurdering_pipeline(monkeypatch) -> None:
    # 1. Mock the four source datasets in the test database
    def _gen_standpunkt_vgs(alias, connection):
        df = pd.DataFrame({
            "snr": ["1", "2"],
            "karakter_standpunkt": ["5", "4"],
            "skoleaar_start": ["2025-08-01", "2025-08-01"],
            "skoleaar_stopp": ["2026-06-30", "2026-06-30"],
            "fagkode": ["REA3022", "NOR1211"],
            "orgnr": ["974760673", "974760673"],
        })
        connection.register("_temp_standpunkt_vgs", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_standpunkt_vgs")

    def _gen_standpunkt_gs(alias, connection):
        df = pd.DataFrame({
            "snr": ["3"],
            "karakter_standpunkt": ["6"],
            "skoleaar_start": ["2024-08-01"],
            "skoleaar_stopp": ["2025-06-30"],
            "fagkode": ["MAT0010"],
            "orgnr": ["987654321"],
        })
        connection.register("_temp_standpunkt_gs", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_standpunkt_gs")

    def _gen_nasjonale_proever(alias, connection):
        df = pd.DataFrame({
            "snr": ["4"],
            "karakter_np": ["3"],
            "gjennomforingsdato": ["2025-09-15"],
            "fagkode": ["ENG05"],
            "orgnr": ["123456789"],
        })
        connection.register("_temp_nasjonale_proever", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_nasjonale_proever")

    def _gen_eksamen(alias, connection):
        df = pd.DataFrame({
            "snr": ["5"],
            "karakter_eksamen": ["4"],
            "eksamensdato": ["2026-05-25"],
            "fagkode": ["NOR1211"],
            "eksamensform": ["SKRIFTLIG"],
            "orgnr": ["974760673"],
        })
        connection.register("_temp_eksamen_karakterer", df)
        connection.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM _temp_eksamen_karakterer")

    # Clean existing registered datasets or cached tables if any
    reset_nudb_database()
    connection = nudb_database.get_connection()

    # Register the mock source generators
    nudb_database._dataset_generators["standpunkt_vgs"] = _gen_standpunkt_vgs
    nudb_database._dataset_generators["standpunkt_gs"] = _gen_standpunkt_gs
    nudb_database._dataset_generators["nasjonale_proever"] = _gen_nasjonale_proever
    nudb_database._dataset_generators["eksamen_karakterer"] = _gen_eksamen

    # 2. Test VGS views
    vgs_karakter = MicroData("fagvurdering_vgs_karakter")
    vgs_karakter_df = vgs_karakter.df()

    # Verify structure and data
    assert list(vgs_karakter_df.columns) == ["id", "verdi", "start", "stop"]
    # Should include standpunkt_vgs (snr 1, 2) and eksamen (snr 5)
    assert len(vgs_karakter_df) == 3
    
    # Check composite ID construction
    expected_ids = ["1_REA3022_STANDPUNKT_VGS", "2_NOR1211_STANDPUNKT_VGS", "5_NOR1211_EKSAMEN_SKRIFTLIG"]
    assert sorted(vgs_karakter_df["id"].tolist()) == sorted(expected_ids)

    # Test GS views
    gs_karakter = MicroData("fagvurdering_gs_karakter")
    gs_karakter_df = gs_karakter.df()
    assert len(gs_karakter_df) == 1
    assert gs_karakter_df["id"].iloc[0] == "3_MAT0010_STANDPUNKT_GS"
    assert gs_karakter_df["verdi"].iloc[0] == "6"

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
    assert vgs_vurderingsform_df[vgs_vurderingsform_df["id"] == "5_NOR1211_EKSAMEN_SKRIFTLIG"]["verdi"].iloc[0] == "EKSAMEN_SKRIFTLIG"

    vgs_skole_df = MicroData("fagvurdering_vgs_skole").df()
    assert vgs_skole_df[vgs_skole_df["id"] == "1_REA3022_STANDPUNKT_VGS"]["verdi"].iloc[0] == "974760673"
