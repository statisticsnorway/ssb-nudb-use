from pathlib import Path
from types import SimpleNamespace

import duckdb as db
import pandas as pd
import pytest

from nudb_use.datasets import NudbData
from nudb_use.datasets import bof
from nudb_use.datasets import nudb_database as database_module
from nudb_use.datasets.microdata import MicroData
from nudb_use.datasets.nudb_database import _NudbDatabase
from nudb_use.variables import derive


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> _NudbDatabase:
    database = _NudbDatabase()
    monkeypatch.setattr(database_module, "nudb_database", database)
    monkeypatch.setattr("nudb_use.datasets.nudb_data.nudb_database", database)
    monkeypatch.setattr(
        "nudb_use.datasets.microdata_variables.first_class_variables.AVAILABLE_VARIABLES",
        None,
    )
    return database


def _input(database: _NudbDatabase, name: str, frame: pd.DataFrame) -> None:
    table = f"input_{name}"
    database.get_connection().register(table, frame)

    def generator(alias: str, connection: db.DuckDBPyConnection) -> None:
        connection.execute(f"CREATE VIEW {alias} AS SELECT * FROM {table}")

    database._dataset_generators[name] = generator


def test_hoeyeste_retains_provenance_of_selected_event(database: _NudbDatabase) -> None:
    _input(
        database,
        "eksamen_avslutta_hoeyeste",
        pd.DataFrame(
            {
                "snr": ["1000001", "1000001"],
                "nus2000": ["411101", "711101"],
                "utd_aktivitet_slutt": pd.to_datetime(["2010-06-01", "2010-06-01"]),
                "uh_eksamen_dato": pd.Series(
                    pd.NaT, index=range(2), dtype="datetime64[ns]"
                ),
                "uh_eksamen_studpoeng": [0, 0],
                "uh_gruppering_nus": ["99", "99"],
                "utd_klassetrinn": ["13", "18"],
                "utd_skoleaar_start": ["2009", "2009"],
                "utd_rectype": ["4", "4"],
                "utd_studieland": ["000", "000"],
                "utd_datakilde": ["04", "04"],
                "utd_hendelse_id": [1, 2],
                "nudb_dataset_id": ["lower_source", "winning_source"],
            }
        ),
    )

    highest = NudbData("utd_hoeyeste").df()
    assert highest["utd_hendelse_id"].tolist() == [2]
    assert highest["nudb_dataset_id"].tolist() == ["winning_source>utd_hoeyeste"]
    assert NudbData("utd_hoeyeste_last").df()["nudb_dataset_id"].tolist() == [
        "winning_source>utd_hoeyeste>utd_hoeyeste_last"
    ]


@pytest.mark.parametrize(
    ("father_rank", "mother_rank", "winner"),
    [
        ("9", "5", "father"),
        ("5", "9", "mother"),
        ("5", "5", "mother"),
        (None, "5", "mother"),
        ("5", None, "father"),
        (None, None, None),
    ],
)
def test_parent_provenance_follows_winning_education(
    database: _NudbDatabase,
    father_rank: str | None,
    mother_rank: str | None,
    winner: str | None,
) -> None:
    _input(
        database,
        "slekt_snr",
        pd.DataFrame({"snr": ["child"], "far_snr": ["father"], "mor_snr": ["mother"]}),
    )
    _input(
        database,
        "_snr2alder16",
        pd.DataFrame(
            {
                "snr": ["child"],
                "pers_foedselsdato": pd.to_datetime(["1994-01-01"]),
                "pers_aar_16": [2010],
            }
        ),
    )
    parents = pd.DataFrame(
        {
            "snr": ["father", "mother", "father", "mother"],
            "utd_hoeyeste_nus2000": ["father_code", "mother_code", "future", "future"],
            "utd_hoeyeste_rangering": pd.Series(
                [father_rank, mother_rank, "99", "99"], dtype="string"
            ),
            "utd_hoeyeste_aar": [2009, 2009, 2020, 2020],
            "utd_foerste_aar": [2009] * 4,
            "nudb_dataset_id": [
                "father_source",
                "mother_source",
                "future_source",
                "future_source",
            ],
        }
    )
    parents = parents[parents["utd_hoeyeste_rangering"].notna()]
    _input(database, "utd_hoeyeste", parents)

    result = NudbData("utd_foreldres_utdnivaa").df()
    if winner is None:
        assert result["utd_foreldres_utdnivaa_16aar_nus2000"].isna().all()
        assert result["nudb_dataset_id"].tolist() == [
            "no-parent-education-hoeyeste>utd_foreldres_utdnivaa"
        ]
    else:
        assert result["utd_foreldres_utdnivaa_16aar_nus2000"].tolist() == [
            f"{winner}_code"
        ]
        assert result["nudb_dataset_id"].tolist() == [
            f"{winner}_source>utd_foreldres_utdnivaa"
        ]


def test_identifier_mapping_keeps_one_row_per_pair(database: _NudbDatabase) -> None:
    _input(
        database,
        "snrkat",
        pd.DataFrame(
            {
                "fnr": ["person", "person"],
                "snr": ["1", "1"],
                "nudb_dataset_id": ["source_b", "source_a"],
            }
        ),
    )
    assert NudbData("_snrkat_fnr2snr").df().to_dict("list") == {
        "fnr": ["person"],
        "snr": ["1"],
        "nudb_dataset_id": ["source_a>_snrkat_fnr2snr"],
    }


def test_bokommune_provenance_follows_selected_residence(
    database: _NudbDatabase,
) -> None:
    _input(database, "_snrkat_fnr2snr", pd.DataFrame({"fnr": ["person"], "snr": ["1"]}))
    _input(
        database,
        "bokommune_16aar_fnr",
        pd.DataFrame(
            {
                "fnr": ["person", "person"],
                "komm_nr": ["0301", "1103"],
                "innflyttingsdato": pd.to_datetime(["2000-01-01", "2001-01-01"]),
                "alderu_ved_innflytting": [15, 16],
                "nudb_dataset_id": ["older_residence", "newer_residence"],
            }
        ),
    )
    result = NudbData("bokommune_16aar_snr").df()
    assert result["komm_nr"].tolist() == ["1103"]
    assert result["nudb_dataset_id"].tolist() == ["newer_residence>bokommune_16aar_snr"]


def test_age_view_preserves_population_source(database: _NudbDatabase) -> None:
    _input(database, "utd_hoeyeste", pd.DataFrame({"utd_hoeyeste_aar": [2025]}))
    _input(
        database,
        "freg_situttak",
        pd.DataFrame(
            {
                "snr": ["1"],
                "foedselsdato": pd.to_datetime(["1990-01-01"]),
                "nudb_dataset_id": ["population_source"],
            }
        ),
    )
    assert NudbData("_snr2alder16").df()["nudb_dataset_id"].tolist() == [
        "population_source>_snr2alder16"
    ]


def test_bu_igang_starts_fresh_provenance(database: _NudbDatabase) -> None:
    _input(
        database,
        "igang",
        pd.DataFrame(
            {
                "snr": ["1000001"],
                "nus2000": ["411101"],
                "utd_skoleaar_start": ["2010"],
                "uh_erhovedaktivitet": [True],
                "fa_erhovedaktivitet": [False],
                "vg_erhovedaktivitet": [False],
                "nudb_dataset_id": ["source_igang"],
            }
        ),
    )
    _input(
        database,
        "avslutta",
        pd.DataFrame({"snr": ["1000001"], "utd_skoleaar_start": ["2010"]}),
    )
    _input(database, "eksamen", pd.DataFrame({"snr": ["1000001"]}))
    _input(
        database,
        "utd_hoeyeste",
        pd.DataFrame(
            {
                "snr": ["1000001"],
                "utd_hoeyeste_nus2000": ["411101"],
                "utd_hoeyeste_aar": [2010],
            }
        ),
    )
    result = NudbData("bu_igang", from_year=2010, to_year=2010).df()
    assert result["nudb_dataset_id"].tolist() == [
        "cross-row-aggregate-igang-avslutta-eksamen-utd_hoeyeste>bu_igang"
    ]
    assert result["bu_2010"].tolist() == ["411101"]


def test_nuskat_starts_with_klass_origin(
    database: _NudbDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "klass.KlassClassification",
        lambda _id: SimpleNamespace(
            get_codes=lambda: SimpleNamespace(
                data=pd.DataFrame({"code": ["411101"], "name": ["Education"]})
            )
        ),
    )
    for variable in (
        "uh_gruppering_nus",
        "utd_klassetrinn_lav_nus",
        "utd_klassetrinn_hoey_nus",
    ):
        monkeypatch.setattr(derive, variable, lambda frame: frame)
    assert NudbData("nuskat").df()["nudb_dataset_id"].tolist() == [
        "nus2000-from-klass>nuskat"
    ]


def test_microdata_provenance_is_an_unmodified_event_variable(
    database: _NudbDatabase,
) -> None:
    for event_id, name in enumerate(("igang", "avslutta", "eksamen"), start=1):
        _input(
            database,
            name,
            pd.DataFrame(
                {
                    "utd_hendelse_id": [event_id],
                    "nudb_dataset_id": [f"original_{name}"],
                    "utd_skoleaar_start": ["2010"],
                    "utd_aktivitet_slutt": pd.to_datetime(["2010-06-01"]),
                    "uh_eksamen_dato": pd.to_datetime(["2010-06-01"]),
                    "snr": ["1000001"],
                }
            ),
        )
    result = MicroData("nudb_dataset_id").df().sort_values("utd_hendelse_id")
    assert result["nudb_dataset_id"].tolist() == [
        "original_igang",
        "original_avslutta",
        "original_eksamen",
    ]
    assert result["utd_hendelse_id"].tolist() == [1, 2, 3]
    assert "nudb_dataset_id" not in MicroData("snr").df().columns


def test_bof_views_track_source_snapshots(
    database: _NudbDatabase, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_path = tmp_path / "bof_p2013-10_v1.parquet"
    new_path = tmp_path / "bof_p2025-10_v1.parquet"
    pd.DataFrame(
        {"org_nr": ["111111111"], "orgnrbed": ["222222222"], "sektor": ["610"]}
    ).to_parquet(old_path)
    pd.DataFrame(
        {
            "org_nr": ["111111111"],
            "orgnrbed": ["222222222"],
            "org_form": ["KOMM"],
            "sektor_2014": ["6500"],
            "undersektor_2014": ["006"],
        }
    ).to_parquet(new_path)
    monkeypatch.setattr(
        bof,
        "_get_all_bof_situttak_october_paths",
        lambda want_cols=None: (
            [new_path] if want_cols == bof.WANT_COLS_LATEST else [old_path, new_path]
        ),
    )

    ownership = NudbData("_bof_eierforhold").df().sort_values("bof_period_date")
    assert ownership["bof_eierforhold"].tolist() == ["1", "4"]
    assert ownership["nudb_dataset_id"].tolist() == [
        f"{old_path.stem}>_bof_eierforhold",
        f"{new_path.stem}>_bof_eierforhold",
    ]
    for name in ("_bof_unique_orgnrbed", "_bof_unique_orgnr_foretak"):
        assert NudbData(name).df()["nudb_dataset_id"].tolist() == [
            f"{new_path.stem}>{name}"
        ]
