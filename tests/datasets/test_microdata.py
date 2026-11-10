import datetime

import duckdb as db
import pytest

from nudb_use.datasets import nudb_database as database_module
from nudb_use.datasets.microdata import MicroData
from nudb_use.datasets.microdata import show_available_microdata_variables
from nudb_use.datasets.microdata_variables import first_class_variables
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.variables import derive


@pytest.fixture
def microdata_sources(monkeypatch):
    database = database_module.nudb_database
    with db.connect(":memory:") as connection:
        monkeypatch.setattr(database, "_connection", connection)
        monkeypatch.setattr(database, "_datasets", {})
        monkeypatch.setattr(first_class_variables, "AVAILABLE_VARIABLES", None)
        for name, query in {
            "igang": "SELECT 1 AS utd_hendelse_id, '2020' AS utd_skoleaar_start, 'a' AS snr",
            "avslutta": """
                SELECT * FROM (VALUES
                    (2, DATE '2021-06-01', 'a'), (3, NULL, 'b')
                ) AS data(utd_hendelse_id, utd_aktivitet_slutt, snr)
            """,
            "eksamen": """
                SELECT * FROM (VALUES
                    (4, DATE '2022-06-01', 'c'), (5, NULL, 'b')
                ) AS data(utd_hendelse_id, uh_eksamen_dato, snr)
            """,
            "snrkat": """
                SELECT * FROM (VALUES ('a', '111'), ('a', '111'), ('b', '222'))
                AS data(snr, fnr)
            """,
        }.items():
            alias = _default_alias_from_name(name)
            connection.execute(f"CREATE TABLE {alias} AS {query}")
            monkeypatch.setitem(
                database._dataset_generators, name, lambda alias, connection: None
            )
        yield connection


@pytest.mark.parametrize(
    ("variable", "event_id", "expected_date"),
    [
        ("utd_aktivitet_slutt", 2, datetime.date(2021, 6, 1)),
        ("uh_eksamen_dato", 4, datetime.date(2022, 6, 1)),
    ],
)
def test_microdata_date_dependencies(microdata_sources, variable, event_id, expected_date):
    assert variable in show_available_microdata_variables()

    data = MicroData(variable)
    result = microdata_sources.execute(
        f"SELECT utd_hendelse_id, {variable}, "
        f"utd_hendelse_dato_start, utd_hendelse_dato_slutt FROM {data.alias}"
    ).fetchall()

    assert result == [(event_id, expected_date, expected_date, expected_date)]


@pytest.mark.parametrize("person_variable", [True, False])
def test_microdata_person_identifier(microdata_sources, monkeypatch, person_variable):
    from nudb_config import settings

    variable = "snr_mrk"
    monkeypatch.setattr(
        settings.variables[variable],
        "derived_from",
        ["snr"] if person_variable else ["snr", "utd_aktivitet_slutt"],
    )
    monkeypatch.setattr(
        derive, variable, lambda df: df.assign(snr_mrk=True), raising=False
    )

    result = MicroData(variable).df()

    assert list(result.columns) == [
        "fnr" if person_variable else "utd_hendelse_id",
        variable,
        "utd_hendelse_dato_start",
        "utd_hendelse_dato_slutt",
    ]
    assert len(result) == 3
    if person_variable:
        assert result["fnr"].dropna().tolist() == ["111", "111"]
        assert result["fnr"].isna().sum() == 1


@pytest.mark.parametrize("conflicting_values", [False, True])
def test_microdata_person_status_duplicates(
    microdata_sources, monkeypatch, conflicting_values
):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(
        f"INSERT INTO {alias} VALUES (6, '2020', 'a'), (7, '2020', 'a')"
    )
    monkeypatch.setattr(
        derive,
        "snr_mrk",
        lambda df: df.assign(
            snr_mrk=df["utd_hendelse_id"].eq(1) if conflicting_values else True
        ),
        raising=False,
    )

    if conflicting_values:
        with pytest.raises(ValueError, match=r"1 \(fnr, start date\) groups"):
            MicroData("snr_mrk")
        assert microdata_sources.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [_default_alias_from_name("_microdata_snr_mrk")],
        ).fetchone() == (0,)
    else:
        result = MicroData("snr_mrk").df()
        assert len(result) == 3
        assert len(result.loc[result["fnr"].eq("111")]) == 2
