import datetime

import duckdb as db
import pandas as pd
import pytest

from nudb_use.datasets import NudbData
from nudb_use.datasets import nudb_database as database_module
from nudb_use.datasets.microdata import MicroData
from nudb_use.datasets.microdata import show_available_microdata_variables
from nudb_use.datasets.microdata_variables import first_class_variables
from nudb_use.datasets.microdata_variables.identifiers import _can_use_original_fnr
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
            "igang": "SELECT 1 AS utd_hendelse_id, '2020' AS utd_skoleaar_start, 'a' AS snr, '100' AS fnr",
            "avslutta": """
                SELECT * FROM (VALUES
                    (2, DATE '2021-06-01', 'a', '101'), (3, NULL, 'b', '202')
                ) AS data(utd_hendelse_id, utd_aktivitet_slutt, snr, fnr)
            """,
            "eksamen": """
                SELECT * FROM (VALUES
                    (4, DATE '2022-06-01', 'c', '303'), (5, NULL, 'b', '202')
                ) AS data(utd_hendelse_id, uh_eksamen_dato, snr, fnr)
            """,
            "snrkat": """
                SELECT * FROM (VALUES
                    ('a', '100', '111'), ('a', '101', '111'), ('b', '202', '222')
                ) AS data(snr, fnr, fnr_naa)
            """,
        }.items():
            alias = _default_alias_from_name(name)
            connection.execute(f"CREATE TABLE {alias} AS {query}")
            monkeypatch.setitem(
                database._dataset_generators, name, lambda alias, connection: None
            )
        yield connection


def test_snrkat_mapping_directions(microdata_sources):
    current = NudbData("_snrkat_snr2fnr").df()
    historical = NudbData("_snrkat_fnr2snr").df()

    assert sorted(current.itertuples(index=False, name=None)) == [
        ("a", "111"),
        ("b", "222"),
    ]
    assert sorted(historical.itertuples(index=False, name=None)) == [
        ("100", "a"),
        ("101", "a"),
        ("202", "b"),
    ]


@pytest.mark.parametrize(
    ("variable", "event_id", "expected_date"),
    [
        ("utd_aktivitet_slutt", 2, datetime.date(2021, 6, 1)),
        ("uh_eksamen_dato", 4, datetime.date(2022, 6, 1)),
    ],
)
def test_microdata_date_dependencies(
    microdata_sources, variable, event_id, expected_date
):
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

    assert list(result.columns) == (
        ["fnr"] if person_variable else ["utd_hendelse_id"]
    ) + [
        variable,
        "utd_hendelse_dato_start",
        "utd_hendelse_dato_slutt",
    ]
    assert len(result) == 3
    if person_variable:
        assert sorted(result["fnr"].tolist()) == ["111", "111", "303"]


@pytest.mark.parametrize("conflicting_values", [False, True])
def test_microdata_person_status_duplicates(
    microdata_sources, monkeypatch, conflicting_values
):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(
        f"INSERT INTO {alias} VALUES (6, '2020', 'a', '100'), (7, '2020', 'a', '101')"
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


@pytest.mark.parametrize(
    "variable", ["fnr", "utd_skoleaar_start", "utd_skoleaar_slutt"]
)
def test_microdata_original_fnr_with_missing_or_unmapped_snr(
    microdata_sources, variable
):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(f"""
        INSERT INTO {alias} VALUES
            (6, '2020', NULL, '404'),
            (7, '2020', 'unknown', '505'),
            (8, '2020', 'b', NULL)
    """)

    result = MicroData(variable).df().set_index("utd_hendelse_id")

    if variable == "fnr":
        expected = {1: "111", 6: "404", 7: "505", 8: "222"}
        expected.update({2: "111", 4: "303"})
        assert result["fnr"].to_dict() == expected
        assert list(result.columns).count("fnr") == 1
    else:
        assert "fnr" not in result.columns
        assert set(result.index) == {1, 6, 7, 8}
    if variable == "utd_skoleaar_slutt":
        assert result[variable].tolist() == ["2021"] * 4


def test_microdata_simple_snr_derive_keeps_missing_snr(microdata_sources):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(f"INSERT INTO {alias} VALUES (6, '2020', NULL, '404')")

    result = MicroData("snr_mrk").df()

    assert result.loc[result["fnr"].eq("404"), "snr_mrk"].tolist() == [False]


def test_microdata_complex_derive_only_keeps_mapped_fnr(
    microdata_sources, monkeypatch, caplog
):
    variable = "utd_hoeyeste_nus2000"
    monkeypatch.setattr(derive, variable, lambda df: df.assign(**{variable: "123456"}))

    result = MicroData(variable).df()

    assert result["fnr"].tolist() == ["111", "111"]
    assert "original FNR fallback is disabled" in caplog.text
    assert "snrkat.fnr_naa" in caplog.text


@pytest.mark.parametrize("complex_dependency", [False, True])
def test_microdata_derives_with_person_dependencies(
    microdata_sources, monkeypatch, caplog, complex_dependency
):
    from nudb_config import settings

    dependency = "utd_hoeyeste_nus2000" if complex_dependency else "snr_mrk"
    variable = "utd_skoleaar_slutt"
    monkeypatch.setattr(settings.variables[variable], "derived_from", [dependency])
    monkeypatch.setattr(
        derive, dependency, lambda df: df.assign(**{dependency: "value"})
    )
    monkeypatch.setattr(derive, variable, lambda df: df.assign(**{variable: "result"}))

    result = MicroData(variable).df()

    assert _can_use_original_fnr(variable) is not complex_dependency
    assert set(result["utd_hendelse_id"]) == {1, 2, 4}
    assert "fnr" not in result.columns
    if complex_dependency:
        assert (
            f"MicroData '{dependency}': original FNR fallback is disabled"
            in caplog.text
        )


def test_microdata_changed_event_rows_disable_fallback(
    microdata_sources, monkeypatch, caplog
):
    variable = "snr_mrk"

    def duplicate_rows(df):
        return df.loc[df.index.repeat(2)].reset_index(drop=True).assign(snr_mrk=True)

    monkeypatch.setattr(derive, variable, duplicate_rows)
    result = MicroData(variable).df()

    assert result["fnr"].tolist() == ["111", "111"]
    assert "did not preserve the input event rows" in caplog.text


def test_microdata_original_fnr_status_conflicts(microdata_sources, monkeypatch):
    alias = _default_alias_from_name("eksamen")
    microdata_sources.execute(
        f"INSERT INTO {alias} VALUES (6, DATE '2022-06-01', NULL, '303')"
    )
    monkeypatch.setattr(
        derive, "snr_mrk", lambda df: df.assign(snr_mrk=df["snr"].notna())
    )

    with pytest.raises(ValueError, match=r"1 \(fnr, start date\) groups"):
        MicroData("snr_mrk")


def test_microdata_source_without_fnr_column(microdata_sources):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(f"ALTER TABLE {alias} DROP COLUMN fnr")

    result = MicroData("fnr").df().set_index("utd_hendelse_id")

    assert result.loc[1, "fnr"] == "111"


def test_microdata_record_type_keeps_event_identifier(microdata_sources):
    result = MicroData("utd_record_type").df().set_index("utd_hendelse_id")

    assert "fnr" not in result.columns
    assert set(result.index) == {1, 2, 3, 4, 5}


@pytest.mark.parametrize("person_variable", [False, True])
@pytest.mark.parametrize(
    "values",
    [
        pd.Series([None, "", "NaN"], dtype=object),
        pd.Series([pd.NA, "", "NaN"], dtype="string"),
        pd.Series([pd.NA, 0, 1], dtype="Int64"),
        pd.Series([float("nan"), 0.0, 1.0]),
        pd.Series([pd.NA, False, True], dtype="boolean"),
        pd.Series([pd.NaT, pd.Timestamp("2020-01-01"), pd.Timestamp("2021-01-01")]),
    ],
    ids=["None", "pd.NA-string", "pd.NA-integer", "NaN", "pd.NA-boolean", "NaT"],
)
def test_microdata_null_measures_not_stored(
    microdata_sources, monkeypatch, person_variable, values
):
    from nudb_config import settings

    variable = "snr_mrk"
    monkeypatch.setattr(
        settings.variables[variable],
        "derived_from",
        ["snr"] if person_variable else ["snr", "utd_aktivitet_slutt"],
    )

    def derive_with_missing(df):
        df = df.sort_values("utd_hendelse_id").reset_index(drop=True)
        return df.assign(snr_mrk=values)

    monkeypatch.setattr(derive, variable, derive_with_missing)
    data = MicroData(variable)
    stored = microdata_sources.execute(
        f"SELECT {variable} FROM {data.alias} ORDER BY utd_hendelse_dato_start"
    ).fetchall()

    assert [value for (value,) in stored] == values.iloc[1:].tolist()
    assert len(data.df()) == 2


def test_microdata_missing_measure_does_not_conflict(microdata_sources, monkeypatch):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(f"INSERT INTO {alias} VALUES (6, '2020', 'a', '100')")
    monkeypatch.setattr(
        derive,
        "snr_mrk",
        lambda df: df.assign(
            snr_mrk=df["utd_hendelse_id"].eq(1).where(df["utd_hendelse_id"].ne(6))
        ),
    )

    result = MicroData("snr_mrk").df()

    assert len(result) == 3
    assert not result["snr_mrk"].isna().any()


def test_microdata_snr_nulls_filtered_but_original_fnr_input_kept(microdata_sources):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(f"INSERT INTO {alias} VALUES (6, '2020', NULL, '404')")

    assert not MicroData("snr").df()["snr"].isna().any()
    assert "404" in MicroData("snr_mrk").df()["fnr"].tolist()


def test_microdata_fnr_without_mapping_or_original_is_omitted(microdata_sources):
    alias = _default_alias_from_name("igang")
    microdata_sources.execute(
        f"INSERT INTO {alias} VALUES (6, '2020', 'unknown', NULL)"
    )

    result = MicroData("fnr").df()

    assert 6 not in result["utd_hendelse_id"].tolist()
    assert not result["fnr"].isna().any()


def test_microdata_physical_float_nan_is_omitted(microdata_sources):
    alias = _default_alias_from_name("igang")
    variable = "uh_eksamen_studpoeng"
    microdata_sources.execute(f"ALTER TABLE {alias} ADD COLUMN {variable} DOUBLE")
    microdata_sources.execute(f"UPDATE {alias} SET {variable} = 'NaN'::DOUBLE")
    microdata_sources.execute(f"""
        INSERT INTO {alias} VALUES
            (6, '2020', 'a', '100', NULL),
            (7, '2020', 'a', '100', 0.0),
            (8, '2020', 'a', '100', 1.0)
    """)

    result = MicroData(variable).df()

    assert sorted(result[variable].tolist()) == [0.0, 1.0]
