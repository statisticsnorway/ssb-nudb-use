from pathlib import Path
from typing import Any

import pandas as pd

import nudb_use
from nudb_use.datasets import NudbData
from nudb_use.datasets import nudb_database as nudb_database_module
from nudb_use.datasets import reset_nudb_database
from nudb_use.datasets.external import _generate_view
from nudb_use.datasets.nudb_data import _fetch_string_column
from nudb_use.datasets.nudb_data import _is_in_database
from nudb_use.datasets.nudb_data import _is_table
from nudb_use.datasets.nudb_data import _is_view
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.datasets.utils import _select_if_contains_index_col_0
from nudb_use.metadata.nudb_config.variable_names import update_colnames


def patch_nudb_database(
    igang: pd.DataFrame,
    avslutta: pd.DataFrame,
    eksamen: pd.DataFrame,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reset_nudb_database()

    basepath = tmp_path / "local" / "nudb-data"
    nudbpath = basepath / "klargjorte-data"
    nudbpath.mkdir(parents=True)

    igang = update_colnames(igang)
    avslutta = update_colnames(avslutta)
    eksamen = update_colnames(eksamen)

    eksamen["uh_eksamen_ergjentak"] = eksamen["uh_eksamen_ergjentak"] == "d"

    igang.to_parquet(nudbpath / "igang_p1970_p1971_v1.parquet")
    avslutta.to_parquet(nudbpath / "avslutta_p1970_p1971_v1.parquet")
    eksamen.to_parquet(nudbpath / "eksamen_p1970_p1971_v1.parquet")

    # legg inn i config at alle registreringer trenger flere (potensielt) dato-kolonner
    monkeypatch.setattr(nudb_use.paths.latest, "POSSIBLE_PATHS", [basepath])


def test_nudbdata(
    igang: pd.DataFrame,
    avslutta: pd.DataFrame,
    eksamen: pd.DataFrame,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    patch_nudb_database(igang, avslutta, eksamen, tmp_path, monkeypatch)

    NudbData("utd_hoeyeste")
    NudbData("igang")


def test_fetch_string_column_and_tables() -> None:
    reset_nudb_database()
    connection = nudb_database_module._NUDB_DATABASE.get_connection()
    connection.execute("CREATE TABLE test_table (name VARCHAR)")
    connection.execute("INSERT INTO test_table VALUES ('a'), ('b')")
    connection.execute("CREATE VIEW test_view AS SELECT * FROM test_table")

    result = _fetch_string_column("SELECT name FROM test_table", "name")
    assert result == ["a", "b"]
    assert _is_table("test_table")
    assert _is_view("test_view")
    assert _is_in_database("test_table")


def test_nudb_database_reset_clears_tables() -> None:
    database = nudb_database_module.NudbDatabase()
    connection = database.get_connection()
    connection.execute("CREATE TABLE reset_table (value INT)")
    assert connection.sql("SHOW TABLES").df()["name"].tolist() == ["reset_table"]

    database._reset()
    tables_after_reset = (
        database.get_connection().sql("SHOW TABLES").df()["name"].tolist()
    )
    assert tables_after_reset == []


def test_utils_select_and_alias(tmp_path: Path) -> None:
    without_index = tmp_path / "without_index.parquet"

    pd.DataFrame({"a": [1, 2]}).to_parquet(without_index, index=False)

    connection = nudb_database_module._NUDB_DATABASE.get_connection()
    assert _select_if_contains_index_col_0(without_index, connection) == "*"
    assert _default_alias_from_name("test-data") == "NUDB_DATA_TEST_DATA"


def test_external_generate_view_uses_alias_and_excludes_index(
    tmp_path: Path, monkeypatch: Any
) -> None:
    reset_nudb_database()
    connection = nudb_database_module._NUDB_DATABASE.get_connection()

    data_path = tmp_path / "external.parquet"
    pd.DataFrame({"col": [1, 2]}).to_parquet(data_path)

    def fake_latest_shared_path(_dataset_name: str) -> tuple[str, Path]:
        return "external_dataset", data_path

    monkeypatch.setattr(
        "nudb_use.datasets.external.latest_shared_path", fake_latest_shared_path
    )

    alias = "EXTERNAL_ALIAS"
    _generate_view("external_dataset", alias=alias, connection=connection)

    columns = connection.sql(f"DESCRIBE {alias}").df()["column_name"].tolist()
    assert "__index_level_0__" not in columns
    assert "col" in columns
    assert "nudb_dataset_id" in columns
