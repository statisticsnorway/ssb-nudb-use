import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pandas as pd

from nudb_use.datasets import bof as bof_module
from nudb_use.datasets.bof import _bof_connection_lookup_sql_parts
from nudb_use.datasets.bof import _bof_dated_orgnr_connections_lookup_sql
from nudb_use.datasets.bof import _bof_foretak_to_orgnrbed_lookup_sql
from nudb_use.datasets.bof import _bof_orgnrbed_to_foretak_lookup_sql
from nudb_use.datasets.bof import _first_date_from_path_period
from nudb_use.datasets.bof import _generate_bof_unique_orgnr_foretak_view
from nudb_use.datasets.bof import _get_all_bof_situttak_october_paths
from nudb_use.datasets.nudb_database import _NudbDatabase


def test_generate_bof_unique_orgnr_foretak_view_uses_latest_placement_cte(
    monkeypatch: Any,
) -> None:
    database = _NudbDatabase()
    connection = database.get_connection()

    monkeypatch.setattr(
        "nudb_use.datasets.bof._bof_latest_orgnr_placement_ctes_sql",
        lambda: """
            latest_placement AS (
                SELECT '111111111' AS orgnr, 'foretak' AS orgnr_type
                UNION ALL
                SELECT '222222222' AS orgnr, 'orgnrbed' AS orgnr_type
            )
        """,
    )

    _generate_bof_unique_orgnr_foretak_view(
        alias="TEST_BOF_UNIQUE_FORETAK",
        connection=connection,
    )

    result = connection.sql("SELECT * FROM TEST_BOF_UNIQUE_FORETAK").df()

    assert result["orgnr"].tolist() == ["111111111"]

    del database


def test_generate_bof_unique_orgnr_foretak_view_creates_empty_view_when_no_paths(
    monkeypatch: Any,
) -> None:
    database = _NudbDatabase()
    connection = database.get_connection()

    monkeypatch.setattr(
        "nudb_use.datasets.bof._bof_latest_orgnr_placement_ctes_sql",
        lambda: None,
    )

    _generate_bof_unique_orgnr_foretak_view(
        alias="TEST_EMPTY_BOF_UNIQUE_FORETAK",
        connection=connection,
    )

    columns = (
        connection.sql("DESCRIBE TEST_EMPTY_BOF_UNIQUE_FORETAK")
        .df()["column_name"]
        .tolist()
    )
    rows = connection.sql("SELECT * FROM TEST_EMPTY_BOF_UNIQUE_FORETAK").df()

    assert columns == ["orgnr"]
    assert rows.empty

    del database


def test_first_date_from_path_period_accepts_sets(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "nudb_use.datasets.bof.get_periods_from_path",
        lambda _path: {
            pd.Timestamp("2024-10-01").date(),
            pd.Timestamp("2023-10-01").date(),
        },
    )

    assert (
        _first_date_from_path_period("ignored_p2023-10_v1.parquet").isoformat()
        == "2023-10-01"
    )


def test_first_date_from_path_period_accepts_datetime(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "nudb_use.datasets.bof.get_periods_from_path",
        lambda _path: pd.Timestamp("2024-10-02 12:34:56"),
    )

    result = _first_date_from_path_period("ignored_p2024-10_v1.parquet")

    assert result.isoformat() == "2024-10-02"


def test_bof_connection_lookup_sql_parts_builds_union_and_latest_cte(
    monkeypatch: Any,
) -> None:
    workdir = Path.cwd() / f".pytest-bof-sql-{uuid4().hex}"
    workdir.mkdir()
    bof_path = workdir / "vof's_p2025-10_v1.parquet"
    bof_path.touch()

    monkeypatch.setattr(
        bof_module,
        "_get_all_bof_situttak_october_paths",
        lambda want_cols=None: [bof_path],
    )
    monkeypatch.setattr(
        bof_module,
        "_first_date_from_path_period",
        lambda _path: pd.Timestamp("2025-10-01").date(),
    )
    monkeypatch.setattr(
        bof_module,
        "_bof_latest_orgnr_placement_ctes_sql",
        lambda relevant_orgnr_cte=None: f"latest_placement AS (SELECT '{relevant_orgnr_cte}' AS source)",
    )

    try:
        result = _bof_connection_lookup_sql_parts()
    finally:
        shutil.rmtree(workdir)

    assert result is not None
    union_sql, latest_cte_sql = result
    assert "CAST(org_nr AS VARCHAR) AS orgnr" in union_sql
    assert "CAST(orgnrbed AS VARCHAR) AS orgnrbed" in union_sql
    assert "2025-10-01" in union_sql
    assert "vof''s_p2025-10_v1.parquet" in union_sql
    assert "relevant_orgnr" in latest_cte_sql


def test_bof_connection_lookup_sql_parts_returns_none_without_inputs(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        bof_module,
        "_get_all_bof_situttak_october_paths",
        lambda want_cols=None: [],
    )
    monkeypatch.setattr(
        bof_module,
        "_bof_latest_orgnr_placement_ctes_sql",
        lambda relevant_orgnr_cte=None: "latest_placement AS (SELECT 1)",
    )

    assert _bof_connection_lookup_sql_parts() is None


def test_bof_connection_lookup_sql_builders_use_shared_parts(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        bof_module,
        "_bof_connection_lookup_sql_parts",
        lambda: (
            "SELECT 'f' AS orgnr, 'b' AS orgnrbed, DATE '2025-10-01' AS bof_period_date",
            "latest_placement AS (SELECT 'f' AS orgnr, 'foretak' AS orgnr_type)",
        ),
    )

    pair_lookup_sql = _bof_dated_orgnr_connections_lookup_sql(
        input_alias="input_pairs",
        orgnr_col="orgnr_foretak",
        orgnrbed_col="orgnrbed",
    )
    bed_lookup_sql = _bof_orgnrbed_to_foretak_lookup_sql(
        input_alias="input_rows",
        orgnrbed_col="orgnrbed",
        join_date_col="join_date",
        row_id_col="_row_id",
    )
    foretak_lookup_sql = _bof_foretak_to_orgnrbed_lookup_sql(
        input_alias="input_rows",
        orgnr_col="orgnr",
        join_date_col="join_date",
        row_id_col="_row_id",
    )

    assert pair_lookup_sql is not None
    assert "FROM input_pairs" in pair_lookup_sql
    assert "input_foretak" in pair_lookup_sql
    assert "SELECT DISTINCT" in pair_lookup_sql

    assert bed_lookup_sql is not None
    assert "TRIM(CAST(orgnrbed AS VARCHAR)) AS orgnrbed" in bed_lookup_sql
    assert "conn_changes" in bed_lookup_sql
    assert "resolved.orgnr" in bed_lookup_sql

    assert foretak_lookup_sql is not None
    assert "TRIM(CAST(orgnr AS VARCHAR)) AS orgnr" in foretak_lookup_sql
    assert "conn_periods" in foretak_lookup_sql
    assert "resolved.orgnrbed" in foretak_lookup_sql


def test_bof_connection_lookup_sql_builders_return_none_without_shared_parts(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(bof_module, "_bof_connection_lookup_sql_parts", lambda: None)

    assert (
        _bof_dated_orgnr_connections_lookup_sql(
            input_alias="input_pairs",
            orgnr_col="orgnr_foretak",
            orgnrbed_col="orgnrbed",
        )
        is None
    )
    assert (
        _bof_orgnrbed_to_foretak_lookup_sql(
            input_alias="input_rows",
            orgnrbed_col="orgnrbed",
            join_date_col="join_date",
            row_id_col="_row_id",
        )
        is None
    )
    assert (
        _bof_foretak_to_orgnrbed_lookup_sql(
            input_alias="input_rows",
            orgnr_col="orgnr",
            join_date_col="join_date",
            row_id_col="_row_id",
        )
        is None
    )


def test_get_all_bof_situttak_october_paths_filters_and_adds_first_last(
    monkeypatch: Any,
) -> None:
    _get_all_bof_situttak_october_paths.cache_clear()

    workdir = Path.cwd() / f".pytest-bof-{uuid4().hex}"
    shared_root = workdir / "shared"
    bof_bucket = shared_root / "bof-team" / "bof-bucket"
    bof_bucket.mkdir(parents=True)

    first = bof_bucket / "bof_2025_01.parquet"
    october = bof_bucket / "bof_2025_10.parquet"
    last_without_cols = bof_bucket / "bof_2025_12.parquet"
    for path in [first, october, last_without_cols]:
        path.touch()

    fake_settings = SimpleNamespace(
        paths=SimpleNamespace(
            daplalab_mounted={"shared_root_external": str(shared_root)}
        ),
        datasets=SimpleNamespace(
            bof_situttak=SimpleNamespace(
                team="bof-team",
                bucket="bof-bucket",
                path_glob="*.parquet",
            )
        ),
    )
    columns_by_path = {
        first: {"org_nr", "orgnrbed"},
        october: {"org_nr", "orgnrbed"},
        last_without_cols: {"org_nr"},
    }
    dates_by_path = {
        first: pd.Timestamp("2025-01-01").date(),
        october: pd.Timestamp("2025-10-01").date(),
        last_without_cols: pd.Timestamp("2025-12-01").date(),
    }

    monkeypatch.setattr(bof_module, "settings", fake_settings)
    monkeypatch.setattr(
        bof_module,
        "pyarrow_columns_from_metadata",
        lambda path: columns_by_path[path],
    )
    monkeypatch.setattr(
        bof_module,
        "_first_date_from_path_period",
        lambda path: dates_by_path[path],
    )
    monkeypatch.setattr(bof_module, "get_latest_fileversions", lambda paths: paths)

    try:
        result = _get_all_bof_situttak_october_paths()
    finally:
        _get_all_bof_situttak_october_paths.cache_clear()
        shutil.rmtree(workdir)

    assert result == [first, october]
