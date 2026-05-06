import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pandas as pd

from nudb_use.datasets import bof as bof_module
from nudb_use.datasets.bof import _first_date_from_path_period
from nudb_use.datasets.bof import _generate_bof_dated_orgnr_connections_view
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


def test_generate_bof_dated_orgnr_connections_view_creates_empty_view_when_no_paths(
    monkeypatch: Any,
) -> None:
    database = _NudbDatabase()
    connection = database.get_connection()

    monkeypatch.setattr(
        "nudb_use.datasets.bof._get_all_bof_situttak_october_paths",
        lambda want_cols=None: [],
    )

    _generate_bof_dated_orgnr_connections_view(
        alias="TEST_EMPTY_BOF_CONNECTIONS",
        connection=connection,
    )

    columns = (
        connection.sql("DESCRIBE TEST_EMPTY_BOF_CONNECTIONS")
        .df()["column_name"]
        .tolist()
    )
    rows = connection.sql("SELECT * FROM TEST_EMPTY_BOF_CONNECTIONS").df()

    assert columns == ["orgnr", "orgnrbed", "bof_period_date"]
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
