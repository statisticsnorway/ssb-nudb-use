from typing import Any

import pandas as pd

from nudb_use.datasets.bof import _date_from_path_period
from nudb_use.datasets.bof import _generate_bof_dated_orgnr_connections_view
from nudb_use.datasets.nudb_database import _NudbDatabase


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


def test_date_from_path_period_accepts_sets(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "nudb_use.datasets.bof.get_periods_from_path",
        lambda _path: {
            pd.Timestamp("2024-10-01").date(),
            pd.Timestamp("2023-10-01").date(),
        },
    )

    assert _date_from_path_period("ignored").isoformat() == "2023-10-01"
