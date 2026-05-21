from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from nudb_use.datasets import reset_nudb_database
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.variables.specific_vars import orgnr as orgnr_module
from nudb_use.variables.specific_vars.orgnr import _empty_orgnr_sentinel_values
from nudb_use.variables.specific_vars.orgnr import _find_orgnr_foretak_bof
from nudb_use.variables.specific_vars.orgnr import _find_orgnrbed_enkelbedforetak_bof
from nudb_use.variables.specific_vars.orgnr import _percent_filled_orgnr
from nudb_use.variables.specific_vars.orgnr import _split_orgnr_col
from nudb_use.variables.specific_vars.orgnr import cleanup_orgnr_bedrift_foretak


def test_percent_filled_orgnr_and_empty_sentinel_values() -> None:
    series = pd.Series(["123456789", "000000000", pd.NA, "987654321"])

    assert _percent_filled_orgnr(series) == 50.0

    cleaned = _empty_orgnr_sentinel_values(series.copy())
    pd.testing.assert_series_equal(
        cleaned.reset_index(drop=True),
        pd.Series(["123456789", pd.NA, pd.NA, "987654321"]),
        check_dtype=False,
    )


def test_split_orgnr_col_uses_bof_and_brreg_api(monkeypatch: Any) -> None:
    class FakeNudbData:
        def __init__(self, name: str) -> None:
            self.name = name

        def df(self) -> pd.DataFrame:
            if self.name == "_bof_unique_orgnrbed":
                return pd.DataFrame({"orgnrbed": ["111111111"]})
            if self.name == "_bof_unique_orgnr_foretak":
                return pd.DataFrame({"orgnr": ["222222222"]})
            raise AssertionError(f"Unexpected dataset: {self.name}")

    calls: list[str] = []

    def fake_orgnr_is_underenhet(orgnr: str) -> bool:
        calls.append(orgnr)
        return orgnr == "333333333"

    monkeypatch.setattr(orgnr_module, "NudbData", FakeNudbData)
    monkeypatch.setattr(
        orgnr_module,
        "_progress",
        lambda iterable: iterable,
    )
    monkeypatch.setattr(orgnr_module, "orgnr_is_underenhet", fake_orgnr_is_underenhet)

    orgnr_foretak, orgnrbed = _split_orgnr_col(
        pd.Series(
            ["111111111", "222222222", "333333333", "444444444", "000000000", pd.NA],
            dtype="string[pyarrow]",
        )
    )

    assert calls == ["333333333", "444444444", "000000000"]
    assert orgnrbed.tolist() == [
        "111111111",
        pd.NA,
        "333333333",
        pd.NA,
        pd.NA,
        pd.NA,
    ]
    assert orgnr_foretak.tolist() == [
        pd.NA,
        "222222222",
        pd.NA,
        "444444444",
        pd.NA,
        pd.NA,
    ]


def test_find_orgnr_foretak_bof_resolves_before_and_after_fallback(
    monkeypatch: Any,
) -> None:
    reset_nudb_database()
    connection = nudb_database.get_connection()
    connection.execute("""
        CREATE TABLE TEST_BOF_CONNECTIONS (
            orgnrbed VARCHAR,
            orgnr VARCHAR,
            bof_period_date DATE
        )
        """)
    connection.execute("""
        INSERT INTO TEST_BOF_CONNECTIONS VALUES
            ('bed-a', 'foretak-old', '2020-01-01'),
            ('bed-a', 'foretak-old', '2020-06-01'),
            ('bed-a', 'foretak-new', '2021-01-01'),
            ('bed-b', 'foretak-b', '2023-01-01')
        """)

    monkeypatch.setattr(
        orgnr_module,
        "_bof_orgnrbed_to_foretak_lookup_sql",
        lambda **_kwargs: """
            WITH input_clean AS (
                SELECT
                    _row_id,
                    TRIM(orgnrbed) AS orgnrbed,
                    CAST(join_date AS DATE) AS join_date
                FROM input_df
                WHERE orgnrbed IS NOT NULL
                  AND TRIM(orgnrbed) <> ''
                  AND orgnrbed <> '000000000'
                  AND join_date IS NOT NULL
            ),

            input_keys AS (
                SELECT DISTINCT
                    orgnrbed,
                    join_date
                FROM input_clean
            ),

            relevant_orgnrbed AS (
                SELECT DISTINCT
                    orgnrbed
                FROM input_keys
            ),

            conn_base AS (
                SELECT
                    CAST(conn.orgnrbed AS VARCHAR) AS orgnrbed,
                    CAST(conn.orgnr AS VARCHAR) AS orgnr,
                    CAST(conn.bof_period_date AS DATE) AS bof_period_date
                FROM TEST_BOF_CONNECTIONS AS conn
                JOIN relevant_orgnrbed AS r
                    ON CAST(conn.orgnrbed AS VARCHAR) = r.orgnrbed
            ),

            conn_changes AS (
                SELECT
                    orgnrbed,
                    orgnr,
                    bof_period_date
                FROM (
                    SELECT
                        orgnrbed,
                        orgnr,
                        bof_period_date,
                        LAG(orgnr) OVER (
                            PARTITION BY orgnrbed
                            ORDER BY bof_period_date
                        ) AS prev_orgnr
                    FROM conn_base
                )
                WHERE prev_orgnr IS NULL
                   OR orgnr IS DISTINCT FROM prev_orgnr
            ),

            resolved_keys AS (
                SELECT
                    k.orgnrbed,
                    k.join_date,
                    COALESCE(
                        (
                            SELECT c.orgnr
                            FROM conn_changes AS c
                            WHERE c.orgnrbed = k.orgnrbed
                              AND c.bof_period_date <= k.join_date
                            ORDER BY c.bof_period_date DESC
                            LIMIT 1
                        ),
                        (
                            SELECT c.orgnr
                            FROM conn_changes AS c
                            WHERE c.orgnrbed = k.orgnrbed
                              AND c.bof_period_date > k.join_date
                            ORDER BY c.bof_period_date ASC
                            LIMIT 1
                        )
                    ) AS orgnr
                FROM input_keys AS k
            ),

            resolved AS (
                SELECT
                    inp._row_id,
                    rk.orgnr
                FROM input_clean AS inp
                LEFT JOIN resolved_keys AS rk
                    ON inp.orgnrbed = rk.orgnrbed
                   AND inp.join_date = rk.join_date
            )

            SELECT
                raw._row_id,
                resolved.orgnr
            FROM input_df AS raw
            LEFT JOIN resolved
                ON raw._row_id = resolved._row_id
            ORDER BY raw._row_id
            """,
    )

    result = _find_orgnr_foretak_bof(
        pd.Series(
            ["bed-a", "bed-a", "bed-b", "missing", "000000000"],
            index=[10, 11, 12, 13, 14],
        ),
        pd.Series(
            pd.to_datetime(
                ["2020-01-01", "2022-01-01", "2022-01-01", "2022-01-01", "2022-01-01"]
            ),
            index=[10, 11, 12, 13, 14],
        ),
    )

    assert result.index.tolist() == [10, 11, 12, 13, 14]
    pd.testing.assert_series_equal(
        result.reset_index(drop=True),
        pd.Series(
            ["foretak-old", "foretak-new", "foretak-b", pd.NA, pd.NA],
            dtype="string",
            name="orgnr",
        ),
    )


def test_find_orgnrbed_enkelbedforetak_bof_requires_one_to_one_period(
    monkeypatch: Any,
) -> None:
    reset_nudb_database()
    connection = nudb_database.get_connection()
    connection.execute("""
        CREATE TABLE TEST_BOF_CONNECTIONS_FORETAK (
            orgnrbed VARCHAR,
            orgnr VARCHAR,
            bof_period_date DATE
        )
        """)
    connection.execute("""
        INSERT INTO TEST_BOF_CONNECTIONS_FORETAK VALUES
            ('bed-1', 'foretak-a', '2020-01-01'),
            ('bed-1', 'foretak-a', '2021-01-01'),
            ('bed-2', 'foretak-b', '2021-01-01'),
            ('bed-3', 'foretak-b', '2021-01-01'),
            ('bed-4', 'foretak-c', '2023-01-01')
        """)

    monkeypatch.setattr(
        orgnr_module,
        "_bof_foretak_to_orgnrbed_lookup_sql",
        lambda **_kwargs: """
            WITH input_clean AS (
                SELECT
                    _row_id,
                    TRIM(orgnr) AS orgnr,
                    CAST(join_date AS DATE) AS join_date
                FROM input_df
                WHERE orgnr IS NOT NULL
                  AND TRIM(orgnr) <> ''
                  AND orgnr <> '000000000'
                  AND join_date IS NOT NULL
            ),

            input_keys AS (
                SELECT DISTINCT
                    orgnr,
                    join_date
                FROM input_clean
            ),

            relevant_orgnr AS (
                SELECT DISTINCT
                    orgnr
                FROM input_keys
            ),

            conn_periods AS (
                SELECT
                    CAST(conn.orgnr AS VARCHAR) AS orgnr,
                    CAST(conn.bof_period_date AS DATE) AS bof_period_date,
                    COUNT(DISTINCT CAST(conn.orgnrbed AS VARCHAR)) AS orgnrbed_count,
                    MIN(CAST(conn.orgnrbed AS VARCHAR)) AS single_orgnrbed
                FROM TEST_BOF_CONNECTIONS_FORETAK AS conn
                JOIN relevant_orgnr AS r
                    ON CAST(conn.orgnr AS VARCHAR) = r.orgnr
                GROUP BY
                    CAST(conn.orgnr AS VARCHAR),
                    CAST(conn.bof_period_date AS DATE)
            ),

            chosen_periods AS (
                SELECT
                    k.orgnr,
                    k.join_date,
                    COALESCE(
                        (
                            SELECT p.bof_period_date
                            FROM conn_periods AS p
                            WHERE p.orgnr = k.orgnr
                              AND p.bof_period_date <= k.join_date
                            ORDER BY p.bof_period_date DESC
                            LIMIT 1
                        ),
                        (
                            SELECT p.bof_period_date
                            FROM conn_periods AS p
                            WHERE p.orgnr = k.orgnr
                              AND p.bof_period_date > k.join_date
                            ORDER BY p.bof_period_date ASC
                            LIMIT 1
                        )
                    ) AS chosen_period
                FROM input_keys AS k
            ),

            resolved_keys AS (
                SELECT
                    cp.orgnr,
                    cp.join_date,
                    CASE
                        WHEN p.orgnrbed_count = 1 THEN p.single_orgnrbed
                        ELSE NULL
                    END AS orgnrbed
                FROM chosen_periods AS cp
                LEFT JOIN conn_periods AS p
                    ON p.orgnr = cp.orgnr
                   AND p.bof_period_date = cp.chosen_period
            ),

            resolved AS (
                SELECT
                    inp._row_id,
                    rk.orgnrbed
                FROM input_clean AS inp
                LEFT JOIN resolved_keys AS rk
                    ON inp.orgnr = rk.orgnr
                   AND inp.join_date = rk.join_date
            )

            SELECT
                raw._row_id,
                resolved.orgnrbed
            FROM input_df AS raw
            LEFT JOIN resolved
                ON raw._row_id = resolved._row_id
            ORDER BY raw._row_id
            """,
    )

    result = _find_orgnrbed_enkelbedforetak_bof(
        pd.Series(
            ["foretak-a", "foretak-b", "foretak-c", "missing"], index=[0, 1, 2, 3]
        ),
        pd.Series(
            pd.to_datetime(["2022-01-01", "2022-01-01", "2022-01-01", "2022-01-01"]),
            index=[0, 1, 2, 3],
        ),
    )

    assert result.tolist() == ["bed-1", pd.NA, "bed-4", pd.NA]


def test_cleanup_orgnr_bedrift_foretak_combines_columns_and_joins(
    monkeypatch: Any,
) -> None:
    split_values = {
        "orgnr": (
            pd.Series([pd.NA, "foretak-existing", pd.NA], dtype="string[pyarrow]"),
            pd.Series(["bed-existing", pd.NA, pd.NA], dtype="string[pyarrow]"),
        ),
        "extra_orgnr": (
            pd.Series(["foretak-extra", pd.NA, pd.NA], dtype="string[pyarrow]"),
            pd.Series([pd.NA, "bed-extra", pd.NA], dtype="string[pyarrow]"),
        ),
    }

    monkeypatch.setattr(
        orgnr_module,
        "_split_orgnr_col",
        lambda s: split_values[s.name],
    )
    monkeypatch.setattr(
        orgnr_module,
        "_find_orgnr_foretak_bof",
        lambda orgnrbed, _time: pd.Series(
            ["foretak-joined", "foretak-from-bed", pd.NA],
            index=orgnrbed.index,
            dtype="string[pyarrow]",
        ),
    )
    monkeypatch.setattr(
        orgnr_module,
        "_find_orgnrbed_enkelbedforetak_bof",
        lambda orgnr, _time: pd.Series(
            [pd.NA, pd.NA, "bed-from-fallback"],
            index=orgnr.index,
            dtype="string[pyarrow]",
        ),
    )

    df = pd.DataFrame(
        {
            "orgnr": ["111111111", "222222222", pd.NA],
            "extra_orgnr": ["333333333", "444444444", pd.NA],
            "utd_skoleaar_start": ["2020", "2021", "2022"],
            "keep": [1, 2, 3],
        }
    )

    result = cleanup_orgnr_bedrift_foretak(
        df,
        extra_orgnr_cols_split_prio=["extra_orgnr"],
    )

    assert result.columns.tolist() == [
        "utd_skoleaar_start",
        "keep",
        "orgnrbed",
        "orgnr_foretak",
    ]
    assert result["orgnrbed"].tolist() == [
        "bed-existing",
        "bed-extra",
        "bed-from-fallback",
    ]
    assert result["orgnr_foretak"].tolist() == [
        "foretak-extra",
        "foretak-existing",
        pd.NA,
    ]


def test_cleanup_orgnr_bedrift_foretak_raises_on_unrecognized_time_dtype() -> None:
    df = pd.DataFrame(
        {
            "orgnr": ["111111111"],
            "utd_skoleaar_start": [2020],
        }
    )

    with pytest.raises(TypeError, match="Unrecognized datatype"):
        cleanup_orgnr_bedrift_foretak(df)
