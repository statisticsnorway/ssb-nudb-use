import duckdb

from nudb_use.datasets.macros import _DUCKDB_MACROS


def test_60_studiepoeng_videreutdanning_rangeres_som_uh_grad() -> None:
    con = duckdb.connect()
    con.execute(_DUCKDB_MACROS)

    result = con.sql("""
        SELECT UTD_HOEYESTE_RANGERING(
            '714197',
            CAST('2006-01-01' AS DATE),
            60,
            '18',
            NULL,
            '11',
            '2005',
            '3',
            NULL,
            '04'
        ) AS rangering
    """).fetchone()

    assert result is not None
    assert result[0].startswith("400000000")


def test_120_studiepoeng_eksamen_bruker_foerste_dato() -> None:
    con = duckdb.connect()
    con.execute(_DUCKDB_MACROS)

    result = con.sql("""
        SELECT UTD_HOEYESTE_RANGERING(
            '637199',
            CAST('2006-01-01' AS DATE),
            120,
            '99',
            NULL,
            '11',
            '2005',
            '3',
            NULL,
            '04'
        ) AS rangering
    """).fetchone()

    assert result is not None
    assert result[0].startswith("320060101")
