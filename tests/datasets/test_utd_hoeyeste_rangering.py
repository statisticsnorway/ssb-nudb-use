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


def test_120_studiepoeng_eksamen_bruker_invertert_utdanningsaar() -> None:
    con = duckdb.connect()
    con.execute(_DUCKDB_MACROS)

    result = con.sql("""
        SELECT
            UTD_HOEYESTE_RANGERING(
                '637199',
                CAST('2006-09-01' AS DATE),
                120,
                '99',
                NULL,
                '11',
                '2005',
                '3',
                NULL,
                '04'
            ) AS foerste_utdanningsaar,
            UTD_HOEYESTE_RANGERING(
                '637199',
                CAST('2006-10-01' AS DATE),
                120,
                '99',
                NULL,
                '11',
                '2006',
                '3',
                NULL,
                '04'
            ) AS neste_utdanningsaar
    """).fetchone()

    assert result is not None
    assert result[0].startswith("379931201")
    assert result[1].startswith("379920101")
    assert result[0] > result[1]


def test_120_studiepoeng_eksamen_bruker_nyeste_dato_i_samme_utdanningsaar() -> None:
    con = duckdb.connect()
    con.execute(_DUCKDB_MACROS)

    result = con.sql("""
        SELECT
            UTD_HOEYESTE_RANGERING(
                '637199',
                CAST('2005-11-01' AS DATE),
                120,
                '99',
                NULL,
                '11',
                '2005',
                '3',
                NULL,
                '04'
            ) AS tidlig_i_utdanningsaar,
            UTD_HOEYESTE_RANGERING(
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
            ) AS senere_i_utdanningsaar
    """).fetchone()

    assert result is not None
    assert result[0].startswith("379930201")
    assert result[1].startswith("379930401")
    assert result[1] > result[0]


def test_utd_hoeyeste_aar_bruker_oktober_som_foerste_maaned() -> None:
    con = duckdb.connect()
    con.execute(_DUCKDB_MACROS)

    result = con.sql("""
        SELECT
            UTD_HOEYESTE_AAR(CAST('2018-09-01' AS DATE)) AS september,
            UTD_HOEYESTE_AAR(CAST('2018-10-01' AS DATE)) AS oktober
    """).fetchone()

    assert result == (2018, 2019)
