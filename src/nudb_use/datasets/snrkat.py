import duckdb as db


def _generate_snrkat_fnr2snr_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    snrkat = NudbData("snrkat")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            fnr, snr
        FROM {snrkat.alias};
    """

    connection.sql(query)


def _generate_snrkat_snr2fnr_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    snrkat = NudbData("snrkat")

    connection.execute(f"""
        CREATE VIEW {alias} AS
        SELECT DISTINCT snr, fnr_naa AS fnr
        FROM {snrkat.alias}
    """)
