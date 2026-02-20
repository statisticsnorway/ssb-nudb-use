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
