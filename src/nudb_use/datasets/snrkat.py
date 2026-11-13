import duckdb as db


def _generate_snrkat_fnr2snr_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    snrkat = NudbData("snrkat")

    # Several source rows may supply the same pair. Retain one provenance value
    # without changing the mapping's existing one-row-per-pair cardinality.
    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT
            fnr,
            snr,
            MIN(nudb_dataset_id) || '>_snrkat_fnr2snr' AS nudb_dataset_id
        FROM {snrkat.alias}
        GROUP BY fnr, snr;
    """

    connection.sql(query)
