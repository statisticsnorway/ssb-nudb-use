import duckdb as db


def _generate_slekt_snr_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    slekt = NudbData("slekt")
    fnr2snr = NudbData("snrkat_fnr2snr")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT
            T2.snr AS snr,
            T3.snr AS far_snr,
            T4.snr AS mor_snr,
            CONCAT(nudb_dataset_id, '>slekt_snr') AS nudb_dataset_id
        FROM
            {slekt.alias} AS T1

        -- fnr -> snr
        LEFT JOIN
            {fnr2snr.alias} AS T2
        ON
            T1.fnr = T2.fnr

        -- far_fnr -> far_snr
        LEFT JOIN
            {fnr2snr.alias} AS T3
        ON
            T1.far_fnr = T3.fnr

        -- mor_fnr -> mor_snr
        LEFT JOIN
            {fnr2snr.alias} AS T4
        ON
            T1.mor_fnr = T4.fnr
    """

    connection.sql(query)
