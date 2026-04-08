import duckdb as db


def _generate_utd_foreldres_utdnivaa_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    slekt_snr = NudbData("slekt_snr")
    snr2alder16 = NudbData("_snr2alder16")
    utd_hoeyeste = NudbData("utd_hoeyeste")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            T1.snr AS snr,
            T2.pers_foedselsdato AS pers_foedselsdato,
            T2.pers_aar_16 AS pers_aar_16,
            T1.far_snr AS far_snr,
            T1.mor_snr AS mor_snr,

            T3.utd_hoeyeste_nus2000 AS utd_hoeyeste_far_nus2000,
            T3.utd_hoeyeste_rangering AS far_utd_hoeyeste_rangering,

            T4.utd_hoeyeste_nus2000 AS utd_hoeyeste_mor_nus2000,
            T4.utd_hoeyeste_rangering AS mor_utd_hoeyeste_rangering,

            CASE
                WHEN far_utd_hoeyeste_rangering IS NULL                      THEN utd_hoeyeste_mor_nus2000 -- if mor_utd_hoeyeste_rangering is NULL then we just get NULL
                WHEN mor_utd_hoeyeste_rangering IS NULL                      THEN utd_hoeyeste_far_nus2000
                WHEN far_utd_hoeyeste_rangering > mor_utd_hoeyeste_rangering THEN utd_hoeyeste_far_nus2000
                ELSE                                                              utd_hoeyeste_mor_nus2000 -- pick utd_hoeyeste_mor_nus2000 if better or equal to far_nus2000
            END AS utd_foreldres_utdnivaa_16aar_nus2000
        FROM
            {slekt_snr.alias} AS T1

        LEFT JOIN
            {snr2alder16.alias} AS T2
        ON
            T1.snr = T2.snr

        ASOF LEFT JOIN
            {utd_hoeyeste.alias} AS T3
        ON
            T1.far_snr = T3.snr AND
            T3.utd_hoeyeste_aar <= T2.pers_aar_16

        ASOF LEFT JOIN
            {utd_hoeyeste.alias} AS T4
        ON
            T1.mor_snr = T4.snr AND
            T4.utd_hoeyeste_aar <= T2.pers_aar_16;
    """

    connection.sql(query)
