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


def _generate_snr2alder16_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    utd_hoeyeste = NudbData("utd_hoeyeste")
    freg = NudbData("freg_situttak")

    last_year = int(
        utd_hoeyeste.select("MAX(utd_hoeyeste_aar) AS last_year").df()["last_year"][0]
    )

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            snr,
            foedselsdato AS pers_foedselsdato,
            EXTRACT(YEAR FROM foedselsdato) AS pers_foedselsaar,
            CASE
                WHEN pers_foedselsaar + 16 >  {last_year} THEN {last_year}
                WHEN pers_foedselsaar + 16 <  1970        THEN 1970
                WHEN pers_foedselsaar + 16 <= {last_year} THEN pers_foedselsaar + 16
            END AS pers_aar_16
        FROM
            {freg.alias};
    """

    connection.sql(query)


def _generate_utd_sosbak_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    slekt_snr = NudbData("slekt_snr")
    snr2alder16 = NudbData("snr2alder16")
    utd_hoeyeste = NudbData("utd_hoeyeste")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT
            T1.snr AS snr,
            T2.pers_foedselsdato AS pers_foedselsdato,
            T2.pers_aar_16 AS pers_aar_16,
            T1.far_snr AS far_snr,
            T1.mor_snr AS mor_snr,

            T3.nus2000 AS far_nus2000,
            T3.utd_hoeyeste_rangering AS far_utd_hoeyeste_rangering,

            T4.nus2000 AS mor_nus2000,
            T4.utd_hoeyeste_rangering AS mor_utd_hoeyeste_rangering,

            CASE
                WHEN far_utd_hoeyeste_rangering IS NULL                      THEN mor_nus2000 -- if mor_utd_hoeyeste_rangering is NULL then we just get NULL
                WHEN mor_utd_hoeyeste_rangering IS NULL                      THEN far_nus2000
                WHEN far_utd_hoeyeste_rangering > mor_utd_hoeyeste_rangering THEN far_nus2000
                ELSE                                                              mor_nus2000 -- pick mor_nus2000 if better or equal to far_nus2000
            END AS utd_sosbak
        FROM
            {slekt_snr.alias} AS T1

        LEFT JOIN
            {snr2alder16.alias} AS T2
        ON
            T1.snr = T2.snr

        LEFT JOIN
            {utd_hoeyeste.alias} AS T3
        ON
            T1.far_snr = T3.snr AND
            T2.pers_aar_16 = T3.utd_hoeyeste_aar

        LEFT JOIN
            {utd_hoeyeste.alias} AS T4
        ON
            T1.mor_snr = T4.snr AND
            T2.pers_aar_16 = T4.utd_hoeyeste_aar
    """

    connection.sql(query)
