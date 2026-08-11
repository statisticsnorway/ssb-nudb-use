import duckdb as db


def _generate_utd_forloep_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    avslutta = NudbData("avslutta")
    igang = NudbData("igang")

    query = f"""
        CREATE VIEW
            {alias}
        AS SELECT
            /* UTD_KURS_ID */
            COALESCE(IGANG.snr, AVSLUTTA.snr) AS snr,
            COALESCE(IGANG.nus2000, AVSLUTTA.nus2000) AS nus2000,
            COALESCE(IGANG.uh_institusjon_id, AVSLUTTA.uh_institusjon_id) AS uh_institusjon_id,
            COALESCE(IGANG.utd_utdanningstype, AVSLUTTA.utd_utdanningstype) AS utd_utdanningstype,
            COALESCE(IGANG.utd_kurs_id, AVSLUTTA.utd_kurs_id) AS utd_kurs_id,

            /* MOST IMPORTANT INFO */
            IGANG.utd_aktivitet_start,
            AVSLUTTA.utd_aktivitet_slutt,
            AVSLUTTA.utd_fullfoertkode,

            /* UTD_HENDELSE_ID */
            IGANG.utd_hendelse_id    AS utd_hendelse_id_igang,
            AVSLUTTA.utd_hendelse_id AS utd_hendelse_id_avslutta,

            /* NUDB_DATASET_ID */
            IGANG.nudb_dataset_id    AS nudb_dataset_id_igang,
            AVSLUTTA.nudb_dataset_id AS nudb_dataset_id_avslutta,

            CASE
                WHEN nudb_dataset_id_igang    IS NULL THEN CONCAT(nudb_dataset_id_avslutta, '>{alias}')
                WHEN nudb_dataset_id_avslutta IS NULL THEN CONCAT(nudb_dataset_id_igang, '>{alias}')
                ELSE CONCAT(
                    '((', nudb_dataset_id_igang, ')+(', nudb_dataset_id_avslutta, '))>',
                    '{alias}'
                )
            END AS nudb_dataset_id

        FROM (
            SELECT
                snr,
                PREP_NUS2000(nus2000)                       AS nus2000,
                PREP_UH_INSTITUSJON_ID(uh_institusjon_id)   AS uh_institusjon_id,
                PREP_UTD_UTDANNINGSTYPE(utd_utdanningstype) AS utd_utdanningstype,
                UTD_KURS_ID(
                    snr,
                    nus2000,
                    uh_institusjon_id,
                    utd_utdanningstype
                ) AS utd_kurs_id,
                utd_aktivitet_start,
                nudb_dataset_id,
                utd_hendelse_id
            FROM
                {igang.alias}
        ) AS IGANG

        FULL OUTER JOIN (
            SELECT
                snr,
                PREP_NUS2000(nus2000)                       AS nus2000,
                PREP_UH_INSTITUSJON_ID(uh_institusjon_id)   AS uh_institusjon_id,
                PREP_UTD_UTDANNINGSTYPE(utd_utdanningstype) AS utd_utdanningstype,
                UTD_KURS_ID(
                    snr,
                    nus2000,
                    uh_institusjon_id,
                    utd_utdanningstype
                ) AS utd_kurs_id,
                utd_aktivitet_slutt,
                utd_fullfoertkode,
                nudb_dataset_id,
                utd_hendelse_id
            FROM
                {avslutta.alias}
        ) AS AVSLUTTA

        ON
            IGANG.utd_kurs_id = AVSLUTTA.utd_kurs_id AND
            IGANG.utd_aktivitet_start <= AVSLUTTA.utd_aktivitet_slutt; /* Can't finish before you've started... */
    """

    connection.sql(query)
