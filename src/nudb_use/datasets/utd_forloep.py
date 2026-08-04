import duckdb as db


def _generate_utd_forloep_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    avslutta = NudbData("avslutta")
    igang = NudbData("igang")

    query = f"""
        CREATE VIEW
            {alias}
        AS SELECT DISTINCT
            /* Identifiers */
            COALESCE(IGANG.snr, AVSLUTTA.snr) AS snr,
            COALESCE(IGANG.nus2000, AVSLUTTA.nus2000) AS nus2000,
            COALESCE(IGANG.uh_institusjon_id, AVSLUTTA.uh_institusjon_id) AS uh_institusjon_id,
            COALESCE(IGANG.utd_utdanningstype, AVSLUTTA.utd_utdanningstype) AS utd_utdanningstype,
            COALESCE(IGANG.utd_kurs_id, AVSLUTTA.utd_kurs_id) AS utd_kurs_id,

            /* Information IGANG */
            IGANG.utd_aktivitet_start,
            IGANG.utd_skoleaar_start_igang,
            IGANG.gro_skolenr_inn_igang,
            IGANG.orgnr_foretak_igang,
            IGANG.orgnrbed_igang,

            /* Information AVSLUTTA */
            AVSLUTTA.utd_aktivitet_slutt,
            AVSLUTTA.utd_fullfoertkode,
            AVSLUTTA.utd_skoleaar_start_avslutta,
            AVSLUTTA.gro_skolenr_inn_avslutta,
            AVSLUTTA.orgnr_foretak_avslutta,
            AVSLUTTA.orgnrbed_avslutta,

            /* NUDB_DATASET_ID */
            IGANG.nudb_dataset_id_igang,
            AVSLUTTA.nudb_dataset_id_avslutta,
            CASE
                WHEN nudb_dataset_id_igang    IS NULL THEN CONCAT(nudb_dataset_id_avslutta, '>{alias}')
                WHEN nudb_dataset_id_avslutta IS NULL THEN CONCAT(nudb_dataset_id_igang, '>{alias}')
                ELSE CONCAT(
                    '((', nudb_dataset_id_igang, ')+(', nudb_dataset_id_avslutta, '))>',
                    '{alias}'
                )
            END AS nudb_dataset_id
        FROM (
            SELECT DISTINCT
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
                utd_skoleaar_start AS utd_skoleaar_start_igang,
                gro_skolenr_inn AS gro_skolenr_inn_igang,
                orgnr_foretak AS orgnr_foretak_igang,
                orgnrbed AS orgnrbed_igang,
                nudb_dataset_id AS nudb_dataset_id_igang
            FROM
                {igang.alias}
            WHERE
                snr IS NOT NULL /* Shouldn't happen, but just in case */
        ) AS IGANG

        FULL OUTER JOIN (
            SELECT DISTINCT
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
                utd_skoleaar_start AS utd_skoleaar_start_avslutta,
                gro_skolenr_inn AS gro_skolenr_inn_avslutta,
                orgnr_foretak AS orgnr_foretak_avslutta,
                orgnrbed AS orgnrbed_avslutta,
                nudb_dataset_id AS nudb_dataset_id_avslutta
            FROM
                {avslutta.alias}
            WHERE
                snr IS NOT NULL /* Shouldn't happen, but just in case */
        ) AS AVSLUTTA

        ON
            IGANG.utd_kurs_id = AVSLUTTA.utd_kurs_id;
    """

    connection.sql(query)
