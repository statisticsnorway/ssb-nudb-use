import duckdb as db


def _generate_utd_person_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    def _select(priority: int) -> str:
        return f"DISTINCT snr, pers_foedselsdato, pers_kjoenn, nudb_dataset_id, {priority} AS nudb_dataset_priority"

    igang = NudbData("igang").select(_select(1))
    avslutta = NudbData("avslutta").select(_select(2))
    eksamen = NudbData("eksamen").select(_select(3))
    snr2fodt = NudbData("freg_situttak").select(
        "DISTINCT snr, foedselsdato AS pers_foedselsdato, kjoenn AS pers_kjoenn"
    )
    slekt_snr = NudbData("slekt_snr")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            T1.snr AS snr,
            -- Keep information from freg, if it's available
            CASE
                WHEN T2.pers_foedselsdato IS NOT NULL THEN T2.pers_foedselsdato
                ELSE                                       T1.pers_foedselsdato
            END AS pers_foedselsdato,
            CASE
                WHEN T2.pers_kjoenn IS NOT NULL THEN T2.pers_kjoenn
                ELSE                                 T1.pers_kjoenn
            END AS pers_kjoenn,

            T3.far_snr AS far_snr,
            T3.mor_snr AS mor_snr,

            CONCAT(T1.nudb_dataset_id, '>utd_person') AS nudb_dataset_id

        FROM (
            SELECT DISTINCT -- Should be DISTINCT by design, but add the clause just in case
                snr,
                FIRST(pers_foedselsdato     ORDER BY nudb_dataset_priority ASC NULLS LAST) AS pers_foedselsdato,
                FIRST(pers_kjoenn           ORDER BY nudb_dataset_priority ASC NULLS LAST) AS pers_kjoenn,
                FIRST(nudb_dataset_id       ORDER BY nudb_dataset_priority ASC NULLS LAST) AS nudb_dataset_id,
                FIRST(nudb_dataset_priority ORDER BY nudb_dataset_priority ASC NULLS LAST) AS nudb_dataset_priority

            FROM (

                SELECT DISTINCT -- Should be DISTINCT by design, but add the clause just in case
                    snr,
                    FIRST(pers_foedselsdato ORDER BY pers_foedselsdato DESC NULLS LAST) AS pers_foedselsdato,
                    FIRST(pers_kjoenn       ORDER BY pers_foedselsdato DESC NULLS LAST) AS pers_kjoenn,
                    nudb_dataset_id,
                    FIRST(nudb_dataset_priority) AS nudb_dataset_priority

                FROM (
                    {igang._get_query()}
                    UNION
                    {avslutta._get_query()}
                    UNION
                    {eksamen._get_query()}
                )

                GROUP BY
                    snr, nudb_dataset_id
            )

            GROUP BY
                snr
        ) AS T1

        LEFT JOIN
            ({snr2fodt._get_query()}) AS T2
        ON
            T1.snr = T2.snr

        LEFT JOIN
            {slekt_snr.alias} AS T3
        ON
            T1.snr = T3.snr;
    """

    connection.sql(query)


def _generate_slekt_snr_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    slekt = NudbData("slekt")
    fnr2snr = NudbData("_snrkat_fnr2snr")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            T2.snr                                           AS snr,
            ANY_VALUE(T3.snr)                                AS far_snr,
            ANY_VALUE(T4.snr)                                AS mor_snr,
            ANY_VALUE(CONCAT(nudb_dataset_id, '>slekt_snr')) AS nudb_dataset_id
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

        GROUP BY
            T2.snr
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
