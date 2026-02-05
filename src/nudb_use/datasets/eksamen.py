from nudb_use.paths.latest import latest_shared_paths


def _generate_eksamen_aggregated_view(alias: str, connection) -> None:
    from nudb_use.datasets.nudb_datasets import NudbDataset

    FAILED_KARAKTER_CODES = ["F", "H", "T", "X"]
    # We have to review this logic a bit
    # this is quite a naive approach
    # we should atleast split this by year, such that we don't
    # attempt to aggregate aarganger where the aggregation has already been done
    # It's pretty close, but we may need to
    query_select_before_2014 = f"""
        SELECT
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            uh_eksamen_er_gjentak,
            utd_orgnr,
            bof_orgnrbed,
            uh_antall_deleksamener,
            uh_antall_deleksamener_bestatt
            fnr,
            uh_eksamen_dato,
            uh_eksamen_karakter,
            uh_eksamen_studpoeng
        FROM
            {NudbDataset("eksamen").alias}
        WHERE
            utd_skoleaar_start < '2014'
    """
    
    query_aggregate_after_2014 = f"""
        SELECT
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            uh_eksamen_er_gjentak,
            utd_orgnr,
            bof_orgnrbed,

            COUNT(*) AS uh_antall_deleksamener,
            SUM(UPPER(uh_eksamen_karakter) NOT IN {FAILED_KARAKTER_CODES}) AS uh_antall_deleksamener_bestatt,
            FIRST(fnr) AS fnr,                                     -- may not be unique per snr
            MAX(uh_eksamen_dato) AS uh_eksamen_dato,               -- pick the date of the last exam
            FIRST(uh_eksamen_karakter) AS uh_eksamen_karakter,     -- Think we used to just pick random before. Carl: 'Using `MAX(uh_eksamen_karakter)` is too correct'
            SUM(uh_eksamen_studpoeng) AS uh_eksamen_studpoeng
        FROM
            {NudbDataset("eksamen").alias}
        WHERE
            utd_skoleaar_start >= '2014'
        GROUP BY
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            uh_eksamen_er_gjentak,
            utd_orgnr,
            bof_orgnrbed,
            EXTRACT(YEAR FROM uh_eksamen_dato) -- used to seperate semesters
    """
    query = f"""
        CREATE VIEW {alias} AS
        -- (
        --     {query_select_before_2014}
        -- )
        -- UNION
        --(
            {query_aggregate_after_2014}
        --)
        ;
    """
    query = f"""
        CREATE VIEW {alias} AS
        (
            {query_aggregate_before_2014}
        )
        ;
    """
    print(query)

    connection.sql(query)


def _generate_eksamen_view(alias: str, connection) -> None:
    path = latest_shared_paths("eksamen")

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT * FROM read_parquet('{path}')
    """

    connection.sql(query)