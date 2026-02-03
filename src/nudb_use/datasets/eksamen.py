def init_eksamen_aggregated(connection):
    from nudb_use.datasets.nudb_datasets import NudbDataSet

    # We have to review this logic a bit
    # this is quite a naive approach
    # we should atleast split this by year, such that we don't
    # attempt to aggregate aarganger where the aggregation has already been done
    query = f"""
        SELECT
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            uh_eksamen_er_gjentak,

            FIRST(fnr), -- may not be unique per snr
            MAX(uh_eksamen_dato), -- pick the date of the last exam
            MAX(uh_eksamen_karakter), -- maybe wrong
            SUM(uh_eksamen_studpoeng)
        FROM
            {NudbDataSet("eksamen").alias}
        GROUP BY
            snr,
            nus2000,
            uh_institusjon_id,
            utd_skoleaar_start,
            uh_studienivaa,
            uh_eksamen_er_gjentak;
    """

    return connection.sql(query).df()
