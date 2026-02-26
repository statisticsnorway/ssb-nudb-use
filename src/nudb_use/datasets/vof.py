import duckdb as db


def _generate_vof_eierforhold_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    vof_situttak = NudbData("vof_situttak")

    query = f"""
        -- KIRK i orgform skal ha eierforhold privat. undersektor med fra 2014 for å skille ut komm fylk --
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            org_nr AS vof_orgnr_foretak,
            CASE
                WHEN (org_form == 'KIRK')                                                          THEN '3'
                WHEN (org_form == 'STAT' AND sektor_2014 == '6100')                                THEN '1'
                WHEN (org_form == 'SÆR'  AND sektor_2014 == '6100'  AND undersektor_2014 == '005') THEN '1'
                WHEN (org_form == 'KOMM' AND sektor_2014 == '6500'  AND undersektor_2014 == '006') THEN '4'
                WHEN (org_form == 'KF'   AND sektor_2014 == '6500'  AND undersektor_2014 == '006') THEN '4'
                WHEN (org_form == 'IKS'  AND sektor_2014 == '6500'  AND undersektor_2014 == '006') THEN '4'
                WHEN (org_form == 'ORGL' AND sektor_2014 == '6500'  AND undersektor_2014 == '006') THEN '4'
                WHEN (org_form == 'FYLK' AND sektor_2014 == '6500'  AND undersektor_2014 == '007') THEN '5'
                ELSE                                                                                    '3'
            END AS vof_eierforhold
        FROM
            {vof_situttak.alias}
        WHERE
            vof_orgnr_foretak IS NOT NULL AND
            orgnrbed          IS NULL
        ;
    """

    # Logikk i grunnskoledata
    #        undersektor_col = 'undersektor_2014'
    #
    #    # Definer betingelser og tilhørende verdier
    #    conditions = [
    #        (df[undersektor_col] == "006"),
    #        (df[undersektor_col] == "007"),
    #        (df[undersektor_col] == "001"),
    #        (df[undersektor_col] == "005"),
    #        (df[undersektor_col].isna())
    #    ]
    #
    #    values = ["4", "5", "3", "1", "3"]

    connection.sql(query)


def _generate_vof_orgnr_bed2foretak_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    vof_situttak = NudbData("vof_situttak")

    query = f"""
        -- KIRK i orgform skal ha eierforhold privat. undersektor med fra 2014 for å skille ut komm fylk --
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            orgnrbed AS vof_orgnrbed,
            org_nr   AS vof_orgnr_foretak
        FROM
            {vof_situttak.alias}
        WHERE
            vof_orgnrbed IS NOT NULL AND
            vof_orgnr_foretak IS NOT NULL
        ;
    """

    connection.sql(query)
