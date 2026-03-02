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
            orgnrbed AS bof_orgnrbed,
            org_form,

            CASE
                WHEN undersektor_2014 == '001' THEN '3'
                WHEN undersektor_2014 == '005' THEN '1'
                WHEN undersektor_2014 == '006' THEN '4'
                WHEN undersektor_2014 == '007' THEN '5'
                ELSE                                '3'
            END AS vof_eierforhold

        FROM
            {vof_situttak.alias}

        WHERE
            vof_orgnr_foretak IS NOT NULL AND
            orgnrbed          IS NULL AND
            org_form          IS NOT NULL
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
            orgnrbed AS bof_orgnrbed,
            org_nr   AS vof_orgnr_foretak
        FROM
            {vof_situttak.alias}
        WHERE
            bof_orgnrbed IS NOT NULL AND
            vof_orgnr_foretak IS NOT NULL
        ;
    """

    connection.sql(query)
