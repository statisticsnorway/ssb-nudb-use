import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_avslutta_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate subset view of avslutta for Microdata."""
    from nudb_use.datasets import NudbData  # Avoids circular import

    logger.info("Deriving 'avslutta_microdata' Microdata variables")


    # Burde defineres bedre i f.eks nudb-config en her
    avslutta_microdata = NudbData("avslutta")
    query = f"""
        CREATE OR REPLACE VIEW  {alias} AS (
            SELECT
                fnr AS fnr,
                snr AS snr, 
                nus2000 AS nus2000,
                -- utd-variabl
                utd_aktivitet_start AS AVSL_utd_aktivitet_start,
                utd_aktivitet_slutt AS AVSL_utd_aktivitet_slutt,
                utd_skoleaar_start AS AVSL_utd_skoleaar_start,
                utd_skolekom AS AVSL_utd_skolekom,
                utd_utdanningstype AS AVSL_utd_utdanningstype,
                utd_fullfoertkode AS AVSL_utd_fullfoertkode,
                utd_aktivitetsnivaa_heltid_deltid AS AVSL_utd_aktivitetsnivaa_heltid_deltid,                
                utd_klassetrinn AS AVSL_utd_klassetrinn, 
                utd_viderutd_nettbasert AS AVSL_utd_viderutd_nettbasert,
                -- bof_eierforhold, -- Er utledbar variabel
                -- gr- og gro-variabler
                gr_grunnskolepoeng AS AVSL_gr_grunnskolepoeng,
                gro_elevstatus AS AVSL_gro_elevstatus,
                gro_programomraade AS AVSL_gro_programomraade,
                -- vg-variabler
                vg_proevekandtype AS AVSL_vg_proevekandtype,
                vg_rettstype_inntak AS AVSL_vg_rettstype_inntak,
                vg_utdprogram AS AVSL_vg_utdprogram,
                vg_fullfoertkode_detaljert AS AVSL_vg_fullfoertkode_detaljert,
                vg_yrkes_og_studiekompetanse AS AVSL_vg_yrkes_og_studiekompetanse,
                vg_kursprosent AS AVSL_vg_kursprosent,
                -- fuh-variabler
                fuh_nett_eller_stedbasert AS AVSL_fuh_nett_eller_stedbasert, 
                -- uh-variabler
                uh_studieprogram AS AVSL_uh_studieprogram, 
                uh_studiepoeng_grad AS AVSL_uh_studiepoeng_grad

            FROM
                {avslutta_microdata.alias}
            );
    """

    connection.execute(query)
