import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_avslutta_subset_view(
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
                fnr, snr, nus2000, utd_aktivitet_start, utd_aktivitet_slutt, utd_skoleaar_start, 
                utd_skolekom, utd_utdanningstype, utd_fullfoertkode, utd_aktivitetsnivaa_heltid_deltid,
                utd_klassetrinn, utd_viderutd_nettbasert
                -- bof_eierforhold, -- Er utledbar variabel
                gr_grunnskolepoeng, gro_elevstatus, gro_programomraade,
                vg_proevekandtype, vg_rettstype_inntak, vg_utdprogram, vg_fullfoertkode_detaljert,
                vg_yrkes_og_studiekompetanse, vg_kursprosent
                fuh_nett_eller_stedbasert, 
                uh_studieprogram, uh_studiepoeng_grad

            FROM
                {avslutta_microdata.alias}
            );
    """

    connection.execute(query)
