import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_igang_subset_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:
    """Generate subset view of igang for Microdata."""
    from nudb_use.datasets import NudbData  # Avoids circular import

    logger.info("Deriving 'igang_microdata' Microdata variables")

    # Burde defineres bedre i f.eks nudb-config en her
    igang_microdata = NudbData("igang")
    query = f"""
        CREATE OR REPLACE VIEW  {alias} AS (
            SELECT
                fnr, snr, nus2000, utd_aktivitet_start, utd_skoleaar_start, 
                utd_skolekom, utd_utdanningstype, utd_aktivitetsnivaa_heltid_deltid,
                utd_klassetrinn, utd_viderutd_nettbasert, utd_utveksling,
                -- bof_eierforhold, -- Er utledbar variabel
                gro_elevstatus, gro_programomraade 
                vg_proevekandtype, vg_rettstype_inntak, vg_utdprogram,
                vg_yrkes_og_studiekompetanse, vg_antall_aarstimer_elevkurs,
                vg_kursprosent, vg_kontraktstype, 
                fuh_nett_eller_stedbasert, fuh_opptaksgrunnlag, fuh_utvekslingsland,
                uh_studieprogram, uh_studierett, uh_utvekslingsavtale,
                uh_studieprogresjon_hoest, uh_studgrunnlagsland, 

            FROM
                {igang_microdata.alias}
            );
    """

    connection.execute(query)
