import duckdb as db

from nudb_use.nudb_logger import logger


def _generate_microdata_igang_view(
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
                fnr, 
                snr, 
                nus2000, 
                utd_aktivitet_start AS IGANG_utd_aktivitet_start,
                utd_skoleaar_start AS IGANG_utd_skoleaar_start, 
                utd_skolekom AS IGANG_utd_skolekom, 
                utd_utdanningstype AS IGANG_utd_utdanningstype, 
                utd_aktivitetsnivaa_heltid_deltid AS IGANG_utd_aktivitetsnivaa_heltid_deltid,
                utd_klassetrinn AS IGANG_utd_klassetrinn, 
                utd_viderutd_nettbasert AS IGANG_utd_viderutd_nettbasert, 
                utd_utveksling AS IGANG_utd_utveksling,
                -- bof_eierforhold, -- Er utledbar variabel
                -- gro-variabler
                gro_elevstatus AS IGANG_gro_elevstatus, 
                gro_programomraade AS IGANG_gro_programomraade,
                -- vg-variabler 
                -- vg_proevekandtype AS IGANG_vg_proevekandtype, 
                vg_rettstype_inntak AS IGANG_vg_rettstype_inntak, 
                vg_utdprogram AS IGANG_vg_utdprogram,
                vg_yrkes_og_studiekompetanse AS IGANG_vg_yrkes_og_studiekompetanse, 
                vg_antall_aarstimer_elevkurs AS IGANG_vg_antall_aarstimer_elevkurs,
                vg_kursprosent AS IGANG_vg_kursprosent, 
                vg_kontraktstype AS IGANG_vg_kontraktstype,
                -- fuh-variabler 
                fuh_nett_eller_stedbasert AS IGANG_fuh_nett_eller_stedbasert,
                fuh_opptaksgrunnlag AS IGANG_fuh_opptaksgrunnlag, 
                fuh_utvekslingsland as IGANG_fuh_utvekslingsland,
                -- uh-variabler
                uh_studieprogram AS IGANG_uh_studieprogram,
                uh_studierett AS IGANG_uh_studierett,
                uh_utvekslingsavtale AS IGANG_uh_utvekslingsavtale,
                uh_studieprogresjon_hoest AS IGANG_uh_studieprogresjon_hoest, 
                uh_studgrunnlagsland AS IGANG_uh_studgrunnlagsland, 

            FROM
                {igang_microdata.alias}
            );
    """

    connection.execute(query)
