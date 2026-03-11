import duckdb as db
from nudb_use.paths.latest import latest_shared_paths

def _generate_vof_all_orgnr(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData
    
    paths = latest_shared_paths("vof_sittuttak")
    
    snrkat = NudbData("snrkat")

    query = f"""
        CREATE VIEW
            {alias} AS
        SELECT DISTINCT
            orgnr as orgnr_foretak, orgnrbed
        FROM {snrkat.alias}
        WHERE orgnrbed != NULL
        ;
    """

    connection.sql(query)
