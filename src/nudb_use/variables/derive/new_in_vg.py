import duckdb
from nudb_use.variables.utled.pick_id_col import detect_pers_id_fnr_used
from nudb_use.paths.latest import latest_shared_paths


def new_in_vg(pers_id_fnr: pd.Series) -> pd.Series:
    id_col = detect_pers_id_fnr_used(pers_id_fnr)
    unique_ids = pers_id_fnr.dropna().unique().to_list()
    
    raise NotImplementedError("Just trying out the deriving part, we need to share some data first")
    
    igang_path = latest_shared_paths()["igangvaerende"]
    avslutta_path = latest_avslutta_path()["avslutta"]

    
    
    sql = """


    """

    duckdb.sql(sql)
    
    # Check igang for the first time these persons did anything in vgs.
    
    # Check avslutta for the first time these persons did anything in vgs

    # Concat igang and avslutta, sort by start_date, keep first date

    # Return first date 
