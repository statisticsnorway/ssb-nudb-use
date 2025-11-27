import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables.derive.pick_id_col import detect_pers_id_fnr_used


def test_detect_pers_id_fnr_used(avslutta: pd.DataFrame) -> None:
    updated_colnames = update_colnames(avslutta)
    assert "fnr" == detect_pers_id_fnr_used(updated_colnames["fnr"])
    assert "pers_id" == detect_pers_id_fnr_used(updated_colnames["pers_id"])
