import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables.var_utils.pick_id_col import detect_snr_fnr_used


def test_detect_snr_fnr_used(avslutta: pd.DataFrame) -> None:
    updated_colnames = update_colnames(avslutta)
    assert "fnr" == detect_snr_fnr_used(updated_colnames["fnr"])
    assert "snr" == detect_snr_fnr_used(updated_colnames["snr"])
