import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables.derive.uh import univ


def test_utd_skoleaar_slutt(avslutta: pd.DataFrame) -> None:
    updated_colnames = update_colnames(avslutta)
    univ_var = univ(updated_colnames)
    assert "1" in univ_var.unique()
    assert "2" in univ_var.unique()
