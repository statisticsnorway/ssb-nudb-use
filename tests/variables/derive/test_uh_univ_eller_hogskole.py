import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables.derive.uh_univ_eller_hoegskole import uh_univ_eller_hoegskole


def test_uh_univ_eller_hoegskole(avslutta: pd.DataFrame) -> None:
    updated_colnames = update_colnames(avslutta)
    df_univ = uh_univ_eller_hoegskole(updated_colnames)
    assert "1" in df_univ["uh_univ_eller_hoegskole"].unique()
    assert "2" in df_univ["uh_univ_eller_hoegskole"].unique()
