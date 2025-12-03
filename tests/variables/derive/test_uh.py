import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables.derive.uh import uh_univ_eller_hogskole


def test_utd_skoleaar_slutt(avslutta: pd.DataFrame) -> None:
    updated_colnames = update_colnames(avslutta)
    df_univ = uh_univ_eller_hogskole(updated_colnames)
    assert "1" in df_univ["uh_univ_eller_hogskole"].unique()
    assert "2" in df_univ["uh_univ_eller_hogskole"].unique()
