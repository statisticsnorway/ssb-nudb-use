import pandas as pd

from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.variables.derive.utd_skoleaar import utd_skoleaar_slutt


def test_utd_skoleaar_slutt(avslutta: pd.DataFrame) -> None:
    updated_colnames = update_colnames(avslutta)
    updated_skoleaar_slutt = utd_skoleaar_slutt(updated_colnames)
    assert "utd_skoleaar_slutt" in updated_skoleaar_slutt.columns
