import pandas as pd
from nudb_config import settings

from nudb_use.variables.derive.derive_decorator import wrap_derive


@wrap_derive
def utd_foreldres_utdnivaa_16aar(
    df: pd.DataFrame, year_col: str | None = None
) -> pd.DataFrame:
    """Derive `utd_foreldres_utdnivaa_16aar`."""
    from nudb_use.datasets.nudb_data import NudbData

    merge_keys = settings.variables.utd_foreldres_utdnivaa_16aar.derived_join_keys

    utd_foreldres_utdnivaa_df = (
        NudbData("utd_foreldres_utdnivaa")
        .select("DISTINCT snr, utd_foreldres_utdnivaa_16aar")
        .df()
    )

    return df.merge(
        utd_foreldres_utdnivaa_df, on=merge_keys, how="left", validate="m:1"
    )
