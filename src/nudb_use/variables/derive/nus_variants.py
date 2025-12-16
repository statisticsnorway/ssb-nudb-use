import pandas as pd
from nudb_config import settings

from nudb_use.metadata.nudb_klass.variants import klass_variant_search_term_mapping

from .derive_decorator import wrap_derive


def _map_nus_variant(df: pd.DataFrame, varname: str) -> pd.Series:
    """Map `nus2000` through the KLASS variant defined for `varname`."""
    var_meta = settings.variables[varname]
    return df["nus2000"].map(klass_variant_search_term_mapping(var_meta))


@wrap_derive
def utd_klassetrinn_lav_hoy_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000."""
    return _map_nus_variant(df, "utd_klassetrinn_lav_hoy_nus")


@wrap_derive
def utd_klassetrinn_lav_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000."""
    utd_klassetrinn_lav_hoy_nus: pd.Series = df["utd_klassetrinn_lav_hoy_nus"]
    return utd_klassetrinn_lav_hoy_nus.str.split("-", n=1, expand=True)[0]


@wrap_derive
def utd_klassetrinn_hoy_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_klassetrinn_lav_hoy_nus from nus2000."""
    utd_klassetrinn_lav_hoy_nus: pd.Series = df["utd_klassetrinn_lav_hoy_nus"]
    return utd_klassetrinn_lav_hoy_nus.str.split("-", n=1, expand=True)[1]


@wrap_derive
def vg_kompetanse_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_kompetanse_nus from nus2000."""
    return _map_nus_variant(df, "vg_kompetanse_nus")


@wrap_derive
def vg_kurstrinn_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive vg_kurstrinn_nus from nus2000."""
    return _map_nus_variant(df, "vg_kurstrinn_nus")


# Until we have discussed this we should not make it easily derivable, might prioritize getting this from archive data
# @wrap_derive
# def fa_erfagskole_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
#    df: pd.DataFrame,
# ) -> pd.Series:
#    """Derive fa_erfagskole_nus from nus2000."""
#    return (_map_nus_variant(df, "fa_erfagskole_nus") == "10").astype("bool[pyarrow]")


@wrap_derive
def utd_erforeldet_kode_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_historisk_foreldet_fag_nus from nus2000."""
    return (_map_nus_variant(df, "utd_erforeldet_kode_nus") == "*").astype(
        "bool[pyarrow]"
    )


@wrap_derive
def fa_studiepoeng_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive fa_studiepoeng_nus from nus2000."""
    return _map_nus_variant(df, "fa_studiepoeng_nus")


@wrap_derive
def uh_studiepoeng_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_studiepoeng_nus from nus2000."""
    return _map_nus_variant(df, "uh_studiepoeng_nus")


@wrap_derive
def uh_gradmerke_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_gradmerke_nus from nus2000."""
    return _map_nus_variant(df, "uh_gradmerke_nus")


@wrap_derive
def uh_gruppering_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive uh_gruppering_nus from nus2000."""
    return _map_nus_variant(df, "uh_gruppering_nus")


@wrap_derive
def utd_samle_eller_enkeltutd_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_samle_eller_enkeltutd_nus from nus2000."""
    return _map_nus_variant(df, "utd_samle_eller_enkeltutd_nus")


@wrap_derive
def utd_varighet_antall_mnd_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_varighet_antall_mnd_nus from nus2000."""
    return _map_nus_variant(df, "utd_varighet_antall_mnd_nus")


@wrap_derive
def utd_utdanningsprogram_nus(  # noqa: DOC101,DOC103,DOC201,DOC203
    df: pd.DataFrame,
) -> pd.Series:
    """Derive utd_utdanningsprogram_nus from nus2000."""
    return _map_nus_variant(df, "utd_utdanningsprogram_nus")
