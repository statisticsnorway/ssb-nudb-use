"""Variable-derivation helpers for NUDB pipelines."""

from .nus_correspondences import utd_isced2011_attainment_nus
from .nus_correspondences import utd_isced2011_programmes_nus
from .nus_correspondences import utd_isced2013_fagfelt_nus
from .nus_variants import fa_erfagskole_nus
from .nus_variants import fa_studiepoeng_nus
from .nus_variants import uh_gradmerke_nus
from .nus_variants import uh_gruppering_nus
from .nus_variants import uh_studiepoeng_nus
from .nus_variants import utd_erhistorisk_foreldet_fag_nus
from .nus_variants import utd_klassetrinn_hoy_nus
from .nus_variants import utd_klassetrinn_lav_hoy_nus
from .nus_variants import utd_klassetrinn_lav_nus
from .nus_variants import utd_samle_eller_enkeltutd_nus
from .nus_variants import utd_utdanningsprogram_nus
from .nus_variants import utd_varighet_antall_mnd_nus
from .nus_variants import vg_kompetanse_nus
from .nus_variants import vg_kurstrinn_nus
from .uh_univ_eller_hogskole import uh_univ_eller_hogskole
from .utd_skoleaar import utd_skoleaar_slutt

__all__ = [
    "fa_erfagskole_nus",
    "fa_studiepoeng_nus",
    "uh_gradmerke_nus",
    "uh_gruppering_nus",
    "uh_studiepoeng_nus",
    "uh_univ_eller_hogskole",
    "utd_erhistorisk_foreldet_fag_nus",
    "utd_isced2011_attainment_nus",
    "utd_isced2011_programmes_nus",
    "utd_isced2013_fagfelt_nus",
    "utd_klassetrinn_hoy_nus",
    "utd_klassetrinn_lav_hoy_nus",
    "utd_klassetrinn_lav_nus",
    "utd_samle_eller_enkeltutd_nus",
    "utd_skoleaar_slutt",
    "utd_utdanningsprogram_nus",
    "utd_varighet_antall_mnd_nus",
    "vg_kompetanse_nus",
    "vg_kurstrinn_nus",
]
