"""Variable-derivation helpers for NUDB pipelines."""

from .nus_variants import utd_historisk_foreldet_fag_nus
from .nus_variants import utd_klassetrinn_lav_hoy_nus
from .uh_univ_eller_hogskole import uh_univ_eller_hogskole
from .utd_skoleaar import utd_skoleaar_slutt

__all__ = [
    "uh_univ_eller_hogskole",
    "utd_historisk_foreldet_fag_nus",
    "utd_klassetrinn_lav_hoy_nus",
    "utd_skoleaar_slutt",
]
