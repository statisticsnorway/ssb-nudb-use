"""Variable-derivation helpers for NUDB pipelines."""

module = __import__(__name__, fromlist=[""])  # pass nonempty fromlist
import nudb_use.variables.derive.klass_labels as klass_labels
from nudb_use.nudb_logger import logger

from .derive_decorator import get_derive_function
from .klass_labels import __all__ as label_funcs
from .nus_correspondences import utd_isced2011_attainment_nus
from .nus_correspondences import utd_isced2011_programmes_nus
from .nus_correspondences import utd_isced2013_fagfelt_nus
from .nus_variants import fa_studiepoeng_nus
from .nus_variants import uh_gradmerke_nus
from .nus_variants import uh_gruppering_nus
from .nus_variants import uh_studiepoeng_nus
from .nus_variants import utd_erforeldet_kode_nus
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
    "fa_studiepoeng_nus",
    "uh_gradmerke_nus",
    "uh_gruppering_nus",
    "uh_studiepoeng_nus",
    "uh_univ_eller_hogskole",
    "utd_erforeldet_kode_nus",
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


for label_func in label_funcs:
    try:
        setattr(module, label_func, getattr(klass_labels, label_func))
        __all__.append(label_func)  # could also just do __all__ += label_funcs
    except Exception as err:
        logger.warning(
            f"Unable to attach '{label_func}' function to 'derive' module!\nMessage: {err}"
        )
