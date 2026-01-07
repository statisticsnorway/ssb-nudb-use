"""Variable-derivation helpers for NUDB pipelines."""

module = __import__(__name__, fromlist=[""])  # pass nonempty fromlist
from collections.abc import Callable
from types import ModuleType

import nudb_use.variables.derive.klass_correspondences_and_variants as klass_correspondences_and_variants
import nudb_use.variables.derive.klass_labels as klass_labels
from nudb_use.nudb_logger import logger

from .derive_decorator import get_derive_function
from .klass_correspondences_and_variants import __all__ as klass_derive_funcs
from .klass_labels import __all__ as label_funcs
from .nus_variants import utd_erforeldet_kode_nus
from .nus_variants import utd_klassetrinn_hoy_nus
from .nus_variants import utd_klassetrinn_lav_nus
from .uh_univ_eller_hogskole import uh_univ_eller_hogskole
from .utd_skoleaar import utd_skoleaar_slutt

__all__ = [
    "uh_univ_eller_hogskole",
    "utd_erforeldet_kode_nus",
    "utd_klassetrinn_hoy_nus",
    "utd_klassetrinn_lav_nus",
    "utd_skoleaar_slutt",
]


def add_function(name: str, submodule: ModuleType) -> None:
    try:
        setattr(module, name, getattr(submodule, name))
        __all__.append(name)  # could also just do __all__ += label_funcs
    except Exception as err:
        logger.warning(
            f"Unable to attach '{name}' function to 'derive' module!\nMessage: {err}"
        )


for label_func in label_funcs:
    add_function(label_func, klass_labels)

for derive_func in klass_derive_funcs:
    add_function(derive_func, klass_correspondences_and_variants)
