"""Variable-derivation helpers for NUDB pipelines."""

module = __import__(__name__, fromlist=[""])  # pass nonempty fromlist
from collections.abc import Callable
from types import ModuleType

from nudb_use.nudb_logger import logger

from . import fullfoert
from . import fullfoert_foerste
from . import klass_correspondences_and_variants
from . import klass_labels
from . import registrert
from . import registrert_foerste
from . import hoyeste_utd
from .derive_decorator import get_derive_function

derive_all_submodules = (
    fullfoert_foerste,
    fullfoert,
    registrert_foerste,
    registrert,
    klass_correspondences_and_variants,
    klass_labels,
    hoyeste_utd,
)

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
        __all__.append(name)
    except Exception as err:
        logger.warning(
            f"Unable to attach '{name}' function to 'derive' module!\nMessage: {err}"
        )


def add_all_from_module(submodule: ModuleType) -> None:
    for func in submodule.__all__:
        add_function(func, submodule)


for submodule in derive_all_submodules:
    add_all_from_module(submodule)
