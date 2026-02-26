"""Variable-derivation helpers for NUDB pipelines."""

module = __import__(__name__, fromlist=[""])  # pass nonempty fromlist
from collections.abc import Callable
from types import ModuleType

from nudb_use.nudb_logger import logger

from . import fullfoert
from . import fullfoert_foerste
from . import klass_correspondences_and_variants
from . import klass_labels
from . import land
from . import person
from . import person_idents
from . import registrert
from . import registrert_foerste
from . import utd_foreldres_utdnivaa
from . import utd_hoeyeste
from . import vof
from .derive_decorator import get_derive_function

derive_all_submodules = (
    fullfoert_foerste,
    fullfoert,
    utd_hoeyeste,
    utd_foreldres_utdnivaa,
    registrert_foerste,
    registrert,
    klass_correspondences_and_variants,
    klass_labels,
    land,
    person_idents,
    person,
    vof,
)

from .nus_variants import utd_erforeldet_kode_nus
from .nus_variants import utd_klassetrinn_hoey_nus
from .nus_variants import utd_klassetrinn_lav_nus
from .person import pers_foedselsdato
from .person import pers_invkat
from .person import pers_kjoenn
from .uh_univ_eller_hoegskole import uh_univ_eller_hoegskole
from .utd_foreldres_utdnivaa import utd_foreldres_utdnivaa_16aar
from .utd_hoeyeste import utd_hoeyeste_nus2000
from .utd_hoeyeste import utd_hoeyeste_rangering
from .utd_skoleaar import utd_skoleaar_slutt
from .vof import vof_eierforhold
from .vof import vof_orgnr_foretak

__all__ = [
    "pers_foedselsdato",
    "pers_invkat",
    "pers_kjoenn",
    "uh_univ_eller_hoegskole",
    "utd_erforeldet_kode_nus",
    "utd_foreldres_utdnivaa_16aar",
    "utd_hoeyeste_nus2000",
    "utd_hoeyeste_rangering",
    "utd_klassetrinn_hoey_nus",
    "utd_klassetrinn_lav_nus",
    "utd_skoleaar_slutt",
    "vof_eierforhold",
    "vof_orgnr_foretak",
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
