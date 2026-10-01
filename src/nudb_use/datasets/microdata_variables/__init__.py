"""Module with NudbData generators for Microdata variables."""

from nudb_use.datasets.microdata_variables.avslutta_microdata import (
    _generate_microdata_avslutta_view,
)
from nudb_use.datasets.microdata_variables.igang_microdata import (
    _generate_microdata_igang_view,
)
from nudb_use.datasets.microdata_variables.eksamen_microdata import (
    _generate_microdata_eksamen_view,
)
from nudb_use.datasets.microdata_variables.utd_foreldres_utdnivaa_16aar import (
    _generate_microdata_utd_foreldres_utdnivaa_16aar_view,
)
from nudb_use.datasets.microdata_variables.utd_hoeyeste_nus2000 import (
    _generate_microdata_utd_hoeyeste_nus2000_view,
)
from nudb_use.datasets.microdata_variables.pers_fullfoert_foerste import (
    _generate_microdata_fullfoert_foerste_view
)
from nudb_use.datasets.microdata_variables.pers_registrert_foerste import (
    _generate_microdata_registrert_foerste_view
)
from nudb_use.datasets.microdata_variables.pers_semester import (
    _generate_microdata_pers_semester_view
)
from nudb_use.datasets.microdata_variables.pers_bokommune_16aar import (
    _generate_microdata_pers_bokommune_16aar_view
)
from nudb_use.datasets.microdata_variables.vgs_fagvurdering import (
    _generate_microdata_vgs_fagvurdering_view,
)
from nudb_use.datasets.microdata_variables.gs_fagvurdering import (
    _generate_microdata_gs_fagvurdering_view,
)
from nudb_use.datasets.microdata_variables.nasjprov_fagvurdering import (
    _generate_microdata_nasjprov_fagvurdering_view,
)
__all__ = [
    "_generate_microdata_avslutta_view",
    "_generate_microdata_igang_view", 
    "_generate_microdata_eksamen_view"
    "_generate_microdata_utd_foreldres_utdnivaa_16aar_view",
    "_generate_microdata_utd_hoeyeste_nus2000_view",
    "_generate_microdata_fullfoert_foerste_view",
    "_generate_microdata_pers_semester_view"
    "_generate_microdata_registrert_foerste_view"
    "_generate_microdata_pers_bokommune_16aar_view", 
    "_generate_microdata_vgs_fagvurdering_view",
    "_generate_microdata_gs_fagvurdering_view",
    "_generate_microdata_nasjprov_fagvurdering_view",
]
