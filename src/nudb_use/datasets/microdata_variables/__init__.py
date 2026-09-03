"""Module with NudbData generators for Microdata variables."""

from nudb_use.datasets.microdata_variables.avslutta_microdata import (
    _generate_microdata_avslutta_subset_view,
)
from nudb_use.datasets.microdata_variables.nasjprov import (
    _generate_microdata_nasjprov_view,
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

from nudb_use.datasets.microdata_variables.pers_bokommune_16aar import (
    _generate_microdata_pers_bokommune_16aar_view
)
from nudb_use.datasets.microdata_variables.fagvurdering import (
    _generate_microdata_fagvurdering_vgs_karakter_view,
    _generate_microdata_fagvurdering_vgs_fagkode_view,
    _generate_microdata_fagvurdering_vgs_vurderingsform_view,
    _generate_microdata_fagvurdering_vgs_skole_view,
    _generate_microdata_fagvurdering_gs_karakter_view,
    _generate_microdata_fagvurdering_gs_fagkode_view,
    _generate_microdata_fagvurdering_gs_vurderingsform_view,
    _generate_microdata_fagvurdering_gs_skole_view,
    _generate_microdata_fagvurdering_nasjprov_karakter_view,
    _generate_microdata_fagvurdering_nasjprov_fagkode_view,
    _generate_microdata_fagvurdering_nasjprov_vurderingsform_view,
    _generate_microdata_fagvurdering_nasjprov_skole_view,
)
__all__ = [
    "_generate_microdata_avslutta_subset_view",
    "_generate_microdata_nasjprov_view",
    "_generate_microdata_utd_foreldres_utdnivaa_16aar_view",
    "_generate_microdata_utd_hoeyeste_nus2000_view",
    "_generate_microdata_fullfoert_foerste_view",
    "_generate_microdata_pers_bokommune_16aar_view", 
    "_generate_microdata_fagvurdering_vgs_karakter_view",
    "_generate_microdata_fagvurdering_vgs_fagkode_view",
    "_generate_microdata_fagvurdering_vgs_vurderingsform_view",
    "_generate_microdata_fagvurdering_vgs_skole_view",
    "_generate_microdata_fagvurdering_gs_karakter_view",
    "_generate_microdata_fagvurdering_gs_fagkode_view",
    "_generate_microdata_fagvurdering_gs_vurderingsform_view",
    "_generate_microdata_fagvurdering_gs_skole_view",
    "_generate_microdata_fagvurdering_nasjprov_karakter_view",
    "_generate_microdata_fagvurdering_nasjprov_fagkode_view",
    "_generate_microdata_fagvurdering_nasjprov_vurderingsform_view",
    "_generate_microdata_fagvurdering_nasjprov_skole_view",
]
