"""Module with NudbData generators for Microdata variables."""

from nudb_use.datasets.microdata_variables.avslutta_microdata import (
    _generate_microdata_avslutta_subset_view,
)
from nudb_use.datasets.microdata_variables.utd_hoeyeste_nus2000 import (
    _generate_microdata_utd_hoeyeste_nus2000_view,
)
from nudb_use.datasets.microdata_variables.nasjprov import (
    _generate_microdata_nasjprov_view,
)

__all__ = [
    "_generate_microdata_utd_hoeyeste_nus2000_view",
    "_generate_microdata_avslutta_subset_view",
    "_generate_microdata_nasjprov_view",
]
