from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import polars as pl

import nudb_use.datasets.external as external_datasets
from nudb_use.datasets.avslutta import _generate_avslutta_fullfoert_table
from nudb_use.datasets.avslutta import _generate_avslutta_view
from nudb_use.datasets.eksamen import _generate_eksamen_aggregated_view
from nudb_use.datasets.eksamen import _generate_eksamen_avslutta_hoeyeste_view
from nudb_use.datasets.eksamen import _generate_eksamen_hoeyeste_table
from nudb_use.datasets.eksamen import _generate_eksamen_view
from nudb_use.datasets.igang import _generate_igang_view
from nudb_use.datasets.person import _generate_bokommune_16aar_snr
from nudb_use.datasets.person import _generate_slekt_snr_view
from nudb_use.datasets.person import _generate_snr2alder16_view
from nudb_use.datasets.person import _generate_utd_person_view
from nudb_use.datasets.snrkat import _generate_snrkat_fnr2snr_view
from nudb_use.datasets.utd_foreldres_utdnivaa import (
    _generate_utd_foreldres_utdnivaa_view,
)
from nudb_use.datasets.utd_hoeyeste import _generate_utd_hoeyeste_table
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.metadata.nudb_config.map_get_dtypes import STRING_DTYPE_NAME
from nudb_use.nudb_logger import logger

if TYPE_CHECKING:
    from nudb_use.datasets.nudb_data import NudbData

STRING_DTYPE = DTYPE_MAPPINGS["pandas"][STRING_DTYPE_NAME]
GeneratorFunc = Callable[..., None] | Callable[[], pl.LazyFrame]


class _NudbDatabase:
    """Private class for internal NUDB database.

    Please do not use this class directly, get it out of the nudb_database module attribute instead.
    It is nice to have this as a non-singleton class for testing purposes.
    """

    def __init__(self) -> None:

        self._dataset_generators: dict[str, GeneratorFunc] = {
            "eksamen_aggregated": _generate_eksamen_aggregated_view,
            "eksamen": _generate_eksamen_view,
            "avslutta": _generate_avslutta_view,
            "avslutta_fullfoert": _generate_avslutta_fullfoert_table,
            "igang": _generate_igang_view,
            "eksamen_hoeyeste": _generate_eksamen_hoeyeste_table,
            "eksamen_avslutta_hoeyeste": _generate_eksamen_avslutta_hoeyeste_view,
            "utd_hoeyeste": _generate_utd_hoeyeste_table,
            "_snrkat_fnr2snr": _generate_snrkat_fnr2snr_view,
            "slekt_snr": _generate_slekt_snr_view,
            "_snr2alder16": _generate_snr2alder16_view,
            "utd_foreldres_utdnivaa": _generate_utd_foreldres_utdnivaa_view,
            "utd_person": _generate_utd_person_view,
            "bokommune_16aar_snr": _generate_bokommune_16aar_snr,
        }

        for dataset_name in external_datasets.EXTERNAL_DATASETS:
            self._dataset_generators[dataset_name] = getattr(
                external_datasets, f"_generate_{dataset_name}_view"
            )

        self._dataset_names = list(self._dataset_generators.keys())
        self._datasets: dict[str, NudbData] = {}

    def _reset(self) -> None:
        self._datasets = {}

    def __del__(self) -> None:
        """Destructor for _NudbDatabase."""
        self._connection.close()  # close before deleting

    def show_datasets(self, show_private: bool = False) -> list[str]:
        """Get datasets in _NudbDatabase."""
        return sorted([x for x in self._dataset_names if x[0] != "_" or show_private])


nudb_database = _NudbDatabase()


def reset_nudb_database() -> None:
    """Reset (I.e., clear) the internal database."""
    nudb_database._reset()


def show_nudb_datasets(show_private: bool = False) -> list[str]:
    """Get datasets in _NudbDatabase.

    Args:
        show_private: Should private datasets be shown?

    Returns:
        list[str]: A list with dataset names.
    """
    return nudb_database.show_datasets(show_private)
