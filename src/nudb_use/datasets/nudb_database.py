from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import duckdb as db

import nudb_use.datasets.external as external_datasets
from nudb_use.datasets.avslutta import _generate_avslutta_fullfoert_table
from nudb_use.datasets.avslutta import _generate_avslutta_view
from nudb_use.datasets.eksamen import _generate_eksamen_aggregated_view
from nudb_use.datasets.eksamen import _generate_eksamen_avslutta_hoeyeste_view
from nudb_use.datasets.eksamen import _generate_eksamen_hoeyeste_table
from nudb_use.datasets.eksamen import _generate_eksamen_view
from nudb_use.datasets.igang import _generate_igang_view
from nudb_use.datasets.snrkat import _generate_snrkat_fnr2snr_view
from nudb_use.datasets.sosbak import _generate_slekt_snr_view
from nudb_use.datasets.utd_hoeyeste import _generate_utd_hoeyeste_table
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.metadata.nudb_config.map_get_dtypes import STRING_DTYPE_NAME

if TYPE_CHECKING:
    from nudb_use.datasets.nudb_data import NudbData

STRING_DTYPE = DTYPE_MAPPINGS["pandas"][STRING_DTYPE_NAME]
GeneratorFunc = Callable[..., None] | Callable[[str, db.DuckDBPyConnection], None]


class NudbDatabase:
    """Singleton for internal NUDB database."""

    def __init__(self) -> None:
        self._connection: db.DuckDBPyConnection = db.connect(":memory:")

        self._dataset_generators: dict[str, GeneratorFunc] = {
            "eksamen_aggregated": _generate_eksamen_aggregated_view,
            "eksamen": _generate_eksamen_view,
            "avslutta": _generate_avslutta_view,
            "avslutta_fullfoert": _generate_avslutta_fullfoert_table,
            "igang": _generate_igang_view,
            "eksamen_hoeyeste": _generate_eksamen_hoeyeste_table,
            "eksamen_avslutta_hoeyeste": _generate_eksamen_avslutta_hoeyeste_view,
            "utd_hoeyeste": _generate_utd_hoeyeste_table,
            "snrkat_fnr2snr": _generate_snrkat_fnr2snr_view,
            "slekt_snr": _generate_slekt_snr_view,
        }

        for dataset_name in external_datasets.EXTERNAL_DATASETS:
            self._dataset_generators[dataset_name] = getattr(
                external_datasets, f"_generate_{dataset_name}_view"
            )

        self._dataset_names = list(self._dataset_generators.keys())
        self._datasets: dict[str, NudbData] = {}

    def _reset(self) -> None:
        self._connection.close()
        self._connection = db.connect(":memory:")
        self._datasets = {}

    def __del__(self) -> None:
        """Destructor for NudbDatabase."""
        self._connection.close()  # close before deleting

    def get_connection(self) -> db.DuckDBPyConnection:
        """Get database connection."""
        return self._connection


_NUDB_DATABASE = NudbDatabase()


def reset_nudb_database() -> None:
    """Reset (I.e., clear) the internal database."""
    _NUDB_DATABASE._reset()
