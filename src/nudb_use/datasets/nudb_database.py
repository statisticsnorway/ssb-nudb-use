from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import duckdb as db

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
GeneratorFunc = Callable[..., None] | Callable[[str, db.DuckDBPyConnection], None]


class _NudbDatabase:
    """Private class for internal NUDB database.

    Please do not use this class directly, get it out of the nudb_database module attribute instead.
    It is nice to have this as a non-singleton class for testing purposes.
    """

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
        self._connection.close()
        self._connection = db.connect(":memory:")
        self._datasets = {}

    def __del__(self) -> None:
        """Destructor for _NudbDatabase."""
        self._connection.close()  # close before deleting

    def get_connection(self) -> db.DuckDBPyConnection:
        """Get database connection."""
        return self._connection

    def show_datasets(self, show_private: bool = False) -> list[str]:
        """Get datasets in _NudbDatabase."""
        return sorted([x for x in self._dataset_names if x[0] != "_" or show_private])

    def configure(
        self,
        memory_limit: str = "32GB",
        threads: int = 4,
        temp_directory: str | Path = "/var/tmp/duckdb_tmp",
        max_temp_directory_size: str = "55GiB",
        preserve_insertion_order: bool = True,
    ) -> None:
        """Configure DuckDB runtime settings for stable large workloads.

        Args:
            memory_limit: Maximum memory DuckDB may use, by default duckdb uses 80% of available RAM,
                this may cause crashes if you are making pandas dataframes etc.
            threads: Number of execution threads. This should probably match your CPU m / 1000.
            temp_directory: Directory for spill-to-disk operations. /var/tmp is a good spot on ubuntu-based machines.
            max_temp_directory_size: Maximum size of temp spill directory.
                Duckdb default is 90% of remaining free space. This may cause crashes if something else is using the disk...
            preserve_insertion_order: Whether to preserve insertion order (uses more memory if True).
                Can save memory if you set it to False. Because this can scramble row order, sort on all columns later...
        """
        temp_directory = Path(temp_directory)
        temp_directory.mkdir(parents=True, exist_ok=True)
        conf_string = f"""
            SET memory_limit = '{memory_limit}';
            SET threads = {threads}");
            SET preserve_insertion_order = {str(preserve_insertion_order).lower()};
            SET temp_directory = '{temp_directory.as_posix()}';
            SET max_temp_directory_size = '{max_temp_directory_size}';
        """
        logger.info("Setting duckdb config with settings-string:\n%s", conf_string)
        self._connection.execute(conf_string)

    def log_config(self) -> dict[str, Any]:
        """Log and return current DuckDB configuration settings.

        Returns:
            dict[str, Any]: Dictionary of key DuckDB runtime settings (that we care about).

        Raises:
            RuntimeError: If we fail getting the Duckdb settings when putting them in the dict.
        """
        settings: list[str] = [
            "memory_limit",
            "threads",
            "temp_directory",
            "max_temp_directory_size",
            "preserve_insertion_order",
        ]

        config: dict[str, Any] = {}

        for setting in settings:
            row = self._connection.execute(
                f"SELECT current_setting('{setting}')"
            ).fetchone()
            if row is None:
                msg = f"Failed to read DuckDB setting: {setting}"
                raise RuntimeError(msg)
            value = row[0]
            config[setting] = value

        logger.info("DuckDB configuration:")
        for key, value in config.items():
            logger.info(f"  {key}: {value}")

        return config


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
