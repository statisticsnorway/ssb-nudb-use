from typing import Any

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.datasets.nudb_database import MICRODATA_PREFIX
from nudb_use.datasets.nudb_database import show_nudb_datasets


def show_available_microdata_variables() -> list[str]:
    """Get available Microdata variables.

    Returns:
        list[str]: A list with variable names.
    """
    datasets = show_nudb_datasets(show_private=True)

    return [
        dataset.removeprefix(MICRODATA_PREFIX)
        for dataset in datasets
        if dataset.startswith(MICRODATA_PREFIX)
    ]


def MicroData(name: str, *args: Any, **kwargs: Any) -> NudbData:
    """Get Microdata variable as NudbData.

    Args:
        name: Name of the dataset.
        *args: Unnamed arguments passed on to NudbData
        **kwargs: Named arguments passed on to the NudbData.

    Returns:
        NudbData: Variable in NudbData format

    Raises:
        ValueError: If the dataset name isn't recognized.
    """
    available = show_available_microdata_variables()

    if name not in available:
        raise ValueError(
            f"Unrecognized Microdata Variable!\nAvailable variables:\n\t{available}"
        )

    microdata_name = MICRODATA_PREFIX + name
    return NudbData(microdata_name, *args, **kwargs)
