from typing import Any

from nudb_config import settings

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.datasets.nudb_database import MICRODATA_PREFIX


def show_available_microdata_variables() -> list[str]:
    """Get available Microdata variables.

    Returns:
        list[str]: A list with variable names.
    """
    return [
        dataset.removeprefix(MICRODATA_PREFIX)
        for dataset in settings.datasets.keys()
        if dataset.startswith(MICRODATA_PREFIX)
    ]


class MicroData(NudbData):
    """Lazy representation of a Microdata variable as an NUDB dataset.

    Args:
        name: Name of the microdata variable.
        *args: Unnamed arguments passed on to the dataset generator.
        **kwargs: Named arguments passed on to the dataset generator.

    Raises:
        ValueError: If the variable name isn't recognized.
    """

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:

        available = show_available_microdata_variables()

        if name not in available:
            raise ValueError(
                f"Unrecognized Microdata Variable!\nAvailable variables:\n\t{available}"
            )

        microdata_name = MICRODATA_PREFIX + name
        super().__init__(microdata_name, *args, **kwargs)
