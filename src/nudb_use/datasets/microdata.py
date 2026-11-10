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


class MicroData(NudbData):
    """Lazy representation of a Microdata variable as an NUDB dataset.

    Event datasets use ``utd_hendelse_id``; ``MicroData("fnr")`` maps events to
    ``snrkat.fnr_naa``, falling back to reported FNR. Derives depending only
    on SNR use FNR as their identifier. These allow reported-FNR fallback
    only when their dependencies have no join/dataset metadata and the
    derive preserves input event rows. Otherwise they keep mapped FNRs only
    and log a warning.

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
                f"Unrecognized Microdata Variable: {name}!\nAvailable variables:\n\t{available}"
            )

        microdata_name = MICRODATA_PREFIX + name
        super().__init__(microdata_name, *args, **kwargs)
