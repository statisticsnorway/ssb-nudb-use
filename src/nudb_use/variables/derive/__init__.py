"""Variable-derivation helpers for NUDB pipelines."""

from .skoleaar import utd_skoleaar_slutt
from .uh import uh_univ_eller_hogskole

__all__ = [
    "uh_univ_eller_hogskole",
    "utd_skoleaar_slutt",
]
