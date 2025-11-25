from __future__ import annotations

import warnings
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec
from typing import TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def move_to_use_deprecate(
    func: Callable[P, R],
    *,
    old_path: str | None = None,
    new_path: str | None = None,
    message: str | None = None,
    warn_once: bool = True,
    category: type[Warning] = DeprecationWarning,
    stacklevel: int = 2,
) -> Callable[P, R]:
    """Wrap a function so calling it emits a deprecation warning indicating it has moved.

    The warning is emitted only when the function is *used*, not when it is imported.
    By default, the warning is emitted at most once per process.

    Args:
        func: The function that has been moved to nudb_use (the new location).
        old_path: Optional dotted path of the old import location used by callers,
                  e.g. "nudb_build.paths.path_utils".
        new_path: Optional dotted path of the new import location,
                  e.g. "nudb_use.paths.path_utils".
        message: Optional custom warning message to display. If provided, it overrides
                 the auto-generated message.
        warn_once: If True (default), warn only on the first call; subsequent calls
                   won't repeat the warning.
        category: The warning category to use. Defaults to DeprecationWarning.
        stacklevel: Passed to warnings.warn to point at the caller. Defaults to 2.

    Returns:
        A wrapper callable with the same signature as `func` that emits the warning on use.
    """
    emitted: bool = False

    # Build a default message if the caller doesn't provide one
    if message is None:
        name = getattr(func, "__name__", "this function")
        if old_path or new_path:
            old_loc = f"`{old_path}`" if old_path else "the old location"
            new_loc = f"`{new_path}`" if new_path else "the new location"
            default_msg = (
                f"Using `{name}` from {old_loc} is deprecated. "
                f"Please import it from {new_loc} instead."
            )
        else:
            default_msg = (
                f"Using `{name}` from its old package is deprecated. "
                "Please import it from its new location."
            )
    else:
        default_msg = message

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        nonlocal emitted
        if not (warn_once and emitted):
            warnings.warn(default_msg, category, stacklevel=stacklevel)
            emitted = True
        return func(*args, **kwargs)

    return wrapper
