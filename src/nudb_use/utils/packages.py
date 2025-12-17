from __future__ import annotations

import re
import subprocess
import sys
import warnings
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec
from typing import Self
from typing import TypeVar

from nudb_use.nudb_logger import logger

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
        Callable[P, R]: Wrapper callable with the same signature as `func`
        that emits the warning on use.
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


class _VersionNumber(tuple[int, int, int]):
    def __new__(cls, numbers: tuple[int, int, int]) -> Self:
        return super().__new__(cls, numbers)

    def __str__(self) -> str:
        return ".".join([str(x) for x in self])


def _parse_version_number(version: str) -> _VersionNumber:
    nums: tuple[int, ...] = tuple([int(num) for num in version.strip().split(".")])

    if len(nums) != 3:
        raise ValueError(f"Unexpected number of numbers in version: {version}")

    return _VersionNumber(nums)


def _check_package_version(package: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pip", "index", "versions", package],
        capture_output=True,
    )
    stdout = result.stdout.decode("utf-8")

    version_pattern = "([\\.0-9\\-]+)"
    installed = re.search(f"INSTALLED:\\s+{version_pattern}", stdout)
    latest = re.search(f"LATEST:\\s+{version_pattern}", stdout)

    if not installed:
        logger.warning(f"Unable to determine installed version of `{package}`")
        return None

    elif not latest:
        logger.warning(f"Unable to determine latest version of `{package}`")
        return None

    v_installed = _parse_version_number(installed.group(1))
    v_latest = _parse_version_number(latest.group(1))

    if v_installed < v_latest:
        logger.warning(
            f"`{package}` is outdated, installed: {v_installed}, latest: {v_latest}."
        )


def _try_check_package_version(package: str) -> None:
    try:
        _check_package_version(package)
    except Exception as err:
        logger.warning(f"Unable to validate `{package}` version!\nMessage: {err}")
