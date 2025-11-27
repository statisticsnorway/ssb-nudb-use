"""Utilities for raising or logging grouped exceptions."""

from collections.abc import Sequence

from nudb_use.nudb_logger import logger


def raise_exception_group(errors: Sequence[Exception]) -> None:
    """Raise grouped exceptions as ExceptionGroup (Py3.11+) or ValueError.

    Args:
        errors: Exceptions that should be raised together.

    Raises:
        ExceptionGroup: On Python 3.11+, raised with every collected error.
    """
    if not errors:
        return  # No errors to raise
    raise ExceptionGroup("Multiple errors occurred", list(errors))


def warn_exception_group(errors: Sequence[Exception]) -> None:
    """Log each exception in the provided list using the active logger.

    Args:
        errors: Exceptions that should be emitted as warnings in sequence.
    """
    if not errors:
        return  # No errors to raise

    for error in errors:
        logger.warning(str(error))
