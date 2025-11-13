"""Utilities for raising or logging grouped exceptions."""

import sys

from nudb_use.nudb_logger import logger


def raise_exception_group(errors: list[Exception]) -> None:
    """Raise grouped exceptions as ExceptionGroup (Py3.11+) or ValueError.

    Args:
        errors: Exceptions that should be raised together.

    Raises:
        ExceptionGroup: On Python 3.11+, raised with every collected error.
        ValueError: On earlier versions, raised with a combined error message.
    """
    if not errors:
        return  # No errors to raise

    if sys.version_info >= (3, 11):
        # Use ExceptionGroup for Python 3.11+
        raise ExceptionGroup("Multiple errors occurred", errors)
    else:
        # Combine errors into a single ValueError for older Python versions
        error_messages = "; ".join(str(e) for e in errors)
        raise ValueError(f"Multiple errors occurred: {error_messages}")


def warn_exception_group(errors: list[Exception]) -> None:
    """Log each exception in the provided list using the active logger.

    Args:
        errors: Exceptions that should be emitted as warnings in sequence.
    """
    if not errors:
        return  # No errors to raise

    for error in errors:
        logger.warning(str(error))
