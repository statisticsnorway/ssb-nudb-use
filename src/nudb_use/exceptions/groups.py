import sys
from nudb_use.nudb_logger import logger

def raise_exception_group(errors: list[Exception]) -> None:
    """Raise multiple exceptions using ExceptionGroup if Python is 3.11+, 
    otherwise wrap them into a single ValueError.
    
    Args:
        errors: List of exception errors to raise together.

    Returns:
        None
        
    Raises:
        ExceptionGroup: If running on Python 3.11 or newer, an `ExceptionGroup` containing
        all exceptions from the list is raised.
        ValueError: If running on Python versions earlier than 3.11, a `ValueError` is 
        raised containing a combined message of all exception strings.
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
    """Log each exception in the provided list using the active logger's `warning` method.

    Args:
        errors: List of exception instances to log as warnings.
        
    Returns:
        None
    """
    
    if not errors:
        return  # No errors to raise

    for error in errors:
        logger.warning(str(error))

