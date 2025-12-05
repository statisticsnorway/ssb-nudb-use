"""Exception types used across nudb_use."""


class NudbQualityError(Exception):
    """Domain-specific error for NUDB quality validations."""

    ...


class NudbDerivedFromNotFoundError(Exception):
    """Domain-specific error for NUDB deriving issues when looking for columns needed."""

    ...
