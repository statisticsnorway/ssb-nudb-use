from typing import Any

from nudb_use.exceptions.exception_classes import NudbQualityError


def validate_NudbQualityError_list(errors: list[Any] | None, n: int = 0) -> None:
    errors_list: list[Any]

    if errors is None:
        errors_list = []
    elif isinstance(errors, list):
        errors_list = errors

    assert len(errors_list) == n

    for err in errors_list:
        assert isinstance(err, NudbQualityError)
