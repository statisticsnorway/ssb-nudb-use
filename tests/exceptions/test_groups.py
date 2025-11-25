import logging
from typing import Any

import pytest

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group


def test_raise_exception_group() -> None:
    assert raise_exception_group([]) is None  # type: ignore[func-returns-value]
    with pytest.raises(ExceptionGroup):
        raise_exception_group([NudbQualityError("TestError")])


def test_warn_exception_group(caplog: pytest.LogCaptureFixture) -> None:
    assert warn_exception_group([]) is None  # type: ignore[func-returns-value]
    with caplog.at_level(logging.WARNING):
        warn_exception_group([NudbQualityError("TestError")])
        assert "TestError" in caplog.text


def validate_NudbQualityError_list(errors: list[Any] | None, n: int = 0) -> None:
    errors_list: list[Any]

    if errors is None:
        errors_list = []
    elif isinstance(errors, list):
        errors_list = errors

    assert len(errors_list) == n

    for err in errors_list:
        assert isinstance(err, NudbQualityError)
