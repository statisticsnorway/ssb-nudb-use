import logging

import pytest

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.exceptions.groups import warn_exception_group


def test_raise_exception_group() -> None:
    assert raise_exception_group([]) is None
    with pytest.raises(ExceptionGroup):
        raise_exception_group([NudbQualityError("TestError")])


def test_warn_exception_group(caplog) -> None:
    assert warn_exception_group([]) is None
    with caplog.at_level(logging.WARNING):
        warn_exception_group([NudbQualityError("TestError")])
        assert "TestError" in caplog.text
