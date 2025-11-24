import pytest

from nudb_use.exceptions.exception_classes import NudbQualityError


def test_nudb_quality_error_class() -> None:
    with pytest.raises(NudbQualityError):
        raise NudbQualityError("Test error")
