import warnings

import pytest

from nudb_use.utils.packages import move_to_use_deprecate


def _dummy(x: int) -> int:
    return x + 1


def test_move_to_use_deprecate_warns_with_paths() -> None:
    wrapped = move_to_use_deprecate(
        _dummy,
        old_path="old.path.func",
        new_path="new.path.func",
    )

    with pytest.warns(DeprecationWarning, match="old.path.func") as record:
        assert wrapped(1) == 2
    # warn_once should suppress subsequent warnings
    wrapped(1)
    assert len(record) == 1


def test_move_to_use_deprecate_custom_message_and_category() -> None:
    class CustomWarning(UserWarning): ...

    wrapped = move_to_use_deprecate(
        _dummy,
        message="custom message",
        warn_once=False,
        category=CustomWarning,
        stacklevel=1,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", CustomWarning)
        wrapped(1)
        wrapped(1)

    assert len(caught) == 2
    assert all("custom message" in str(w.message) for w in caught)
