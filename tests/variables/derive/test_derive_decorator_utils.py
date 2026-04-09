import logging

import pandas as pd
import pytest

from nudb_use.variables.derive.derive_decorator_utils import TEMP_DERIVE_RENAME_POSTFIX
from nudb_use.variables.derive.derive_decorator_utils import (
    swap_temp_colnames_from_temp,
)
from nudb_use.variables.derive.derive_decorator_utils import swap_temp_colnames_to_temp


def test_swap_temp_colnames_to_temp_ignores_non_relevant_renames(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame({"source": [1], "other": [2]})

    with caplog.at_level(logging.INFO):
        renamed, rename_state = swap_temp_colnames_to_temp(
            df,
            derived_from=["expected"],
            temp_col_renames={"source": "not_expected"},
        )

    pd.testing.assert_frame_equal(renamed, df)
    assert rename_state == {"renamed_sources": {}, "backed_up_targets": {}}
    assert "Found no relevant renames" in caplog.text


def test_swap_temp_colnames_to_temp_skips_missing_relevant_sources(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame({"source_present": [1]})

    with caplog.at_level(logging.WARNING):
        renamed, rename_state = swap_temp_colnames_to_temp(
            df,
            derived_from=["expected", "expected_2"],
            temp_col_renames={
                "source_missing": "expected",
                "source_present": "expected_2",
            },
        )

    expected = pd.DataFrame({"expected_2": [1]})
    pd.testing.assert_frame_equal(renamed, expected)
    assert rename_state == {
        "renamed_sources": {"source_present": "expected_2"},
        "backed_up_targets": {},
    }
    assert "Unable to temporarily rename missing source columns" in caplog.text


def test_swap_temp_colnames_to_temp_skips_duplicate_targets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame({"source_a": [1], "source_b": [2]})

    with caplog.at_level(logging.WARNING):
        renamed, rename_state = swap_temp_colnames_to_temp(
            df,
            derived_from=["expected"],
            temp_col_renames={
                "source_a": "expected",
                "source_b": "expected",
            },
        )

    pd.testing.assert_frame_equal(renamed, df)
    assert rename_state == {"renamed_sources": {}, "backed_up_targets": {}}
    assert "Multiple source columns map to the same temporary target" in caplog.text


def test_swap_temp_colnames_to_temp_skips_backup_collisions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.DataFrame(
        {
            "source": [1],
            "expected": [2],
            f"expected{TEMP_DERIVE_RENAME_POSTFIX}": [3],
        }
    )

    with caplog.at_level(logging.WARNING):
        renamed, rename_state = swap_temp_colnames_to_temp(
            df,
            derived_from=["expected"],
            temp_col_renames={"source": "expected"},
        )

    pd.testing.assert_frame_equal(renamed, df)
    assert rename_state == {"renamed_sources": {}, "backed_up_targets": {}}
    assert "Temporary backup columns already exist in dataframe" in caplog.text


def test_swap_temp_colnames_round_trips_with_existing_target_column() -> None:
    df = pd.DataFrame({"source": [1], "expected": [2], "untouched": [3]})

    renamed, rename_state = swap_temp_colnames_to_temp(
        df,
        derived_from=["expected"],
        temp_col_renames={"source": "expected"},
    )

    assert list(renamed.columns) == [
        "expected",
        f"expected{TEMP_DERIVE_RENAME_POSTFIX}",
        "untouched",
    ]
    assert renamed["expected"].tolist() == [1]
    assert renamed[f"expected{TEMP_DERIVE_RENAME_POSTFIX}"].tolist() == [2]

    restored = swap_temp_colnames_from_temp(renamed, rename_state)

    pd.testing.assert_frame_equal(restored, df)


def test_swap_temp_colnames_from_temp_only_restores_applied_mappings() -> None:
    df = pd.DataFrame({"source_a": [1], "source_b": [2], "expected_b": [3]})

    renamed, rename_state = swap_temp_colnames_to_temp(
        df,
        derived_from=["expected_a", "expected_b"],
        temp_col_renames={
            "source_a": "expected_a",
            "missing_source": "expected_b",
        },
    )

    restored = swap_temp_colnames_from_temp(renamed, rename_state)

    pd.testing.assert_frame_equal(restored, df)
