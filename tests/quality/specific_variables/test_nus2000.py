from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from nudb_use import settings
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.quality.specific_variables import nus2000
from nudb_use.quality.specific_variables.nus2000 import check_nus2000
from nudb_use.quality.specific_variables.nus2000 import subcheck_nus2000_outdated

MAPPING_TARGET = (
    "nudb_use.variables.derive.klass_correspondences_and_variants."
    "klass_variant_search_term_mapping"
)


@pytest.fixture(autouse=True)
def delivery_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        nus2000,
        "settings_use",
        SimpleNamespace(
            datasets=settings.datasets,
            constants={
                "delivering_teams": ["utd-uhfagskole", "utd-vg", "utd-bhgskole"]
            },
        ),
    )


@pytest.mark.parametrize(
    "dataset_name",
    ["avslutta_grunnskole", "eksamen_hoeyereutdanning", "igang_videregaaende"],
)
def test_outdated_codes_reported_for_deliveries(dataset_name: str) -> None:
    df = pd.DataFrame(
        {
            "nus2000": pd.array(
                ["211101", "211101", "211102", "211103", None], dtype="string"
            )
        },
        index=[10, 11, 12, 13, 14],
    )
    original = df.copy(deep=True)
    with patch(MAPPING_TARGET, return_value={"211101": "*", "211102": "*"}) as mapping:
        errors = check_nus2000(
            df, dataset_name=dataset_name, range_valid_nus=range(0, 9)
        )

    outdated_errors = [err for err in errors if "Found outdated" in str(err)]
    assert len(outdated_errors) == 1
    message = str(outdated_errors[0])
    assert dataset_name in message
    assert "3 of 5 rows" in message
    assert "211101: 2, 211102: 1" in message
    assert "211103" not in message
    assert mapping.call_count == 1
    pd.testing.assert_frame_equal(df, original)


@pytest.mark.parametrize(
    "dataset_name", ["avslutta", "igang", "eksamen", "innvbef", "unknown", None]
)
def test_historical_and_other_datasets_skip_outdated_check(
    dataset_name: str | None,
) -> None:
    with patch(MAPPING_TARGET) as mapping:
        error = subcheck_nus2000_outdated(pd.Series(["211101"]), dataset_name)

    assert error is None
    mapping.assert_not_called()


@pytest.mark.parametrize(
    "codes", [None, pd.Series([], dtype="string"), pd.Series([pd.NA], dtype="string")]
)
def test_missing_or_empty_codes_skip_derivation(codes: pd.Series | None) -> None:
    with patch(MAPPING_TARGET) as mapping:
        error = subcheck_nus2000_outdated(codes, "avslutta_grunnskole")

    assert error is None
    mapping.assert_not_called()


def test_current_and_unmapped_codes_are_not_outdated() -> None:
    with patch(MAPPING_TARGET, return_value={"211101": ""}):
        error = subcheck_nus2000_outdated(
            pd.Series(["211101", "211102", pd.NA], dtype="string"),
            "avslutta_grunnskole",
        )

    assert error is None


def test_derivation_failure_is_reported() -> None:
    with patch(MAPPING_TARGET, side_effect=RuntimeError("KLASS unavailable")):
        error = subcheck_nus2000_outdated(pd.Series(["211101"]), "avslutta_grunnskole")

    assert isinstance(error, NudbQualityError)
    assert "Unable to check outdated" in str(error)
    assert "KLASS unavailable" in str(error)


def test_missing_delivery_teams_skip_derivation() -> None:
    with (
        patch("nudb_use.quality.specific_variables.nus2000.settings_use.constants", {}),
        patch(MAPPING_TARGET) as mapping,
    ):
        error = subcheck_nus2000_outdated(pd.Series(["211101"]), "avslutta_grunnskole")

    assert error is None
    mapping.assert_not_called()
