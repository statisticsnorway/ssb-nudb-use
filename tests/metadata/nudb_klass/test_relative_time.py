from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from nudb_config.pydantic.variables import Variable

from nudb_use.metadata.nudb_klass import codes
from nudb_use.metadata.nudb_klass import correspondence
from nudb_use.metadata.nudb_klass import klass_utils
from nudb_use.metadata.nudb_klass import variants
from nudb_use.variables.derive import derive_decorator
from nudb_use.variables.derive import klass_correspondences_and_variants as derive_klass


@pytest.fixture
def klass_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    def bounds(_id: int) -> tuple[str, str]:
        return "2000-01-01", "2025-01-01"

    monkeypatch.setattr(klass_utils, "find_earliest_latest_klass_version_date", bounds)
    monkeypatch.setattr(
        correspondence, "find_earliest_latest_klass_version_date", bounds
    )


@pytest.mark.parametrize("relative", [False, True])
@pytest.mark.parametrize("variant", [False, True])
def test_validation_uses_configured_time(
    monkeypatch: pytest.MonkeyPatch,
    klass_bounds: None,
    relative: bool,
    variant: bool,
) -> None:
    variable = Variable(
        name="test",
        unit="utd",
        dtype="STRING",
        klass_codelist=131,
        klass_variant_search_term="Grouping" if variant else None,
        klass_use_relative_time=relative,
    )
    metadata = pd.DataFrame([variable.model_dump()], dtype=object).set_index("name")
    version = Mock()
    version.get_variant.return_value.to_dict.return_value = {"01": "Group"}
    get_version = Mock(return_value=version)
    get_codes = Mock(return_value=["01"])
    monkeypatch.setattr(variants, "_get_klass_version", get_version)
    monkeypatch.setattr(codes, "get_klass_codes", get_codes)

    assert (
        codes._check_column_against_klass(
            pd.Series(["01"]), "test", metadata, "1990-01-01", "2030-01-01"
        )
        == []
    )

    if variant:
        get_version.assert_called_once_with(
            131, None, "1990-01-01", "2030-01-01", relative
        )
    else:
        get_codes.assert_called_once_with(
            131,
            data_time_start="2000-01-01" if relative else None,
            data_time_end="2025-01-01" if relative else None,
        )


@pytest.mark.parametrize(
    ("relative", "configured", "start", "end", "expected"),
    [
        (False, "2001-01-01", "2005-01-01", "2010-01-01", None),
        (True, None, "2005-01-01", "2010-01-01", 2),
        (True, "2001-01-01", "2005-01-01", "2010-01-01", 1),
        (True, None, "1990-01-01", None, 1),
        (True, None, "2030-01-01", None, 3),
        (True, None, None, None, 3),
    ],
)
def test_variant_version_selection(
    monkeypatch: pytest.MonkeyPatch,
    relative: bool,
    configured: str | None,
    start: str | None,
    end: str | None,
    expected: int | None,
) -> None:
    classification = Mock(
        versions=[
            {"validFrom": "2025-01-01", "version_id": 3},
            {"validFrom": "2000-01-01", "version_id": 1},
            {"validFrom": "2010-01-01", "version_id": 2},
        ]
    )
    monkeypatch.setattr(
        variants,
        "klass",
        SimpleNamespace(KlassClassification=lambda _id: classification),
    )

    variants._get_klass_version(36, configured, start, end, relative)

    if expected is None:
        classification.get_version.assert_called_once_with()
    else:
        classification.get_version.assert_called_once_with(version_id=expected)


@pytest.mark.parametrize(
    ("relative", "start", "end", "expected"),
    [
        (False, "2005-01-01", "2010-01-01", ("2025-01-01", None)),
        (True, "2005-01-01", "2010-01-01", ("2005-01-01", "2010-01-01")),
        (True, "1990-01-01", "2030-01-01", ("2000-01-01", "2025-01-01")),
        (True, "1990-01-01", "1995-01-01", ("2000-01-01", None)),
        (True, "2030-01-01", None, ("2025-01-01", None)),
        (True, None, None, ("2025-01-01", None)),
    ],
)
def test_correspondence_dates(
    monkeypatch: pytest.MonkeyPatch,
    klass_bounds: None,
    relative: bool,
    start: str | None,
    end: str | None,
    expected: tuple[str, str | None],
) -> None:
    variable = Variable(
        name="test",
        unit="utd",
        dtype="STRING",
        klass_codelist=131,
        klass_correspondence_to=104,
        klass_use_relative_time=relative,
    )
    request = Mock()
    request.return_value.to_dict.return_value = {"01": "02"}
    monkeypatch.setattr(
        correspondence, "klass", SimpleNamespace(KlassCorrespondence=request)
    )

    assert correspondence.klass_correspondence_to_mapping(variable, start, end) == {
        "01": "02"
    }
    request.assert_called_once_with(
        source_classification_id=104,
        target_classification_id=131,
        from_date=expected[0],
        to_date=expected[1],
    )


@pytest.mark.parametrize("variant", [False, True])
def test_generated_derivation_passes_dates(
    monkeypatch: pytest.MonkeyPatch, variant: bool
) -> None:
    variable = Variable(
        name="test",
        unit="utd",
        dtype="STRING",
        klass_codelist=131,
        klass_correspondence_to=None if variant else 104,
        klass_variant_search_term="Grouping" if variant else None,
        klass_use_relative_time=True,
        derived_from=["source"],
    )
    settings = SimpleNamespace(variables={"test": variable})
    monkeypatch.setattr(derive_klass, "settings", settings)
    monkeypatch.setattr(derive_decorator, "settings", settings)
    mapping = Mock(return_value={"01": "02"})
    monkeypatch.setattr(
        derive_klass,
        (
            "klass_variant_search_term_mapping"
            if variant
            else "klass_correspondence_to_mapping"
        ),
        mapping,
    )
    function = derive_klass._generate_klass_derive_function("test")
    assert function is not None
    result = function(
        pd.DataFrame({"source": ["01"]}),
        data_time_start="2005-01-01",
        data_time_end="2010-01-01",
        raise_errors=True,
    )

    assert result["test"].tolist() == ["02"]
    if variant:
        mapping.assert_called_once_with(
            variable, data_time_start="2005-01-01", data_time_end="2010-01-01"
        )
    else:
        mapping.assert_called_once_with(variable, "2005-01-01", "2010-01-01")
