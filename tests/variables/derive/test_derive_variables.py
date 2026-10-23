from typing import Literal
from unittest.mock import Mock

import pandas as pd
import pytest

from nudb_use.nudb_logger import logger
from nudb_use.variables import derive
from nudb_use.variables.derive.derive_decorator import DeriveError


@pytest.mark.parametrize("priority", ["old", "new"])
def test_derive_variables_uses_wrappers_and_options(
    priority: Literal["old", "new"],
) -> None:
    source = pd.DataFrame(
        {
            "school_year": ["2020", "2021"],
            "utd_skoleaar_slutt": ["1999", None],
            "utd_skolekom": ["0025", "0301"],
        }
    )

    result = source.pipe(
        derive.derive_variables,
        ["utd_skoleaar_slutt", "utd_erutland"],
        priority=priority,
        temp_col_renames={"school_year": "utd_skoleaar_start"},
        raise_errors=True,
    )

    assert result["utd_skoleaar_slutt"].tolist() == [
        "1999" if priority == "old" else "2021",
        "2022",
    ]
    assert result["utd_erutland"].tolist() == [True, False]
    assert result["school_year"].tolist() == ["2020", "2021"]
    assert "utd_skoleaar_start" not in result.columns


def test_derive_variables_pipes_results_in_list_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = Mock(side_effect=lambda df, **kwargs: df.assign(first=df["source"] + 1))
    second = Mock(side_effect=lambda df, **kwargs: df.assign(second=df["first"] * 2))
    monkeypatch.setattr(derive, "first", first, raising=False)
    monkeypatch.setattr(derive, "second", second, raising=False)
    monkeypatch.setattr(derive, "__all__", [*derive.__all__, "first", "second"])

    result = derive.derive_variables(
        pd.DataFrame({"source": [1, 2]}), ["first", "second"], custom_option=True
    )

    assert result["second"].tolist() == [4, 6]
    assert first.call_args.kwargs["custom_option"] is True
    assert second.call_args.kwargs["custom_option"] is True


def test_derive_variables_empty_list() -> None:
    source = pd.DataFrame({"source": [1]})
    assert derive.derive_variables(source, []) is source


def test_derive_variables_unknown_name_warns_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warning = Mock()
    monkeypatch.setattr(logger, "warning", warning)

    result = derive.derive_variables(
        pd.DataFrame({"utd_skolekom": ["0025"]}), ["unknown", "utd_erutland"]
    )

    assert result["utd_erutland"].tolist() == [True]
    assert any("unknown" in call.args[0] for call in warning.call_args_list)


def test_derive_variables_unknown_name_raises() -> None:
    with pytest.raises(DeriveError, match="unknown"):
        derive.derive_variables(pd.DataFrame(), ["unknown"], raise_errors=True)


def test_derive_variables_missing_prerequisite_raises() -> None:
    with pytest.raises(DeriveError, match="utd_skoleaar_slutt"):
        derive.derive_variables(
            pd.DataFrame({"unrelated": [1]}),
            ["utd_skoleaar_slutt"],
            raise_errors=True,
        )
