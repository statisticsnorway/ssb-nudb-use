from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from nudb_use.variables import derive
from nudb_use.variables.derive import relevant_variables


def set_variables(monkeypatch, variables):
    monkeypatch.setattr(
        relevant_variables,
        "settings",
        SimpleNamespace(
            variables={
                name: SimpleNamespace(derived_relevant_datasets=datasets)
                for name, datasets in variables.items()
            }
        ),
    )


def test_relevant_variables_pipe_in_config_order(monkeypatch):
    set_variables(
        monkeypatch,
        {
            "first": ["igang", "avslutta"],
            "other_dataset": ["igang"],
            "unset": None,
            "empty": [],
            "second": ["avslutta"],
        },
    )
    first = Mock(side_effect=lambda df: df.assign(first=df["source"] + 1))
    second = Mock(side_effect=lambda df: df.assign(second=df["first"] * 2))
    monkeypatch.setattr(derive, "first", first, raising=False)
    monkeypatch.setattr(derive, "second", second, raising=False)
    monkeypatch.setattr(derive, "__all__", [*derive.__all__, "first", "second"])
    source = pd.DataFrame({"source": [1, 2]})

    result = source.pipe(derive.pipe_all_relevant_variables_dataset, "avslutta")

    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame({"source": [1, 2], "first": [2, 3], "second": [4, 6]}),
    )
    first.assert_called_once()
    second.assert_called_once()
    pd.testing.assert_frame_equal(first.call_args.args[0], source)


@pytest.mark.parametrize("dataset_name", ["unknown", "avslutt", "AVSLUTTA"])
def test_no_exact_dataset_match_returns_unchanged(monkeypatch, dataset_name):
    set_variables(monkeypatch, {"first": ["avslutta"], "unset": None, "empty": []})
    lookup = Mock()
    monkeypatch.setattr(relevant_variables, "get_derive_function", lookup)
    source = pd.DataFrame({"source": [1]})

    result = source.pipe(derive.pipe_all_relevant_variables_dataset, dataset_name)

    pd.testing.assert_frame_equal(result, source)
    lookup.assert_not_called()


def test_missing_function_warns_and_continues(monkeypatch):
    set_variables(monkeypatch, {"missing": ["avslutta"], "found": ["avslutta"]})
    lookup = Mock(side_effect=[None, lambda df: df.assign(found=True)])
    warning = Mock()
    monkeypatch.setattr(relevant_variables, "get_derive_function", lookup)
    monkeypatch.setattr(relevant_variables.logger, "warning", warning)

    result = pd.DataFrame({"source": [1]}).pipe(
        derive.pipe_all_relevant_variables_dataset, "avslutta"
    )

    assert result["found"].tolist() == [True]
    assert [call.args[0] for call in lookup.call_args_list] == ["missing", "found"]
    warning.assert_called_once()
    assert "missing" in warning.call_args.args[0]


def test_uses_config_metadata_and_existing_derive_wrapper(monkeypatch):
    metadata = relevant_variables.settings.variables["utd_erutland"]
    assert "avslutta" in metadata.derived_relevant_datasets
    monkeypatch.setattr(
        relevant_variables,
        "settings",
        SimpleNamespace(variables={"utd_erutland": metadata}),
    )
    source = pd.DataFrame({"utd_skolekom": ["0025", "0301"]})

    result = derive.pipe_all_relevant_variables_dataset(source, "avslutta")

    assert result["utd_erutland"].tolist() == [True, False]
