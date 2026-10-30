import importlib
from collections.abc import Callable
from functools import partial
from types import SimpleNamespace

import pandas as pd
import pytest
from nudb_config import settings

from nudb_use.metadata.nudb_config._renames import _rename_candidates
from nudb_use.metadata.nudb_config._renames import _resolve_rename
from nudb_use.metadata.nudb_config.find_var_missing import find_var
from nudb_use.metadata.nudb_config.find_var_missing import find_var_renames
from nudb_use.metadata.nudb_config.find_var_missing import find_var_renames_for_dataset
from nudb_use.metadata.nudb_config.find_var_missing import find_vars
from nudb_use.metadata.nudb_config.map_get_dtypes import get_dtypes
from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.quality.check_drop_cols import check_drop_cols_for_valid_cols
from nudb_use.quality.outdated_variables import check_outdated_variables
from nudb_use.quality.outdated_variables import find_outdated_variables_in_df


@pytest.fixture
def rename_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    settings = SimpleNamespace(
        variables={"contract": {"renamed_from": ["old_contract"]}, "exam": {}},
        datasets={
            "ongoing": SimpleNamespace(
                variables=["contract"],
                dataset_specific_renames={"ftype": "contract", "ftypef": "contract"},
            ),
            "completed": SimpleNamespace(
                variables=["exam"],
                dataset_specific_renames={"ftype": "exam", "ftypef": "exam"},
            ),
        },
    )
    lookup = importlib.import_module("nudb_use.metadata.nudb_config.find_var_missing")
    columns = importlib.import_module("nudb_use.metadata.nudb_config.variable_names")
    monkeypatch.setattr(lookup, "settings", settings)
    monkeypatch.setattr(columns, "settings_use", settings)
    monkeypatch.setattr(
        columns,
        "get_var_metadata",
        lambda: pd.DataFrame.from_dict(settings.variables, orient="index"),
    )
    return settings


@pytest.mark.parametrize("alias", ["ftype", "FTYPEF"])
@pytest.mark.parametrize(
    "api", ["find_var", "find_vars", "find_var_renames", "update_colnames"]
)
def test_ambiguous_rename(
    rename_settings: SimpleNamespace, alias: str, api: str
) -> None:
    operation: Callable[[], object]
    if api == "update_colnames":
        data = pd.DataFrame({alias: [1]})
        operation = partial(update_colnames, data)
    elif api == "find_vars":
        operation = partial(find_vars, [alias])
    elif api == "find_var_renames":
        operation = partial(find_var_renames, [alias])
    else:
        operation = partial(find_var, alias)
    with pytest.raises(ValueError) as error:
        operation()
    assert all(
        text in str(error.value).lower()
        for text in [alias.lower(), "contract", "exam", "ongoing", "completed"]
    )


@pytest.mark.parametrize("alias", ["ftype", "ftypef"])
@pytest.mark.parametrize("dataset", [None, "ongoing", "completed"])
def test_dataset_rename(
    rename_settings: SimpleNamespace, alias: str, dataset: str | None
) -> None:
    if dataset is None:
        # Repeated mappings to the same target are unambiguous.
        rename_settings.datasets["completed"].dataset_specific_renames[
            alias
        ] = "contract"
    target = "exam" if dataset == "completed" else "contract"
    result = find_var(alias.upper(), dataset_name=dataset)
    assert result is not None
    assert result["name"] == target
    assert alias in find_var_renames([alias], dataset_name=dataset)[target]
    data = pd.DataFrame({alias.upper(): [1]})
    renamed = update_colnames(data, dataset_name=dataset or "")
    assert renamed.to_dict("list") == {target: [1]}
    assert list(data.columns) == [alias.upper()]


def test_dataset_renames_combined(rename_settings: SimpleNamespace) -> None:
    expected = {"contract": ["old_contract", "ftype", "ftypef"]}
    assert find_var_renames_for_dataset("ongoing") == expected
    assert find_var_renames(["contract"]) == expected
    # Current names remain usable even when their historical aliases are ambiguous.
    assert list(update_colnames(pd.DataFrame({"contract": [1]})).columns) == [
        "contract"
    ]
    # An explicit dataset override takes precedence over a global alias.
    rename_settings.variables["exam"]["renamed_from"] = ["ftype"]
    rename_settings.datasets["ongoing"].dataset_specific_renames["exam"] = "contract"
    result = find_var("exam", dataset_name="ongoing")
    assert result is not None
    assert result["name"] == "contract"
    result = find_var("ftype", dataset_name="ongoing")
    assert result is not None
    assert result["name"] == "contract"
    assert list(update_colnames(pd.DataFrame({"ftype": [1]}), "ongoing").columns) == [
        "contract"
    ]


@pytest.mark.parametrize(
    "datasets,expected",
    [
        ({"eksamen": "exam", "igang": "contract", "avslutta": "finished"}, "finished"),
        ({"igang_hoeyereutdanning": "contract", "eksamen": "exam"}, "contract"),
        ({"eksamen_hoeyereutdanning": "exam"}, "exam"),
    ],
)
def test_default_dataset_priority(datasets: dict[str, str], expected: str) -> None:
    candidates = _rename_candidates(
        {},
        {
            name: {"dataset_specific_renames": {"old": target}}
            for name, target in datasets.items()
        },
    )
    assert _resolve_rename("OLD", candidates) == expected


def test_exact_dataset_overrides_family() -> None:
    datasets = {
        "igang": {"dataset_specific_renames": {"old": "base", "inherited": "base"}},
        "igang_special": {"dataset_specific_renames": {"old": "special"}},
    }
    scoped = _rename_candidates({}, datasets, "igang_special")
    assert _resolve_rename("old", scoped) == "special"
    assert _resolve_rename("inherited", scoped) == "base"
    candidates = _rename_candidates({}, datasets)
    with pytest.raises(ValueError, match=r"base.*special"):
        _resolve_rename("old", candidates)


@pytest.mark.parametrize(
    "alias,dataset,target",
    [
        ("ftype", None, "vg_proevekandtype"),
        ("ftypef", None, "vg_proevekandtype"),
        ("studrett", None, "uh_studierett"),
        ("ftype", "igang", "vg_kontraktstype"),
        ("ftypef", "igang_hoeyereutdanning", "vg_kontraktstype"),
        ("studrett", "igang_hoeyereutdanning", "uh_studierett"),
        ("ftypef", "avslutta_grunnskole", "vg_proevekandtype"),
        ("studrett", "eksamen", "uh_ereksamenrett"),
        ("studrett", "eksamen_hoeyereutdanning", "uh_ereksamenrett"),
    ],
)
def test_installed_config_renames(
    monkeypatch: pytest.MonkeyPatch,
    alias: str,
    dataset: str | None,
    target: str,
) -> None:
    lookup = importlib.import_module("nudb_use.metadata.nudb_config.find_var_missing")
    monkeypatch.setattr(
        lookup,
        "klass",
        SimpleNamespace(
            KlassClassification=lambda value: value,
            KlassVariant=lambda value: value,
        ),
    )
    assert not any(
        alias in (variable.renamed_from or [])
        for variable in settings.variables.values()
    )
    result = find_var(alias.upper(), dataset_name=dataset)
    assert result is not None
    assert result["name"] == target
    assert find_vars([alias], dataset_name=dataset)[alias] == find_var(
        alias, dataset_name=dataset
    )
    assert alias in find_var_renames([alias], dataset_name=dataset)[target]
    data = pd.DataFrame({alias.upper(): ["A"]})
    assert update_colnames(data, dataset_name=dataset or "").to_dict("list") == {
        target: ["A"]
    }
    if dataset:
        expected_dtype = (
            "bool[pyarrow]" if target == "uh_ereksamenrett" else "string[pyarrow]"
        )
        assert get_dtypes([alias], dataset_name=dataset) == {alias: expected_dtype}
        if target in (settings.datasets[dataset].variables or []):
            assert alias in find_var_renames_for_dataset(dataset)[target]
    elif alias == "studrett":
        with pytest.raises(
            ValueError, match=r"different dtypes.*STRING.*BOOLEAN"
        ) as error:
            get_dtypes([alias])
        assert all(
            text in str(error.value)
            for text in [
                "historical name 'studrett'",
                "uh_studierett",
                "uh_ereksamenrett",
                "dataset 'igang'",
                "dataset 'eksamen'",
                "Use a current variable name",
            ]
        )
        assert get_dtypes(["uh_studierett", "uh_ereksamenrett"]) == {
            "uh_studierett": "string[pyarrow]",
            "uh_ereksamenrett": "bool[pyarrow]",
        }
    else:
        assert get_dtypes([alias]) == {alias: "string[pyarrow]"}
    found = find_outdated_variables_in_df(data, dataset_name=dataset)
    assert target in found[alias]["renamed_to"]
    assert target in str(check_outdated_variables(data, dataset_name=dataset)[0])


def test_installed_config_merge_and_warning(caplog: pytest.LogCaptureFixture) -> None:
    data = pd.DataFrame(
        {
            "ftype": pd.Series(["A", None], dtype="string[pyarrow]"),
            "ftypef": pd.Series([None, "B"], dtype="string[pyarrow]"),
        }
    )
    for dataset, target in [
        ("", "vg_proevekandtype"),
        ("igang_hoeyereutdanning", "vg_kontraktstype"),
    ]:
        result = update_colnames(data, dataset_name=dataset)
        assert list(result.columns) == [target]
        assert result[target].tolist() == ["A", "B"]
        assert result[target].dtype == data["ftype"].dtype
    caplog.clear()
    check_drop_cols_for_valid_cols(["ftype", "studrett"])
    assert all(
        name in caplog.text
        for name in ["ftype", "studrett", "avslutta", "igang", "eksamen"]
    )
    caplog.clear()
    check_drop_cols_for_valid_cols(["studrett"], dataset_name="igang_hoeyereutdanning")
    assert "uh_studierett" in caplog.text
    assert "uh_ereksamenrett" not in caplog.text
    assert set(find_outdated_variables_in_df(data)["ftype"]["renamed_to"]) == {
        "vg_proevekandtype",
        "vg_kontraktstype",
    }
