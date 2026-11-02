import importlib
from collections.abc import Callable
from functools import partial
from types import SimpleNamespace

import pandas as pd
import pytest
from nudb_config import settings

from nudb_use.metadata.nudb_config._renames import _rename_candidates
from nudb_use.metadata.nudb_config._renames import _resolve_rename
from nudb_use.metadata.nudb_config.find_var_missing import explain_rename
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
    # Equally ranked general renames remain ambiguous; dataset order breaks dataset ties.
    for variable in rename_settings.variables.values():
        variable["renamed_from"] = ["ftype", "ftypef"]
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
    assert _resolve_rename("old", _rename_candidates({}, datasets)) == "base"


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


@pytest.fixture
def ranked_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    variables = {
        name: {"dtype": "STRING", "unit": "valid", "renamed_from": []}
        for name in [
            "general",
            "base",
            "sibling",
            "second",
            "exact",
            "finished",
            "exam",
            "external",
        ]
    }
    variables["general"]["renamed_from"] = ["alias", "family_alias", "general_only"]
    datasets = {
        "external": {"dataset_specific_renames": {"alias": "external"}},
        "eksamen": {"dataset_specific_renames": {"alias": "exam"}},
        "igang_videregaaende": {
            "dataset_specific_renames": {"alias": "sibling", "family_alias": "sibling"}
        },
        "avslutta": {
            "dataset_specific_renames": {
                "alias": "finished",
                "foreign_only": "finished",
            }
        },
        "igang": {"dataset_specific_renames": {"alias": "base"}},
        "igang_other": {"dataset_specific_renames": {"family_alias": "second"}},
        "igang_hoeyereutdanning": {
            "dataset_specific_renames": {"exact_alias": "exact"}
        },
    }
    config = SimpleNamespace(variables=variables, datasets=datasets)
    for path, attribute in [
        ("find_var_missing", "settings"),
        ("variable_names", "settings_use"),
        ("map_get_dtypes", "SETTINGS"),
    ]:
        module = importlib.import_module(f"nudb_use.metadata.nudb_config.{path}")
        monkeypatch.setattr(
            module, attribute, config if attribute != "SETTINGS" else vars(config)
        )
    module = importlib.import_module("nudb_use.metadata.nudb_config.variable_names")
    monkeypatch.setattr(
        module,
        "get_var_metadata",
        lambda: pd.DataFrame.from_dict(config.variables, orient="index"),
    )
    return config


@pytest.mark.parametrize(
    "alias,dataset,target",
    [
        ("alias", None, "general"),
        ("alias", "igang_hoeyereutdanning", "base"),
        ("family_alias", "igang_hoeyereutdanning", "sibling"),
        ("exact_alias", "igang_hoeyereutdanning", "exact"),
        ("general_only", "igang_hoeyereutdanning", "general"),
        ("foreign_only", "igang_hoeyereutdanning", "finished"),
    ],
)
def test_ranked_lookup_fallbacks(
    ranked_settings: SimpleNamespace, alias: str, dataset: str | None, target: str
) -> None:
    result = find_var(alias, dataset_name=dataset)
    assert result is not None
    assert result["name"] == target
    assert list(
        update_colnames(
            pd.DataFrame({alias: ["value"]}), dataset_name=dataset or ""
        ).columns
    ) == [target]
    assert get_dtypes([alias], dataset_name=dataset) == {alias: "string[pyarrow]"}
    explanation = explain_rename(alias, dataset_name=dataset)
    assert explanation.loc[explanation["selected"], "new_name"].tolist() == [target]


def test_rank_introspection_and_config_reordering(
    ranked_settings: SimpleNamespace,
) -> None:
    explanation = explain_rename("alias")
    ranks = explanation.set_index("new_name")["rank"].to_dict()
    assert ranks == {
        "general": (1, 0),
        "external": (5, 0),
        "exam": (4, 0),
        "sibling": (3, 1),
        "finished": (2, 0),
        "base": (3, 0),
    }
    # Ranking is derived again after the config order changes, without adding settings fields.
    ranked_settings.datasets = dict(reversed(list(ranked_settings.datasets.items())))
    result = find_var("family_alias", dataset_name="igang_hoeyereutdanning")
    assert result is not None
    assert result["name"] == "second"
    assert set(vars(ranked_settings)) == {"variables", "datasets"}
    assert all(
        set(dataset) == {"dataset_specific_renames"}
        for dataset in ranked_settings.datasets.values()
    )


def test_other_dataset_order_and_ambiguous_general_renames() -> None:
    datasets = {
        "first": {"dataset_specific_renames": {"old": "one"}},
        "second": {"dataset_specific_renames": {"old": "two"}},
    }
    assert _resolve_rename("old", _rename_candidates({}, datasets)) == "one"
    assert (
        _resolve_rename(
            "old", _rename_candidates({}, dict(reversed(list(datasets.items()))))
        )
        == "two"
    )
    candidates = _rename_candidates(
        {"one": {"renamed_from": ["old"]}, "two": {"renamed_from": ["old"]}}, datasets
    )
    with pytest.raises(ValueError, match=r"one.*two"):
        _resolve_rename("old", candidates)


@pytest.mark.parametrize(
    "dataset", [None, "igang", "igang_hoeyereutdanning", "eksamen"]
)
def test_current_variable_context(
    monkeypatch: pytest.MonkeyPatch, dataset: str | None
) -> None:
    lookup = importlib.import_module("nudb_use.metadata.nudb_config.find_var_missing")
    monkeypatch.setattr(
        lookup, "klass", SimpleNamespace(KlassClassification=lambda value: value)
    )
    target = "uh_ereksamenrett" if dataset == "eksamen" else "uh_studierett"
    dtype = "bool[pyarrow]" if dataset == "eksamen" else "string[pyarrow]"
    result = find_var("uh_studierett", dataset_name=dataset)
    assert result is not None
    assert result["name"] == target
    data = pd.DataFrame({"uh_studierett": ["A"]})
    assert list(update_colnames(data, dataset_name=dataset or "").columns) == [target]
    assert get_dtypes(["uh_studierett"], dataset_name=dataset) == {
        "uh_studierett": dtype
    }
    explanation = explain_rename("uh_studierett", dataset_name=dataset)
    assert explanation.loc[explanation["selected"], "new_name"].tolist() == [target]
    found = find_outdated_variables_in_df(data, dataset_name=dataset)
    assert ("uh_studierett" in found) == (dataset == "eksamen")


def test_family_dtype_selection_and_ambiguous_explanation(
    ranked_settings: SimpleNamespace,
) -> None:
    ranked_settings.variables["sibling"]["dtype"] = "INTEGER"
    assert get_dtypes(["family_alias"], dataset_name="igang_hoeyereutdanning") == {
        "family_alias": "Int64"
    }
    with pytest.raises(ValueError, match=r"different dtypes.*STRING.*INTEGER"):
        get_dtypes(["family_alias"])
    ranked_settings.variables["second"]["renamed_from"] = ["alias"]
    explanation = explain_rename("alias")
    assert not explanation["selected"].any()
