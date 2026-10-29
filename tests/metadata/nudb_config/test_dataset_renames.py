import importlib
from types import SimpleNamespace

import pandas as pd
import pytest

from nudb_use.metadata.nudb_config.find_var_missing import find_var
from nudb_use.metadata.nudb_config.find_var_missing import find_var_renames
from nudb_use.metadata.nudb_config.find_var_missing import find_var_renames_for_dataset
from nudb_use.metadata.nudb_config.find_var_missing import find_vars
from nudb_use.metadata.nudb_config.variable_names import update_colnames


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
    with pytest.raises(ValueError) as error:
        if api == "update_colnames":
            update_colnames(pd.DataFrame({alias: [1]}))
        elif api == "find_vars":
            find_vars([alias])
        elif api == "find_var_renames":
            find_var_renames([alias])
        else:
            find_var(alias)
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
    assert result is not None and result["name"] == target
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
    assert result is not None and result["name"] == "contract"
    result = find_var("ftype", dataset_name="ongoing")
    assert result is not None and result["name"] == "contract"
    assert list(update_colnames(pd.DataFrame({"ftype": [1]}), "ongoing").columns) == [
        "contract"
    ]
