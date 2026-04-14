import pandas as pd

from nudb_use.variables.derive.all_data_helpers import join_variable_data


def test_join_variable_data_preserves_left_index() -> None:
    df_left = pd.DataFrame({"snr": ["a", "b"]}, index=[10, 20])
    df_right = pd.DataFrame(
        {"snr": ["a", "b"], "pers_kjoenn": ["1", "2"]},
    )

    result = join_variable_data("pers_kjoenn", df_right=df_right, df_left=df_left)

    assert result.index.tolist() == [10, 20]
    assert result["pers_kjoenn"].tolist() == ["1", "2"]
