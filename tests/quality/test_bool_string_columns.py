import pandas as pd

from nudb_use.quality.check_bool_string_columns import check_bool_string_columns


def test_bool_string_columns_detects_literals() -> None:
    df = pd.DataFrame(
        {
            "flag": ["True", "False", "True"],
            "name": ["Ada", "Bjarne", "Claude"],
        }
    )

    errors = check_bool_string_columns(df, raise_errors=False)

    assert len(errors) == 1
    assert "flag" in str(errors[0])
