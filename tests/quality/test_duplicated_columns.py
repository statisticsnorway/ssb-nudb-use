import pandas as pd

from nudb_use.quality.duplicated_columns import check_duplicated_columns
from tests.utils_testing.validate_errors import validate_NudbQualityError_list


def test_duplicated_columns(avslutta: pd.DataFrame) -> None:
    avslutta_copy = avslutta.copy().iloc[:, 0:4]
    avslutta_copy.columns = [
        "dup_col_1",
        "dup_col_1",
        "dup_col_2",
        "dup_col_2",
    ]

    errors = check_duplicated_columns(avslutta_copy)
    validate_NudbQualityError_list(errors, n=2)


def test_non_duplicated_columns(avslutta: pd.DataFrame) -> None:
    errors = check_duplicated_columns(avslutta)
    validate_NudbQualityError_list(errors, n=0)
