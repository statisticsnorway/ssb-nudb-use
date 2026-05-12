import pandas as pd

from nudb_use.quality.duplicated_columns import check_dtypes
from tests.utils_testing.validate_errors import validate_NudbQualityError_list


def test_check_dtypes():
    df = pd.DataFrame(
        {
            "snr": [
                "sdfjk10",
                "2a78skd",
                "hx1sd1a",
            ],  # don't worry Carl, these are not real snrs
            "snr_mrk": ["J", "N", "J"],
            "utd_aktivitet_slutt": ["202404", "197101", "177608"],
        }
    ).astype("string[pyarrow]")

    errors = check_dtypes(df, raise_errors=False)
    validate_NudbQualityError_list(errors, n=2)
