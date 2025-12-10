import pandas as pd

from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.metadata.nudb_config.get_variable_info import get_var_metadata
from nudb_use.metadata.nudb_config.variable_names import update_colnames
from nudb_use.metadata.nudb_klass import check_klass_codes
from tests.utils_testing.validate_errors import validate_NudbQualityError_list


def subtest_check_klass_codes(df: pd.DataFrame) -> None:
    # Check klass_codes in our data, assuming there are mistakes.
    # Here we assume every klass classification in our dataframe has at least
    # one mistake...

    metadata: pd.DataFrame = get_var_metadata()
    klass_metadata: pd.DataFrame = metadata.query(
        "klass_codelist.notna() & klass_codelist > 0 & name in @df.columns & dtype != 'BOOLEAN'"
    )
    n_klass_vars: int = (
        klass_metadata.shape[0] - (klass_metadata.dtype == "BOOLEAN").sum()
    )  # we skip boolean

    errors: list[NudbQualityError] = check_klass_codes(df, raise_errors=False)
    validate_NudbQualityError_list(errors, n=n_klass_vars)


def test_check_klass_codes_igang(igang: pd.DataFrame) -> None:
    subtest_check_klass_codes(update_colnames(igang))


def test_check_klass_codes_eksamen(eksamen: pd.DataFrame) -> None:
    subtest_check_klass_codes(update_colnames(eksamen))


def test_check_klass_codes_avslutta(avslutta: pd.DataFrame) -> None:
    subtest_check_klass_codes(update_colnames(avslutta))
