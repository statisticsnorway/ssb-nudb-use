import pandas as pd

from nudb_use import LoggerStack
from nudb_use import logger
from nudb_use.exceptions.exception_classes import NudbQualityError

from .utils import add_err2list
from .utils import args_have_None
from .utils import get_column

UNIQUE_PER_PERSON_COLS = ["pers_kjoenn", "pers_foedselsdato", "gr_grunnskolepoeng"]


def check_unique_per_person(df: pd.DataFrame, **kwargs) -> list[NudbQualityError]:
    pers_id = get_column(df, col="pers_id")
    fnr = get_column(df, col="fnr")

    errors: list[NudbQualityError] = []

    unique_cols_in_df = [
        col for col in df.columns if col.lower() in UNIQUE_PER_PERSON_COLS
    ]
    if len(unique_cols_in_df):
        with LoggerStack(
            f"Checking columns that should be unique per person: {UNIQUE_PER_PERSON_COLS}"
        ):
            for unique_col in unique_cols_in_df:
                add_err2list(
                    errors,
                    subcheck_unique_per_person(
                        fnr, pers_id, df[unique_col], unique_col
                    ),
                )

    return errors


def subcheck_unique_per_person(
    fnr: pd.Series, pers_id: pd.Series, unique_col: pd.Series, unique_col_name: str
) -> NudbQualityError | None:
    if args_have_None(fnr=fnr, pers_id=pers_id, unique_col=unique_col):
        return None

    test_df = pd.DataFrame({"pers_id": pers_id.fillna(fnr), "unique_col": unique_col})
    test_mask = test_df.groupby("pers_id")["unique_col"].transform("nunique") > 1
    if not test_mask.sum():
        return None

    err_msg = f"Found several values per person, in {unique_col_name} that should only have a single value per person."
    logger.warning(err_msg)
    return NudbQualityError(err_msg)
