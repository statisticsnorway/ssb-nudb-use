import pandas as pd

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.exceptions.exception_classes import NudbQualityError
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.variables.specific_vars.orgnr import _split_orgnr_col

from .utils import add_err2list
from .utils import get_column
from .utils import require_series_present


def check_outdated_orgnr_cols(
    df: pd.DataFrame, **kwargs: object
) -> list[NudbQualityError]:
    """Check for orgnr-columns with old names in the dataset.

    Args:
        df: Dataset whose column names should be checked.
        **kwargs: Unused extra arguments for compatibility.

    Returns:
        Collected validation errors.
    """
    outdated_cols = ["org_nr", "utd_orgnr", "bof_orgnrbed", "orgnr"]
    current_col_names = ["orgnr_foretak", "orgnrbed"]
    outdated_in_df = [c for c in outdated_cols if c in df.columns.str.lower()]
    if outdated_in_df:
        errors: list[NudbQualityError] = []
        err_msg = f"Found outdated orgnr in your dataset: {outdated_in_df}, we want these: {current_col_names}"
        add_err2list(errors, err_msg)
    return errors


def check_orgnr_foretak(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Validate the orgnr_foretak column against external orgnr sources.

    Args:
        df: Dataset containing the orgnr_foretak column.
        **kwargs: Extra validation options. Set use_external_datasets to run this check.

    Returns:
        Collected validation errors.
    """
    errors: list[NudbQualityError] = []
    if kwargs.get("use_external_datasets"):
        ...  # Doing it this way to have the guard set up right...
    else:
        logger.info(
            "Skipping checking orgnr_foretak with external dataset, because the use_external_datasets param is set to False."
        )
        return errors

    with LoggerStack("Validating orgnr_foretak column."):
        col_name = "orgnr_foretak"
        col = get_column(df, col=col_name)
        add_err2list(errors, subcheck_col_contains_invalid_orgnr(col, col_name))
        add_err2list(errors, subcheck_col_contains_orgnr_foretak(col, col_name))
        return errors


def check_orgnrbed(df: pd.DataFrame, **kwargs: object) -> list[NudbQualityError]:
    """Validate the orgnrbed column against external orgnr sources.

    Args:
        df: Dataset containing the orgnrbed column.
        **kwargs: Extra validation options. Set use_external_datasets to run this check.

    Returns:
        Collected validation errors.
    """
    errors: list[NudbQualityError] = []
    if not kwargs.get("use_external_datasets", True):
        logger.info(
            "Skipping checking orgnrbed with external dataset, because the use_external_datasets param is set to False."
        )
        return errors
    with LoggerStack("Validating orgnrbed column."):
        col_name = "orgnrbed"
        col = get_column(df, col=col_name)
        add_err2list(errors, subcheck_col_contains_invalid_orgnr(col, col_name))
        add_err2list(errors, subcheck_col_contains_orgnrbed(col, col_name))

        want_col = "orgnr_foretak"
        want_series = get_column(df, col=want_col)
        validated = require_series_present(orgnr_foretak_col=want_series)
        if validated is not None:
            add_err2list(
                errors,
                subcheck_orgnrbed_orgnr_foretak_connected(
                    col_foretak=want_series,
                    col_foretak_name=want_col,
                    col_orgnrbed=col,
                    col_orgnrbed_name=col_name,
                ),
            )
        else:
            logger.info(
                f"Didnt find accompanying {want_col} column when I found {col_name}, hard to look at the connections then."
            )
        return errors


def subcheck_col_contains_invalid_orgnr(
    col: pd.Series | None, col_name: str
) -> NudbQualityError | None:
    """Check whether a column contains values that are neither foretak nor orgnrbed.

    Args:
        col: Column with orgnr values to validate.
        col_name: Name of the column being checked.

    Returns:
        Validation error when invalid orgnr values are found, otherwise None.
    """
    orgnr_foretak, orgnrbed = _split_orgnr_col(col, put_invalid_in_orgnr_foretak=False)
    invalid_values: list[str] = list(
        col[orgnr_foretak.isna() & orgnrbed.isna() & col.notna()].unique()
    )
    if invalid_values:
        return NudbQualityError(
            f"Found invalid values in {col_name} when looking them all up in BoF and Brreg (first 10): {invalid_values[:11] if len(invalid_values) >= 10 else invalid_values}"
        )
    return None


def subcheck_col_contains_orgnr_foretak(
    col: pd.Series | None, col_name: str
) -> NudbQualityError | None:
    """Check whether an orgnr_foretak column contains orgnrbed values.

    Args:
        col: Column expected to contain only orgnr_foretak values.
        col_name: Name of the column being checked.

    Returns:
        Validation error when orgnrbed values are found, otherwise None.
    """
    _, orgnrbed = _split_orgnr_col(col)
    orgnrbed_in_foretak = list(orgnrbed[orgnrbed.notna()].unique())
    if orgnrbed_in_foretak:
        return NudbQualityError(
            f"Found {len(orgnrbed_in_foretak)} orgnrbed-values in {col_name}-column (first 10): {orgnrbed_in_foretak[:11] if len(orgnrbed_in_foretak) >= 10 else orgnrbed_in_foretak}"
        )
    return None


def subcheck_col_contains_orgnrbed(
    col: pd.Series | None, col_name: str
) -> NudbQualityError | None:
    """Check whether an orgnrbed column contains orgnr_foretak values.

    Args:
        col: Column expected to contain only orgnrbed values.
        col_name: Name of the column being checked.

    Returns:
        Validation error when orgnr_foretak values are found, otherwise None.
    """
    orgnr_foretak, _ = _split_orgnr_col(col, put_invalid_in_orgnr_foretak=False)
    foretak_in_orgnrbed = list(orgnr_foretak[orgnr_foretak.notna()].unique())
    if foretak_in_orgnrbed:
        return NudbQualityError(
            f"Found {len(foretak_in_orgnrbed)} orgnr_foretak-values in {col_name}-column (first 10): {foretak_in_orgnrbed[:11] if len(foretak_in_orgnrbed) >= 10 else foretak_in_orgnrbed}"
        )
    return None


def subcheck_orgnrbed_orgnr_foretak_connected(
    col_foretak: pd.Series | None,
    col_foretak_name: str,
    col_orgnrbed: pd.Series | None,
    col_orgnrbed_name: str,
) -> NudbQualityError | None:
    """Check whether orgnr_foretak and orgnrbed pairs exist in BOF.

    Args:
        col_foretak: Column containing orgnr_foretak values.
        col_foretak_name: Name of the orgnr_foretak column.
        col_orgnrbed: Column containing orgnrbed values.
        col_orgnrbed_name: Name of the orgnrbed column.

    Returns:
        Validation error when pairs are missing from BOF, otherwise None.
    """
    validated = require_series_present(
        orgnr_foretak_col=col_foretak,
        orgnrbed_col=col_orgnrbed,
    )
    if validated is None:
        return None

    check_connections = pd.concat([col_foretak, col_orgnrbed], axis=1)
    check_connections.columns = [col_foretak_name, col_orgnrbed_name]
    check_connections = check_connections[check_connections.notna().all(axis=1)]
    check_connections = check_connections.astype("string").drop_duplicates()

    if check_connections.empty:
        return None

    orgnr_foretak_str = "', '".join(
        str(value).replace("'", "''")
        for value in check_connections[col_foretak_name].unique()
    )
    orgnrbed_str = "', '".join(
        str(value).replace("'", "''")
        for value in check_connections[col_orgnrbed_name].unique()
    )
    bof_connections = (
        NudbData("_bof_dated_orgnr_connections")
        .select("DISTINCT orgnr, orgnrbed")
        .where(
            f"""orgnr in ('{orgnr_foretak_str}') OR orgnrbed in ('{orgnrbed_str}')"""
        )
        .df()
        .astype("string")
        .rename(columns={"orgnr": col_foretak_name})
    )

    missing_connections_df = check_connections.merge(
        bof_connections,
        on=[col_foretak_name, col_orgnrbed_name],
        how="left",
        indicator=True,
    )
    missing_connections_df = missing_connections_df[
        missing_connections_df["_merge"] == "left_only"
    ][[col_foretak_name, col_orgnrbed_name]]

    if missing_connections_df.empty:
        return None

    missing_connections = {
        str(orgnr_foretak): sorted(
            group[col_orgnrbed_name].dropna().astype(str).unique().tolist()
        )
        for orgnr_foretak, group in missing_connections_df.groupby(
            col_foretak_name, sort=False
        )
    }
    first_ten_missing_connections = dict(list(missing_connections.items())[:10])
    return NudbQualityError(
        f"Found {len(missing_connections_df)} {col_foretak_name}/{col_orgnrbed_name}-connections that don't exist in BoF (first 10): {first_ten_missing_connections}"
    )
