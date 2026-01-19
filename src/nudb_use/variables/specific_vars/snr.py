import uuid
from pathlib import Path

import pandas as pd
from fagfunksjoner.paths.versions import latest_version_path
from fagfunksjoner.paths.versions import next_version_path

from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.metadata.nudb_config.map_get_dtypes import STRING_DTYPE_NAME
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]
STRING_DTYPE = DTYPE_MAPPINGS["pandas"][STRING_DTYPE_NAME]


def derive_snr_mrk(df: pd.DataFrame, snr_col: str = "snr") -> pd.DataFrame:
    """Derive the column snr_mrk from snr-column, True if values in snr_col is notna and has a length of 7.

    Args:
        df: The dataframe to insert/overwrite the snr_mrk-column into.
        snr_col: The name of the snr-column, if it isnt "snr".

    Returns:
        pd.DataFrame: the dataframe with the added snr_mrk column.
    """
    df["snr_mrk"] = ((df[snr_col].notna()) & (df[snr_col].str.len() == 7)).astype(
        BOOL_DTYPE
    )
    return df


def generate_uuid_for_snr_with_fnr_col(
    df: pd.DataFrame, snr_col: str = "snr", fnr_col: str = "fnr"
) -> pd.DataFrame:
    """Fill missing SNR values using FNR-based UUIDs, then per-row UUIDs.

    For rows where `snr_col` is missing and `fnr_col` is present, the function
    generates a UUID4 per unique `fnr_col` value and fills those SNRs. If any
    SNRs remain missing (typically due to missing FNRs), it assigns a unique
    UUID4 per remaining row.

    Args:
        df: Input DataFrame to update (modified in place).
        snr_col: Name of the SNR column to fill.
        fnr_col: Name of the FNR column used as the grouping key.

    Returns:
        pd.DataFrame: The same DataFrame instance with filled SNR values.
    """
    with LoggerStack(
        f"Generating UUID4 into {snr_col} based on unique values in {fnr_col}"
    ):
        unique_fnr_missing_snr = (
            df[df[snr_col].isna()][fnr_col].dropna().unique()
        )  # The dropna is important, so we dont get a fnr = NA join-key.
        fnr_uuid_katalog = pd.DataFrame(
            {
                fnr_col: unique_fnr_missing_snr,
                snr_col: [str(uuid.uuid4()) for _ in unique_fnr_missing_snr],
            }
        )
        amount_na_pre_first_fill = df[snr_col].isna().sum()
        df[snr_col] = df[snr_col].fillna(
            df.drop(columns=snr_col, errors="ignore").merge(
                fnr_uuid_katalog, on=fnr_col, how="left", validate="m:1"
            )[snr_col]
        )

        amount_na_post_first_fill = df[snr_col].isna().sum()
        diff_first_fill = amount_na_pre_first_fill - amount_na_post_first_fill
        percent_diff = round(diff_first_fill / len(df), 2)
        logger.info(
            f"Filled {percent_diff}% of `{snr_col}` with UUIDs based on unique, non-missing values in `{fnr_col}`"
        )

        if amount_na_post_first_fill:
            logger.warning(
                f"""Still empty cells in `{snr_col}` after filling from unique values in `{fnr_col}`, `{fnr_col}` might contain NA-values?
                        Assuming the rest of data is one-person-per row, giving each row a unique UUID4.
                        To avoid this, add an identifier to all rows of `{fnr_col}` before running the function generate_uuid_for_snr_with_fnr_col."""
            )
            mask = df[snr_col].isna()
            df.loc[mask, snr_col] = [str(uuid.uuid4()) for _ in range(mask.sum())]

        df[fnr_col] = df[fnr_col].astype(STRING_DTYPE)
        df[snr_col] = df[snr_col].astype(STRING_DTYPE)

        return df


def generate_uuid_for_snr_with_fnr_catalog(
    df: pd.DataFrame,
    fnr_catalog_path: str | Path,
    snr_col: str = "snr",
    fnr_col: str = "fnr",
) -> pd.DataFrame:
    """Fill missing SNR values using a persisted FNR-to-UUID catalog.

    Loads an existing catalog from `fnr_catalog_path` (if present), uses it to
    fill missing `snr_col` values based on `fnr_col`, then generates new UUIDs
    for any remaining missing SNRs via `generate_uuid_for_snr_with_fnr_col`.
    Newly created FNR/SNR pairs are appended to the catalog and written back
    to disk.

    Args:
        df: Input DataFrame to update (modified in place).
        fnr_catalog_path: Path to a parquet file holding FNR/SNR mappings.
        snr_col: Name of the SNR column to fill.
        fnr_col: Name of the FNR column used as the key.

    Returns:
        pd.DataFrame: The same DataFrame instance with filled SNR values.
    """
    with LoggerStack(
        "Using a catalog for persisting invalid FNR -> UUIDs through a catalog"
    ):
        # Open existing catalog from path if it exists
        catalog = pd.DataFrame(columns=[fnr_col, snr_col]).astype(
            {fnr_col: STRING_DTYPE, snr_col: STRING_DTYPE}
        )
        catalog_path = latest_version_path(Path(fnr_catalog_path))
        if catalog_path.exists():
            catalog = pd.read_parquet(catalog_path)
            catalog = catalog[catalog[fnr_col].notna()]
        else:
            logger.info(
                f"Fnr-uuid catalog does not exist, so we are starting with an empty one, and writing to: {fnr_catalog_path}"
            )

        # Log a warning if there are FNR that are empty, they will not be stored in the catalog
        len_missing_fnr = df[fnr_col].isna().sum()
        if len_missing_fnr:
            logger.warning(
                f"There are {len_missing_fnr} rows with NA in {fnr_col}, they will not be stored in the catalog. Each row will recieve a unique UUID in {snr_col}."
            )

        # Apply the previously generated uuids into the snr_col
        df[snr_col] = df[snr_col].fillna(
            df[[fnr_col]].merge(catalog, on=fnr_col, how="left")[snr_col]
        )
        snr_missing_pre_generate_mask = df[snr_col].isna()

        # Generate uuids into snrcol for those still missing snr_col values
        df = generate_uuid_for_snr_with_fnr_col(df, fnr_col=fnr_col, snr_col=snr_col)

        # The new values in the snr_col that weren't filled previously
        filled_fnr_snr = df[
            snr_missing_pre_generate_mask
            & (df[fnr_col].notna())
            & (df[snr_col].notna())
        ][[fnr_col, snr_col]].drop_duplicates()
        # Write updated catalog back to drive with added generated values
        catalog = (
            pd.concat([catalog, filled_fnr_snr])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        catalog.to_parquet(next_version_path(catalog_path))

        df[fnr_col] = df[fnr_col].astype(STRING_DTYPE)
        df[snr_col] = df[snr_col].astype(STRING_DTYPE)

        return df
