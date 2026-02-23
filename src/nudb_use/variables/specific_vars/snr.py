import uuid
from pathlib import Path

import pandas as pd
from fagfunksjoner.paths.versions import latest_version_path
from fagfunksjoner.paths.versions import next_version_path

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.metadata.nudb_config.map_get_dtypes import BOOL_DTYPE_NAME
from nudb_use.metadata.nudb_config.map_get_dtypes import DTYPE_MAPPINGS
from nudb_use.metadata.nudb_config.map_get_dtypes import STRING_DTYPE_NAME
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.utils.packages import move_to_use_deprecate

# Moved function
from nudb_use.variables.derive.person_idents import snr_mrk

derive_snr_mrk = move_to_use_deprecate(
    snr_mrk,
    old_path="nudb_use.variables.specific_vars.snr",
    new_path="nudb_use.variables.derive.person_idents",
)

BOOL_DTYPE = DTYPE_MAPPINGS["pandas"][BOOL_DTYPE_NAME]
STRING_DTYPE = DTYPE_MAPPINGS["pandas"][STRING_DTYPE_NAME]


def _ensure_join_cols_present(
    df: pd.DataFrame, snr_col_name: str, fnr_col_name: str
) -> None:
    """Validate that at least one join column exists.

    Args:
        df: DataFrame that should contain join columns.
        snr_col_name: Name of the SNR column to look for.
        fnr_col_name: Name of the FNR column to look for.

    Raises:
        KeyError: If none of the join columns exist.
    """
    join_cols_in_df = [c for c in [snr_col_name, fnr_col_name] if c in df.columns]
    if not join_cols_in_df:
        raise KeyError(
            f"Expecting there to some of these columns to join on: {snr_col_name}, {fnr_col_name}."
        )


def _ensure_temp_cols_absent(df: pd.DataFrame, temp_cols: list[str]) -> None:
    """Validate that temporary columns do not already exist.

    Args:
        df: DataFrame to validate.
        temp_cols: Column names reserved for temporary use.

    Raises:
        KeyError: If any temporary columns already exist.
    """
    exist_temp_cols = [c for c in temp_cols if c in df.columns]
    if exist_temp_cols:
        raise KeyError(
            f"These already exist, what are you doing dude? {exist_temp_cols}"
        )


def _load_snrkat(update_fnr: bool) -> pd.DataFrame:
    """Load snrkat with the required columns.

    Args:
        update_fnr: Whether to include current FNR in the selection.

    Returns:
        pd.DataFrame: snrkat contents.
    """
    want_cols = ["fnr", "snr_utgatt", "snr"]
    if update_fnr:
        want_cols += ["fnr_naa"]
    return NudbData("snrkat").select(", ".join(want_cols)).df()


def _merge_cols(
    df: pd.DataFrame,
    snrkat: pd.DataFrame,
    ident_col_name: str,
    snrkat_renames: dict[str, str],
) -> pd.DataFrame:
    """Merge new columns onto the dataset using snrkat.

    Args:
        df: The dataframe we are merging onto.
        snrkat: The dataframe we have content we want to update with.
        ident_col_name: The ident column of the original dataset.
        snrkat_renames: How the snrkat-columns should be renamed to fit into the logic.

    Returns:
        pd.DataFrame: The dataframe with added columns from snrkat.
    """
    logger.info(
        f"Merging {list(snrkat_renames.keys())[-1]} from snrkat using {next(iter(snrkat_renames.keys()))} -> {ident_col_name}"
    )
    return df.merge(
        snrkat[list(snrkat_renames.keys())]
        .dropna(how="any")
        .drop_duplicates()
        .rename(columns=snrkat_renames),
        left_on=ident_col_name,
        right_on=next(iter(snrkat_renames.values())),
        how="left",
    )


def _apply_snrkat_merges(
    df: pd.DataFrame,
    snrkat: pd.DataFrame,
    snr_col_name: str,
    fnr_col_name: str,
    update_fnr: bool,
) -> pd.DataFrame:
    """Apply snrkat merges and validate length invariants.

    Args:
        df: Input dataframe.
        snrkat: snrkat contents.
        snr_col_name: Name of the SNR column.
        fnr_col_name: Name of the FNR column.
        update_fnr: Whether to update FNR values as well.

    Returns:
        pd.DataFrame: The merged dataframe.

    Raises:
        ValueError: If the dataframe length changes.
    """
    df_lengths = {"read": len(df)}

    if fnr_col_name in df.columns:
        df = _merge_cols(
            df,
            snrkat,
            ident_col_name=fnr_col_name,
            snrkat_renames={"fnr": "fnr", "snr": "snr_from_fnr"},
        )
        df_lengths["after fnr > snr merge"] = len(df)

    if snr_col_name in df.columns:
        df = _merge_cols(
            df,
            snrkat,
            ident_col_name=snr_col_name,
            snrkat_renames={"snr_utgatt": "snr", "snr": "snr_from_snr"},
        )
        df_lengths["after snr > snr merge"] = len(df)

    if update_fnr and fnr_col_name in df.columns:
        logger.warning(
            "We want original FNR as reported in, in most cases. Consider carefully before updating fnr. Ask a friend."
        )
        df = _merge_cols(
            df,
            snrkat,
            ident_col_name=fnr_col_name,
            snrkat_renames={"fnr": "fnr", "fnr_naa": "fnr_from_fnr"},
        )
        df_lengths["after fnr merge"] = len(df)

    if not all(length == len(df) for length in df_lengths.values()):
        raise ValueError(
            f"Lengths changed during snr refresh, should not happen: {df_lengths}"
        )

    return df


def _merge_and_log(
    df: pd.DataFrame,
    original_col_name: str,
    merge_col_name: str,
    return_dupes: bool,
) -> tuple[pd.DataFrame, bool]:
    """Merge updated ident values into the original column, with duplicate checks.

    Args:
        df: The dataset with the merged columns on.
        original_col_name: The original column name to update.
        merge_col_name: The merged column name.
        return_dupes: Whether to return a dataframe of dupes immediately.

    Returns:
        tuple[pd.DataFrame, bool]: The updated dataframe and a flag indicating
            whether the caller should return immediately.
    """
    with LoggerStack(f"Combining {merge_col_name} into {original_col_name}"):
        if merge_col_name not in df.columns:
            return df, False

        mask = (
            (df[original_col_name] != df[merge_col_name])
            & (df[merge_col_name].notna())
            & (df[merge_col_name].str.len().isin([7, 11]))
        )
        mask_sum = mask.sum()
        logger.info(
            f"Updating with {merge_col_name} on {mask_sum} rows, {round(mask.sum() / len(df) * 100, 2)}% of total rows."
        )

        df["new_col"] = df[original_col_name].copy()
        df.loc[mask, "new_col"] = df[merge_col_name]

        dupes = df[df.groupby("new_col")[original_col_name].transform("nunique") >= 2]
        if return_dupes and len(dupes):
            return dupes, True
        if len(dupes):
            old_nunique = dupes[original_col_name].nunique()
            new_nunique = dupes["new_col"].nunique()
            logger.warning(
                f"[DUPLICATES?] {old_nunique} -> {new_nunique}: Number of unique changed when {merge_col_name} -> {original_col_name}!"
            )
        else:
            logger.info(
                f"No duplicate warning for you. {original_col_name} seems to have 1:1 values with {merge_col_name}."
            )

        df[original_col_name] = df["new_col"]
        df = df.drop(columns=[merge_col_name, "new_col"])

    return df, False


def _apply_merged_columns(
    df: pd.DataFrame,
    snr_col_name: str,
    fnr_col_name: str,
    return_dupes: bool,
) -> tuple[pd.DataFrame, bool]:
    """Apply merged column updates in a stable order.

    Args:
        df: Dataframe with merged columns.
        snr_col_name: Name of the SNR column.
        fnr_col_name: Name of the FNR column.
        return_dupes: Whether to return duplicates early.

    Returns:
        tuple[pd.DataFrame, bool]: The updated dataframe and a flag indicating
            whether the caller should return immediately.
    """
    merge_plan = [
        ("snr_from_fnr", snr_col_name),
        ("snr_from_snr", snr_col_name),
        ("fnr_from_fnr", fnr_col_name),
    ]
    for merge_col, original_col in merge_plan:
        df, return_now = _merge_and_log(
            df, original_col, merge_col, return_dupes=return_dupes
        )
        if return_now:
            return df, True

    return df, False


def _maybe_derive_snr_mrk(
    df: pd.DataFrame, create_snr_mrk: None | bool
) -> pd.DataFrame:
    """Conditionally re-derive snr_mrk.

    Args:
        df: Dataframe to update.
        create_snr_mrk: Whether to (re)create snr_mrk.

    Returns:
        pd.DataFrame: Updated dataframe.
    """
    if create_snr_mrk or (create_snr_mrk is None and "snr_mrk" in df.columns):
        logger.info("Re-deriving snr_mrk.")
        return df.drop(columns=["snr_mrk"], errors="ignore").pipe(snr_mrk)

    logger.info(
        "Not re-deriving snr_mrk, set create_snr_mrk to True if you want snr_mrk created/updated."
    )
    return df


def update_snr_with_snrkat(
    df: pd.DataFrame,
    update_fnr: bool = False,
    create_snr_mrk: None | bool = None,
    return_dupes: bool = False,
    snr_col_name: str = "snr",
    fnr_col_name: str = "fnr",
) -> pd.DataFrame:
    """Update snr and possibly fnr using snrkat.

    Args:
        df: The pandas dataframe you want updated with personal idents.
        update_fnr: Set this to True if you want to update fnr also, no longer considered "as it came in".
            Warning! We want original FNR as reported in, in most cases. Consider carefully before updating fnr.
        create_snr_mrk: Set this to True, if you want to create/re-derive snr_mrk,
            set it to False if you dont want the function to re-derive an existing snr_mrk column.
        return_dupes: If you want to take a look at the dupes that arise from the first operation. Set to True.
        snr_col_name: If you want your snr-col to stay named something different than "snr".
        fnr_col_name: If you want your fnr-col to stay named something different than "fnr".

    Returns:
        pd.DataFrame: The Dataframe with a modified snr column, and optionally updated fnr.
    """
    with LoggerStack("Updating snr (maybe fnr) using snrkat"):
        _ensure_join_cols_present(df, snr_col_name, fnr_col_name)
        _ensure_temp_cols_absent(
            df, ["fnr_from_fnr", "snr_from_fnr", "snr_from_snr", "new_col"]
        )

        snrkat = _load_snrkat(update_fnr)
        df = _apply_snrkat_merges(df, snrkat, snr_col_name, fnr_col_name, update_fnr)
        df, return_dupes_now = _apply_merged_columns(
            df, snr_col_name, fnr_col_name, return_dupes
        )
        if return_dupes_now:
            return df

        return _maybe_derive_snr_mrk(df, create_snr_mrk)


def generate_uuid_for_snr_with_fnr_col(
    df: pd.DataFrame,
    snr_col: str = "snr",
    fnr_col: str = "fnr",
    subset: list[str] | None = None,
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
        subset: Name of subsetting variables to find unique FNRs within.

    Returns:
        pd.DataFrame: The same DataFrame instance with filled SNR values.
    """
    subset = subset or []

    with LoggerStack(
        f"Generating UUID4 into {snr_col} based on unique values in {fnr_col}"
    ):
        # Build identifier used for stable mapping (fnr + optional subset vars)
        identificator = df[fnr_col].astype("string").copy()

        for subsetvar in subset:
            addition = df[subsetvar].astype("string").fillna("<NA>")
            identificator = identificator + "-" + addition

        df["_fnr_identificator"] = identificator

        # Treat missing SNR as NA or empty/whitespace
        snr_s = df[snr_col].astype("string")
        invalid = snr_s.isna() | (snr_s.str.strip() == "")

        # Only map non-missing identifiers for rows that need filling
        unique_id_missing_snr = df.loc[invalid, "_fnr_identificator"].dropna().unique()

        fnr_uuid_katalog = pd.DataFrame(
            {
                "_fnr_identificator": unique_id_missing_snr,
                snr_col: [str(uuid.uuid4()) for _ in range(len(unique_id_missing_snr))],
            }
        )

        amount_na_pre_first_fill = int(invalid.sum())

        # Preferred pattern: merge a temporary column into df, then fill from it.
        # This avoids index-alignment pitfalls with Series.fillna(other_series).
        df = df.merge(
            fnr_uuid_katalog,
            on="_fnr_identificator",
            how="left",
            validate="m:1",
            suffixes=("", "_new"),
        )

        new_col = f"{snr_col}_new"
        df[snr_col] = df[snr_col].fillna(df[new_col])
        df = df.drop(columns=[new_col])

        # Recompute missing after first fill (same "missing" definition)
        snr_s_after = df[snr_col].astype("string")
        invalid_after = snr_s_after.isna() | (snr_s_after.str.strip() == "")

        amount_na_post_first_fill = int(invalid_after.sum())
        diff_first_fill = amount_na_pre_first_fill - amount_na_post_first_fill
        percent_diff = round(100 * diff_first_fill / len(df), 2) if len(df) else 0.0

        logger.info(
            f"Filled {percent_diff}% of `{snr_col}` with UUIDs based on unique, non-missing values in `{fnr_col}`"
        )

        if amount_na_post_first_fill:
            logger.warning(
                f"""Still empty cells in `{snr_col}` after filling from unique values in `{fnr_col}`.
`{fnr_col}` might contain NA-values (or identifiers became NA after subsetting).
Assuming the rest of data is one-person-per-row, giving each row a unique UUID4.
To avoid this, ensure all rows have a non-missing `{fnr_col}` (and subset columns, if used)
before running generate_uuid_for_snr_with_fnr_col."""
            )
            mask = invalid_after
            df.loc[mask, snr_col] = [str(uuid.uuid4()) for _ in range(int(mask.sum()))]

        df[fnr_col] = df[fnr_col].astype(STRING_DTYPE)
        df[snr_col] = df[snr_col].astype(STRING_DTYPE)

        return df.drop(columns=["_fnr_identificator"])


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
