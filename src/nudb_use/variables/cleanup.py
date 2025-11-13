"""Utilities for reorganizing and trimming NUDB datasets."""

from pathlib import Path

import pandas as pd

from nudb_use.paths.path_utils import metadatapath_from_path


def move_col_after_col(
    df: pd.DataFrame, col_anchor: str, col_move_after: str
) -> pd.DataFrame:
    """Move a specified column in a DataFrame to immediately follow another specified column.

    Args:
        df: Input pandas DataFrame.
        col_anchor: Name of the column after which the specified column will be moved.
        col_move_after: Name of the column to move.

    Returns:
        pd.DataFrame: New DataFrame with the specified column moved to follow the anchor column.
    """
    col_move_content = df[col_move_after]
    df = df.drop(columns=col_move_after)
    ind = list(df.columns).index(col_anchor)  # Important to do this after the drop...
    df.insert(ind + 1, col_move_after, col_move_content)
    return df


def move_content_from_col_to(
    df: pd.DataFrame, from_col: str, to_col: str
) -> pd.DataFrame:
    """Fill empty values (NA) in one column with values from another column.

    Args:
        df: DataFrame
        from_col: Column where information is taken.
        to_col: Column where information is moved to.

    Returns:
        pd.Dataframe: DataFrame with values filled out.
    """
    if from_col in df.columns:
        df[to_col] = df[to_col].fillna(df[from_col])
        return df.drop(columns=[from_col])
    return df


def remove_cols_store(
    in_path: Path,
    out_path: Path,
    cols_keep: list[str] | None = None,
    cols_drop: list[str] | None = None,
    exist_ok: bool = False,
    testing: bool = False,
) -> None:
    """Load a dataset and its associated metadata, remove specified columns, and store the minimized version together with updated metadata.

    This function ensures metadata exists before processing, removes columns
    based on the provided keep/drop rules, and writes both the reduced dataset
    (in Parquet format) and updated metadata to the specified output path.

    Args:
        in_path: Path to the input dataset file.
        out_path: Path where the minimized dataset will be saved.
        cols_keep: List of column names to keep.
        cols_drop: List of column names to drop.
        exist_ok: If False, raises an error when `out_path` already exists.
                  If True, overwrites the existing file.
        testing: If True, skips actual file writing to facilitate testing.

    Raises:
        OSError: If metadata file corresponding to `in_path` is missing.
        OSError: If the output file already exists and `exist_ok=False`.
    """
    # Maybe not copy if already exists in output folder !?!?!?!
    if not testing:
        exists_msg = f"File already exists at {out_path}."
        if out_path.is_file() and not exist_ok:
            raise OSError(exists_msg)
        elif out_path.is_file() and exist_ok:
            print(exists_msg)

    # We want the user to fill metadata before sharing the data further
    in_meta_path = metadatapath_from_path(in_path)
    if not in_meta_path.is_file():
        raise OSError(
            f"Missing metadata at path {in_meta_path}, create and check metadata before minimizing and sharing."
        )

    # Remove cols
    df = read_data_and_remove_cols(in_path, cols_keep, cols_drop)
    print(f"Columns left: {df.columns}")
    meta = read_meta_and_remove_cols(in_path, cols_keep, cols_drop)

    # Store modified data and metadata
    print(f"Save minimized dataframe to {out_path}")
    if not testing:
        df.to_parquet(out_path)
    meta = change_meta_path(meta, out_path)
    print(f"Save changed metadata to {meta.metadata_document}")
    if not testing:
        meta.write_metadata_document()  # Dumps content to json, writes to file with pathlib.Path.write_text
