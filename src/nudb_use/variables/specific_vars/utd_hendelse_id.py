import pandas as pd

from nudb_use.datasets import NudbData
from nudb_use.datasets.macros import (
    _WIDTH_UTD_HENDELSE_ID,  # defining it here gives us a circular import...
)
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.nudb_logger import function_logger_context
from nudb_use.nudb_logger import logger

# Gosh, I hope this doensn't change later on...
DATASET_IDS = {"0": "AVSLUTTA", "1": "IGANG", "2": "EKSAMEN"}
MISSING_HENDELSE_ID = 99999999999999999


@function_logger_context
def join_variables_on_hendelse_id(
    df: pd.DataFrame,
    variables: list[str],
    prefix: str = "",
    utd_hendelse_id_col: str = "utd_hendelse_id",
) -> pd.DataFrame:
    """Join NUDB variables from AVSLUTTA, IGANG or EKSAMEN on data using utd_hendelse_id.

    Args:
        df: The dataframe to join the variables on (must contain the utd_hendelse_id column).
        variables: List containing the variables you want.
        prefix: Optional string, adding a specified prefix to the new joined variables.
        utd_hendelse_id_col: Optional string, naming the column containing utd_hendelse_id values. Defaults to 'utd_hendelse_id'.

    Returns:
        pd.DataFrame: The modified dataframe with the corrected column.

    Raises:
        KeyError: If utd_hendelse_id is not in the columns of df.
        ValueError: If variables is not a non-empty list.
    """
    if utd_hendelse_id_col not in df.columns:
        raise KeyError(f"`df` must contain `{utd_hendelse_id_col}`!")

    if not isinstance(variables, list) or not variables:
        raise ValueError("`variables` must be a non-empty list!")

    df_valid = df.query(f"{utd_hendelse_id_col}.notna()")
    df_not_valid = df.query(f"{utd_hendelse_id_col}.isna()")

    utd_hendelse_id = (
        df_valid[utd_hendelse_id_col]
        .astype("string[pyarrow]")
        .str.zfill(
            _WIDTH_UTD_HENDELSE_ID
        )  # it's a bit clunky, since avslutta = 0, so we might not have the first digit...
    )

    if (utd_hendelse_id.str.len() > _WIDTH_UTD_HENDELSE_ID).any():
        raise ValueError(
            f"Did not expect {utd_hendelse_id_col} to be longer than {_WIDTH_UTD_HENDELSE_ID} characters!"
        )

    logger.info(f"Identifying unique datasets in `{utd_hendelse_id_col}`...")
    datasets = (
        pd.Series(utd_hendelse_id.str.slice(0, 1).unique(), dtype="string[pyarrow]")
        .map(DATASET_IDS)
        .to_list()
    )
    logger.info(f"unique datasets in `{utd_hendelse_id_col}`: {', '.join(datasets)}")
    want = set(variables)
    queries = []

    if "utd_hendelse_id" in want:
        logger.warning(
            "`utd_hendelse_id` should not be included in `variables`, ignoring it!"
        )
        want = want - {"utd_hendelse_id"}

    for dataset in datasets:
        nudb_data = NudbData(dataset)
        have = set(nudb_data.get_available_cols())
        add = list(want & have)
        add_prefix = [prefix + variable for variable in add]
        add_alias = [f"{add[i]} AS {add_prefix[i]}" for i in range(len(add))]

        queries.append(f"""
            SELECT
                utd_hendelse_id, '{dataset}' AS {prefix}nudb_dataset, {', '.join(add_alias)}
            FROM
                {nudb_data.alias}
        """)

    query = "\nUNION ALL BY NAME\n".join(queries)
    logger.debug(f"SQL query:\n{query}")
    connection = nudb_database.get_connection()

    logger.info("Getting data from NUDB...")
    right = connection.sql(query).df()

    logger.info("Merging...")
    df_valid_merged = pd.merge(
        left=df_valid,
        right=right,
        left_on=utd_hendelse_id_col,
        right_on="utd_hendelse_id",
        how="left",
        validate="m:1",  # if this isn't true something is wrong with OUR (i.e., the NUDB team) data/code
    )

    return pd.concat([df_valid_merged, df_not_valid], ignore_index=True)
