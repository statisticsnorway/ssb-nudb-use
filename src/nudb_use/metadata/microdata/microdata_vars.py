"""Module to fetch and summarize Microdata variables and metadata."""

from pathlib import Path
from typing import Literal

import pandas as pd
from dapla_metadata.variable_definitions import Vardef
from nudb_config import settings

from nudb_use.nudb_logger import logger


def get_microdata_variables_overview(dataset_name: str) -> pd.DataFrame:
    """Get an overview of variables in a Microdata dataset.

    For a given microdata dataset, retrieves the column names, full names,
    descriptions from Vardef, and calculates the min and max values of the
    school year/date column where each variable is not null or empty.

    Args:
        dataset_name: Name of the microdata dataset (with or without '_microdata_' prefix).

    Returns:
        pd.DataFrame: A DataFrame with the overview of the variables.
    """
    from nudb_use.datasets.microdata import MicroData

    # Normalize dataset name
    microdata_name = dataset_name.removeprefix("_microdata_")

    logger.info(
        f"Loading microdata dataset '{microdata_name}' to generate variables overview."
    )

    try:
        microdata_obj = MicroData(microdata_name)
        df = microdata_obj.df()
    except Exception as e:
        logger.error(f"Failed to load microdata dataset '{microdata_name}': {e}")
        raise ValueError(
            f"Could not load microdata dataset '{microdata_name}': {e}"
        ) from e

    # Find the appropriate school year / date column
    year_col = None
    possible_cols = [
        "utd_skoleaar_start",
        "np_utd_skoleaar_start",
        "utd_aktivitet_start",
        "gyldig_fra_dato",
        "utd_skoleaar",
    ]
    for col in possible_cols:
        if col in df.columns:
            year_col = col
            break

    if not year_col:
        # Fallback search for any column containing date/year keywords
        for col in df.columns:
            col_lower = col.lower()
            if "skoleaar" in col_lower or "start" in col_lower or "dato" in col_lower:
                year_col = col
                break

    if year_col:
        logger.info(
            f"Using '{year_col}' as the school year/date column for temporal boundaries."
        )
    else:
        logger.warning(
            f"No date or school year column found in dataset '{microdata_name}'."
        )

    # Determine which variables to include based on the datasets config
    prefixed_name = f"_microdata_{microdata_name}"
    variables_to_include = []

    if (
        prefixed_name in settings.datasets
        and settings.datasets[prefixed_name].variables
    ):
        variables_to_include = settings.datasets[prefixed_name].variables
    else:
        # If not defined in datasets_microdata.toml variables, fallback to df.columns
        variables_to_include = [
            c
            for c in df.columns
            if c not in ("snr", "fnr", "nudb_dataset_id", "__index_level_0__")
        ]

    overview_list = []

    for col in variables_to_include:
        # Determine the name to look up in Vardef (check derived_from in settings.variables)
        lookup_name = col
        if col in settings.variables:
            var_config = settings.variables[col]
            derived_from = getattr(var_config, "derived_from", None)
            if (
                derived_from
                and isinstance(derived_from, list)
                and len(derived_from) > 0
            ):
                lookup_name = derived_from[0]

        # Get metadata from Vardef
        try:
            vardef_info = Vardef.get_variable_definition_by_shortname(
                short_name=lookup_name
            )
            vardef_dict = vardef_info.model_dump()
            full_name = vardef_dict.get("name", {}).get("nb", lookup_name)
            description = vardef_dict.get("definition", {}).get(
                "nb", "Beskrivelse mangler"
            )
        except Exception as e:
            logger.debug(f"Failed to fetch Vardef metadata for '{lookup_name}': {e}")
            full_name = f"{col} (ikke i Vardef)"
            description = "Denne variabelen finnes i NUDB-config, men ikke i Vardef."

        # Determine which column in df to use for computing temporal boundaries (col or lookup_name)
        data_col = None
        if col in df.columns:
            data_col = col
        elif lookup_name in df.columns:
            data_col = lookup_name

        # Calculate temporal boundaries
        min_year = None
        max_year = None

        if year_col and data_col:
            # Filter rows where the data column is not null or empty string
            not_null_series = df[data_col].notna() & (
                df[data_col].astype(str).str.strip() != ""
            )
            valid_years = df.loc[not_null_series, year_col].dropna()
            valid_years = valid_years[valid_years.astype(str).str.strip() != ""]

            if not valid_years.empty:
                min_year = valid_years.min()
                max_year = valid_years.max()

        overview_list.append(
            {
                "Variabel": col,
                "Fullt Navn": full_name,
                "Min_aar": min_year if min_year is not None else "N/A",
                "Max_aar": max_year if max_year is not None else "N/A",
                "Beskrivelse": description,
            }
        )

    return pd.DataFrame(overview_list)


def split_microdata_dataset(
    dataset_or_name: str | pd.DataFrame,
    id_col: str = "fnr",
    keys: list[str] | None = None,
    auto_detect_start_stop: bool = True,
    output_dir: str | Path | None = None,
    file_format: Literal["parquet", "csv", "feather"] = "parquet",
    ignore_variables: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Splits a full derived dataset into individual variable datasets.

    Ensures that the requested ID column ('fnr' or 'snr') is present, automatically
    maps it using '_snrkat_fnr2snr' if necessary, drops any rows where the resolved ID
    is missing (with a logger warning), and splits every other variable into its own
    thin, non-null DataFrame (e.g., [ID, Variable] or [ID, Keys, Variable]).

    Args:
        dataset_or_name: Either a MicroData dataset name (str) or an already loaded pd.DataFrame.
        id_col: The primary ID column to keep ('fnr' or 'snr'). Defaults to 'fnr'.
        keys: Optional list of additional key/timestamp columns to keep in each split
              file (e.g. ['utd_skoleaar_start'] for longitudinal data).
        auto_detect_start_stop: If True, automatically detects and retains columns whose names
                                contain 'start', 'stopp', or 'slutt' (case-insensitive) as keys.
                                Defaults to True.
        output_dir: If provided, saves each variable dataset as a separate file in this folder.
        file_format: File format to use when saving ('parquet', 'csv', 'feather').

    Returns:
        dict[str, pd.DataFrame]: A dictionary mapping variable names to their split DataFrames.
    """
    from nudb_use.datasets.microdata import MicroData
    from nudb_use.datasets.nudb_data import NudbData

    # 1. Resolve source to a Pandas DataFrame
    if isinstance(dataset_or_name, pd.DataFrame):
        df = dataset_or_name.copy()
    elif isinstance(dataset_or_name, str):
        df = MicroData(dataset_or_name).df()
    else:
        raise TypeError("dataset_or_name must be a string or a pandas DataFrame.")

    # 2. Map ID column if missing but the alternative is present, or map missing fnr/snr when using a custom ID
    if id_col == "fnr" and "fnr" not in df.columns:
        if "snr" not in df.columns:
            raise KeyError("Neither 'fnr' nor 'snr' is present in the dataset.")
        fnr2snr = NudbData("_snrkat_fnr2snr").df()
        df = df.merge(fnr2snr, on="snr", how="left")

    elif id_col == "snr" and "snr" not in df.columns:
        if "fnr" not in df.columns:
            raise KeyError("Neither 'fnr' nor 'snr' is present in the dataset.")
        fnr2snr = NudbData("_snrkat_fnr2snr").df()
        df = df.merge(fnr2snr, on="fnr", how="left")

    elif id_col not in ("fnr", "snr"):
        if id_col not in df.columns:
            raise KeyError(
                f"The selected ID column '{id_col}' is not present in the dataset."
            )

        # If either 'fnr' or 'snr' is present but the other is missing, map it!
        if "fnr" in df.columns and "snr" not in df.columns:
            fnr2snr = NudbData("_snrkat_fnr2snr").df()
            df = df.merge(fnr2snr, on="fnr", how="left")
        elif "snr" in df.columns and "fnr" not in df.columns:
            fnr2snr = NudbData("_snrkat_fnr2snr").df()
            df = df.merge(fnr2snr, on="snr", how="left")

    # 3. Handle Dropping of Rows with Missing Primary IDs
    if df[id_col].isna().any():
        null_count = df[id_col].isna().sum()
        logger.warning(
            f"Dropped {null_count} rows from the dataset because the mapped ID '{id_col}' was missing."
        )
        df = df.dropna(subset=[id_col])

    # 4. Resolve Context/Sequence Keys
    keys_to_keep = [k for k in (keys or []) if k in df.columns]

    if auto_detect_start_stop:
        # Detect any column whose name contains 'start', 'stop', 'stopp', or 'slutt' (ignoring casing)
        detected_keys = []
        for col in df.columns:
            col_lower = col.lower()
            if col != id_col and any(
                term in col_lower for term in ("start", "stop", "stopp", "slutt")
            ):
                detected_keys.append(col)

        if detected_keys:
            logger.info(
                f"Automatically detected start/stop columns to use as keys: {detected_keys}"
            )
            for dk in detected_keys:
                if dk not in keys_to_keep:
                    keys_to_keep.append(dk)

    # Filter keys to only those that exist
    keys_to_keep = [k for k in keys_to_keep if k in df.columns]

    extra_ignore = set(ignore_variables or [])
    ignore_cols = (
        {"nudb_dataset_id", "__index_level_0__"} | set(keys_to_keep) | extra_ignore
    )
    variable_cols = [col for col in df.columns if col not in ignore_cols]

    # 5. Split variables into thin DataFrames
    split_dfs = {}
    for var in variable_cols:
        if var == id_col:
            target_cols = [id_col] + keys_to_keep
        else:
            target_cols = [id_col] + [var] + keys_to_keep
        # Keep only rows where the actual variable is not null
        subset_df = df[target_cols].dropna(subset=[var]).copy().reset_index(drop=True)
        split_dfs[var] = subset_df

    # 6. Optionally save to disk
    if output_dir:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        for var, var_df in split_dfs.items():
            file_name = f"{var}.{file_format}"
            file_path = out_path / file_name

            if file_format == "parquet":
                var_df.to_parquet(file_path, index=False)
            elif file_format == "csv":
                var_df.to_csv(file_path, index=False)
            elif file_format == "feather":
                var_df.to_feather(file_path)

    return split_dfs
