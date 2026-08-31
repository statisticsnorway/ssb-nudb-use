"""Module to fetch and summarize Microdata variables and metadata."""

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
