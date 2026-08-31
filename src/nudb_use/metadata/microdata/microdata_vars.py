"""Module to fetch and summarize Microdata variables and metadata."""

from webbrowser import get

import pandas as pd
from dapla_metadata.variable_definitions import Vardef

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

    overview_list = []

    for col in df.columns:
        # Get metadata from Vardef
        try:
            vardef_info = Vardef.get_variable_definition_by_shortname(short_name=col)
            vardef_dict = vardef_info.model_dump()
            full_name = vardef_dict.get("name", {}).get("nb", col)
            description = vardef_dict.get("definition", {}).get(
                "nb", "Beskrivelse mangler"
            )
        except Exception as e:
            logger.debug(f"Failed to fetch Vardef metadata for '{col}': {e}")
            full_name = f"{col} (ikke i Vardef)"
            description = "Denne variabelen finnes i NUDB-config, men ikke i Vardef."

        # Calculate temporal boundaries
        min_year = None
        max_year = None

        if year_col:
            # Filter rows where the column is not null or empty string
            not_null_series = df[col].notna() & (df[col].astype(str).str.strip() != "")
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



nasjprov_test = get_microdata_variables_overview("nasjprov")
print(nasjprov_test)