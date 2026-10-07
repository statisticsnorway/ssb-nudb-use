import pandas as pd
from typing import Literal

from nudb_use.metadata.nudb_config.variable_names import get_cols_in_config
from nudb_use.datasets import NudbData
from nudb_use import derive
from nudb_use.nudb_logger import logger


def derive_har_grunnskolenaering(
    df: pd.DataFrame,
    nace_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Derive a boolean indicating whether the organisation has
    grunnskole industry code (85.201 or 85.202) in any NACE column.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    nace_columns : list[str] | None, default None
        NACE columns to search.

    Returns
    -------
    pd.DataFrame
        Dataframe with column 'har_grunnskolenaering'.
    """

    df = df.copy()

    if nace_columns is None:
        nace_columns = [
            "bof_naering1_sn2025",
            "bof_naering2_sn2025",
            "bof_naering3_sn2025",
        ]

    df["har_grunnskolenaering"] = (
        df[nace_columns]
        .isin(["85.201", "85.202"])
        .any(axis=1)
    )

    return df


def get_required_cols_for_exclusion(
    df: pd.DataFrame,
    start_year: int,
) -> pd.DataFrame:
    """
    Adds required columns for exclusion logic to the DataFrame.
    Uses the function 'assign_preferred_naering' to add næring.

    Required columns:
        - naering (to select only grunnskole-næringer)
        - pers_alder (to select everyone under 17)

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    start_year : int
        Reference year used when calculating age.

    Returns
    -------
    pd.DataFrame
        Dataframe with required columns added.

    Examples
    --------
    df = get_required_cols_for_exclusion(df=df, start_year=2024)
    """
    
    missing_cols = {"naering", "pers_alder"} - set(df.columns)

    if not missing_cols:
        return df

    if "naering" in missing_cols:

        if start_year >= 2025:
            df = (
                df
                .pipe(derive.bof_naering1_sn2025)
                .pipe(derive.bof_naering2_sn2025)
                .pipe(derive.bof_naering3_sn2025)
            )

            df = derive_har_grunnskolenaering(df)
            logger.info("Derived har_grunnskolenaering from industry codes from 2025")

        else:
            orgnr = "', '".join(
            df["orgnrbed"].dropna().astype(str).unique()
            )

            bof = (
                NudbData("bof_situttak")
                .select("orgnrbed, nace1_sn07, nace2_sn07, nace3_sn07")
                .where(f"orgnrbed in ('{orgnr}')")
                .df()
            )
    
            df = df.merge(bof, on="orgnrbed", how="left")
            df = derive_har_grunnskolenaering(df, nace_columns=["nace1_sn07", "nace2_sn07", "nace3_sn07"])
            logger.info("Derived har_grunnskolenaering from industry codes from 2007")

    if "pers_alder" in missing_cols:
        df["pers_alder"] = (
            start_year - df["pers_foedselsdato"].dt.year
        )

    return df

    
def create_boolean_variables_for_exclusion(
    df: pd.DataFrame,
    res: bool = True,
) -> pd.DataFrame:
    """
    Create boolean variables used for population exclusion.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    res : bool, default=True
        Whether to create exclusion variable based on
        gr_grunnskolepoeng.
        If True, gr_grunnskolepoeng is a required column.

    Returns
    -------
    pd.DataFrame
        Dataframe with boolean variables for exclusion added.

    Examples
    --------
    For karakterer: 
        df = create_boolean_variables_for_exclusion((df, res=False)
    For resultat:
        df = create_boolean_variables_for_exclusion((df)
    """

    required_cols = [
        "har_grunnskolenaering",
        "gro_skolenavn_inn",
        "utd_skolekom",
        "orgnrbed",
        "pers_alder",
        "gro_elevstatus",
    ]

    if res:
        required_cols.append("gr_grunnskolepoeng")

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(
            "Cannot create exclusion variables. Missing required columns: "
            f"{', '.join(missing_cols)}"
        )

    exclusion_vars = [
        "er_ikke_grunnskolenaering",
        "er_steinerskole",
        "er_norskskoleiutlandet",
        "er_ukjentorgnrbed",
        "er_over16aar",
        "er_ikke_elevstatus_es",
    ]

    df["er_ikke_grunnskolenaering"] = (
        ~df["har_grunnskolenaering"]
    )

    df["er_steinerskole"] = df["gro_skolenavn_inn"].str.contains(
        r"steiner|waldorf",
        case=False,
        na=False,
        regex=True,
    )

    df["er_norskskoleiutlandet"] = (
        df["utd_skolekom"]
        .eq("2599")
        .fillna(False)
        .astype(bool)
    )

    df["er_ukjentorgnrbed"] = df["orgnrbed"].isna()

    df["er_over16aar"] = df["pers_alder"] > 16

    if df["gro_elevstatus"].notna().any():
        df["er_ikke_elevstatus_es"] = ~df["gro_elevstatus"].isin(
            ["E", "S"]
        )
    else:
        df["er_ikke_elevstatus_es"] = False

    if res:
        df["har_ikke_grunnskolepoeng"] = (
            df["gr_grunnskolepoeng"] == 0.0
        )
        exclusion_vars.append("har_ikke_grunnskolepoeng")

    logger.info(
        f"Created exclusion variables: %s",
        f", ".join(exclusion_vars),
    )

    return df


def exclude_population(
    df: pd.DataFrame, 
    ignore_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Excludes rows based on exclusion flags.

    Parameters
    ----------
    df : pandas.DataFrame
        Population to filter.
    ignore_cols : list[str] | None, optional
        List of exclusion flag column(s) to ignore in exclusion.

    Returns
    -------
    pd.DataFrame
        DataFrame that is filtered based on exclusion flags. 

    Examples
    --------
    Without missing grunnskolepoeng:
        df = exclude_population(df)
    With missing grunnskolepoeng:
        df = exclude_population(df, ignore_cols=["har_ikke_grunnskolepoeng"])
    """
    
    exclusion_cols = [
        "er_ikke_grunnskolenaering",
        "er_steinerskole",
        "er_norskskoleiutlandet",
        "er_ukjentorgnrbed",
        "er_over16aar",
        "er_ikke_elevstatus_es",
        "har_ikke_grunnskolepoeng"
    ]

    if ignore_cols:
        exclusion_cols = [
            col for col in exclusion_cols
            if col not in ignore_cols
        ]

    skal_ekskluderes = df[exclusion_cols].any(axis=1)

    logger.info(
        f"Ekskluderer {skal_ekskluderes.sum():,} personer "
        f"({skal_ekskluderes.mean():.1%})."
    )

    return df.loc[~skal_ekskluderes].copy()


def create_grunnskole_population(
    start_year: int, 
    population: Literal["without_null_points", "with_null_points"], 
) -> pd.DataFrame:
    """
    Creates a grunnskole population based on the NUDB dataset 'avslutta'.
    
    The function retrieves pupils who completed lower secondary education (utd_datakilde='10') for a given school year, 
    and applies the same exclusion logic that is used when producing the official statistics for 'kargrs'.

    The returned population can either exclude or include pupils with missing grunnskolepoeng.

    Parameters
    ----------
    start_year: int
        Start year of the school year to retrieve. For example, 2024 represents school year 2024/2025.

    population: Literal 
        Determines how pupils with missing grunnskolepoeng are handled.
        Supported values:
            - 'without_null_points': Exclude pupils with missing grunnskolepoeng.
            - 'with_null_points': Include pupils with missing grunnskolepoeng.
            
    Returns
    -------
    pd.DataFrame
        Dataframe containing the selected grunnskole population after required variables have been derived and 
        exclusion rules have been applied.

    Examples
    --------
    df = create_grunnskole_population(start_year=2024, population="without_null_points")
    """
    
    keep_cols = get_cols_in_config(name="avslutta_grunnskole")
    df = NudbData("avslutta").where(f"utd_datakilde='10' and utd_skoleaar_start='{start_year}'").select(",".join(keep_cols)).df()

    df = get_required_cols_for_exclusion(df, start_year)

    df = create_boolean_variables_for_exclusion(df)

    if population == "without_null_points":
        logger.info("Create population without null grunnskolepoeng:")
        df_out = exclude_population(df)

    if population == "with_null_points":
        logger.info("Create population with null grunnskolepoeng:")
        df_out = exclude_population(df, ignore_cols=["har_ikke_grunnskolepoeng"])

    return df_out
