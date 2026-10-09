import pandas as pd

from nudb_use import NudbData
from nudb_use import derive
from nudb_use.metadata.nudb_config.variable_names import get_cols_in_config
from nudb_use.nudb_logger import logger

GRUNNSKOLE_NACE_CODES = {"85.201", "85.202"}

NACE_COLUMNS = ["bof_naering1_sn2025", "bof_naering2_sn2025", "bof_naering3_sn2025"]

VALID_ELEVSTATUS = {"E", "S"}

EXCLUSION_VARS = [
    "er_ikke_grunnskolenaering",
    "er_steinerskole",
    "er_norskskoleiutlandet",
    "er_ukjentorgnrbed",
    "er_over16aar",
    "er_ikke_elevstatus_es",
]

REQUIRED_COLS_FOR_EXCLUSION = [
    "har_grunnskolenaering",
    "gro_skolenavn_inn",
    "utd_skolekom",
    "orgnrbed",
    "pers_alder",
]

REQUIRED_COLS_FOR_RES_EXCLUSION = ["gr_grunnskolepoeng"]


def _validate_required_cols_for_exclusion(
    df: pd.DataFrame,
    required_cols: list[str],
) -> None:
    """Validate that all required columns exist.

    Args:
        df:
            Input dataframe.
        required_cols:
            Columns that must be present.

    Raises:
        ValueError:
            If one or more columns are missing.
    """
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")


def _derive_har_grunnskolenaering(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive 'har_grunnskolenaering' from existing SN2025 industry codes.

    Args:
        df:
            pd.DataFrame.
            Input dataframe containing the SN2025 industry code columns.

    Returns:
        pd.DataFrame:
            Dataframe with derived column 'har_grunnskolenaering'.
    """
    _validate_required_cols_for_exclusion(df, NACE_COLUMNS)

    df = df.copy()

    df["har_grunnskolenaering"] = (
        df[NACE_COLUMNS].isin(GRUNNSKOLE_NACE_CODES).any(axis=1)
    )

    return df


def _add_har_grunnskolenaering(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add a boolean column indicating whether the organisation has grunnskole industry code.

    Args:
        df:
            pd.DataFrame.
            Input dataframe.

    Returns:
        pd.DataFrame:
            Dataframe with column 'har_grunnskolenaering' added.
    """
    if "har_grunnskolenaering" in df.columns:
        return df

    df = (
        df.pipe(derive.bof_naering1_sn2025)
        .pipe(derive.bof_naering2_sn2025)
        .pipe(derive.bof_naering3_sn2025)
        .pipe(_derive_har_grunnskolenaering)
    )

    logger.info("Derived 'har_grunnskolenaering' from SN2025 industry codes.")

    return df


def _add_age_at_school_start(
    df: pd.DataFrame,
    start_year: int,
) -> pd.DataFrame:
    """Add age at the start of the school year.

    Args:
        df:
            pd.DataFrame.
            Input dataframe.
        start_year:
            int.
            School year start year.

    Returns:
        pd.DataFrame:
            Dataframe with column 'pers_alder' added.
    """
    if "pers_alder" in df.columns:
        return df

    _validate_required_cols_for_exclusion(
        df,
        ["pers_foedselsdato"],
    )

    df = df.copy()

    df["pers_alder"] = start_year - df["pers_foedselsdato"].dt.year

    return df


def _get_required_cols_for_exclusion(
    df: pd.DataFrame,
    start_year: int,
) -> pd.DataFrame:
    """Add columns required by the exclusion logic.

    Args:
        df:
            pd.DataFrame.
            Input dataframe.
        start_year:
            int.
            Start year of the school year, ussed when deriving pers_alder.

    Returns:
        pd.DataFrame:
            Dataframe with required columns added for use in _create_boolean_vars_for_exclusion().
    """
    df = _add_har_grunnskolenaering(df)

    df = _add_age_at_school_start(
        df=df,
        start_year=start_year,
    )

    return df


def _create_boolean_vars_for_exclusion(
    df: pd.DataFrame,
    res: bool = True,
) -> pd.DataFrame:
    """Create boolean variables used for population exclusion.

    Args:
        df:
            pd.DataFrame
            Input DataFrame.
        res:
            bool, default=True
            Whether to create 'har_ikke_grunnskolepoeng'.
            If True, gr_grunnskolepoeng is a required column.

    Returns:
        pd.DataFrame:
            Dataframe with boolean variables for exclusion added.

    Examples:
        karakterer:
            df = _create_boolean_variables_for_exclusion(df, res=False)
        resultat:
            df = _create_boolean_variables_for_exclusion(df)
    """
    _validate_required_cols_for_exclusion(df, REQUIRED_COLS_FOR_EXCLUSION)

    df = df.copy()

    df["er_ikke_grunnskolenaering"] = ~df["har_grunnskolenaering"]

    df["er_steinerskole"] = df["gro_skolenavn_inn"].str.contains(
        r"steiner|waldorf",
        case=False,
        na=False,
        regex=True,
    )

    df["er_norskskoleiutlandet"] = (
        df["utd_skolekom"].eq("2599").fillna(False).astype(bool)
    )

    df["er_ukjentorgnrbed"] = df["orgnrbed"].isna()

    df["er_over16aar"] = df["pers_alder"] > 16

    if df["gro_elevstatus"].notna().any():
        df["er_ikke_elevstatus_es"] = ~df["gro_elevstatus"].isin(VALID_ELEVSTATUS)
    else:
        df["er_ikke_elevstatus_es"] = False

    exclusion_vars = EXCLUSION_VARS.copy()

    if res:
        _validate_required_cols_for_exclusion(df, REQUIRED_COLS_FOR_RES_EXCLUSION)

        df["har_ikke_grunnskolepoeng"] = df["gr_grunnskolepoeng"].eq(0)

        exclusion_vars.append("har_ikke_grunnskolepoeng")

    logger.info(
        "Created exclusion variables: %s",
        ", ".join(exclusion_vars),
    )

    return df


def _exclude_to_grunnskole_population(
    df: pd.DataFrame,
    ignore_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Exclude rows based on exclusion flags.

    Args:
        df:
            pd.DataFrame
            Input dataframe. Population to filter.
        ignore_cols:
            list[str] | None, optional
            List of exclusion variables to ignore when filtering.

    Returns:
        pd.DataFrame:
            DataFrame that is filtered based on exclusion flags.

    Examples:
        df = _exclude_to_grunnskole_population(df)
        df = _exclude_to_grunnskole_population(df, ignore_cols=["har_ikke_grunnskolepoeng"])
    """
    exclusion_cols = EXCLUSION_VARS.copy()

    if "har_ikke_grunnskolepoeng" in df.columns:
        exclusion_cols.append("har_ikke_grunnskolepoeng")

    if ignore_cols:
        logger.info(
            "Ignoring exclusion variables: %s",
            ", ".join(sorted(ignore_cols)),
        )

        exclusion_cols = [col for col in exclusion_cols if col not in ignore_cols]

    logger.info(
        "Applying exclusion variables: %s",
        ", ".join(sorted(exclusion_cols)),
    )

    _validate_required_cols_for_exclusion(df, exclusion_cols)

    skal_ekskluderes = df[exclusion_cols].fillna(False).any(axis=1)

    logger.info(
        "Ekskluderer %s personer (%s).",
        f"{skal_ekskluderes.sum():,}",
        f"{skal_ekskluderes.mean():.1%}",
    )

    return df.loc[~skal_ekskluderes].copy()


def create_grunnskole_population_from_nudb(
    start_year: int,
    ignore_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Creates a grunnskole population based on the NUDB dataset 'avslutta', filtered similarly to kargrs.

    Args:
        start_year:
            int.
            Start year of the school year to retrieve. For example, 2024 represents school year 2024/2025.
        ignore_cols:
            list[str] | None, optional.
            List of exclusion columns to ignore when filtering.

    Returns:
        pd.DataFrame:
            Grunnskole population after exclusion rules have been applied.

    Examples:
        df = create_grunnskole_population_from_nudb(start_year=2024, ignore_cols=["har_ikke_grunnskolepoeng"])
    """
    keep_cols = get_cols_in_config(name="avslutta_grunnskole")

    df = (
        NudbData("avslutta")
        .where(f"utd_datakilde='10' and utd_skoleaar_start='{start_year}'")
        .select(",".join(keep_cols))
        .df()
    )

    df = _get_required_cols_for_exclusion(df=df, start_year=start_year)

    df = _create_boolean_vars_for_exclusion(df=df, res=True)

    return _exclude_to_grunnskole_population(
        df,
        ignore_cols=ignore_cols,
    )
