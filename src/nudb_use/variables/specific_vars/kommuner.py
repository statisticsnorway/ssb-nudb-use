import datetime

import klass
import pandas as pd

from nudb_use import LoggerStack
from nudb_use import logger

EXTRA_KOMMNR = {
    "2580": "360s definerte Utland",
    "2111": "Longyearbyen arealplanområde",
}


def keep_only_valid_kommune_codes(
    komm_col: pd.Series, from_date: str = "1960-01-01", to_date: str | None = None
) -> pd.Series:
    """Filter a column of country codes down to the ones who have existed.

    Args:
        komm_col: A pandas series that we should modify to only contain valid codes.
        from_date: The date we should include valid kommune-codes from.
        to_date: The date we should include valid kommune-codes until. If set to None, defaults to todays-date.

    Returns:
        pd.Series: The modified column that only contains valid kommune cols.
    """
    with LoggerStack("Keeping valid kommune-codes."):
        komm_col = komm_col.copy()
        amount_missing_pre_empty = ((komm_col.isna()) | (komm_col == "9999")).sum()
        if to_date is None:
            to_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            to_date_str = to_date
        kommuner_alle_aar = list(
            klass.KlassClassification(131)
            .get_codes(
                from_date=from_date,  # Har funnet kommuner i dataene som sist eksisterte i 1961
                to_date=to_date_str,
            )
            .to_dict()
            .keys()
        )  # Alle kommunekoder mellom 1960 og nå

        kommuner_alle_aar += list(EXTRA_KOMMNR.keys())
        # De med kjent fylke, men ukjent kommune har "00" i kommunefeltet, men er noe vi godtar i utdanningsdata
        kommuner_alle_aar += list({x[:2] + "00" for x in kommuner_alle_aar})

        # Det er noen som har "99" etter gyldig fylke, disse byttes til "00"
        komm_col.loc[komm_col.str.endswith("99")] = komm_col.str[:2] + "00"
        # Om denne oppstod nå, så korrigerer vi den tilbake
        komm_col.loc[komm_col == "9900"] = "9999"
        behold_komm_maske = komm_col.isin(kommuner_alle_aar)

        komm_col.loc[~behold_komm_maske] = pd.NA
        amount_missing_post_empty = ((komm_col.isna()) | (komm_col == "9999")).sum()
        logger.info(
            f"Emptying kommunenr-ene {list(komm_col[~behold_komm_maske].unique())}. Removed kommunenummer from { round((amount_missing_post_empty - amount_missing_pre_empty) / len(komm_col) * 100, 2)}% of the rows."
        )
        return komm_col


def correct_kommune_single_values(
    df: pd.DataFrame, col_name: str = "utd_skolekom"
) -> pd.DataFrame:
    """Correct a kommune-column where we know there to be only one correct value a certain incorrect value can map to.

    Args:
        df: The dataframe to mutate the kommune-column in.
        col_name: The string-name of the kommune-column we want to correct.

    Returns:
        pd.DataFrame: The dataframe with the modified Kommune-column.

    Raises:
        ValueError: If we find some weird kommune-code values that are not 4 digits.
    """
    with LoggerStack("Correcting kommune-code with known 1:1 mappings."):
        col_temp = df[col_name].copy()
        weird_ones = (col_temp.notna()) & (
            (col_temp.str.len() != 4)
            | (~col_temp.str.isdigit().fillna(False).astype("bool[pyarrow]"))
        )
        if weird_ones.any():
            raise ValueError(
                f"Found some weird kommune-values in {col_name}: {col_temp[weird_ones].unique()} - fix these first? Sentinel-value is `9999` not missing."
            )
        missing_val = "9999"
        col_temp = col_temp.fillna(missing_val)
        mapping = {
            "0300": "0301",
            "2100": "2111",
            "2500": "2580",
            "2400": "2580",  # VIGOs "utland"
            "9900": missing_val,
            "9998": missing_val,
            "0000": missing_val,
        }
        logger.info(
            f"Remapping {col_temp.isin(mapping.keys()).sum()} of {len(df)} rows with the known kommune-mappings."
        )
        col_temp = col_temp.map({c: c for c in col_temp.unique()} | mapping)
        logger.info(
            f"Setting {col_temp.isna().sum()} of {len(col_temp)} {col_name} cells to {missing_val} because they were empty."
        )
        col_temp.loc[col_temp.isna()] = missing_val
        df[col_name] = col_temp
        return df
