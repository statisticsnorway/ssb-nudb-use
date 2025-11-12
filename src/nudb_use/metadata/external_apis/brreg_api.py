import pandas as pd
from brreg.enhetsregisteret import Client
from brreg.enhetsregisteret import EnhetQuery, UnderenhetQuery
from pydantic import BaseModel
from collections.abc import Iterable
from typing import Any
import csv
import gzip
import requests
from io import StringIO
from nudb_use import logger
from nudb_use.config import settings


def download_csv_content_enheter() -> pd.DataFrame:
    """Download and parse organisation data from Brønnøysundregisteret and convert it to a DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing organisation data from 
            the Brønnøysundregisteret.
    """
    logger.info("Downloading CSV from brreg...")
    url = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned/csv"
    response = requests.get(url)
    response.raise_for_status()
    
    logger.info("Decompressing the response from brreg")
    decompressed = gzip.decompress(response.content).decode("utf-8")
    
    logger.info("Use csv module to parse robustly, from brreg")
    reader = csv.DictReader(
        StringIO(decompressed),
        delimiter=",",
        quotechar='"',
        doublequote=True
    )
    
    logger.info("Convert data from brreg to DataFrame")
    return pd.DataFrame(reader)


def filter_utd_csv_enheter() -> pd.DataFrame:
    """
    Download organisation data from Brønnøysundregisteret using the download_csv_content_enheter function and extract UTD-nacecodes.

    Returns: 
        pd.DataFrame: Dataframe contaaining UTD-nacecodes from Brønnøysundregisteret.
    """
    df = download_csv_content_enheter()
    logger.info("Filtering brreg-data down to UTD-nacecodes.")
    return df[
            df["naeringskode1.kode"].isin(settings.utd_nacekoder) |
            df["naeringskode2.kode"].isin(settings.utd_nacekoder) |
            df["naeringskode3.kode"].isin(settings.utd_nacekoder)
            ].convert_dtypes()


def orgnr_is_underenhet(orgnr: str) -> bool:
    """Check if a given organisation is a sub-unit (underenhet).

        Args:
            orgnr: Organisation number to check.

        Returns:
            bool: True if the organisation number is an underenhet, False otherwise.
    """
    with Client() as client:
        return client.get_underenhet("".join([c for c in orgnr if c.isdigit()])) is not None

def get_enhet(orgnr: str) -> None | dict[str, str]:
    """Check if a given organisation is either a main unit (enhet, foretak) or sub-unit (underenhet, bedrift).

        Args:
            orgnr: The organisation number to look up.
    
        Returns:
            dict or None: Information about the main unit or sub-unit, or None if not found.
    """
    orgnr_clean = "".join([c for c in orgnr if c.isdigit()])
    with Client() as client:
        result = client.get_enhet(orgnr_clean)
        if result is None:
            result = client.get_underenhet(orgnr_clean)
    return result


def search_nace(naces: list[str]) -> pd.DataFrame:
    """Validate that NACE codes are in the correct format (a dot in
       the third position), then attempts to query the business entity register for
       each code. It initiates API searches and logs the total pages found for each
       NACE code.

    Note:
        The function is currently incomplete: it logs total pages but does not
        collect or return the actual entity data beyond empty DataFrames because
        the loop that processes pages and extracts data is skipped due to a
        `continue` statement. Data collection logic following the logging is never executed.

    Args:
        naces: List of NACE codes to query.

    Returns:
        pd.DataFrame: Concatenated DataFrame of search results per NACE code.
                      Currently always empty due to incomplete implementation.

    Raises:
        ValueError: If any NACE code does not have a dot '.' at the third character.
    """
    # Make sure codes follow standard
    if not all([x[2] == "." for x in naces]):
        raise ValueError("One of the nace-codes is missing a point in the third position.")
    dataframe_list = []
    for nacekode in naces:
        sok = UnderenhetQuery()
        sok.naeringskode = [nacekode]
        with Client() as client:
            search = client.search_enhet(sok)
            for elem in search.get_page(0):
                if elem[0] == "total_pages":
                    total_pages = elem[1]
                    logger.info(f"Total pages for nacekode {nacekode}: {total_pages}")
                    logger.debug(elem)
            continue
            for page_num in range(total_pages):
                for elem in search.get_page(page_num):
                    if elem[0] == "items":
                        for item in elem[1:]:
                            for enhet in item:
                                dataframe_list.append(pd.DataFrame([flatten(enhet)]))
    return pd.concat(dataframe_list)


def flatten(obj: Any, prefix: str = '', sep: str = '_') -> dict[str, Any]:
    """Recursively flatten a nested dictionary, list, or Pydantic model into a flat dictionary.

    Args:
        obj: Input object to flatten. Can be a dict, list, Pydantic BaseModel, or any other type.
        prefix: The string prefix for keys in the output dictionary. Defaults to an empty string.
        sep: Separator used between nested keys. Defaults to '_'.

    Returns:
        dict: A flat dictionary with keys representing nested paths.
    """
    flat = {}

    if isinstance(obj, BaseModel):
        obj = obj.model_dump()  # Updated for Pydantic v2

    if isinstance(obj, dict):
        for k, v in obj.items():
            full_key = f"{prefix}{sep}{k}" if prefix else k
            flat.update(flatten(v, full_key, sep=sep))

    elif isinstance(obj, list):
        if all(isinstance(i, (str, int, float, bool, type(None))) for i in obj):
            flat[prefix] = ' - '.join(map(str, obj))
        else:
            for i, item in enumerate(obj):
                flat.update(flatten(item, f"{prefix}{sep}{i}", sep=sep))

    else:
        flat[prefix] = obj

    return flat
