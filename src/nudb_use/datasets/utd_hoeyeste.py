import gc

import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def _generate_utd_hoeyeste_table(
    alias: str,
    connection: db.DuckDBPyConnection,
    first_year: int = 1970,
    last_year: int | None = None,
    valid_snrs: None | pd.Series = None,
) -> None:
    from nudb_use.datasets.nudb_datasets import NudbData
    from nudb_use.variables.derive import utd_hoeyeste_rangering

    def keep_valid_snrs(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["snr"].isin(valid_snrs)] if valid_snrs else df

    eksamen_avslutta_hoeyeste_rangert = (
        NudbData("eksamen_avslutta_hoeyeste")
        .df()
        .pipe(keep_valid_snrs)
        .pipe(utd_hoeyeste_rangering)
        .sort_values(by="utd_hoeyeste_rangering", ascending=False)
        .assign(
            utd_hoeyeste_dato=lambda df: df["utd_aktivitet_slutt"].fillna(
                df["uh_eksamen_dato"]
            )
        )[["snr", "nus2000", "utd_hoeyeste_dato", "utd_hoeyeste_rangering"]]
    )

    last_year_data = eksamen_avslutta_hoeyeste_rangert["utd_hoeyeste_dato"].max().year

    if not last_year:
        last_year = last_year_data
    else:
        last_year = max(last_year, last_year_data)

    years = list(range(last_year, first_year - 1, -1))
    utd_hoeyeste_year = {}

    with LoggerStack(f"Deriving `utd_hoeyeste` [{first_year}-{last_year}]."):
        for i, year in enumerate(years):

            logger.info(
                f"Getting `utd_hoeyeste` for {year} in period {first_year}-{last_year}... [{100*(i+1)/len(years):6.2f}%]"
            )
            cutoff_date = pd.to_datetime(f"{year}-10-01")

            eksamen_avslutta_hoeyeste_rangert = eksamen_avslutta_hoeyeste_rangert[
                eksamen_avslutta_hoeyeste_rangert["utd_hoeyeste_dato"] <= cutoff_date
            ]

            gc.collect()

            utd_hoeyeste_year[year] = (
                eksamen_avslutta_hoeyeste_rangert[["snr", "nus2000"]]
                .groupby("snr", as_index=False)
                .first()
                .assign(utd_hoeyeste_aar=str(year))
                .astype({"utd_hoeyeste_aar": "string[pyarrow]"})
            )

    _utd_hoeyeste_pandas = pd.concat(utd_hoeyeste_year)
    del utd_hoeyeste_year
    gc.collect()

    connection.execute(f"CREATE TABLE {alias} AS SELECT * FROM _utd_hoeyeste_pandas")
