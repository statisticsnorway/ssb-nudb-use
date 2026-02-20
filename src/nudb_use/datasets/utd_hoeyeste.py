import duckdb as db
import pandas as pd

from nudb_use.nudb_logger import logger


def _generate_utd_hoeyeste_table(
    alias: str,
    connection: db.DuckDBPyConnection,
    first_year: int = 1970,
    last_year: int | None = None,
    valid_snrs: None | pd.Series = None,
) -> None:
    from nudb_use.datasets.nudb_data import NudbData
    from nudb_use.variables.derive import utd_hoeyeste_rangering

    def keep_valid_snrs(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["snr"].isin(valid_snrs)] if (valid_snrs is not None) else df

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

    base = eksamen_avslutta_hoeyeste_rangert
    # Ensure datetime64[ns]
    base["utd_hoeyeste_dato"] = pd.to_datetime(base["utd_hoeyeste_dato"])

    last_year_data = int(base["utd_hoeyeste_dato"].max().year)
    last_year = last_year_data if last_year is None else max(last_year, last_year_data)

    years_df = pd.DataFrame(
        {"utd_hoeyeste_aar": list(range(last_year, first_year - 1, -1))}
    )
    years_df["cutoff_date"] = pd.to_datetime(
        years_df["utd_hoeyeste_aar"].astype(str) + "-10-01"
    )
    years_df["utd_hoeyeste_aar"] = years_df["utd_hoeyeste_aar"].astype(
        "string[pyarrow]"
    )

    connection.register(
        "_base", base[["snr", "nus2000", "utd_hoeyeste_dato", "utd_hoeyeste_rangering"]]
    )
    connection.register("_years", years_df)

    logger.info(f"Deriving `utd_hoeyeste` [{first_year}-{last_year}].")

    query = f"""
        CREATE TABLE
            {alias} AS
        SELECT
            snr, nus2000, utd_hoeyeste_rangering, utd_hoeyeste_aar
        FROM (
          SELECT
            y.utd_hoeyeste_aar,
            b.snr,
            b.nus2000,
            b.utd_hoeyeste_rangering,
            row_number() OVER (
              PARTITION BY y.utd_hoeyeste_aar, b.snr
              ORDER BY b.utd_hoeyeste_rangering DESC
            ) AS rn
          FROM _years y
          JOIN _base b
            ON b.utd_hoeyeste_dato <= y.cutoff_date
        )
        WHERE rn = 1
    """

    connection.execute(query)
