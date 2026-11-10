from collections.abc import Callable

import duckdb as db
from nudb_config import settings

from nudb_use.nudb_logger import logger

# Dates are also needed as values when deriving other MicroData variables.
EXCLUDE_VARS = ["utd_hendelse_id"]

# Get all first class variable. I.e., variables which aren't
# derived, but are stored on the data
FIRST_CLASS_VARIABLES = [
    variable
    for variable in settings.variables.keys()
    if (
        not settings.variables[variable].derived_from  # not a derived variable
        and variable not in EXCLUDE_VARS  # not to be excluded
        and settings.variables[variable].unit != "utdatert"  # not an outdated variable
    )
]

# all first class variables might not be available in the database
# this then has to be checked at run time
AVAILABLE_VARIABLES: list[str] | None = None  # this has to be checked at runtime


def _nonmissing_measure_filter(variable: str) -> str:
    # Pandas missing values scan as SQL NULL. Parquet/SQL floating NaN may
    # remain a non-null NaN; do not mistake the literal string "NaN" for NA.
    return f"""{variable} IS NOT NULL AND CASE
        WHEN typeof({variable}) IN ('FLOAT', 'DOUBLE')
        THEN NOT isnan(TRY_CAST({variable} AS DOUBLE))
        ELSE TRUE END"""


def _get_available_variables() -> list[str]:
    global AVAILABLE_VARIABLES

    # if we've already run the function, we just return the result
    if AVAILABLE_VARIABLES is not None:
        return AVAILABLE_VARIABLES

    from nudb_use.datasets import NudbData  # avoid circular import

    # cache and return
    AVAILABLE_VARIABLES = list(
        set(
            NudbData("igang").get_available_cols()
            + NudbData("avslutta").get_available_cols()
            + NudbData("eksamen").get_available_cols()
        )
    )

    return AVAILABLE_VARIABLES


def _get_generator_first_class_variable(
    variable: str,
    keep_original_fnr: bool = False,
) -> Callable[[str, db.DuckDBPyConnection], None]:
    logger.debug(f"Creating generator for {variable}")

    def _generate_variable_view(
        alias: str,
        connection: db.DuckDBPyConnection,
    ) -> None:

        from nudb_use.datasets import NudbData  # avoid cirular import

        logger.info(f"Creating MicroData dataset for {variable}...")

        if variable != "fnr" and variable not in _get_available_variables():
            msg = f"{variable} is not available in either IGANG, AVSLUTTA or EKSAMEN!"
            logger.error(msg)
            raise ValueError(msg)

        igang = NudbData("igang")
        avslutta = NudbData("avslutta")
        eksamen = NudbData("eksamen")

        def value_filter(dataset: NudbData) -> str:
            # Keep original-FNR observations in the SNR input for simple derives.
            if (
                keep_original_fnr
                and variable == "snr"
                and "fnr" in dataset.get_available_cols()
            ):
                return "(snr IS NOT NULL OR fnr IS NOT NULL)"
            if variable == "fnr":
                columns = dataset.get_available_cols()
                conditions = [
                    f"{column} IS NOT NULL"
                    for column in ("fnr", "snr")
                    if column in columns
                ]
                return "(" + (" OR ".join(conditions) or "FALSE") + ")"
            return _nonmissing_measure_filter(variable)

        def value_select(dataset: NudbData) -> str:
            if variable == "fnr" and "fnr" not in dataset.get_available_cols():
                return "NULL::VARCHAR AS fnr"
            return variable

        igang_query = f"""
            SELECT
                utd_hendelse_id,
                {value_select(igang)},
                make_date(CAST(utd_skoleaar_start AS INTEGER), 10, 1) AS utd_hendelse_dato_start, /* The best we've got for IGANG */
                make_date(CAST(utd_skoleaar_start AS INTEGER), 10, 1) AS utd_hendelse_dato_slutt
            FROM
                {igang.alias}
            WHERE
                {value_filter(igang)}
        """

        avslutta_query = f"""
            SELECT
                utd_hendelse_id,
                {value_select(avslutta)},
                utd_aktivitet_slutt AS utd_hendelse_dato_start,
                utd_aktivitet_slutt AS utd_hendelse_dato_slutt
            FROM
                {avslutta.alias}
            WHERE
                {value_filter(avslutta)} AND
                utd_aktivitet_slutt IS NOT NULL
        """

        eksamen_query = f"""
            SELECT
                utd_hendelse_id,
                {value_select(eksamen)},
                uh_eksamen_dato AS utd_hendelse_dato_start,
                uh_eksamen_dato AS utd_hendelse_dato_slutt
            FROM
                {eksamen.alias}
            WHERE
                {value_filter(eksamen)} AND
                uh_eksamen_dato IS NOT NULL
        """

        query_list = []
        for dataset, dataset_query in (
            (igang, igang_query),
            (avslutta, avslutta_query),
            (eksamen, eksamen_query),
        ):
            columns = dataset.get_available_cols()
            if variable in columns or (variable == "fnr" and "snr" in columns):
                query_list.append(dataset_query)

        values = " UNION ALL BY NAME ".join(query_list)
        query = f"CREATE OR REPLACE VIEW {alias} AS {values}"
        if variable == "fnr":
            identifiers = NudbData("_utd_hendelse_idents")
            query = f"""
                CREATE OR REPLACE VIEW {alias} AS
                SELECT
                    observations.utd_hendelse_id,
                    COALESCE(identifiers.mapped_fnr, identifiers.original_fnr) AS fnr,
                    observations.utd_hendelse_dato_start,
                    observations.utd_hendelse_dato_slutt
                FROM ({values}) AS observations
                LEFT JOIN {identifiers.alias} AS identifiers USING (utd_hendelse_id)
                WHERE COALESCE(identifiers.mapped_fnr, identifiers.original_fnr) IS NOT NULL
            """

        logger.debug(f"QUERY:\n{query}")

        connection.execute(query)

    return _generate_variable_view


def _generate_microdata_variable_rectype_view(
    alias: str,
    connection: db.DuckDBPyConnection,
) -> None:

    from nudb_use.datasets import NudbData  # avoid cirular import

    logger.info("Creating MicroData dataset for rectype...")

    igang = NudbData("igang")
    avslutta = NudbData("avslutta")
    eksamen = NudbData("eksamen")

    query = f"""
            CREATE OR REPLACE VIEW
                {alias} AS
            SELECT
                utd_hendelse_id, 'igang' AS utd_record_type,
                NULL AS utd_hendelse_dato_start,
                NULL AS utd_hendelse_dato_slutt
            FROM {igang.alias}

            UNION ALL BY NAME

            SELECT
                utd_hendelse_id, 'avslutta' AS utd_record_type,
                NULL AS utd_hendelse_dato_start,
                NULL AS utd_hendelse_dato_slutt
            FROM {avslutta.alias}

            UNION ALL BY NAME

            SELECT
                utd_hendelse_id, 'eksamen' AS utd_record_type,
                NULL AS utd_hendelse_dato_start,
                NULL AS utd_hendelse_dato_slutt
            FROM {eksamen.alias}
        """

    connection.execute(query)
