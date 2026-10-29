from collections.abc import Callable

import duckdb as db
from nudb_config import settings

from nudb_use.nudb_logger import logger

EXCLUDE_VARS = ["utd_hendelse_id", "utd_aktivitet_slutt", "uh_eksamen_dato"]

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
) -> Callable[[str, db.DuckDBPyConnection], None]:
    logger.debug(f"Creating generator for {variable}")

    def _generate_variable_view(
        alias: str,
        connection: db.DuckDBPyConnection,
    ) -> None:

        from nudb_use.datasets import NudbData  # avoid cirular import

        logger.info(f"Creating MicroData dataset for {variable}...")

        if variable not in _get_available_variables():
            msg = f"{variable} is not available in either IGANG, AVSLUTTA or EKSAMEN!"
            logger.error(msg)
            raise ValueError(msg)

        igang = NudbData("igang")
        avslutta = NudbData("avslutta")
        eksamen = NudbData("eksamen")

        igang_query = f"""
            SELECT
                utd_hendelse_id,
                {variable},
                make_date(CAST(utd_skoleaar_start AS INTEGER), 8, 1) AS utd_hendelse_dato_start, /* The best we've got for IGANG */
                NULL AS utd_hendelse_dato_slutt
            FROM
                {igang.alias}
            WHERE
                {variable} IS NOT NULL
        """

        avslutta_query = f"""
            SELECT
                utd_hendelse_id,
                {variable},
                utd_aktivitet_slutt AS utd_hendelse_dato_start,
                NULL AS utd_hendelse_dato_slutt
            FROM
                {avslutta.alias}
            WHERE
                {variable} IS NOT NULL
        """

        eksamen_query = f"""
            SELECT
                utd_hendelse_id,
                {variable},
                uh_eksamen_dato AS utd_hendelse_dato_start,
                NULL AS utd_hendelse_dato_slutt
            FROM
                {eksamen.alias}
            WHERE
                {variable} IS NOT NULL
        """

        query_list = []
        if variable in igang.get_available_cols():
            query_list.append(igang_query)
        if variable in avslutta.get_available_cols():
            query_list.append(avslutta_query)
        if variable in eksamen.get_available_cols():
            query_list.append(eksamen_query)

        query = f"""
            CREATE OR REPLACE VIEW
                {alias} AS
        """ + "UNION ALL BY NAME".join(query_list)

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
