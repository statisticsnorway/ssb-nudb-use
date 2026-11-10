from collections.abc import Callable

import duckdb as db
from nudb_config import settings

from nudb_use.datasets.microdata_variables.first_class_variables import (
    _get_available_variables,
)
from nudb_use.nudb_logger import logger

EXCLUDE_VARS = ["utd_hendelse_id"]

# Get all derive variables
DERIVE_VARIABLES = [
    variable
    for variable in settings.variables.keys()
    if (
        settings.variables[variable].derived_from  # is a derived variable
        and variable not in EXCLUDE_VARS  # not to be excluded
        and settings.variables[variable].unit != "utdatert"  # not an outdated variable
    )
]


def _get_generator_derive_variable(
    variable: str,
) -> Callable[[str, db.DuckDBPyConnection], None]:
    logger.debug(f"Creating generator for {variable}")

    def _generate_variable_view(
        alias: str,
        connection: db.DuckDBPyConnection,
    ) -> None:

        from nudb_use.datasets.microdata import MicroData  # avoid circular import
        from nudb_use.variables import derive

        need = settings.variables[variable].derived_from

        if not need:
            msg = f"'{variable}' doesn't seem to be derived from anything. This is probably a bug!"
            logger.error(msg)
            raise ValueError(msg)

        # after we know that need is not None
        missing = list(
            set(need) - set(_get_available_variables()) - set(DERIVE_VARIABLES)
        )

        if missing:
            msg = f"'{variable}' seems to be derived from variables which aren't available! Unavailable variables: {missing}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info(f"Creating MicroData dataset for {variable}...")

        # This is likely not the most efficient way of doing this, but it's clean...
        # Also, I (Kjell) suspect the derive.<variable>() call is going to be more
        # expensive anyways...

        aliases = []
        for dvar in need:
            mdat = MicroData(
                dvar
            )  # the call to MicroData() doesn't only retrieve the alias, but also initializes the dataset (if needed)
            aliases.append(mdat.alias)

        # we should have no missing values in the keys, and all keys should be present in all microdata variable datasets.
        # so INNER, LEFT, RIGHT and FULL JOIN should all yield the same results. Here we use a FULL JOIN just in case...
        join_type = "FULL"

        def get_select(i: int) -> str:
            return f"utd_hendelse_id, {need[i]}" + (
                ", utd_hendelse_dato_start, utd_hendelse_dato_slutt" if i == 0 else ""
            )

        def get_using(i: int) -> str:
            return "USING (utd_hendelse_id)" if i > 0 else ""

        basequery = (
            "SELECT * FROM (\n"
            + f"\n{join_type} JOIN\n".join(
                [
                    f"(SELECT {get_select(i)} FROM {alias} AS {alias}) {get_using(i)}"
                    for i, alias in enumerate(aliases)
                ]
            )
            + "\n)"
        )

        logger.debug(f"Base query:\n{basequery}")

        # derive
        df = getattr(derive, variable)(connection.sql(basequery).df())
        connection.register("tmp_df_basequery", df)

        if df.utd_hendelse_id.isna().any():
            logger.warning(
                "Found missing values in `utd_hendelse_id` column. This should not be possible, and is likely a bug!"
            )

        ident_column = "utd_hendelse_id"
        source = "tmp_df_basequery"
        if need == ["snr"]:
            from nudb_use.datasets.nudb_data import NudbData

            snrkat = NudbData("_snrkat_snr2fnr")
            ident_column = "fnr"
            source += f" LEFT JOIN {snrkat.alias} USING (snr)"

        query = f"""
            SELECT {"DISTINCT" if need == ["snr"] else ""}
                {ident_column},
                {variable},
                utd_hendelse_dato_start,
                utd_hendelse_dato_slutt
            FROM
                {source}
        """

        if need == ["snr"]:
            # Identical STATUS observations may come from different events.
            # After DISTINCT, multiple rows per person/date are conflicts.
            conflicts = connection.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT fnr, utd_hendelse_dato_start
                    FROM ({query}) AS observations
                    WHERE fnr IS NOT NULL
                    GROUP BY fnr, utd_hendelse_dato_start
                    HAVING COUNT(*) > 1
                ) AS conflicts
                """
            ).fetchone()
            if conflicts is not None and conflicts[0]:
                raise ValueError(
                    f"Conflicting STATUS observations for '{variable}': "
                    f"{conflicts[0]} (fnr, start date) groups have multiple values "
                    "or stop dates. Identical rows were deduplicated; "
                    "no winning value was selected and no dataset was created."
                )

        connection.execute(f"CREATE OR REPLACE TABLE {alias} AS {query}")

    return _generate_variable_view
