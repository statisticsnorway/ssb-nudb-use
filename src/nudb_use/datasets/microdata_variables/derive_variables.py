from collections.abc import Callable
from fnmatch import fnmatchcase

import duckdb as db
from nudb_config import settings

from nudb_use.datasets.microdata_variables.first_class_variables import (
    _get_available_variables,
)
from nudb_use.datasets.microdata_variables.first_class_variables import (
    _nonmissing_measure_filter,
)
from nudb_use.datasets.microdata_variables.identifiers import _can_use_original_fnr
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
        from nudb_use.datasets.nudb_data import NudbData
        from nudb_use.variables import derive

        metadata = settings.variables[variable]
        need = metadata.derived_from

        if not need:
            msg = f"'{variable}' doesn't seem to be derived from anything. This is probably a bug!"
            logger.error(msg)
            raise ValueError(msg)

        person_level = (
            metadata.derived_join_keys == ["snr"]
            if metadata.derived_join_keys is not None
            else need == ["snr"]
        )
        milestone_date = (
            person_level
            and metadata.dtype == "DATETIME"
            and (
                fnmatchcase(variable, "*fullfoert_dato")
                or fnmatchcase(variable, "*registrert*_dato")
            )
        )

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
            # The private SNR input retains original-FNR rows with missing SNR;
            # exported MicroData measures themselves never contain null values.
            mdat = NudbData("_utd_hendelse_snr") if dvar == "snr" else MicroData(dvar)
            source = mdat.alias
            if "utd_hendelse_id" not in mdat.get_available_cols():
                # Person dependencies have FNR as their key; recover event rows
                # by matching the person and the observation's STATUS date.
                events = NudbData("_utd_hendelse_snr")
                identifiers = NudbData("_utd_hendelse_idents")
                source = f"""(
                    SELECT events.utd_hendelse_id, person.{dvar},
                        events.utd_hendelse_dato_start, events.utd_hendelse_dato_slutt
                    FROM {events.alias} AS events
                    LEFT JOIN {identifiers.alias} AS identifiers USING (utd_hendelse_id)
                    LEFT JOIN {mdat.alias} AS person
                    ON person.fnr = COALESCE(identifiers.mapped_fnr, identifiers.original_fnr)
                    AND person.utd_hendelse_dato_start IS NOT DISTINCT FROM events.utd_hendelse_dato_start
                    AND person.utd_hendelse_dato_slutt IS NOT DISTINCT FROM events.utd_hendelse_dato_slutt
                )"""
            aliases.append(source)

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
                    f"(SELECT {get_select(i)} FROM {source}) {get_using(i)}"
                    for i, source in enumerate(aliases)
                ]
            )
            + "\n)"
        )

        logger.debug(f"Base query:\n{basequery}")

        # derive
        input_df = connection.sql(basequery).df()
        input_event_ids = input_df["utd_hendelse_id"].copy()
        df = getattr(derive, variable)(input_df)
        connection.register("tmp_df_basequery", df)

        if df.utd_hendelse_id.isna().any():
            logger.warning(
                "Found missing values in `utd_hendelse_id` column. This should not be possible, and is likely a bug!"
            )

        ident_columns = "observations.utd_hendelse_id"
        # Filter while DuckDB scans the derived dataframe, before identifier
        # joins, DISTINCT, conflict checks, and materializing the final table.
        source = (
            f"(SELECT * FROM tmp_df_basequery WHERE {_nonmissing_measure_filter(variable)}) "
            "AS observations"
        )
        if person_level:
            use_original = _can_use_original_fnr(variable) and df[
                "utd_hendelse_id"
            ].equals(input_event_ids)
            identifiers = NudbData("_utd_hendelse_idents")
            fnr = "identifiers.mapped_fnr"
            if use_original:
                fnr = f"COALESCE({fnr}, identifiers.original_fnr)"
            else:
                logger.warning(
                    f"MicroData '{variable}': original FNR fallback is disabled because "
                    "the derive logic or its dependencies use joins/lookups across rows, "
                    "or did not preserve the input event rows. Only keeping FNRs that "
                    "map back from SNR through snrkat.fnr_naa."
                )
            ident_columns = f"{fnr} AS fnr"
            source += (
                f" LEFT JOIN {identifiers.alias} AS identifiers USING (utd_hendelse_id)"
            )
            if not use_original:
                source += " WHERE identifiers.mapped_fnr IS NOT NULL"

        start_column = (
            f"observations.{variable}"
            if milestone_date
            else "observations.utd_hendelse_dato_start"
        )
        stop_column = (
            f"observations.{variable}"
            if milestone_date
            else "observations.utd_hendelse_dato_slutt"
        )
        query = f"""
            SELECT {"DISTINCT" if person_level else ""}
                {ident_columns},
                observations.{variable},
                {start_column} AS utd_hendelse_dato_start,
                {stop_column} AS utd_hendelse_dato_slutt
            FROM
                {source}
        """

        if person_level:
            # Identical STATUS observations may come from different events.
            # After DISTINCT, multiple rows per person/date are conflicts.
            conflicts = connection.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT fnr, utd_hendelse_dato_start
                    FROM ({query}) AS observations
                    WHERE fnr IS NOT NULL
                    GROUP BY fnr, utd_hendelse_dato_start
                    HAVING COUNT(*) > 1
                ) AS conflicts
                """).fetchone()
            if conflicts is not None and conflicts[0]:
                raise ValueError(
                    f"Conflicting STATUS observations for '{variable}': "
                    f"{conflicts[0]} (fnr, start date) groups have multiple values "
                    "or stop dates. Identical rows were deduplicated; "
                    "no winning value was selected and no dataset was created."
                )

        connection.execute(f"CREATE OR REPLACE TABLE {alias} AS {query}")

    return _generate_variable_view
