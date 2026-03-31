import duckdb as db
import klass
import pandas as pd


def _generate_nuskat_table(alias: str, connection: db.DuckDBPyConnection) -> None:
    from nudb_use.variables.derive import (  # type: ignore[attr-defined]
        uh_gruppering_nus,
    )
    from nudb_use.variables.derive import utd_klassetrinn_hoey_nus
    from nudb_use.variables.derive import utd_klassetrinn_lav_nus

    nusklass = klass.KlassClassification(36).get_codes().data
    _nuskat = (
        pd.DataFrame({"nus2000": nusklass["code"], "nus2000_label": nusklass["name"]})
        .pipe(uh_gruppering_nus)
        .pipe(utd_klassetrinn_lav_nus)
        .pipe(utd_klassetrinn_hoey_nus)
    )

    query = f"""
        CREATE TABLE {alias} AS SELECT DISTINCT * FROM _nuskat
    """

    connection.execute(query)
