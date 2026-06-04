import duckdb as db

from nudb_use.datasets.nudb_read_parquet import _nudb_read_parquet
from nudb_use.datasets.utils import _default_alias_from_name
from nudb_use.paths.latest import latest_shared_path


def _generate_f_utd_kurs_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("f_utd_kurs")
    if not alias:
        alias = _default_alias_from_name(last_key)

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *
    FROM
        {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)


def _generate_f_utd_demografi_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    last_key, last_path = latest_shared_path("f_utd_demografi")
    if not alias:
        alias = _default_alias_from_name(last_key)

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *
    FROM
        {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)


def _generate_tab_utd_person_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    last_key, last_path = latest_shared_path("tab_utd_person")
    if not alias:
        alias = _default_alias_from_name(last_key)

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *
    FROM
        {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)


def _generate_f_utd_person_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("f_utd_person")
    if not alias:
        alias = _default_alias_from_name(last_key)

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *
    FROM
        {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)


def _generate_tab_alle_snr_view(alias: str, connection: db.DuckDBPyConnection) -> None:
    last_key, last_path = latest_shared_path("tab_alle_snr")
    if not alias:
        alias = _default_alias_from_name(last_key)

    query = f"""
    CREATE VIEW
        {alias} AS
    SELECT
        *
    FROM
        {_nudb_read_parquet(last_path, alias)}
    """

    connection.sql(query)
