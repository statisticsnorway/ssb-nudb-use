import duckdb as db
from nudb_config import settings


def _can_use_original_fnr(variable: str, visiting: set[str] | None = None) -> bool:
    """Conservatively allow fallback through dependencies without joins or lookups."""
    visiting = set() if visiting is None else visiting
    if variable in visiting or variable not in settings.variables:
        return False
    metadata = settings.variables[variable]
    if not metadata.derived_from:
        return True
    if metadata.derived_join_keys or metadata.derived_uses_datasets:
        return False
    return all(
        _can_use_original_fnr(dependency, visiting | {variable})
        for dependency in metadata.derived_from or []
    )


def _generate_utd_hendelse_idents_view(
    alias: str, connection: db.DuckDBPyConnection
) -> None:
    from nudb_use.datasets.nudb_data import NudbData

    queries = []
    for name in ("igang", "avslutta", "eksamen"):
        dataset = NudbData(name)
        columns = dataset.get_available_cols()
        snr = "snr" if "snr" in columns else "NULL::VARCHAR"
        fnr = "fnr" if "fnr" in columns else "NULL::VARCHAR"
        queries.append(
            f"SELECT utd_hendelse_id, {snr} AS snr, {fnr} AS original_fnr "
            f"FROM {dataset.alias}"
        )
    mapping = NudbData("_snrkat_snr2fnr")
    events = " UNION ".join(queries)
    connection.execute(f"""
        CREATE VIEW {alias} AS
        SELECT events.*, mapping.fnr AS mapped_fnr
        FROM ({events}) AS events
        LEFT JOIN {mapping.alias} AS mapping USING (snr)
    """)
