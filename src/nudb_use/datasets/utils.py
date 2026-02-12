def _default_alias_from_name(name: str) -> str:
    return f"NUDB_DATA_{name.upper().replace("-","_")}"
