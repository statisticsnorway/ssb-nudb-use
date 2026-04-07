def _on_syntax_from_merge_keys(
    merge_keys: list[str], left: str = "T1", right: str = "T2"
) -> str:
    if not merge_keys:
        raise ValueError(f"zero merge keys provided! {merge_keys}")

    return " AND\n".join(
        [
            f"{left}.{key}={right}.{key}"  # AND {right}.{key} IS NOT NULL AND {left}.{key} IS NOT NULL"
            for key in merge_keys
        ]
    )
