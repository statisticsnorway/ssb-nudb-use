"""Shared variable rename lookup, including dataset overrides."""

from collections.abc import Mapping
from typing import Any

RenameCandidates = dict[str, dict[str, list[str]]]


def _value(source: Any, key: str) -> Any:
    return (
        source.get(key) if isinstance(source, Mapping) else getattr(source, key, None)
    )


def _rename_candidates(
    variables: Mapping[str, Any],
    datasets: Mapping[str, Any],
    dataset_name: str | None = None,
) -> RenameCandidates:
    candidates: RenameCandidates = {}
    for name, variable in variables.items():
        aliases = _value(variable, "renamed_from") or []
        if isinstance(aliases, str):
            aliases = [aliases]
        if not isinstance(aliases, list | tuple):
            continue
        for alias in aliases:
            candidates.setdefault(str(alias).lower(), {}).setdefault(name, []).append(
                "variables.renamed_from"
            )

    selected = {dataset_name: datasets[dataset_name]} if dataset_name else datasets
    for dataset, metadata in selected.items():
        for alias, target in (
            _value(metadata, "dataset_specific_renames") or {}
        ).items():
            key = str(alias).lower()
            if dataset_name:
                candidates[key] = {}
            candidates.setdefault(key, {}).setdefault(str(target), []).append(
                f"dataset {dataset!r}"
            )
    return candidates


def _resolve_rename(name: str, candidates: RenameCandidates) -> str | None:
    matches = candidates.get(name.lower(), {})
    if len(matches) > 1:
        details = "; ".join(
            f"{target!r} ({', '.join(sources)})"
            for target, sources in sorted(matches.items())
        )
        raise ValueError(
            f"Ambiguous variable name {name!r}: multiple rename mappings: {details}. "
            "Specify dataset_name or use a current variable name."
        )
    return next(iter(matches), None)
