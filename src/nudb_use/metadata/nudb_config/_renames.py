"""Shared variable rename lookup, including dataset overrides."""

from collections.abc import Mapping
from typing import Any

from nudb_use.nudb_logger import logger

RenameCandidates = dict[str, dict[str, list[str]]]
DATASET_PRIORITY = ("avslutta", "igang", "eksamen")


def _dataset_family(name: str) -> str:
    return next(
        (
            family
            for family in DATASET_PRIORITY
            if name == family or name.startswith(f"{family}_")
        ),
        name,
    )


def _datasets_for_renames(
    datasets: Mapping[str, Any], dataset_name: str | None
) -> Mapping[str, Any]:
    if not dataset_name:
        return datasets
    family = _dataset_family(dataset_name)
    selected = {family: datasets[family]} if family in datasets else {}
    if dataset_name in datasets:
        selected[dataset_name] = datasets[dataset_name]
    elif not selected:
        raise KeyError(
            f"Unknown dataset {dataset_name!r}; no dataset or family rename configuration found."
        )
    return selected


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

    # Family rules are applied first; exact dataset rules replace matching aliases.
    selected = _datasets_for_renames(datasets, dataset_name)
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


def _source_priority(source: str) -> int:
    if source.startswith("dataset "):
        family = _dataset_family(source[8:].strip("'\""))
        if family in DATASET_PRIORITY:
            return DATASET_PRIORITY.index(family)
    return len(DATASET_PRIORITY)


def _resolve_rename(name: str, candidates: RenameCandidates) -> str | None:
    matches = candidates.get(name.lower(), {})
    if len(matches) > 1:
        ranks = {
            target: min(
                (_source_priority(source) for source in sources),
                default=len(DATASET_PRIORITY),
            )
            for target, sources in matches.items()
        }
        preferred = {
            target: matches[target]
            for target, rank in ranks.items()
            if rank == min(ranks.values())
        }
        if len(preferred) == 1:
            target = next(iter(preferred))
            logger.warning(
                f"Resolving {name!r} to {target!r} using dataset priority "
                f"avslutta > igang > eksamen. Available mappings: {matches}"
            )
            return target
        matches = preferred
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
