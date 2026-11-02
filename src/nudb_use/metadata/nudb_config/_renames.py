"""Shared variable rename lookup, including dataset overrides."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from nudb_use.nudb_logger import logger

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


def _dataset_tier(name: str) -> int:
    family = _dataset_family(name)
    return DATASET_PRIORITY.index(family) + 2 if family in DATASET_PRIORITY else 5


@dataclass(frozen=True)
class RenameSource:
    """Temporary lookup context derived from settings, without modifying settings."""

    dataset_name: str | None = None
    position: int = 0

    @property
    def rank(self) -> tuple[int, int]:
        """Compute the source's tier and position within that tier."""
        return (
            (1, 0)
            if self.dataset_name is None
            else (_dataset_tier(self.dataset_name), self.position)
        )

    @property
    def label(self) -> str:
        """Describe the source for messages and public metadata."""
        return (
            "variables.renamed_from"
            if self.dataset_name is None
            else f"dataset {self.dataset_name!r}"
        )


RenameMatches = dict[str, list[RenameSource]]
RenameCandidates = dict[str, RenameMatches]


def _describe_renames(matches: RenameMatches) -> dict[str, list[str]]:
    return {
        target: [source.label for source in sources]
        for target, sources in matches.items()
    }


def _preferred_matches(matches: RenameMatches) -> RenameMatches:
    ranks = {
        target: min(source.rank for source in sources)
        for target, sources in matches.items()
    }
    if not ranks:
        return {}
    best_rank = min(ranks.values())
    return {
        target: matches[target] for target, rank in ranks.items() if rank == best_rank
    }


def _has_context_override(matches: RenameMatches, dataset_name: str | None) -> bool:
    if not dataset_name:
        return False
    family = _dataset_family(dataset_name)
    return any(
        source.dataset_name is not None
        and _dataset_family(source.dataset_name) == family
        for sources in matches.values()
        for source in sources
    )


def _scope_matches(matches: RenameMatches, dataset_name: str) -> RenameMatches:
    exact = {
        target: [source for source in sources if source.dataset_name == dataset_name]
        for target, sources in matches.items()
    }
    exact = {target: sources for target, sources in exact.items() if sources}
    if exact:
        return exact
    family = _dataset_family(dataset_name)
    related = {
        target: [
            source
            for source in sources
            if source.dataset_name is not None
            and _dataset_family(source.dataset_name) == family
        ]
        for target, sources in matches.items()
    }
    related = {target: sources for target, sources in related.items() if sources}
    if related:
        return _preferred_matches(related)
    general = {
        target: [source for source in sources if source.dataset_name is None]
        for target, sources in matches.items()
    }
    general = {target: sources for target, sources in general.items() if sources}
    return general or matches


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
                RenameSource()
            )

    # Positions are rebuilt on every lookup from the loaded config's insertion order.
    positions = {2: 1, 3: 1, 4: 1, 5: 0}
    for dataset, metadata in datasets.items():
        tier = _dataset_tier(dataset)
        position = 0 if dataset in DATASET_PRIORITY else positions[tier]
        if dataset not in DATASET_PRIORITY:
            positions[tier] += 1
        for alias, target in (
            _value(metadata, "dataset_specific_renames") or {}
        ).items():
            key = str(alias).lower()
            candidates.setdefault(key, {}).setdefault(str(target), []).append(
                RenameSource(dataset, position)
            )
    if dataset_name:
        family = _dataset_family(dataset_name)
        if dataset_name not in datasets and not any(
            _dataset_family(name) == family for name in datasets
        ):
            raise KeyError(
                f"Unknown dataset {dataset_name!r}; no dataset or family rename configuration found."
            )
        candidates = {
            alias: _scope_matches(matches, dataset_name)
            for alias, matches in candidates.items()
        }
    return candidates


def _resolve_rename(name: str, candidates: RenameCandidates) -> str | None:
    matches = candidates.get(name.lower(), {})
    if len(matches) > 1:
        preferred = _preferred_matches(matches)
        if len(preferred) == 1:
            target = next(iter(preferred))
            logger.warning(
                f"Resolving {name!r} to {target!r} at rank {min(source.rank for source in preferred[target])}. "
                f"Priority: variable rename > avslutta > igang > eksamen > other datasets; "
                f"base datasets precede their children, then config order decides. Available mappings: {_describe_renames(matches)}"
            )
            return target
        matches = preferred
    if len(matches) > 1:
        details = "; ".join(
            f"{target!r} ({', '.join(source.label for source in sources)})"
            for target, sources in sorted(matches.items())
        )
        raise ValueError(
            f"Ambiguous variable name {name!r}: multiple rename mappings: {details}. "
            "Specify dataset_name or use a current variable name."
        )
    return next(iter(matches), None)
