"""Utilities for manipulating NUDB file paths and metadata locations."""

from pathlib import Path

from fagfunksjoner.paths.versions import next_version_path


def next_path_mkdir(path: str | Path) -> Path:
    """Generate a bumped version path for a new file version, including creating any missing folders.

    Args:
        path: Current path as a string.

    Returns:
        Path: Path to the newly created directory.
    """
    path = Path(next_version_path(str(path)))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def metadatapath_from_path(path: str | Path) -> Path:
    """Generate a path for JSON metadata file corresponding to input file.

    Args:
        path: Path to input file.

    Returns:
        Path: Path to the metadata JSON file for the input file.
    """
    metapath = Path(path)  # Type-narrowing
    metapath = metapath.parent / (metapath.stem + "__DOC.json")
    return metapath
