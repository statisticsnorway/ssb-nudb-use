import os
import re
import git
import pandas as pd
from datetime import datetime
from importlib import metadata

def get_file_details(file_path: str) -> dict:
    """
    Gathers metadata about a file.

    Args:
        file_path: The path to the file.

    Returns:
        A dictionary containing file metadata.
    """
    file_name = os.path.basename(file_path)
    file_version = re.search(r"(v\d+)", file_name).group(1) if re.search(r"(v\d+)", file_name) else None
    year = re.search(r"(\d{4})", file_name).group(1) if re.search(r"(\d{4})", file_name) else None
    
    df = pd.read_parquet(file_path)
    row_count, column_count = df.shape

    return {
        "file_name": file_name,
        "file_version": file_version,
        "year": int(year) if year else None,
        "creation_date": datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
        "row_count": row_count,
        "column_count": column_count,
    }

def get_environment_details(repo_path: str) -> dict:
    """
    Gathers metadata about the execution environment.

    Args:
        repo_path: The path to the git repository.

    Returns:
        A dictionary containing environment metadata.
    """
    repo = git.Repo(repo_path, search_parent_directories=True)
    
    try:
        branch = repo.active_branch.name
    except TypeError:
        branch = "DETACHED HEAD"
        
    commit_hash = repo.head.object.hexsha
    
    # Construct commit URL from remote URL
    commit_url = None
    try:
        remote_url = repo.remotes.origin.url
        # Clean up URL to create a shareable link
        if "@" in remote_url:
            remote_url = "https://" + remote_url.split("@")[1]
            
        if remote_url.startswith("https://"):
            repo_url = remote_url.removesuffix(".git")
            commit_url = f"{repo_url}/commit/{commit_hash}"
        elif remote_url.startswith("git@"):
            repo_url = remote_url.replace("git@github.com:", "https://github.com/").removesuffix(".git")
            commit_url = f"{repo_url}/commit/{commit_hash}"
    except Exception:
        commit_url = "Could not determine remote URL."

    packages = {dist.name: dist.version for dist in metadata.distributions()}

    return {
        "git": {
            "branch": branch,
            "commit_hash": commit_hash,
            "commit_url": commit_url,
        },
        "python_packages": packages,
    }

def calculate_source_contribution(series: pd.Series) -> dict:
    """Calculates the contribution of each source to the dataset."""
    total_count = len(series)
    source_counts = series.value_counts()
    
    contribution_dict = {}
    for source, count in source_counts.items():
        contribution_dict[source] = {
            "count": count,
            "percentage": round(count / total_count if total_count > 0 else 0, 2),
        }
    return contribution_dict

def generate_file_metadata(file_path: str, repo_path: str) -> dict:
    """
    Generates a dictionary with file and environment metadata.

    Args:
        file_path: The path to the file.
        repo_path: The path to the git repository.

    Returns:
        A dictionary with file and environment metadata.
    """
    return {
        "file_details": get_file_details(file_path),
        "environment": get_environment_details(repo_path),
    }
