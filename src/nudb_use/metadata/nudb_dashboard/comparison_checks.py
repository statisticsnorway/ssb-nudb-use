from typing import Set

def compare_unique_values(current_values: Set[str], baseline_values: Set[str]) -> dict:
    """
    Compares the current set of unique values against a baseline set.

    Args:
        current_values: The set of unique values from the current data.
        baseline_values: The set of unique values from the baseline data.

    Returns:
        A dictionary with 'new_values' and 'missing_values'.
    """
    new_values = current_values - baseline_values
    missing_values = baseline_values - current_values

    return {
        "new_values": list(new_values),
        "missing_values": list(missing_values)
    }
