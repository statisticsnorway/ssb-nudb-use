from typing import Literal

import pandas as pd

from nudb_use.nudb_logger import logger

TEMP_DERIVE_RENAME_POSTFIX = "__temp_derive_rename__"
TempColRenameState = dict[str, dict[str, str]]


def fillna_by_priority(
    newvals: pd.Series | None,
    oldvals: pd.Series | None,
    priority: Literal["old", "new"] = "old",
) -> pd.Series | None:
    """Fill missing values in prioritized order when a column already exists.

    Args:
        newvals: A pandas series with the newly added values.
        oldvals: A pandas series with the old values.
        priority: "old" if we should prioritze the old values, "new" if we should prioritize the new.

    Returns:
        pd.Series | None: The resulting merged columns using fillna-methods. Returns None if both newvals and oldvals is None.

    Raises:
        ValueError: If you are sending in a non-specific Literal for the priority-arg.
    """
    if newvals is None:
        logger.info("Newvals is None, just returning oldvals.")
        return oldvals
    if oldvals is None:
        logger.info("Oldvals is None, just returning newvals.")
        return newvals

    if priority not in ["new", "old"]:
        raise ValueError("priority must be either 'old' or 'new'!")

    def perc_changed(
        first_col: pd.Series,
        second_col: pd.Series,
        priority: Literal["old", "new"] = "old",
    ) -> None:
        ok = first_col.notna()
        if ok.sum():
            nchanged = (second_col[ok] != first_col[ok]).sum()
            pchanged = 100 * nchanged / ok.sum() if ok.sum() else 0.0
            logger.info(
                f"{nchanged} ({pchanged:.2f}%) rows with different values were discarded when combining new (derived) values with priority `{priority}`."
            )
        else:
            logger.info(
                "No existing values in the second column, so nothing was overwritten."
            )

    if priority == "old":
        logger.info("Filling missing values in existing variable...")
        out = oldvals.fillna(newvals)
        perc_changed(newvals, out)
        return out

    logger.info("Filling missing values in derived variable with existing ones...")
    out = newvals.fillna(oldvals)
    perc_changed(oldvals, out)

    return out


def swap_temp_colnames_to_temp(
    df: pd.DataFrame,
    derived_from: list[str],
    temp_col_renames: dict[str, str] | None,
) -> tuple[pd.DataFrame, TempColRenameState]:
    """Temporarily rename input columns to the prerequisite names used by derive functions.

    Args:
        df: Dataframe that may contain columns to rename before derivation.
        derived_from: Prerequisite column names expected by the derive function.
        temp_col_renames: Mapping from existing column names in `df` to the
            prerequisite names the derive function expects.

    Returns:
        tuple[pd.DataFrame, TempColRenameState]: Dataframe with relevant columns
        renamed for derivation, together with the applied rename state needed
        to restore the original names.
    """
    rename_state: dict[str, dict[str, str]] = {
        "renamed_sources": {},
        "backed_up_targets": {},
    }

    if not temp_col_renames:
        return df, rename_state

    temp_col_renames_copy = temp_col_renames.copy()
    relevant_renames = {
        source: target
        for source, target in temp_col_renames_copy.items()
        if target in derived_from
    }
    if not relevant_renames:
        logger.info(
            "Found no relevant renames in overlap between derived_from and temp_col_renames targets."
        )
        return df, rename_state

    missing_sources = set(relevant_renames) - set(df.columns)
    missing_relevant_sources = [
        source for source in relevant_renames if source in missing_sources
    ]
    if missing_relevant_sources:
        logger.warning(
            "Unable to temporarily rename missing source columns: "
            f"{missing_relevant_sources}. Skipping those mappings."
        )
        relevant_renames = {
            source: target
            for source, target in relevant_renames.items()
            if source not in missing_relevant_sources
        }

    if not relevant_renames:
        return df, rename_state

    rename_targets = list(relevant_renames.values())
    duplicate_targets = {
        target for target in rename_targets if rename_targets.count(target) > 1
    }
    if duplicate_targets:
        skipped_sources = [
            source
            for source, target in relevant_renames.items()
            if target in duplicate_targets
        ]
        logger.warning(
            "Multiple source columns map to the same temporary target. "
            f"Skipping conflicting mappings for targets {sorted(duplicate_targets)} "
            f"from sources {skipped_sources}."
        )
        relevant_renames = {
            source: target
            for source, target in relevant_renames.items()
            if target not in duplicate_targets
        }

    occupied_targets = {
        target for target in relevant_renames.values() if target in df.columns
    }
    backup_renames = {
        target: f"{target}{TEMP_DERIVE_RENAME_POSTFIX}" for target in occupied_targets
    }

    backup_collisions = set(backup_renames.values()) & set(df.columns)
    if backup_collisions:
        blocked_targets = {
            target
            for target, backup in backup_renames.items()
            if backup in backup_collisions
        }
        logger.warning(
            "Temporary backup columns already exist in dataframe: "
            f"{sorted(backup_collisions)}. Skipping mappings that need those backups."
        )
        relevant_renames = {
            source: target
            for source, target in relevant_renames.items()
            if target not in blocked_targets
        }
        backup_renames = {
            target: backup
            for target, backup in backup_renames.items()
            if target not in blocked_targets
        }

    if not relevant_renames:
        return df, rename_state

    if backup_renames:
        df = df.rename(columns=backup_renames)
        rename_state["backed_up_targets"] = backup_renames.copy()

    df = df.rename(columns=relevant_renames)
    rename_state["renamed_sources"] = relevant_renames.copy()

    return df, rename_state


def swap_temp_colnames_from_temp(
    df: pd.DataFrame,
    rename_state: TempColRenameState | None,
) -> pd.DataFrame:
    """Restore original column names after temporary derive-time renaming.

    Args:
        df: Dataframe whose columns should be restored after derivation.
        rename_state: Rename operations that were actually applied in
            `swap_temp_colnames_to_temp`.

    Returns:
        pd.DataFrame: Dataframe with original source names restored.
    """
    if not rename_state:
        return df

    renamed_sources = rename_state.get("renamed_sources", {})
    backed_up_targets = rename_state.get("backed_up_targets", {})

    reverse_renames = {
        target: source
        for source, target in renamed_sources.items()
        if target in df.columns
    }
    if reverse_renames:
        df = df.rename(columns=reverse_renames)

    restore_backups = {
        backup: target
        for target, backup in backed_up_targets.items()
        if backup in df.columns
    }
    if restore_backups:
        df = df.rename(columns=restore_backups)

    return df
