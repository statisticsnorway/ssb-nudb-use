from nudb_config import settings


def datasetname_teams_buckets(
    teams_include: list[str] | None = None,
    exclude_nudbs_indata: bool = True
) -> dict[str, dict[str, str]]:
    """Get the dataset names, teams and buckets you should get permissions for, that nudb uses.

    Args:
        teams_include: Team names that we should include from, if left as None, all teams are included.
        exclude_nudbs_indata: Exclude data from the education teams that deliver data to NUDB,
            set to False if you want to include them.

    Returns:
        dict[str, dict[str, str]]: Keys are dataset name, team and bucketname in the nested dict.
    """
    result: dict[str, dict[str, str]] = {
        dataset_name: {
            "team": dataset_values.team,
            "bucket": dataset_values.bucket,
        }
        for dataset_name, dataset_values in settings.datasets.items()
        if (not teams_include or dataset_values.team in teams_include)
        and (
            not exclude_nudbs_indata
            or dataset_values.team not in ["utd-uhfagskole", "utd-vg", "utd-bhgskole"]
        )
    }

    return result


def unique_teams_buckets(
    teams_include: list[str] | None = None, exclude_nudbs_indata: bool = True
) -> set[tuple[str, str]]:
    """Get the teams and buckets you should get permissions for, that nudb uses.

    Args:
        teams_include: Team names that we should include from, if left as None, all teams are included.
        exclude_nudbs_indata: Exclude data from the education teams that deliver data to NUDB,
            set to False if you want to include them.

    Returns:
        set[tuple[str, str]]: A set of unique teams and buckets that ssb-nudb-config is set up to use.
    """
    result: set[tuple[str, str]] = {
        (
            dataset_values.team,
            dataset_values.bucket,
        )
        for dataset_values in settings.datasets.values()
        if (not teams_include or dataset_values.team in teams_include)
        and (
            not exclude_nudbs_indata
            or dataset_values.team not in ["utd-uhfagskole", "utd-vg", "utd-bhgskole"]
        )
    }

    return result
