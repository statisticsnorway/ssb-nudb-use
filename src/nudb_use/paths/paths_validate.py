import asyncio
from pathlib import Path

from dapla_metadata.standards import check_naming_standard
from dapla_metadata.standards import generate_validation_report
from dapla_metadata.standards.name_validator import NamingStandardReport

from nudb_use.exceptions.groups import raise_exception_group


class SsbPathValidationError(Exception):
    """If there is something wrong with a file path according to SSB naming standard."""

    pass


def _get_single_report(path: str | Path) -> NamingStandardReport:
    results = asyncio.run(check_naming_standard(path))
    return generate_validation_report(results)


def _make_errors_list(report: NamingStandardReport) -> list[SsbPathValidationError]:
    return [
        SsbPathValidationError(
            f"{result.file_path}: {result.to_dict()['violations']} - {result.to_dict()['messages']}"
        )
        for result in report.validation_results
        if result.success is False
    ]


def validate_path(path: str | Path, raise_errors: bool = False) -> bool:
    """Validate a single path according to the naming standard from SSB.

    Args:
        path: A single path to a file.
        raise_errors: If you want the function to raise errors instead of returning a bool.

    Returns:
        bool: If we are not raising errors: the function returns True if no errors where found,
            False if we found errors.s
    """
    report = _get_single_report(path)

    # Constructing errors is hard work, so lets leave bool validations as early exists
    if report.num_failures >= 1 and not raise_errors:
        return False
    elif report.num_failures == 0:
        return True

    # There should be errors here, because the num_failures is not 0
    raise_exception_group(_make_errors_list(report))
    return False


def validate_paths(paths: list[str | Path], raise_errors: bool = False) -> bool:
    """Validate a list of paths, accoring to the naming standard of SSB.

    Args:
        paths: a list of paths to files you want to check.
        raise_errors: If you want the function to raise the validation errors it finds as errors.

    Returns:
        bool: If we are not raising errors: the function returns True if no errors where found,
            False if we found at least one error, on one of the files.
    """
    bool_results: list[bool] = []
    errored_reports: list[NamingStandardReport] = []
    for path in paths:
        report = _get_single_report(path)
        if report.num_failures >= 1:
            bool_results.append(False)
            errored_reports.append(report)
        elif report.num_failures == 0:
            bool_results.append(True)

    if not raise_errors:
        return all(bool_results)

    errors = [
        x for y in [_make_errors_list(report) for report in errored_reports] for x in y
    ]
    raise_exception_group(errors)
    return False
