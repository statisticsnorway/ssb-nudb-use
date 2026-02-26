import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dapla_metadata.standards import check_naming_standard
from dapla_metadata.standards.name_validator import NamingStandardReport
from dapla_metadata.standards.name_validator import ValidationResult

from nudb_use.exceptions.groups import raise_exception_group
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def generate_validation_report_dont_print(
    validation_results: list[ValidationResult],
) -> NamingStandardReport:
    """Generate a standard validation report.

    This function takes a list of `ValidationResult` objects and creates a
    `NamingStandardReport` instance.

    Args:
        validation_results: A list of ValidationResult objects that
            contain the outcomes of the name standard checks.

    Returns:
        NamingStandardReport: An instance of `NamingStandardReport` containing
            the validation results.
    """
    report = NamingStandardReport(validation_results=validation_results)
    return report


class SsbPathValidationError(Exception):
    """If there is something wrong with a file path according to SSB naming standard."""

    pass


def _get_single_report(path: str | Path) -> NamingStandardReport:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        results = asyncio.run(check_naming_standard(path))
    else:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return generate_validation_report_dont_print(
                ex.submit(asyncio.run, check_naming_standard(path)).result()
            )
    return generate_validation_report_dont_print(
        results,
    )


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
    with LoggerStack(f"Validationg the path {path}"):
        report = _get_single_report(path)

        # Constructing errors is hard work, so lets leave bool validations as early exists
        if report.num_failures >= 1 and not raise_errors:
            logger.info(
                "Found {report.num_failures} naming validation failures: {path}"
            )
            return False
        elif report.num_failures == 0:
            logger.info("0 naming validation failures.")
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
            logger.info("Found {report.num_failures} naming validation failures.")
            errored_reports.append(report)
        elif report.num_failures == 0:
            logger.info("0 naming validation failures.")
            bool_results.append(True)

    if not raise_errors:
        return all(bool_results)

    errors = [
        x for y in [_make_errors_list(report) for report in errored_reports] for x in y
    ]
    raise_exception_group(errors)
    return False
