from pathlib import Path

import pytest

from nudb_use.paths import paths_validate


class DummyResult:
    def __init__(
        self,
        file_path: str,
        success: bool,
        violations: list[str] | None = None,
        messages: list[str] | None = None,
    ) -> None:
        self.file_path = file_path
        self.success = success
        self._violations = violations or []
        self._messages = messages or []

    def to_dict(self) -> dict[str, list[str]]:
        return {"violations": self._violations, "messages": self._messages}


class DummyReport:
    def __init__(self, results: list[DummyResult]) -> None:
        self.validation_results = results
        self.num_failures = sum(1 for result in results if not result.success)


def test_validate_path_returns_true_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    report = DummyReport([DummyResult("/tmp/a.parquet", True)])
    monkeypatch.setattr(paths_validate, "_get_single_report", lambda _: report)

    assert paths_validate.validate_path("/tmp/a.parquet") is True


def test_validate_path_returns_false_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = DummyReport(
        [DummyResult("/tmp/a.parquet", False, violations=["bad"], messages=["oops"])]
    )
    monkeypatch.setattr(paths_validate, "_get_single_report", lambda _: report)

    assert paths_validate.validate_path("/tmp/a.parquet") is False


def test_validate_path_raises_on_failure_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = DummyReport(
        [DummyResult("/tmp/a.parquet", False, violations=["bad"], messages=["oops"])]
    )
    monkeypatch.setattr(paths_validate, "_get_single_report", lambda _: report)

    with pytest.raises(ExceptionGroup) as excinfo:
        paths_validate.validate_path("/tmp/a.parquet", raise_errors=True)

    errors = excinfo.value.exceptions
    assert any(isinstance(err, paths_validate.SsbPathValidationError) for err in errors)


def test_validate_paths_returns_false_when_any_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reports = iter(
        [
            DummyReport([DummyResult("/tmp/a.parquet", True)]),
            DummyReport([DummyResult("/tmp/b.parquet", False)]),
        ]
    )
    monkeypatch.setattr(paths_validate, "_get_single_report", lambda _: next(reports))

    assert paths_validate.validate_paths(["/tmp/a.parquet", "/tmp/b.parquet"]) is False


def test_validate_paths_raises_all_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    reports = iter(
        [
            DummyReport([DummyResult("/tmp/a.parquet", False, violations=["v1"])]),
            DummyReport([DummyResult("/tmp/b.parquet", False, violations=["v2"])]),
        ]
    )
    monkeypatch.setattr(paths_validate, "_get_single_report", lambda _: next(reports))

    with pytest.raises(ExceptionGroup) as excinfo:
        paths_validate.validate_paths(
            [
                Path("/tmp/a.parquet"),
                Path("/tmp/b.parquet"),
            ],
            raise_errors=True,
        )

    assert len(excinfo.value.exceptions) == 2
