import json
import logging
from pathlib import Path

import pytest

from nudb_use import nudb_logger


def test_add_logrecord_raises_when_json_fields_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nudb_logger, "STACK_LABELS", [])
    monkeypatch.setattr(nudb_logger, "ID_COUNTERS", [0])
    monkeypatch.setattr(nudb_logger, "JSON_FIELDS", [])

    record = logging.LogRecord("n", logging.INFO, __file__, 1, "msg", None, None)

    with pytest.raises(RuntimeError):
        nudb_logger.add_log_record_to_json(record)


def test_formatter_validates_indent_width(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(nudb_logger, "INDENT_WIDTH", 0)
    monkeypatch.setattr(nudb_logger, "STACK_LEVEL", 0)
    monkeypatch.setattr(nudb_logger, "STACK_LABELS", ["x"])
    monkeypatch.setattr(nudb_logger, "ID_COUNTERS", [0])
    monkeypatch.setattr(nudb_logger, "JSON", {})
    monkeypatch.setattr(nudb_logger, "JSON_FIELDS", [nudb_logger.JSON])

    record = logging.LogRecord("n", logging.INFO, __file__, 1, "msg", None, None)

    with pytest.raises(ValueError):
        nudb_logger.formatter.format(record)


def test_formatter_validates_stack_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(nudb_logger, "INDENT_WIDTH", 2)
    monkeypatch.setattr(nudb_logger, "STACK_LEVEL", -1)
    monkeypatch.setattr(nudb_logger, "STACK_LABELS", ["x"])
    monkeypatch.setattr(nudb_logger, "ID_COUNTERS", [0])
    monkeypatch.setattr(nudb_logger, "JSON", {})
    monkeypatch.setattr(nudb_logger, "JSON_FIELDS", [nudb_logger.JSON])

    record = logging.LogRecord("n", logging.INFO, __file__, 1, "msg", None, None)

    with pytest.raises(ValueError):
        nudb_logger.formatter.format(record)


def test_loggerstack_default_label_uses_stack_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nudb_logger, "STACK_LEVEL", 0)
    stack = nudb_logger.LoggerStack()

    assert stack.label == "1"


def test_loggerstack_enter_raises_when_json_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nudb_logger, "STACK_LEVEL", 0)
    monkeypatch.setattr(nudb_logger, "STACK_LABELS", [])
    monkeypatch.setattr(nudb_logger, "ID_COUNTERS", [0])
    monkeypatch.setattr(nudb_logger, "JSON_FIELDS", [])

    with pytest.raises(RuntimeError):
        with nudb_logger.LoggerStack():
            pass


def test_enter_and_exit_helpers_manage_state(monkeypatch: pytest.MonkeyPatch) -> None:
    base_json: dict[str, object] = {}
    monkeypatch.setattr(nudb_logger, "STACK_LEVEL", 0)
    monkeypatch.setattr(nudb_logger, "STACK_LABELS", [])
    monkeypatch.setattr(nudb_logger, "ID_COUNTERS", [0])
    monkeypatch.setattr(nudb_logger, "JSON", base_json)
    monkeypatch.setattr(nudb_logger, "JSON_FIELDS", [base_json])

    nudb_logger.enter_new_logger_stack("LBL")

    assert nudb_logger.STACK_LEVEL == 1
    assert nudb_logger.STACK_LABELS == ["LBL"]
    assert "LBL-0" in nudb_logger.JSON

    nudb_logger.exit_current_logger_stack("LBL")

    assert nudb_logger.STACK_LEVEL == 0
    assert nudb_logger.STACK_LABELS == []
    assert nudb_logger.JSON_FIELDS == [base_json]


def test_get_current_json_returns_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    data = {"a": {"b": 1}}
    monkeypatch.setattr(nudb_logger, "JSON", data)

    copy_data = nudb_logger.get_current_json()
    copy_data["a"]["b"] = 2

    assert nudb_logger.JSON["a"]["b"] == 1


def test_save_current_json_writes_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(nudb_logger, "JSON", {"saved": True})
    target = tmp_path / "log.json"

    nudb_logger.save_current_json(target)

    with target.open() as fh:
        saved = json.load(fh)

    assert saved == {"saved": True}
