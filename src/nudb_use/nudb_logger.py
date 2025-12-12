"""Custom logging utilities that add hierarchical context to log output."""

from __future__ import annotations

import copy
import json
import logging
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import TypeVar

from colorama import Back
from colorama import Fore
from colorama import Style
from fagfunksjoner import logger as faglogger  # type: ignore[attr-defined]

STACK_LEVEL: int = 0
STACK_LABELS: list[str] = []
INDENT_WIDTH: int = 4
ENTERING_STACK: bool = False
EXITING_STACK: bool = False
WIDTH_LEVEL_NAME: int = 8


T = TypeVar("T")

ID_COUNTERS: list[int] = [0]
JSON: dict[str, Any] = {}
JSON_FIELDS: list[dict[str, Any]] = [JSON]
__all__ = [
    "LoggerStack",
    "_enter_new_logger_stack",
    "_exit_current_logger_stack",
    "_get_current_json",
    "_save_current_json",
    "add_log_record_to_json",
    "logger",
]


def last(items: Sequence[T]) -> T | None:
    """Return the last item of a list or None if empty."""
    return None if not items else items[-1]


def add_log_record_to_json(record: logging.LogRecord) -> None:
    """Persist the current log record into the in-memory JSON structure."""
    global STACK_LABELS, ID_COUNTERS, JSON_FIELDS
    stack_label = last(STACK_LABELS)

    level = record.levelname
    msg = record.msg

    CURRENT_ID_COUNTER = ID_COUNTERS.pop()
    name = f"{level}-{stack_label}-{CURRENT_ID_COUNTER}"
    ID_COUNTERS.append(CURRENT_ID_COUNTER + 1)

    current_json_field = last(JSON_FIELDS)
    if current_json_field is None:
        raise RuntimeError("JSON_FIELDS is unexpectedly empty while logging.")

    current_json_field[name] = {
        "id": CURRENT_ID_COUNTER,
        "level": level,
        "msg": msg,
        "time": str(datetime.now()),
    }


class ColoredFormatter(logging.Formatter):
    """Colored log formatter."""

    def __init__(
        self,
        *args: Any,
        colors: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.colors = colors if colors else {}
        self.level = 0

    def format(self, record: logging.LogRecord) -> str:
        """Format the specified record as text."""
        record.color = self.colors.get(record.levelname, "")
        record.reset = Style.RESET_ALL

        global INDENT_WIDTH, STACK_LEVEL, EXITING_STACK, WIDTH_LEVEL_NAME, ENTERING_STACK

        if not ENTERING_STACK and not EXITING_STACK:
            add_log_record_to_json(record)

        if INDENT_WIDTH < 1:
            raise ValueError(
                f"INDENT_WIDTH must have at least length 1! ({INDENT_WIDTH})"
            )
        if STACK_LEVEL < 0:
            raise ValueError(f"STACK_LEVEL is negative! ({STACK_LEVEL})")

        if ENTERING_STACK:
            nlpad = 11
            lpad = " " * nlpad
            width = nlpad * 2 + len(record.msg)
            prepad = ("│" + " " * (INDENT_WIDTH - 1)) * (STACK_LEVEL)

            line1 = prepad + "┌" + "─" * width + "┐"
            line2 = prepad + "│" + lpad + record.msg + lpad + "│"
            line3 = prepad + "├" + "─" * width + "┘"
            line4 = prepad + "│"

            return prepad + "\n" + line1 + "\n" + line2 + "\n" + line3 + "\n" + line4

        if STACK_LEVEL:
            prepad = ("│" + " " * (INDENT_WIDTH - 1)) * (STACK_LEVEL - 1)

            if EXITING_STACK:
                pad_l1 = prepad + "└" + "─" * (INDENT_WIDTH - 1)
                pad_l2 = prepad + " " * INDENT_WIDTH + " " * WIDTH_LEVEL_NAME + "     "
            else:
                pad_l1 = prepad + "├" + "─" * (INDENT_WIDTH - 1)
                pad_l2 = (
                    prepad
                    + "│"
                    + " " * (INDENT_WIDTH - 1)
                    + " " * WIDTH_LEVEL_NAME
                    + "     "
                )
        else:
            pad_l1 = ""
            pad_l2 = " " * WIDTH_LEVEL_NAME + "     "

        return pad_l1 + super().format(record).replace("\n", "\n" + pad_l2)


formatter = ColoredFormatter(
    "{color} {levelname:" + str(WIDTH_LEVEL_NAME) + "} {reset} {message}",
    style="{",
    colors={
        "DEBUG": Fore.CYAN,
        "INFO": Fore.GREEN,
        "WARNING": Fore.MAGENTA,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Back.WHITE + Style.BRIGHT,
    },
)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.handlers[:] = []
logger.addHandler(handler)
logger.setLevel(logging.INFO)


faglogger.handlers[:] = []
faglogger.addHandler(handler)
faglogger.setLevel(logging.INFO)


class LoggerStack:
    """Context manager that adds structured stack labels to log output."""

    def __init__(self, label: str | None = None) -> None:
        if label is None:
            label = str(STACK_LEVEL + 1)
        self.label = label

    def __enter__(self) -> LoggerStack:
        """Enter the stack scope and adjust global logging state."""
        global STACK_LEVEL, STACK_LABELS, ENTERING_STACK, JSON_FIELDS, ID_COUNTERS

        CURRENT_ID_COUNTER = 0
        FIELD_NAME = f"{self.label}-{CURRENT_ID_COUNTER}"
        ID_COUNTERS.append(CURRENT_ID_COUNTER + 1)

        current_json_field = last(JSON_FIELDS)
        if current_json_field is None:
            raise RuntimeError("JSON_FIELDS is unexpectedly empty while entering.")

        current_json_field[FIELD_NAME] = {}
        JSON_FIELDS.append(current_json_field[FIELD_NAME])

        ENTERING_STACK = True
        logger.info(f"ENTERING STACK [{self.label}|{STACK_LEVEL}]")
        ENTERING_STACK = False

        STACK_LABELS.append(self.label)
        STACK_LEVEL += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the stack scope and clean up state."""
        global STACK_LEVEL, STACK_LABELS, EXITING_STACK, JSON_FIELDS

        JSON_FIELDS.pop()
        EXITING_STACK = True
        logger.info(f"EXITING STACK [{self.label}|{STACK_LEVEL}]\n")

        EXITING_STACK = False
        STACK_LABELS.pop()
        STACK_LEVEL -= 1


def _enter_new_logger_stack(label: str | None = None) -> None:
    """Enter a new logger stack context with the given label."""
    stack = LoggerStack(label)
    stack.__enter__()


def _exit_current_logger_stack(label: str | None = last(STACK_LABELS)) -> None:
    """Exit the current logger stack context in a safe way."""
    stack = LoggerStack(label)
    stack.__exit__(None, None, None)


def _get_current_json() -> dict[str, Any]:
    """Return a shallow copy of the accumulated logging JSON structure."""
    return copy.deepcopy(JSON)


def _save_current_json(path: str | Path, indent: int = 4) -> None:
    """Persist the accumulated logging JSON structure to disk."""
    path_path = Path(path)
    with path_path.open("w") as file:
        json.dump(JSON, file, indent=indent)
