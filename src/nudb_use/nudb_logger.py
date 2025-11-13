from __future__ import annotations

import copy
import json
import logging
import sys
from datetime import datetime
from typing import Any

from colorama import Back
from colorama import Fore
from colorama import Style
from fagfunksjoner import logger as faglogger

STACK_LEVEL: int = 0
STACK_LABELS: list[str] = []
INDENT_WIDTH: int = 4
ENTERING_STACK: bool = False
EXITING_STACK: bool = False
WIDTH_LEVEL_NAME: int = 8


ID_COUNTERS = [0]
JSON = {}
JSON_FIELDS = [JSON]


def last(x: list):
    return None if len(x) <= 0 else x[len(x) - 1]


def add_LogRecord_to_json(record: logging.LogRecord) -> None:
    global STACK_LABELS, ID_COUNTERS, JSON_FIELDS
    stack_label = last(STACK_LABELS)

    level = record.levelname
    msg = record.msg

    CURRENT_ID_COUNTER = ID_COUNTERS.pop()
    name = f"{level}-{stack_label}-{CURRENT_ID_COUNTER}"
    ID_COUNTERS.append(CURRENT_ID_COUNTER + 1)

    CURRENT_JSON_FIELD = last(JSON_FIELDS)
    CURRENT_JSON_FIELD[name] = {
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
        """Initialize the formatter with specified format strings."""
        super().__init__(*args, **kwargs)

        self.colors = colors if colors else {}
        self.level = 0

    def format(self, record: logging.LogRecord) -> str:
        """Format the specified record as text."""
        record.color = self.colors.get(record.levelname, "")
        record.reset = Style.RESET_ALL

        global INDENT_WIDTH, STACK_LEVEL, EXITING_STACK, WIDTH_LEVEL_NAME, ENTERING_STACK

        if not ENTERING_STACK and not EXITING_STACK:
            add_LogRecord_to_json(record)

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
    def __init__(self, label: str = STACK_LEVEL + 1):
        self.label = label

    def __enter__(self):
        global STACK_LEVEL, STACK_LABELS, ENTERING_STACK, JSON_FIELDS, ID_COUNTERS

        CURRENT_ID_COUNTER = 0
        FIELD_NAME = f"{self.label}-{CURRENT_ID_COUNTER}"
        ID_COUNTERS.append(CURRENT_ID_COUNTER + 1)

        CURRENT_JSON_FIELD = last(JSON_FIELDS)
        CURRENT_JSON_FIELD[FIELD_NAME] = {}
        JSON_FIELDS.append(CURRENT_JSON_FIELD[FIELD_NAME])

        ENTERING_STACK = True
        logger.info(f"ENTERING STACK [{self.label}|{STACK_LEVEL}]")
        ENTERING_STACK = False

        STACK_LABELS.append(self.label)
        STACK_LEVEL += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        global STACK_LEVEL, STACK_LABELS, EXITING_STACK, JSON_FIELDS, CURRENT_ID_COUNTERS

        JSON_FIELDS.pop()
        EXITING_STACK = True
        logger.info(f"EXITING STACK [{self.label}|{STACK_LEVEL}]\n")

        EXITING_STACK = False
        STACK_LABELS.pop()
        STACK_LEVEL -= 1


def ENTER_NEW_LOGGER_STACK(label: str = STACK_LEVEL + 1):
    loggerStack = LoggerStack(label)
    loggerStack.__enter__()


def EXIT_CURRENT_LOGGER_STACK(label=last(STACK_LABELS)):
    loggerStack = LoggerStack(label)
    loggerStack.__exit__(None, None, None)


def GET_CURRENT_JSON():
    return copy.deepcopy(JSON)


def SAVE_CURRENT_JSON(path: str, indent: int = 4):
    with open(path, "w") as file:
        json.dump(JSON, file, indent=indent)
