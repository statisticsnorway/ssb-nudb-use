"""Entry points for specific-variable validation routines."""

from .grunnskolepoeng import check_grunnskolepoeng
from .nus2000 import check_nus2000
from .run_all import run_all_specific_variable_tests

__all__ = [
    "check_grunnskolepoeng",
    "check_nus2000",
    "run_all_specific_variable_tests",
]
