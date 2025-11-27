"""NUDB Use - is a usage-package for the Norwegian National Education Database cloud-data. Both for data-consumers and data-deliverers. Requires access to NUDBs shared data in most instances."""

from nudb_config import settings

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger

__all__ = ["LoggerStack", "logger", "settings"]
