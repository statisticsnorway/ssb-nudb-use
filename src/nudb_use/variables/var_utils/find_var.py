from nudb_use.metadata.nudb_config.find_var_missing import find_var as _find_var
from nudb_use.metadata.nudb_config.find_var_missing import find_vars as _find_vars
from nudb_use.metadata.nudb_config.find_var_missing import (
    variables_missing_from_config as _variables_missing_from_config,
)
from nudb_use.utils.packages import move_to_use_deprecate

OLD_PATH = "nudb_use.variables.var_utils.find_var"
NEW_PATH = "nudb_use.metadata.nudb_config.find_var_missing"

find_var = move_to_use_deprecate(
    _find_var,
    old_path=OLD_PATH,
    new_path=NEW_PATH,
)
find_vars = move_to_use_deprecate(
    _find_vars,
    old_path=OLD_PATH,
    new_path=NEW_PATH,
)
variables_missing_from_config = move_to_use_deprecate(
    _variables_missing_from_config,
    old_path=OLD_PATH,
    new_path=NEW_PATH,
)
