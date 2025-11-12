# maybe not the correct place for this script


import pandas as pd
from nudb_config import settings as settings_use
from nudb_use import logger


def get_toml_field(toml, field):
    return None if field not in toml.keys() else toml[field]

def get_var_metadata(variables: list[str] | None = None) -> pd.DataFrame:
    """Get at pandas dataframe of the variable-data from the config.

    Args:
        variables: Variables to return data on, if None returns all variables.

    Returns:
        pd.DataFrame: The information from the metadata.
    """

    variables_list_of_dicts = [{"variable": var_name} | dict(settings_use.variables[var_name]) for var_name in settings_use.variables.keys()]
    df = pd.DataFrame(variables_list_of_dicts).set_index("variable")
    
    return df.loc[variables, :] if variables else df
        
