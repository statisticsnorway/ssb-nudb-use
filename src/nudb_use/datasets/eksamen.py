import pandas as pd

from nudb_use.datasets.nudb_datasets import NudbDataSet
from nudb_use.datasets.nudb_datasets import EKSAMEN, NUDB_DATABASE_CONNECTION



def init_eksamen_aggregated():

    query = f"""
        SELECT * FROM {EKSAMEN.alias}
        GROUP BY
    """
    EKSAMEN.alias
