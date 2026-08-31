#%%
from nudb_use import MicroData
from nudb_use import get_microdata_variables_overview
from nudb_use.datasets.microdata import show_available_microdata_variables
#%%
show_available_microdata_variables()
#%% 
test = get_microdata_variables_overview("utd_hoeyeste_nus2000")
test

# %%

test = MicroData("personvariabler").df()

# %%
print(nasjprov_test.head())

# %%
nasjprov_test[:25]

# %%
import pandas as pd
nasj_test = pd.read_parquet("/buckets/shared/utd-bhgskole/nasjprov/nasjprov/klargjorte-data/nasjonaleprover_p2007_p2025_v1.parquet")
nasj_test.head()
# %%
