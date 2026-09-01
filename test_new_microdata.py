#%%
import pandas as pd
from nudb_use import MicroData, NudbData
from nudb_use import get_microdata_variables_overview
from nudb_use.datasets.microdata import show_available_microdata_variables
#%%
show_available_microdata_variables()
#%% 
test = get_microdata_variables_overview("pers_fullfoert_foerste")
test

# %%

test = MicroData("utd_foreldres_utdnivaa_16aar").df()
test.head()

# %%

test2 = MicroData("utd_hoeyeste_nus2000").df()
test2.head()

#%%
len(test)
# %%
test[test["utd_foreldres_utdnivaa_16aar_nus2000"].notna()]

# %%
nasj_test = pd.read_parquet("/buckets/shared/utd-bhgskole/nasjprov/nasjprov/klargjorte-data/nasjonaleprover_p2007_p2025_v1.parquet")
nasj_test.head()
# %%
nudb_database.get_connection().sql("SELECT current_setting('temp_directory')").df()


# %%
test = NudbData("avslutta").select("""
    snr, nus2000, utd_skoleaar_start
""").where("""
    utd_skoleaar_start > '2023'
""").df()
# %%

from nudb_use.variables.derive import utd_foreldres_utdnivaa_16aar
# %%
test = utd_foreldres_utdnivaa_16aar(test)
# %%
test.head()
# %%
from nudb_use.variables.derive import utd_hoeyeste_far_nus2000
# %%
test = utd_hoeyeste_far_nus2000(test)
# %%
test.head()
# %%
# TEST UTD_FORELDRES_UTDNIVAA

