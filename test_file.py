#%%
from nudb_use import NudbData
import pandas as pd
#%%
avslutta = NudbData("avslutta").select("""
    snr, nus2000, utd_skoleaar_start
    """).where("""
    utd_utdanningstype = '211'
    """).df()
# %%
avslutta.head()
# %%
import fagfunksjoner
print(fagfunksjoner.__file__)
# %%
print(fagfunksjoner.__version__)
# %%
