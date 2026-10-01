#%%
from re import split

import pandas as pd
from nudb_use import MicroData, NudbData
from nudb_use import get_microdata_variables_overview
from nudb_use import split_microdata_dataset
pd.set_option("display.max_colwidth", None)


#%%
# Viser hvilke datasett som er tilgjengelige
show_available_microdata_variables()

#%% 
# Gir oversikt over:
#   - Variabler på datasettet
#   - Kortnavn og Fullt Navn til variabelen
#   - Start og Sluttår til Variabelen
#   - Kort beskrivelse av variabelen (Hentet fra Vardef)
test = get_microdata_variables_overview("microdata_eksamen")
test



# %%

# Last inn et MicroData variabel-sett
test = MicroData("gs_fagvurdering").df()
test


#%%
test["fagvurdering_grsk_fagkode"].value_counts()[:50]

test[test["fagvurdering_grsk_fagkode"]=="MAT"]

#%%
test[test["snr"]=="stJZbOg"]

#%%
# Test split funksjon

test2= split_microdata_dataset(dataset_or_name=test, id_col="fnr", auto_detect_start_stop=True, output_dir="./tests", file_format="parquet")

#%%
test[test["aar_uh_foerste_registrert_dato"].notna()]

#%%
test[test["aar_foerste_reg_gr"] == "1900"]
# %%
test.columns
test["fagvurdering_grsk_vurderingsform"].value_counts()

# %%
print(test["aar_forste_fullf_cmg"].min())
print(test["aar_forste_fullf_cmg"].max())

# %%
test.head()

# %%
len(test)
test["komm_16"].value_counts(dropna=False)

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
    snr, nus2000, utd_skoleaar_start, utd_hendelse_id
""").where("""
    utd_skoleaar_start > '2023'
""").df()

# %%
test.head()

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

