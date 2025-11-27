1. Klass "check against codelist" / getting klass data
    1.  metadata/nudb_klass/codes
    2.  variables/checks
    3.  variables/var_utils/find_var
2. Column renames from config:
    1. variables/var_utils/renames
    2. metadata/nudb_config/variable_names
    3. nudb_build/variables/var_utils/renames
3.  Nudb config around:
    1. config.py
    2. config_tomls/
    3. metadata/nudb_config
4. Checking column "presence" / in config / drop / keep
    1. variables/var_utils/find_var
    2. variables/checks.py
    3. metadata/nudb_config/variable_names

# Build vs. Use
1. Moves from Build to Use?
    1. nudb_build/variables/cleanup.py -> nudb_use/variables/
    2. metadata/datadoc.py
    3. metadata/klass_nudb.py
    4. metadata/merge.py (inn i datadoc.py)?
    5. quality?
    6. "columns in pyarrow metadata" better place in use: nudb_build/variables/checks.py
1. Paths in build, what is in use?
    1. Remove hardcoding in functions, depend on config?
2. Moves in build package
    1. Make "exceptions" folder, move temporality-exeption there
    2.
