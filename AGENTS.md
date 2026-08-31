# SSB-NUDB-USE: Microdata Delivery Project handover (AGENTS.md)

This document provides a summary of the current project state, active branches, architecture, and package relationships for future developers/agents.

---

## 1. Current State & Active Branch

* **Active Branch:** `microdata-delivery`
* **Goal:** Implement and test the prototype for delivering pivoted and subsetted variables to Microdata.no.
* **Core Achievements:**
  * Implemented pivoted and subsetted view generators under `src/nudb_use/datasets/microdata_variables/`:
    * `utd_hoeyeste_nus2000` (highest education level over time).
    * `avslutta_subset` (subset of the main `avslutta` dataset).
    * `nasjprov` (national test results pivoted from `provekode` into standardized columns like `np_skalapoeng_npeng05`).
  * Registered these custom generators inside `_NudbDatabase` in `nudb_database.py`.
  * Verified that **all 194 unit tests pass** cleanly and successfully.
  * Resolved a critical bug in `show_available_microdata_variables()` where hardcoded microdata datasets were hidden when checking only external `settings` keys. It now correctly checks both hardcoded and config-defined datasets.
  * Added a new metadata module `nudb_use.metadata.microdata` with a function `get_microdata_variables_overview(dataset_name)` that summarizes all variables in a Microdata dataset, matching each to its full name/description via `Vardef` and calculating min/max start years (e.g. `utd_skoleaar_start`).

---

## 2. Architecture & Package Relationships

The `ssb-nudb-use` library facilitates lazy querying, transformations, and verification on the National Education Database (NUDB). Below is the relation between the core files/packages:

```
                  +---------------------+
                  |   ssb-nudb-config   | <--- Defines metadata, datasets, and paths
                  +----------+----------+
                             | (provides settings.datasets)
                             v
+------------------+   +-----+---------------+
|  microdata.py    |-->|  nudb_database.py   | <--- Controls in-memory DuckDB database
|                  |   |                     |      and registers view generators
|  Provides:       |   +---------+-----------+
|  - MicroData     |             | (manages)
|  - show_avail... |             v
+--------+---------+   +---------+-----------+
         |             |    nudb_data.py     | <--- Standard lazy NudbData class for SQL/DF
         +------------>|                     |
           (inherits)  +---------------------+
```

### `ssb-nudb-config` (External PyPI package)
* **Role:** The main metadata provider.
* **Usage:** Imported via `from nudb_config import settings`. It contains specifications for all variables, names, data types, and datasets.
* **Relation:** It dictates which datasets are available globally and provides the configuration for dynamic fallback loading.

### `nudb_database.py` (Local)
* **Role:** Manages the internal DuckDB in-memory database instance (`_NudbDatabase` singleton).
* **Relation:**
  * Maps dataset names to Python generator functions.
  * Dynamically registers fallback view loaders for microdata datasets configured in `ssb-nudb-config` starting with `_microdata_` that do not have custom generators implemented in `ssb-nudb-use`.

### `nudb_data.py` (Local)
* **Role:** Implements `NudbData`, which is a lazy query-builder interface over DuckDB tables/views.
* **Relation:** Standard operations (e.g., `.select()`, `.where()`, `.left_join()`, `.df()`) are handled here.

### `microdata.py` (Local)
* **Role:** Implements the `MicroData` class (which inherits from `NudbData`) and provides public APIs like `show_available_microdata_variables()`.
* **Relation:** Acts as the public interface for statisticians to query or request variables intended for Microdata.no. It automatically prefixes names with `_microdata_` before passing them to `NudbData`.

### `microdata_vars.py` (Local under `nudb_use/metadata/microdata/`)
* **Role:** Implements `get_microdata_variables_overview(dataset_name)`.
* **Relation:** Resolves the variables defined in a Microdata dataset (from either hardcoded custom views or configured dynamic fallback views), fetches their metadata (full name and description) via `Vardef`, and dynamically calculates their temporal boundaries (`min_year` and `max_year`) based on identified date or school-year start columns (e.g. `utd_skoleaar_start`, `np_utd_skoleaar_start`). It is exposed at both the `nudb_use.metadata` and root `nudb_use` levels.

---

## 3. Environments & Testing Context

* **Development/Test Environment (Current):**
  * We are in a local test environment where related PyPI packages (like `ssb-nudb-config`) are installed locally within the Poetry virtual environment.
  * Synthetic and mocked dataframes are utilized for standard testing.
* **Verification Environment:**
  * Fully populated files and real-world pipelines are tested in a separate, dedicated development/production environment.
