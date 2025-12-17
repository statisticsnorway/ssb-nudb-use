# SSB-NUDB-USE

[![PyPI](https://img.shields.io/pypi/v/ssb-nudb-use.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-nudb-use.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-nudb-use)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-nudb-use)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-nudb-use/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-nudb-use/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-nudb-use&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-nudb-use&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-nudb-use/
[documentation]: https://statisticsnorway.github.io/ssb-nudb-use
[tests]: https://github.com/statisticsnorway/ssb-nudb-use/actions?workflow=Tests
[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-nudb-use
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-nudb-use
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

# Description

NUDB is the National Education Database of Norway. It is operated by Statsitics Norway - section 360.
This package is the main "usage-package" for those seeking to use NUDB-data, or deliver data to NUDB.

NUDBs data is kept as parquet files in GCP, and you will need seperate access to this data to utilize this package.
Some features in this package might require access to other data, like BRREG (Brønnøysundregisteret), BOF (befolkningsregisteret), VOF (virksomhetsregisteret) etc.


## Installation

You can install _SSB Nudb Use_ via [poetry] from [PyPI]:

```console
poetry add ssb-nudb-use
```

## Dependencies

This package depends on the package "ssb-nudb-config", which contains metadata, but also points to content in other metadatasystems like Vardef, Klass and Datadoc.


## Usage

Please see the [Reference Guide] for details.


### Usage for extraction (data from NUDB)

Find the latest of each file shared.
```python
from nudb_use import latest_shared_paths
latest_shared_paths()
```

Get the periods out of any paths following the SSB-naming standard.
```python
from nudb_use import get_periods_from_path
get_periods_from_path(path)
```

Deriving variables not stored in data, is done by the derive module:
```python
from nudb_use import derive
df = derive.utd_skoleaar_slutt(df)
```


### Usage for delivery (data to NUDB)

We have renamed a lot of our variables transitioning from the old on-prem systems. If you are looking for the new or old names of variables, you can use the find_var or find_vars functions:
```python
from nudb_use import find_vars
find_vars(["snr", "sosbak"])
```

Find the dtype and length (char-width) of strings using a dataeset name:
```python
from nudb_use import look_up_dtype_length_for_dataset
print(look_up_dtype_length_for_dataset("igang_videregaaende"))
```


If you want to update the column names you have in a pandas dataframe, to the new column names - there's a function for that:
```python
from nudb_use import update_colnames
df = update_colnames(df)
```

After renaming, you can get the pandas dtypes the columns should have with get_dtypes:
```python
from nudb_use import get_dtypes
dtypes = get_dtypes(df)
df = df.astype(dtypes)
```
If you are delivering to NUDB, we want you to run our quality suite before sharing the data with us:
```python
from nudb_use import run_quality_suite
run_quality_suite(df, "avslutta")
```
Data about your delivery, like "avslutta", should first have its data entered into, and released in the ssb-nudb-config package before available in this function. Contact the NUDB-team to define a new delivery.




## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_SSB Nudb Use_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/ssb-nudb-use/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/ssb-nudb-use/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/ssb-nudb-use/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/ssb-nudb-use/reference.html
