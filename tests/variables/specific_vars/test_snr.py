from __future__ import annotations

import uuid as uuidlib
from typing import Any

import pandas as pd

from nudb_use.variables.specific_vars import snr as snr_module
from nudb_use.variables.specific_vars.snr import derive_snr_mrk
from nudb_use.variables.specific_vars.snr import generate_uuid_for_snr_with_fnr_catalog
from nudb_use.variables.specific_vars.snr import generate_uuid_for_snr_with_fnr_col


def test_derive_snr_mrk() -> None:
    df = pd.DataFrame({"snr": ["1234567", "123", pd.NA]})

    result = derive_snr_mrk(df, snr_col="snr")

    assert result["snr_mrk"].tolist() == [True, False, False]
    assert str(result["snr_mrk"].dtype) == "bool[pyarrow]"


def test_generate_uuid_for_snr_with_fnr_col(monkeypatch: Any) -> None:
    uuids = iter(
        [
            uuidlib.UUID("00000000-0000-0000-0000-000000000001"),
            uuidlib.UUID("00000000-0000-0000-0000-000000000002"),
        ]
    )
    monkeypatch.setattr(snr_module.uuid, "uuid4", lambda: next(uuids))  # type: ignore[attr-defined]

    df = pd.DataFrame(
        {
            "fnr": ["1", "1", pd.NA, "2"],
            "snr": [pd.NA, pd.NA, pd.NA, "existing"],
        }
    )

    result = generate_uuid_for_snr_with_fnr_col(df, snr_col="snr", fnr_col="fnr")

    assert result["snr"].tolist() == [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
        "existing",
    ]
    assert str(result["snr"].dtype) in ["string", "string[pyarrow]"]


def test_generate_uuid_for_snr_with_fnr_catalog(
    tmp_path: Any, monkeypatch: Any
) -> None:
    uuids = iter(
        [
            uuidlib.UUID("00000000-0000-0000-0000-000000000010"),
            uuidlib.UUID("00000000-0000-0000-0000-000000000011"),
        ]
    )
    monkeypatch.setattr(snr_module.uuid, "uuid4", lambda: next(uuids))  # type: ignore[attr-defined]

    catalog_path = tmp_path / "fnr_catalog.parquet"
    existing_catalog = pd.DataFrame({"fnr": ["1"], "snr": ["existing-uuid"]}).astype(
        {"fnr": "string[pyarrow]", "snr": "string[pyarrow]"}
    )
    existing_catalog.to_parquet(catalog_path)

    monkeypatch.setattr(
        snr_module,
        "latest_version_path",
        lambda path: path,
    )
    monkeypatch.setattr(
        snr_module,
        "next_version_path",
        lambda path: path,
    )

    df = pd.DataFrame({"fnr": ["1", "2", pd.NA], "snr": [pd.NA, pd.NA, pd.NA]})

    result = generate_uuid_for_snr_with_fnr_catalog(
        df, fnr_catalog_path=catalog_path, snr_col="snr", fnr_col="fnr"
    )

    assert result["snr"].tolist() == [
        "existing-uuid",
        "00000000-0000-0000-0000-000000000010",
        "00000000-0000-0000-0000-000000000011",
    ]

    updated_catalog = pd.read_parquet(catalog_path).sort_values("fnr")
    assert updated_catalog[["fnr", "snr"]].values.tolist() == [
        ["1", "existing-uuid"],
        ["2", "00000000-0000-0000-0000-000000000010"],
    ]
