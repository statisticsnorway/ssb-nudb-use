import pandas as pd

from nudb_use.variables.derive.utd_hendelse import _safe_sha256_hash
from nudb_use.variables.derive.utd_hendelse import utd_hendelse_id


def test_utd_skoleaar_slutt() -> None:
    df = pd.DataFrame(
        {
            "nudb_dataset_id": [
                "/some/random/path_to/a/parquet_file/with_a_version_number"
            ]
        }
    )

    hashed = utd_hendelse_id(df.copy())
    unhashed = utd_hendelse_id(df.copy(), hashed=False)

    # Our expected values
    expected_unhashed = df["nudb_dataset_id"].iloc[0] + "[0]"
    expected_hashed = _safe_sha256_hash(expected_unhashed)

    # Make sure the variables actually get derived
    assert "utd_hendelse_id" in hashed.columns
    assert "utd_hendelse_id" in unhashed.columns

    # We should not have any missing values
    assert unhashed["utd_hendelse_id"].notna().all()
    assert hashed["utd_hendelse_id"].notna().all()

    # Make sure we actually get what we expect
    assert (
        hashed["utd_hendelse_id"].iloc[0] == expected_hashed
    )  # we alrady made sure we don't have any NAs
    assert unhashed["utd_hendelse_id"].iloc[0] == expected_unhashed
