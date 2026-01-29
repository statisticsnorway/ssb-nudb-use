import pandas as pd

from nudb_use.variables.derive.land import utd_erutland


def test_utd_erutland_flags_kommuner() -> None:
    df = pd.DataFrame(
        {
            "utd_skolekom": [
                "0025",
                "1025",
                "2025",
                "2400",
                "2580",
                "9999",
                None,
            ]
        }
    )

    result = utd_erutland(df)

    assert result["utd_erutland"].tolist() == [
        True,
        True,
        True,
        True,
        True,
        False,
        False,
    ]
    assert str(result["utd_erutland"].dtype) == "bool[pyarrow]"
