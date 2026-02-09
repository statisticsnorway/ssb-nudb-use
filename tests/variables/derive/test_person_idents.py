import pandas as pd

from nudb_use.variables.derive.person_idents import snr_mrk


def test_snr_mrk() -> None:
    df = pd.DataFrame({"snr": ["1234567", "123", pd.NA]})

    result = snr_mrk(df)

    assert result["snr_mrk"].tolist() == [True, False, False]
    assert str(result["snr_mrk"].dtype) == "bool[pyarrow]"
