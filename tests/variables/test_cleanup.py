import pandas as pd

from nudb_use.variables.cleanup import move_col_after_col
from nudb_use.variables.cleanup import move_content_from_col_to


def test_move_col_after_col(avslutta: pd.DataFrame) -> None:
    avslutta_moved = move_col_after_col(avslutta, "fnr", "snr")
    col_list = avslutta_moved.columns.to_list()
    assert col_list.index("fnr") < col_list.index("snr")
    avslutta_moved = move_col_after_col(
        avslutta_moved,
        "snr",
        "fnr",
    )
    col_list = avslutta_moved.columns.to_list()
    assert col_list.index("fnr") > col_list.index("snr")


def test_move_content_from_col_to(avslutta: pd.DataFrame) -> None:
    df = avslutta.copy()
    df["snr"] = df["snr"].mask(df["snr"].notna(), pd.NA)

    result = move_content_from_col_to(df, from_col="fnr", to_col="snr")

    assert "fnr" not in result.columns
    assert result["snr"].notna().all()
