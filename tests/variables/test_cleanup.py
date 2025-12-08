import pandas as pd

from nudb_use.variables.cleanup import move_col_after_col
from nudb_use.variables.cleanup import move_content_from_col_to


def test_move_col_after_col(avslutta: pd.DataFrame) -> None:
    avslutta_moved = move_col_after_col(avslutta, "fnr", "pers_id")
    col_list = avslutta_moved.columns.to_list()
    assert col_list.index("fnr") < col_list.index("pers_id")
    avslutta_moved = move_col_after_col(
        avslutta_moved,
        "pers_id",
        "fnr",
    )
    col_list = avslutta_moved.columns.to_list()
    assert col_list.index("fnr") > col_list.index("pers_id")


def test_move_content_from_col_to(avslutta: pd.DataFrame) -> None:
    df = avslutta.copy()
    df["pers_id"] = df["pers_id"].mask(df["pers_id"].notna(), pd.NA)

    result = move_content_from_col_to(df, from_col="fnr", to_col="pers_id")

    assert "fnr" not in result.columns
    assert result["pers_id"].notna().all()
