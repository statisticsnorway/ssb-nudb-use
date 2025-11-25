import pandas as pd
import pytest

pytest.importorskip("jinja2")  # pandas Styler depends on jinja2

from nudb_use.quality import colored_views


def test_empty_cols_in_time_colored_highlights_ok_and_na() -> None:
    df = pd.DataFrame(
        {
            "year": [2020, 2020, 2021],
            "present_all_year": [1, 2, 3],
            "always_missing": [pd.NA, pd.NA, pd.NA],
        }
    )

    styled = colored_views.empty_cols_in_time_colored(df, "year")
    data = styled.data  # type: ignore[attr-defined]

    assert data.loc[2020, "present_all_year"] == "OK"
    assert data.loc[2021, "always_missing"] == "NA"

    html = styled.to_html()
    assert "background-color: green" in html
    assert "background-color: red" in html


def test_grade_cell_by_time_col_formats_and_colors_percentages() -> None:
    df = pd.DataFrame(
        {
            "period": [1, 1, 2, 2],
            "value": [1, pd.NA, 1, None],
        }
    )

    styled = colored_views.grade_cell_by_time_col(df, "period")
    data = styled.data  # type: ignore[attr-defined]

    assert data.loc[1, "value"] == "50.00%"
    assert data.loc[2, "value"] == "50.00%"

    expected_style = colored_views._grade_cell(
        "50.00%", scaling_fun=lambda value: value**4
    ).split("color: white; ")[-1]
    assert expected_style in styled.to_html()
