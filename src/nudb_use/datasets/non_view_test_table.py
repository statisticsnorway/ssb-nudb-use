import pandas as pd

def _generate_test_non_view_table(alias: str, connection) -> None:
    test_df = pd.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": ["H", "E", "L", "L", "O"]
    })

    connection.sql(f"CREATE TABLE {alias} AS SELECT * FROM test_df")