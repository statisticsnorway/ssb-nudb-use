from collections import Counter

import pandas as pd


def find_duplicated_columns(df: pd.DataFrame) -> list[str]:
    counts = Counter(list(df.columns))
    return [item for item, count in counts.items() if count > 1]
