import pandas as pd
from collections import Counter

def find_duplicated_columns(df: pd.DataFrame) -> list[str]:
    counts = Counter(list(df.columns))
    return[item for item, count in counts.items() if count > 1]