import pandas as pd


def row_count_check(df: pd.DataFrame) -> int:
    return len(df)


def duplicate_check(df: pd.DataFrame, col: str) -> int:
    return int(df.duplicated(subset=[col]).sum())


def null_percentage_check(df: pd.DataFrame, columns: list[str]) -> dict:
    results = {}
    for col in columns:
        if col in df.columns:
            results[col] = round(df[col].isna().mean() * 100, 2)
    return results


def negative_value_check(df: pd.DataFrame, numeric_cols: list[str]) -> dict:
    results = {}
    for col in numeric_cols:
        if col in df.columns:
            results[col] = int((df[col] < 0).sum())
    return results