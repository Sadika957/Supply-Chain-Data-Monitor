import pandas as pd


def total_revenue(df: pd.DataFrame, sales_col: str) -> float:
    return round(float(df[sales_col].sum()), 2)


def average_profit(df: pd.DataFrame, profit_col: str) -> float:
    return round(float(df[profit_col].mean()), 2)


def average_quantity(df: pd.DataFrame, quantity_col: str) -> float:
    return round(float(df[quantity_col].mean()), 2)


def daily_revenue(df: pd.DataFrame, date_col: str, sales_col: str) -> pd.DataFrame:
    temp = df.copy()
    temp[date_col] = pd.to_datetime(temp[date_col], errors="coerce")
    result = (
        temp.groupby(temp[date_col].dt.date)[sales_col]
        .sum()
        .reset_index(name="daily_revenue")
    )
    return result