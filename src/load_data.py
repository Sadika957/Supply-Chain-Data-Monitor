import pandas as pd
from sqlalchemy import create_engine
from config import RAW_DATA_PATH, DB_PATH, RAW_TABLE_NAME


def load_csv() -> pd.DataFrame:
    df = pd.read_csv(RAW_DATA_PATH, encoding="ISO-8859-1")

    # Standardize column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )

    return df


def save_to_sqlite(df: pd.DataFrame) -> None:
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df.to_sql(RAW_TABLE_NAME, con=engine, if_exists="replace", index=False)


if __name__ == "__main__":
    df = load_csv()
    print("Loaded shape:", df.shape)
    print("Columns:", df.columns.tolist())
    save_to_sqlite(df)
    print("Data saved to SQLite successfully.")