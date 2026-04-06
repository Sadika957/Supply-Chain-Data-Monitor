import pandas as pd
from pathlib import Path
from config import RAW_DATA_PATH


def main():
    df = pd.read_csv(RAW_DATA_PATH, encoding="ISO-8859-1")

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )

    # 1. Row count drop: remove 30% of rows
    df = df.sample(frac=0.70, random_state=42).copy()

    # 2. Missing value spike: set 25% of customer_id to null
    missing_idx = df.sample(frac=0.25, random_state=42).index
    df.loc[missing_idx, "customer_id"] = None

    # 3. Revenue spike: multiply sales by 5 for 500 rows
    sales_idx = df.sample(n=500, random_state=7).index
    df.loc[sales_idx, "sales"] = df.loc[sales_idx, "sales"] * 5

    # 4. Negative quantity issue: set 100 rows to negative quantity
    qty_idx = df.sample(n=100, random_state=21).index
    df.loc[qty_idx, "order_item_quantity"] = -1

    output_path = Path("data/raw/supply_chain_bad.csv")
    df.to_csv(output_path, index=False, encoding="ISO-8859-1")

    print(f"Bad dataset created successfully at: {output_path}")
    print("New shape:", df.shape)


if __name__ == "__main__":
    main()