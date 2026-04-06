import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from config import (
    RAW_DATA_PATH,
    DB_PATH,
    METRICS_TABLE_NAME,
    ANOMALY_TABLE_NAME,
)
from data_checks import (
    row_count_check,
    duplicate_check,
    null_percentage_check,
    negative_value_check,
)
from metrics_calculator import (
    total_revenue,
    average_profit,
    average_quantity,
)
from anomaly_detection import detect_anomalies


def load_data() -> pd.DataFrame:
    df = pd.read_csv(RAW_DATA_PATH, encoding="ISO-8859-1")
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )
    return df


def get_previous_metric_value(engine, metric_name: str):
    query = f"""
        SELECT metric_value
        FROM {METRICS_TABLE_NAME}
        WHERE metric_name = '{metric_name}'
        ORDER BY run_date DESC
        LIMIT 1
    """
    try:
        prev = pd.read_sql(query, engine)
        if not prev.empty:
            return float(prev.iloc[0]["metric_value"])
    except Exception:
        pass
    return None


def pct_change(current, previous):
    if previous is None or previous == 0:
        return None
    return round(((current - previous) / previous) * 100, 2)


def main():
    df = load_data()
    engine = create_engine(f"sqlite:///{DB_PATH}")

    important_null_cols = [
        "customer_id",
        "product_name",
        "category_name",
        "delivery_status",
        "shipping_mode",
    ]

    metrics = {
        "run_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "row_count": row_count_check(df),
        "duplicate_order_item_ids": duplicate_check(df, "order_item_id"),
        "total_revenue": total_revenue(df, "sales"),
        "average_profit": average_profit(df, "order_profit_per_order"),
        "average_quantity": average_quantity(df, "order_item_quantity"),
        "null_percentages": null_percentage_check(df, important_null_cols),
        "negative_counts": negative_value_check(df, ["sales", "order_item_quantity"]),
    }

    # previous-run comparison
    for metric_name in ["row_count", "total_revenue", "average_quantity"]:
        prev_val = get_previous_metric_value(engine, metric_name)
        metrics[f"{metric_name}_previous"] = prev_val
        metrics[f"{metric_name}_pct_change"] = pct_change(metrics[metric_name], prev_val)

    anomalies = detect_anomalies(metrics)

    metrics_rows = []
    for key, value in metrics.items():
        if isinstance(value, dict):
            for sub_key, sub_val in value.items():
                metrics_rows.append({
                    "run_date": metrics["run_date"],
                    "metric_name": f"{key}_{sub_key}",
                    "metric_value": sub_val,
                })
        else:
            metrics_rows.append({
                "run_date": metrics["run_date"],
                "metric_name": key,
                "metric_value": value,
            })

    metrics_df = pd.DataFrame(metrics_rows)

    if anomalies:
        anomalies_df = pd.DataFrame(anomalies)
        anomalies_df["run_date"] = metrics["run_date"]
    else:
        anomalies_df = pd.DataFrame(
            columns=["run_date", "anomaly_type", "metric_name", "metric_value", "severity", "message"]
        )

    metrics_df.to_sql(METRICS_TABLE_NAME, con=engine, if_exists="append", index=False)
    anomalies_df.to_sql(ANOMALY_TABLE_NAME, con=engine, if_exists="append", index=False)

    # export for Power BI
    metrics_df.to_csv("outputs/monitoring_metrics.csv", index=False)
    anomalies_df.to_csv("outputs/anomaly_log.csv", index=False)

    print("Monitoring run completed.")
    print("\nMetrics:")
    print(metrics_df)

    print("\nAnomalies:")
    if anomalies_df.empty:
        print("No anomalies detected.")
    else:
        print(anomalies_df)


if __name__ == "__main__":
    main()