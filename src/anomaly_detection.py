def detect_anomalies(metrics: dict) -> list[dict]:
    anomalies = []

    # Duplicate row-level IDs
    if metrics.get("duplicate_order_item_ids", 0) > 0:
        anomalies.append({
            "anomaly_type": "Duplicate Row IDs",
            "metric_name": "duplicate_order_item_ids",
            "metric_value": metrics["duplicate_order_item_ids"],
            "severity": "HIGH",
            "message": "Duplicate order_item_id values found."
        })

    # Missing values
    nulls = metrics.get("null_percentages", {})
    for col, pct in nulls.items():
        if pct > 10:
            anomalies.append({
                "anomaly_type": "Missing Values Spike",
                "metric_name": col,
                "metric_value": pct,
                "severity": "MEDIUM",
                "message": f"Null percentage in {col} is above 10%."
            })

    # Negative suspicious values
    negatives = metrics.get("negative_counts", {})
    for col, count in negatives.items():
        if count > 0:
            anomalies.append({
                "anomaly_type": "Negative Values Found",
                "metric_name": col,
                "metric_value": count,
                "severity": "HIGH",
                "message": f"Negative values found in {col}."
            })

    # Percent change anomalies
    for metric_name in ["row_count", "total_revenue", "average_quantity"]:
        pct_change = metrics.get(f"{metric_name}_pct_change")
        if pct_change is not None and abs(pct_change) > 20:
            anomalies.append({
                "anomaly_type": "Metric Change Anomaly",
                "metric_name": metric_name,
                "metric_value": pct_change,
                "severity": "HIGH",
                "message": f"{metric_name} changed by more than 20% compared to previous run."
            })

    return anomalies