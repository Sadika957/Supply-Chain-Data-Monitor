import streamlit as st
import pandas as pd
import plotly.express as px
import subprocess

st.set_page_config(
    page_title="Supply Chain Data Monitoring",
    layout="wide"
)

# -----------------------------
# Load data
# -----------------------------
metrics = pd.read_csv("outputs/monitoring_metrics.csv")
alerts = pd.read_csv("outputs/anomaly_log.csv")

# -----------------------------
# Clean / convert types
# -----------------------------
metrics["run_date"] = pd.to_datetime(metrics["run_date"], errors="coerce")
alerts["run_date"] = pd.to_datetime(alerts["run_date"], errors="coerce")
metrics["metric_value"] = pd.to_numeric(metrics["metric_value"], errors="coerce")

# Remove invalid rows
metrics = metrics.dropna(subset=["run_date"])
alerts = alerts.dropna(subset=["run_date"])

# -----------------------------
# Helper function
# -----------------------------
def get_latest_metric_value(df: pd.DataFrame, metric_name: str):
    temp = df[df["metric_name"] == metric_name].dropna(subset=["metric_value"]).copy()
    temp = temp.sort_values("run_date")
    if temp.empty:
        return None
    return temp["metric_value"].iloc[-1]

# -----------------------------
# Latest KPI values
# -----------------------------
row_count = get_latest_metric_value(metrics, "row_count")
revenue = get_latest_metric_value(metrics, "total_revenue")
quantity = get_latest_metric_value(metrics, "average_quantity")
alerts_count = len(alerts)
latest_run = metrics["run_date"].max() if not metrics.empty else None
pipeline_runs = metrics["run_date"].nunique()

# -----------------------------
# Header
# -----------------------------
st.title("Supply Chain Data Monitoring Dashboard")
st.caption("Automated monitoring for dataset health and anomaly detection")

col_run, col_empty = st.columns([1,5])

with col_run:
    if st.button("Run Monitoring Pipeline"):
        with st.spinner("Running monitoring pipeline..."):
            subprocess.run(["python", "src/main.py"])
        st.success("Pipeline executed successfully!")
        st.rerun()

if latest_run is not None:
    st.caption(f"Last pipeline run: {latest_run.strftime('%Y-%m-%d %H:%M:%S')}")

# -----------------------------
# Pipeline health status
# -----------------------------
if alerts_count == 0:
    st.success("Pipeline Status: Healthy")
elif alerts_count <= 3:
    st.warning("Pipeline Status: Warning - anomalies detected")
else:
    st.error("Pipeline Status: Critical - multiple anomalies detected")

st.divider()

# -----------------------------
# KPI cards
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Records", f"{int(row_count):,}" if row_count is not None else "N/A")
col2.metric("Total Revenue", f"${float(revenue):,.0f}" if revenue is not None else "N/A")
col3.metric("Average Quantity", f"{float(quantity):.2f}" if quantity is not None else "N/A")
col4.metric("Total Alerts", int(alerts_count))

st.caption(f"Pipeline runs recorded: {pipeline_runs}")

st.divider()

# -----------------------------
# Metric Trends
# -----------------------------
st.subheader("Metric Trends")

row_df = metrics[metrics["metric_name"] == "row_count"].dropna(subset=["metric_value"]).copy()
rev_df = metrics[metrics["metric_name"] == "total_revenue"].dropna(subset=["metric_value"]).copy()
qty_df = metrics[metrics["metric_name"] == "average_quantity"].dropna(subset=["metric_value"]).copy()

c1, c2, c3 = st.columns(3)

fig1 = px.line(
    row_df,
    x="run_date",
    y="metric_value",
    title="Row Count Trend",
    markers=True,
)
fig1.update_layout(
    xaxis_title="Run Date",
    yaxis_title="Row Count"
)

fig2 = px.line(
    rev_df,
    x="run_date",
    y="metric_value",
    title="Revenue Trend",
    markers=True,
)
fig2.update_layout(
    xaxis_title="Run Date",
    yaxis_title="Revenue"
)

fig3 = px.line(
    qty_df,
    x="run_date",
    y="metric_value",
    title="Average Quantity Trend",
    markers=True,
)
fig3.update_layout(
    xaxis_title="Run Date",
    yaxis_title="Average Quantity"
)

c1.plotly_chart(fig1, use_container_width=True)
c2.plotly_chart(fig2, use_container_width=True)
c3.plotly_chart(fig3, use_container_width=True)

st.divider()

# -----------------------------
# Anomaly Monitoring
# -----------------------------
st.subheader("Anomaly Monitoring")

if not alerts.empty:
    left, right = st.columns(2)

    severity_fig = px.pie(
        alerts,
        names="severity",
        title="Alert Severity Distribution",
        color="severity",
        color_discrete_map={
            "HIGH": "red",
            "MEDIUM": "orange",
            "LOW": "green"
        }
    )

    type_fig = px.bar(
        alerts,
        x="anomaly_type",
        color="severity",
        color_discrete_map={
            "HIGH": "red",
            "MEDIUM": "orange",
            "LOW": "green"
        },
        title="Alert Types",
        text_auto=True
    )
    type_fig.update_layout(
        xaxis_title="Anomaly Type",
        yaxis_title="Count"
    )

    left.plotly_chart(severity_fig, use_container_width=True)
    right.plotly_chart(type_fig, use_container_width=True)

    st.subheader("Alert Log")

    display_cols = [
        "anomaly_type",
        "metric_name",
        "metric_value",
        "severity",
        "message",
        "run_date",
    ]

    st.dataframe(
        alerts.sort_values("run_date", ascending=False)[display_cols],
        use_container_width=True,
        hide_index=True
    )
else:
    st.success("No anomalies detected.")