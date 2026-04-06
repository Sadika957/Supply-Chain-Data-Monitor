from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DATA_PATH = BASE_DIR / "data" / "raw" / "supply_chain_bad.csv"
DB_PATH = BASE_DIR / "database" / "supply_chain.db"
OUTPUT_DIR = BASE_DIR / "outputs"

RAW_TABLE_NAME = "raw_orders"
METRICS_TABLE_NAME = "monitoring_metrics"
ANOMALY_TABLE_NAME = "anomaly_log"