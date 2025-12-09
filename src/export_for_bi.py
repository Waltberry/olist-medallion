# src/export_for_bi.py
from pathlib import Path
from pyspark.sql import functions as F
from src.spark_utils import get_spark
from src.config import GOLD_DIR, GOLD_DB

def main():
    spark = get_spark("olist-export-bi")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Revenue by region
    revenue = spark.table(f"{GOLD_DB}.revenue_by_region")
    (
        revenue
        .coalesce(1)  # small demo; 1 file per table
        .write.mode("overwrite")
        .parquet(str(GOLD_DIR / "revenue_by_region"))
    )

    # 2) Customer churn features
    churn = spark.table(f"{GOLD_DB}.customer_churn_features")
    (
        churn
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(str(GOLD_DIR / "customer_churn_features"))
    )

    # 3) Delivery SLA KPIs
    sla = spark.table(f"{GOLD_DB}.delivery_sla_kpis")
    (
        sla
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(str(GOLD_DIR / "delivery_sla_kpis"))
    )

    print("Exported gold tables to Parquet under data/gold/")

if __name__ == "__main__":
    main()
