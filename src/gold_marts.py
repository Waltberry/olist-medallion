# src/gold_marts.py
from pyspark.sql import functions as F
from src.config import SILVER_DB, GOLD_DB
from src.spark_utils import get_spark


def build_gold_revenue_by_region(spark):
    orders = spark.table(f"{SILVER_DB}.orders")

    revenue_by_region = (
        orders.filter(F.col("order_status") == "delivered")
        .withColumn(
            "order_month", F.date_trunc("month", F.col("order_date"))
        )
        .groupBy("customer_state", "order_month")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.sum("gross_revenue").alias("gross_revenue"),
            F.avg("gross_revenue").alias("avg_order_value"),
            F.sum("delivered_late_flag").alias("late_orders"),
        )
        .withColumn(
            "late_delivery_rate",
            F.col("late_orders") / F.col("num_orders"),
        )
    )

    (
        revenue_by_region.write.format("delta")
        .mode("overwrite")
        .partitionBy("customer_state")
        .saveAsTable(f"{GOLD_DB}.revenue_by_region")
    )


def build_gold_customer_churn_features(spark):
    """
    Classic RFM-style features:
    - recency_days
    - num_orders
    - total_revenue
    - avg_order_value
    """
    orders = spark.table(f"{SILVER_DB}.orders")

    # Only count delivered orders for “true” revenue behaviour
    delivered = orders.filter(F.col("order_status") == "delivered")

    # Use max order date as reference
    max_date = delivered.agg(
        F.max("order_date").alias("max_order_date")
    ).first()["max_order_date"]

    rfm = (
        delivered.groupBy("customer_id", "customer_state")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.sum("gross_revenue").alias("total_revenue"),
            F.max("order_date").alias("last_order_date"),
            F.min("order_date").alias("first_order_date"),
        )
        .withColumn(
            "recency_days",
            F.datediff(F.lit(max_date), F.col("last_order_date")),
        )
        .withColumn(
            "avg_order_value",
            F.col("total_revenue") / F.col("num_orders"),
        )
    )

    rfm.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD_DB}.customer_churn_features"
    )


def build_gold_delivery_sla_kpis(spark):
    orders = spark.table(f"{SILVER_DB}.orders")

    sla = (
        orders.filter(F.col("order_status").isin("delivered", "shipped"))
        .withColumn(
            "order_month", F.date_trunc("month", F.col("order_date"))
        )
        .groupBy("customer_state", "order_month")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.avg("delivered_late_days").alias("avg_late_days"),
            F.sum("delivered_late_flag").alias("late_orders"),
        )
        .withColumn(
            "late_delivery_rate",
            F.col("late_orders") / F.col("num_orders"),
        )
    )

    (
        sla.write.format("delta")
        .mode("overwrite")
        .partitionBy("customer_state")
        .saveAsTable(f"{GOLD_DB}.delivery_sla_kpis")
    )


def main():
    spark = get_spark("olist-gold")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

    build_gold_revenue_by_region(spark)
    build_gold_customer_churn_features(spark)
    build_gold_delivery_sla_kpis(spark)

    print("Gold marts built.")


if __name__ == "__main__":
    main()
