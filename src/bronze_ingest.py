# src/bronze_ingest.py
from pyspark.sql import functions as F
from src.config import RAW_DATA_DIR, BRONZE_DB
from src.spark_utils import get_spark


def read_csv(spark, filename: str):
    """
    Utility to read a CSV with header + schema inference from data/raw.
    """
    path = RAW_DATA_DIR / filename
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(path))
    )


def main():
    spark = get_spark("olist-bronze")

    # Create bronze database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

    # ---- Customers ----
    customers = read_csv(spark, "olist_customers_dataset.csv")
    customers.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.customers"
    )

    # ---- Orders ----
    orders = read_csv(spark, "olist_orders_dataset.csv")
    orders.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.orders"
    )

    # ---- Order items ----
    order_items = read_csv(spark, "olist_order_items_dataset.csv")
    order_items.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.order_items"
    )

    # ---- Payments ----
    payments = read_csv(spark, "olist_order_payments_dataset.csv")
    payments.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.order_payments"
    )

    # ---- Products ----
    products = read_csv(spark, "olist_products_dataset.csv")
    products.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.products"
    )

    # ---- Category translations ----
    category_translation = read_csv(
        spark, "product_category_name_translation.csv"
    )
    category_translation.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.product_category_translation"
    )

    # ---- Geolocation ----
    geolocation = read_csv(spark, "olist_geolocation_dataset.csv")
    geolocation.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.geolocation"
    )

    # ---- Sellers ----
    sellers = read_csv(spark, "olist_sellers_dataset.csv")
    sellers.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.sellers"
    )

    # ---- Reviews (optional, for SLA / CSAT later) ----
    reviews = read_csv(spark, "olist_order_reviews_dataset.csv")
    reviews.write.format("delta").mode("overwrite").saveAsTable(
        f"{BRONZE_DB}.order_reviews"
    )

    print("Bronze ingestion complete.")


if __name__ == "__main__":
    main()
