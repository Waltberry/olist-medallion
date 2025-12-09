# src/silver_transform.py
from pyspark.sql import functions as F, Window
from src.config import BRONZE_DB, SILVER_DB
from src.spark_utils import get_spark


def build_silver_customers(spark):
    customers = spark.table(f"{BRONZE_DB}.customers")
    geo = spark.table(f"{BRONZE_DB}.geolocation")

    # Pick one geolocation per zip prefix (simple rule: first in each group)
    w = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
        F.col("geolocation_city")
    )
    geo_dedup = (
        geo.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    silver_customers = (
        customers.alias("c")
        .join(
            geo_dedup.alias("g"),
            F.col("c.customer_zip_code_prefix")
            == F.col("g.geolocation_zip_code_prefix"),
            "left",
        )
        .select(
            "c.customer_id",
            "c.customer_unique_id",
            F.col("c.customer_city").alias("customer_city"),
            F.col("c.customer_state").alias("customer_state"),
            "c.customer_zip_code_prefix",
            "g.geolocation_lat",
            "g.geolocation_lng",
            F.col("g.geolocation_city").alias("geo_city"),
            F.col("g.geolocation_state").alias("geo_state"),
        )
    )

    silver_customers.write.format("delta").mode("overwrite").saveAsTable(
        f"{SILVER_DB}.customers"
    )


def build_silver_products(spark):
    products = spark.table(f"{BRONZE_DB}.products")
    cat_trans = spark.table(f"{BRONZE_DB}.product_category_translation")

    silver_products = (
        products.alias("p")
        .join(
            cat_trans.alias("t"),
            F.col("p.product_category_name")
            == F.col("t.product_category_name"),
            "left",
        )
        .select(
            "p.product_id",
            "p.product_category_name",
            F.col("t.product_category_name_english").alias(
                "product_category_english"
            ),
            "p.product_name_lenght",
            "p.product_description_lenght",
            "p.product_photos_qty",
            "p.product_weight_g",
            "p.product_length_cm",
            "p.product_height_cm",
            "p.product_width_cm",
        )
    )

    silver_products.write.format("delta").mode("overwrite").saveAsTable(
        f"{SILVER_DB}.products"
    )


def build_silver_orders(spark):
    orders = spark.table(f"{BRONZE_DB}.orders")
    order_items = spark.table(f"{BRONZE_DB}.order_items")
    payments = spark.table(f"{BRONZE_DB}.order_payments")
    silver_customers = spark.table(f"{SILVER_DB}.customers")

    # Aggregate order_items to order level
    items_agg = (
        order_items.groupBy("order_id")
        .agg(
            F.count("*").alias("num_items"),
            F.sum("price").alias("items_price"),
            F.sum("freight_value").alias("freight_value"),
        )
    )

    # Aggregate payments to order level
    payments_agg = (
        payments.groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("payment_total"),
            F.max("payment_type").alias("main_payment_type"),
        )
    )

    # Base order fields + cast timestamps to dates
    orders_enriched = (
        orders.alias("o")
        .join(items_agg.alias("i"), "order_id", "left")
        .join(payments_agg.alias("p"), "order_id", "left")
        .withColumn(
            "order_date", F.to_date("order_purchase_timestamp")
        )
        .withColumn(
            "delivered_date",
            F.to_date("order_delivered_customer_date"),
        )
        .withColumn(
            "estimated_delivery_date",
            F.to_date("order_estimated_delivery_date"),
        )
        .withColumn(
            "delivered_late_days",
            F.when(
                F.col("delivered_date").isNotNull(),
                F.datediff(
                    F.col("delivered_date"),
                    F.col("estimated_delivery_date"),
                ),
            ),
        )
        .withColumn(
            "delivered_late_flag",
            F.when(F.col("delivered_late_days") > 0, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "gross_revenue",
            F.col("items_price") + F.col("freight_value"),
        )
    )

    silver_orders = (
        orders_enriched.alias("o")
        .join(
            silver_customers.select(
                "customer_id", "customer_state", "customer_city"
            ).alias("c"),
            "customer_id",
            "left",
        )
        .select(
            "order_id",
            "customer_id",
            "customer_state",
            "customer_city",
            "order_status",
            "order_date",
            "delivered_date",
            "estimated_delivery_date",
            "delivered_late_days",
            "delivered_late_flag",
            "num_items",
            "items_price",
            "freight_value",
            "gross_revenue",
            "payment_total",
            "main_payment_type",
        )
    )

    silver_orders.write.format("delta").mode("overwrite").saveAsTable(
        f"{SILVER_DB}.orders"
    )


def main():
    spark = get_spark("olist-silver")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

    build_silver_customers(spark)
    build_silver_products(spark)
    build_silver_orders(spark)

    print("Silver layer build complete.")


if __name__ == "__main__":
    main()
