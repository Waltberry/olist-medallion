# src/query_test.py
from src.spark_utils import get_spark

spark = get_spark("test-query")

spark.sql("SELECT * FROM olist_gold.revenue_by_region LIMIT 10").show()
