-- Example 1: Create a partitioned Delta table for revenue mart
CREATE TABLE IF NOT EXISTS olist_gold.revenue_by_region_opt
USING delta
PARTITIONED BY (customer_state)
AS
SELECT *
FROM olist_gold.revenue_by_region;

-- Example 2: Optimize + ZORDER-like clustering on (customer_state, order_month)
-- (Databricks / Delta Lake with OPTIMIZE support)
OPTIMIZE olist_gold.revenue_by_region_opt
ZORDER BY (customer_state, order_month);

-- Example 3: Query pattern that benefits from partition pruning
SELECT
  customer_state,
  order_month,
  num_orders,
  gross_revenue
FROM olist_gold.revenue_by_region_opt
WHERE customer_state = 'SP'
  AND order_month BETWEEN '2017-01-01' AND '2017-06-01';

-- If you load customer_churn_features into a warehouse (e.g., Postgres),
-- you might create an index on customer_id + recency_days:
-- (Not Delta syntax, but classic warehouse example)

-- CREATE INDEX idx_churn_customer_recency
--   ON customer_churn_features (customer_id, recency_days);
