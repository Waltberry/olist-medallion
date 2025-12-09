# Olist E-Commerce Medallion Pipeline (PySpark + Delta Lake)

End-to-end **medallion lakehouse** built on the  
**Brazilian E-Commerce Public Dataset by Olist** (Kaggle).

The project takes raw transactional CSVs and turns them into **Bronze / Silver / Gold**
Delta tables, then exports **Gold marts** to Parquet for easy consumption in tools
like **Power BI**.

---

## 1. Business Problem

A Brazil-based marketplace wants better visibility into:

- Revenue performance by **state** and **month**
- **Delivery SLA** reliability and late-delivery risk
- Customer behaviour that indicates **churn** or high lifetime value

This repo shows how you can answer those questions with a small but realistic
data engineering stack:

- Ingest raw CSVs → **Bronze**
- Clean and enrich entities → **Silver**
- Build business-ready marts → **Gold**
- Visualize in **Power BI** (example: *“Brazilian E-Commerce – Late Delivery Rate by State”*)

---

## 2. Architecture

Medallion layout on a local Delta Lake:

- **Bronze – `olist_bronze`**
  - One table per raw CSV
  - Minimal transformations (schema inference, standardization)
  - Examples: `customers`, `orders`, `order_items`, `order_payments`,
    `products`, `geolocation`, `sellers`, `order_reviews`

- **Silver – `olist_silver`**
  - Cleaned, conformed entities:
    - `customers`: customers + best geolocation per ZIP prefix
    - `products`: products + English product category
    - `orders`: orders enriched with items, payments, delivery SLA metrics and region

- **Gold – `olist_gold`**
  - Denormalized marts tuned for BI:
    - `revenue_by_region` – monthly revenue and late-delivery rate by customer state
    - `customer_churn_features` – RFM-style features per customer
    - `delivery_sla_kpis` – delivery SLA KPIs by state and month

Gold tables are then exported to **Parquet** under `data/gold/` for BI tools.

A more detailed narrative is available in  
`docs/solution_overview.md`.

---

## 3. Tech Stack

- **Language:** Python (PySpark)
- **Storage / Lakehouse:** Delta Lake (`delta-spark`)
- **Execution:** Local Spark (`local[*]`)
- **Orchestration style:** Simple Python modules (could be wired into Airflow, etc.)
- **BI:** Power BI Desktop (reading Parquet from `data/gold/`)
- **OS:** Tested on Windows with a lightweight Hadoop/winutils setup handled by code

---

## 4. Repository Layout

```text
data/
  raw/              # Raw CSVs from Kaggle (not tracked in Git)
  bronze/           # (Optional) path if you want to also store Delta files here
  silver/
  gold/             # Parquet exports of Gold marts for BI
docs/
  Brazilian E-Commerce – Late Delivery Rate by State.png
  solution_overview.md        # Longer write-up / design doc
metastore_db/       # Local Hive metastore used by Spark
spark-warehouse/    # Spark SQL warehouse directory
src/
  ingest/
    download_brazilian_ecommerce.py  # KaggleHub downloader
  sql/
    performance_tuning_examples.sql  # Example Delta OPTIMIZE / ZORDER script
  bronze_ingest.py       # Build Bronze Delta tables from raw CSVs
  silver_transform.py    # Build Silver dimension / fact tables
  gold_marts.py          # Build Gold marts
  export_for_bi.py       # Export Gold tables to Parquet under data/gold
  query_test.py          # Simple sanity query against olist_gold tables
  config.py              # Paths and database names
  spark_utils.py         # SparkSession factory (with Delta + Hive support)
requirements.txt
README.md
```

---

## 5. Getting Started

### 5.1 Prerequisites

* Python 3.10+
* Java 8+ (required by Spark)
* (Optional) Power BI Desktop, if you want to reproduce the visual
* (Optional) Kaggle account if you use the automated download

On Windows, `src/spark_utils.py` will configure `HADOOP_HOME`/`PATH`
for a simple `winutils` setup (`C:\hadoop`), so you don’t have to manually
export environment variables.

---

### 5.2 Setup and Install

From the repo root:

```bash
# Create & activate a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
# source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

### 5.3 Download the Olist Dataset (raw → data/raw)

Option A – **Automatic via KaggleHub**:

```bash
python -m src.ingest.download_brazilian_ecommerce
```

This downloads the **Brazilian E-Commerce Public Dataset by Olist** from Kaggle,
handles zip extraction if needed, and copies all CSVs into `data/raw/`.

> If you see a Kaggle auth/permissions error, configure your Kaggle API
> credentials as described in the Kaggle docs.

Option B – **Manual**:

1. Download the dataset from Kaggle.
2. Extract the archive.
3. Copy all `.csv` files into `data/raw/`.

---

### 5.4 Build the Medallion Layers

From the repo root (with the venv activated):

```bash
# 1) Bronze – one Delta table per raw CSV
python -m src.bronze_ingest

# 2) Silver – cleaned dimensions and enriched orders
python -m src.silver_transform

# 3) Gold – revenue, churn features, and SLA KPIs
python -m src.gold_marts
```

You can sanity-check the catalog with:

```bash
python -m src.query_test
```

or open a Python shell and run:

```python
from src.spark_utils import get_spark

spark = get_spark("ad-hoc")
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES IN olist_gold").show()
spark.table("olist_gold.revenue_by_region").show(5)
```

---

### 5.5 Export Gold Tables for BI (Parquet)

To export the Gold tables as compact Parquet files under `data/gold/`:

```bash
python -m src.export_for_bi
```

This writes:

* `data/gold/revenue_by_region/`
* `data/gold/customer_churn_features/`
* `data/gold/delivery_sla_kpis/`

Each contains a single `.parquet` file (coalesced for demo purposes).

---

## 6. Example Power BI Visual

The `docs` folder contains a sample report screenshot:

![Brazilian E-Commerce – Late Delivery Rate by State](docs/Brazilian%20E-Commerce%20–%20Late%20Delivery%20Rate%20by%20State.png)


> **Brazilian E-Commerce – Late Delivery Rate by State**
> *(Average late_delivery_rate by customer_state, filtered by order_month)*

The visual is built from the exported **`delivery_sla_kpis`** table:

* **Axis:** `customer_state`
* **Value:** `late_delivery_rate` (aggregation: Average)
* **Slicer:** `order_month` (date range)

Basic steps to reproduce:

1. In Power BI Desktop: **Get Data → Parquet**
2. Point to `data/gold/delivery_sla_kpis/…snappy.parquet`
3. Create a **Clustered column chart**:

   * X axis → `customer_state`
   * Y axis → `late_delivery_rate` (Average)
4. Add a **slicer** with `order_month` to filter the time window.

The screenshot is saved as
`docs/Brazilian E-Commerce – Late Delivery Rate by State.png`.

---

## 7. Performance Tuning (Optional)

`src/sql/performance_tuning_examples.sql` includes a few examples of how you might
tune this workload in a more production-like environment:

* Create a **partitioned Delta table** `olist_gold.revenue_by_region_opt`
  partitioned by `customer_state`
* Use **OPTIMIZE + ZORDER BY** (`customer_state`, `order_month`) on platforms
  that support Delta’s OPTIMIZE command (e.g., Databricks)
* Example query pattern that benefits from partition pruning and clustering

These are not executed by default, but they document how this model could be
lifted into a managed lakehouse.

---

## 8. Possible Extensions

Some ideas for future iterations:

* Add product / category performance marts (GMV, margin, etc.)
* Build cohort tables (first-order month, retention curves)
* Join review scores to delivery performance for CSAT / NPS analysis
* Wrap Bronze → Silver → Gold as a proper orchestration DAG (Airflow, Dagster, etc.)
* Replace local Spark with a managed Spark / Lakehouse service

---

## 9. License

This project is for educational / portfolio purposes.
The Olist dataset is subject to its original license and usage terms on Kaggle.

```

---