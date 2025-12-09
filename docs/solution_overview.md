# Olist E-Commerce Medallion Pipeline (PySpark + Delta)

## 1. Business Context

Our client is a Brazil-based e-commerce marketplace that wants better visibility into:

- Revenue performance by state and month
- Delivery SLA reliability and late-delivery risk
- Customer behaviour that indicates churn or high lifetime value

The goal of this project is to turn raw transactional CSVs into **curated analytics tables** using a **Bronze / Silver / Gold** medallion architecture on a local Delta Lake.

---

## 2. Data Source

Dataset: **Brazilian E-Commerce Public Dataset by Olist** (Kaggle)

Key entities:

- Customers, Orders, Order Items, Payments
- Products & Product Categories
- Sellers & Geolocation
- Order Reviews

---

## 3. Architecture Overview

The pipeline is implemented in **PySpark + Delta Lake**, running on a local laptop:

- **Bronze** (`olist_bronze`)
  - One Delta table per raw CSV
  - Minimal transformations; mostly type inference and standardization
- **Silver** (`olist_silver`)
  - Cleaned, conformed entities:
    - `customers`: customers + best geolocation per ZIP
    - `products`: products + English product category
    - `orders`: orders enriched with items, payments, delivery SLA metrics and customer region
- **Gold** (`olist_gold`)
  - Business-ready marts:
    - `revenue_by_region`: monthly revenue and late-delivery rate by customer state
    - `customer_churn_features`: RFM-style features per customer (recency, frequency, monetary)
    - `delivery_sla_kpis`: SLA KPIs by state and month

The project is structured to mirror how a real client delivery would separate ingestion, curation, and analytics.

---

## 4. Key Design Choices

### 4.1 Medallion Layers

- **Bronze** preserves raw semantics and gives us a single source of truth.
- **Silver** handles business logic:
  - Aggregates items and payments at order level
  - Derives `gross_revenue`, `delivered_late_days`, `delivered_late_flag`
  - Attaches regional attributes from the customer dimension
- **Gold** exposes small, denormalized tables tuned to typical BI queries.

### 4.2 Performance

- Delta tables are **partitioned by `customer_state`** in key Gold marts.
- Sample **ZORDER clustering** is provided for typical query predicates (`customer_state`, `order_month`).
- Query patterns are shaped so the engine can leverage:
  - Partition pruning
  - Clustered data for range scans

---

## 5. Example Use Cases

- **Finance / Sales Ops**
  - Track revenue, order volume, and average ticket size by state and month.
  - Identify underperforming regions and seasonality patterns.

- **Customer Analytics**
  - Use `customer_churn_features` as input to churn or CLV models.
  - Segment customers by recency, frequency, and monetary value.

- **Logistics / Operations**
  - Monitor late-delivery rates and average late days by state.
  - Prioritize SLA improvements where delay impact is highest.

---

## 6. How to Run

1. Create a virtual environment and install requirements:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
