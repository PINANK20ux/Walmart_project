# walmart_db — dbt Lakehouse Transformation Core

This folder contains the core **dbt** project for the **Walmart Lakehouse Data Platform**, configured for **Databricks Delta Lake**.

> 📖 **Architecture & Platform Documentation**: For full business context, Medallion layer details, data flow diagrams, and security guidelines, see the root [README.md](file:///c:/Users/krish/Documents/Code/Walmart_database/README.md).

---

## 🏗️ Model Organization

```text
models/
├── source/
│   └── source.yml         # Bronze raw external Delta table sources
├── silver_tech/           # Incremental ingestion with watermark deduplication
│   ├── customer_tech.sql  # unique_key: customer_id
│   ├── stores_tech.sql    # unique_key: store_id
│   ├── product_tech.sql   # unique_key: product_id
│   ├── employee_tech.sql  # unique_key: employee_id
│   ├── orders_tech.sql    # unique_key: order_id
│   ├── order_items_tech.sql # unique_key: order_item_id
│   └── propeties.yml      # Schema tests (unique, not_null, conditional filters)
├── silver_b/
│   └── obt.sql            # Dynamic Jinja meta-programming One Big Table (OBT)
└── gold/
    ├── ephemeral/         # Ephemeral staging queries feeding SCD2 snapshots
    │   ├── eph_customers.sql
    │   ├── eph_stores.sql
    │   ├── eph_products.sql
    │   ├── eph_employee.sql
    │   └── eph_orders.sql
    └── fact/
        └── fact_orders.sql # Core transactional fact table at order item grain
```

---

## ⏳ Snapshots (SCD Type 2)

Snapshots track entity history using timestamp-based change detection with active records assigned to `9999-12-31`:
- `snapshots/dim_customer.yml`
- `snapshots/dim_stores.yml`
- `snapshots/dim_products.yml`
- `snapshots/dim_employee.yml`
- `snapshots/dim_orders.yml`

Execute snapshots using:
```bash
dbt snapshot
```

---

## 🔒 Configuration & Profiles

Do **not** commit credentials or tokens into Git. Use the sanitized template:
```bash
# Copy template
cp profiles.sample.yml profiles.yml

# Or copy to ~/.dbt/profiles.yml
cp profiles.sample.yml ~/.dbt/profiles.yml
```
Supply credentials via environment variables:
- `DBT_DATABRICKS_HOST`
- `DBT_DATABRICKS_HTTP_PATH`
- `DBT_DATABRICKS_TOKEN`

---

## ⚡ Common dbt Commands

```bash
# Test connection
dbt debug

# Run incremental silver layer
dbt run --select silver_tech

# Rebuild incrementals from scratch
dbt run --select silver_tech --full-refresh

# Run OBT and downstream gold models
dbt run --select obt+

# Run referential integrity and schema tests
dbt test

# Execute full build (run, snapshot, test)
dbt build
```
