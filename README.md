# Walmart Lakehouse Data Platform 🛒📊

[![dbt](https://img.shields.io/badge/dbt-Core%20v1.8+-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.3.2-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Neon%20DB-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://neon.tech/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20v2+-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-0071DC?style=for-the-badge)](#-lakehouse-architecture--data-flow)
[![Modeling](https://img.shields.io/badge/Modeling-Kimball%20%2B%20OBT%20%2B%20SCD2-059669?style=for-the-badge)](#-detailed-layer-breakdown--engineering-decisions)
[![Security](https://img.shields.io/badge/Security-Zero--Credential%20Enforced-34D399?style=for-the-badge)](#-security--credential-management)

An enterprise-grade **Medallion Lakehouse & Modern Data Stack** platform engineered with **PostgreSQL (Neon DB)**, **Databricks Delta Lake**, **dbt Core**, and **Apache Airflow**. This platform models omnichannel retail point-of-sale (POS) and master enterprise entities into high-performance analytical marts, automated slowly changing dimensions (SCD Type 2), and a denormalized One Big Table (OBT), fully orchestrated inside a containerized pipeline.

---

## 📌 Executive Summary & Business Context

In large-scale omnichannel retail, transactions take place across thousands of physical brick-and-mortar stores and digital touchpoints simultaneously. Delivering real-time operational reporting, inventory allocation, and customer intelligence poses four major challenges:

1. **Transaction Granularity vs. Analytical Performance**: Relational point-of-sale systems isolate transactions across normalized tables (`stores`, `customers`, `products`, `employees`, `orders`, `order_items`). Querying these normalized transactional schemas in Business Intelligence (BI) tools causes heavy join latency, cluster memory pressure, and sluggish dashboards.
2. **Historical State Drift (The SCD Problem)**: Over time, customers relocate, store associates change roles, stores undergo restructuring, and product prices fluctuate. Overwriting historical attributes (SCD Type 1) skews historical revenue attribution (e.g., attributing a 2023 transaction to a customer's 2025 address or computing margins using an updated retail price).
3. **Freshness vs. Compute Optimization**: Repeatedly rescanning gigabytes of historical data on every run degrades cluster performance and inflates compute bills. The technical transformation tier must ingest new and modified records incrementally using high-watermark delta merging.
4. **End-to-End Orchestration & Integrity Gates**: Ingestion, data freshness validation, incremental transforms, snapshot versioning, and referential integrity testing must run in a deterministic, observable sequence with automated failure alerting.

### What This Platform Delivers
- **Automated Source Provisioning**: Python ETL pipeline ([`load_data.py`](file:///c:/Users/krish/Documents/Code/Walmart_database/Walmart_dataset/load_data.py)) reading source CSV extracts and loading them with schema validation into a cloud Neon PostgreSQL database.
- **Change Data Capture (CDC) Ingestion**: Databricks Job triggering external CDC extraction into Bronze Delta Lake landing tables (`walmart.bronze.*`).
- **Incremental Technical Ingestion**: Watermark-driven incremental tables in `silver_tech` that merge only newly created or modified records.
- **Unified Analytical Substrate (One Big Table - OBT)**: A consolidated, denormalized wide table combining 6 retail entity domains into a single source of truth, eliminating multi-table joins for downstream analysts.
- **Dynamic Jinja Metaprogramming**: Declarative configuration pattern building wide joins and column projections dynamically, eliminating fragile handwritten SQL.
- **Audit-Proof Historical Tracking (SCD Type 2)**: Automated snapshot models in `gold` recording attribute histories with exact validity windows (`dbt_valid_from` to `dbt_valid_to`), assigning active records `9999-12-31`.
- **Granular Fact Tables**: Transactional order-item grain marts powering KPIs like Average Order Value (AOV), basket size, store efficiency, and category margins.
- **Containerized Airflow Orchestration**: A scalable Apache Airflow 3 cluster (CeleryExecutor, PostgreSQL, Redis) running the full workflow DAG ([`orchestrate.py`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/dags/orchestrate.py)) with sequential health checks and tests.

---

## 🏛️ Lakehouse Architecture & Data Flow

```mermaid
flowchart TD
    subgraph SourceSystem ["0. Source System (OLTP • Neon PostgreSQL)"]
        CSV["CSV Extracts<br/>(customers, stores, products,<br/>employees, orders, order_items)"]
        LOADER["<b>Python Ingestion Engine</b><br/>(load_data.py • SQLAlchemy)"]
        NEON[("Neon PostgreSQL<br/><i>public schema</i>")]
        CSV --> LOADER --> NEON
    end

    subgraph AirflowStack ["Apache Airflow Orchestration (Docker Compose • CeleryExecutor)"]
        DAG["DAG: <b>orchestrate</b> (dags/orchestrate.py)"]
    end

    subgraph Bronze ["1. Bronze Layer (Raw Ingestion • Databricks Delta Lake)"]
        CDC_JOB["Databricks CDC Job<br/><i>(Triggered via Databricks SDK)</i>"]
        B_C[("bronze.customers")]
        B_S[("bronze.stores")]
        B_P[("bronze.products")]
        B_E[("bronze.employees")]
        B_O[("bronze.orders")]
        B_OI[("bronze.order_items")]
        NEON -.->|CDC / Replication| CDC_JOB
        CDC_JOB --> B_C & B_S & B_P & B_E & B_O & B_OI
    end

    subgraph SilverTech ["2. Silver Technical Layer (Incremental Cleansing • silver_tech)"]
        FRESH["<b>dbt source freshness</b><br/><i>(Validates Bronze arrival SLAs)</i>"]
        ST_C["customer_tech<br/><i>Incremental merge by customer_id</i>"]
        ST_S["stores_tech<br/><i>Incremental merge by store_id</i>"]
        ST_P["product_tech<br/><i>Incremental merge by product_id</i>"]
        ST_E["employee_tech<br/><i>Incremental merge by employee_id</i>"]
        ST_O["orders_tech<br/><i>Incremental merge by order_id</i>"]
        ST_OI["order_items_tech<br/><i>Incremental merge by order_item_id</i>"]
        TEST_TECH{{"dbt test --select silver_tech<br/><i>PK Uniqueness & Non-null</i>"}}
    end

    subgraph SilverBus ["3. Silver Business Layer (Denormalization • silver_b)"]
        OBT["<b>obt (One Big Table)</b><br/><i>Denormalized retail substrate</i><br/><i>Dynamic Jinja Metaprogramming</i>"]
        TEST_OBT{{"dbt test --select silver_b<br/><i>Referential Integrity Check</i>"}}
    end

    subgraph GoldEph ["Gold Staging (Ephemeral)"]
        GE_C["eph_customers"]
        GE_S["eph_stores"]
        GE_P["eph_products"]
        GE_E["eph_employee"]
        GE_O["eph_orders"]
    end

    subgraph Gold ["4. Gold Serving Layer (Dimensional Marts & Snapshots • gold)"]
        FACT["<b>fact_orders</b><br/><i>Item-grain transactional fact</i><br/><i>Line amounts, discounts, metrics</i>"]
        DIM_C[("dim_customer<br/><i>SCD Type 2 Snapshot</i>")]
        DIM_S[("dim_stores<br/><i>SCD Type 2 Snapshot</i>")]
        DIM_P[("dim_products<br/><i>SCD Type 2 Snapshot</i>")]
        DIM_E[("dim_employee<br/><i>SCD Type 2 Snapshot</i>")]
        DIM_O[("dim_orders<br/><i>SCD Type 2 Snapshot</i>")]
    end

    %% Flow connections
    DAG -->|1. Trigger CDC Ingest| CDC_JOB
    CDC_JOB --> FRESH
    FRESH --> ST_C & ST_S & ST_P & ST_E & ST_O & ST_OI
    ST_C & ST_S & ST_P & ST_E & ST_O & ST_OI --> TEST_TECH
    TEST_TECH --> OBT
    OBT --> TEST_OBT
    TEST_OBT --> GE_C & GE_S & GE_P & GE_E & GE_O
    TEST_OBT --> FACT
    GE_C --> DIM_C
    GE_S --> DIM_S
    GE_P --> DIM_P
    GE_E --> DIM_E
    GE_O --> DIM_O

    %% Styling
    classDef source fill:#F1F5F9,stroke:#64748B,stroke-width:1.5px,color:#0F172A;
    classDef bronze fill:#FEF3C7,stroke:#D97706,stroke-width:1.5px,color:#78350F;
    classDef silverTech fill:#E0E7FF,stroke:#4F46E5,stroke-width:1.5px,color:#312E81;
    classDef silverBus fill:#E0F2FE,stroke:#0284C7,stroke-width:2px,color:#0369A1;
    classDef gold fill:#D1FAE5,stroke:#059669,stroke-width:2px,color:#065F46;
    classDef airflow fill:#FDF2F8,stroke:#DB2777,stroke-width:2px,color:#831843;

    class CSV,LOADER,NEON source;
    class CDC_JOB,B_C,B_S,B_P,B_E,B_O,B_OI bronze;
    class FRESH,ST_C,ST_S,ST_P,ST_E,ST_O,ST_OI,TEST_TECH silverTech;
    class OBT,TEST_OBT silverBus;
    class GE_C,GE_S,GE_P,GE_E,GE_O,FACT,DIM_C,DIM_S,DIM_P,DIM_E,DIM_O gold;
    class DAG airflow;
```

---

## 🔍 Detailed Layer Breakdown & Engineering Decisions

### 0. Source Layer: Neon PostgreSQL (`Walmart_dataset`)
- **Role**: Transactional relational database representing point-of-sale checkouts, master merchandise catalogs, and associate staffing.
- **Relational DDL**: Defined in [`Walmart_dataset/ddl/walmart_schema.sql`](file:///c:/Users/krish/Documents/Code/Walmart_database/Walmart_dataset/ddl/walmart_schema.sql).
- **Automated Loader**: [`Walmart_dataset/load_data.py`](file:///c:/Users/krish/Documents/Code/Walmart_database/Walmart_dataset/load_data.py) utilizes SQLAlchemy and Pandas to:
  1. Parse the schema definition and safely execute `CREATE TABLE IF NOT EXISTS`.
  2. Ingest raw CSV data in dependency-safe order (`stores` -> `customers` -> `products` -> `employees` -> `orders` -> `order_items`).
  3. Validate database row counts against source CSV files to guarantee complete ingestion.

| Table | Entity Domain | Primary Key | Attributes & Business Role |
| :--- | :--- | :--- | :--- |
| `stores` | Retail Locations | `store_id` | Physical store locations, city, province, country, active flag |
| `customers` | Customer Identity | `customer_id` | Profile demographics, contact info, city, province, country |
| `products` | Merchandising | `product_id` | SKU names, retail categories, brand naming, unit price |
| `employees` | Workforce | `employee_id` | Store associate staffing, job titles, base salary compensation |
| `orders` | POS Transactions | `order_id` | Checkout events, timestamp, payment method, order status, total cost |
| `order_items` | Basket Granularity | `order_item_id` | Individual product rows per order, quantity, unit price, line total |

---

### 1. Bronze Layer (`walmart.bronze`)
- **Role**: Raw landing zone for operational data ingested into Databricks Delta Lake.
- **Design Philosophy**: Unaltered schema fidelity. Data is preserved with historical fidelity for auditability and full pipeline replays.
- **Source Declaration**: Documented and verified in [`source.yml`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/models/source/source.yml).

---

### 2. Silver Technical Layer (`walmart.silver_tech`)
- **Role**: Incremental deduplication, delta upserting, data type standardization, and lineage tagging.
- **Engineering Highlights**:
  - **Delta MERGE Incremental Materialization**: Uses dbt's `incremental` materialization with `unique_key`. When new records arrive, existing rows are merged and new rows are inserted without scanning unchanged historical partitions.
  - **High-Watermark Filtering**: Compares incoming `updated_timestamp` against the target table's maximum timestamp:
    ```sql
    {% if is_incremental() %}
        WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
    {% endif %}
    ```
  - **Audit Lineage**: Injects `current_timestamp() AS processed_at` to record exact ingestion time into the lakehouse.
  - **Automated Data Quality Tests**: Configured in [`propeties.yml`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/models/silver_tech/propeties.yml) for primary key uniqueness, non-null values, and domain constraints (`price > 0`).

---

### 3. Silver Business Layer: One Big Table (`walmart.silver_b`)
- **Role**: High-performance denormalized retail substrate unifying all 6 entity domains.
- **Model**: [`models/silver_b/obt.sql`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/models/silver_b/obt.sql).

#### Why One Big Table (OBT)?
Querying 6 normalized tables across high-cardinality retail transactions creates major drawbacks:
1. **Query Overhead**: Business dashboards in Power BI or Tableau re-execute expensive shuffle joins on every user filter change.
2. **Semantic Drift**: Analysts write slight variations of multi-table joins (e.g. inner vs. left join), producing conflicting revenue metrics.
3. **Compute Costs**: Unnecessary compute credits are spent computing identical join topologies repeatedly.

Pre-computing the **One Big Table (OBT)** gives BI tools and ad-hoc analysts an instant, single-table query surface.

#### Dynamic Jinja Metaprogramming
Rather than maintaining over 140 lines of error-prone SQL `LEFT JOIN` clauses, `obt.sql` uses a declarative configuration array:
- Each joined entity is specified as a dictionary containing its table reference, alias, join key, and selected columns.
- Jinja unpacks the column list and builds the `LEFT JOIN` clauses programmatically.
- Adding attributes or new source entities requires editing only the configuration array.

#### Referential Integrity Gate
To ensure joins produce zero orphan records or cartesian fan-out, [`test_obt.sql`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/tests/test_obt.sql) verifies foreign key integrity across all dimensions:
```sql
SELECT 1 FROM {{ ref('obt') }} AS obt_b
WHERE obt_b.order_id IS NULL
   OR obt_b.product_id IS NULL
   OR obt_b.store_id IS NULL
   OR obt_b.employee_id IS NULL
   OR obt_b.customer_id IS NULL
   OR obt_b.order_item_id IS NULL;
```

---

### 4. Gold Serving Layer (`walmart.gold`)
- **Role**: Kimball-style dimensional star schema and SCD Type 2 tables powering reporting and audit analytics.

#### Transactional Fact: `fact_orders`
- **Model**: [`models/gold/fact/fact_orders.sql`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/models/gold/fact/fact_orders.sql)
- **Granularity**: One row per item inside a customer order (`order_item_id`).
- **Measures**: `quantity`, `unit_price`, `line_amount`, `total_amount`.
- **Dimensions**: Foreign keys linking to `order_id`, `product_id`, `store_id`, `employee_id`, and `customer_id`.

#### Slowly Changing Dimensions (SCD Type 2) Snapshots
Retail entity attributes change dynamically:
- Customers relocate to new cities and provinces.
- Associates receive promotions or department transfers.
- Product retail prices adjust based on seasonal promotions.

If these updates overwrite past values, historical sales attribution is corrupted. The platform employs dbt snapshots defined in [`snapshots/*.yml`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/snapshots/):
- **Change Detection**: Strategy `timestamp` tracks mutations via `updated_timestamp`.
- **Active Record Identification**: Uses `dbt_valid_to_current: "to_date('9999-12-31')"`, allowing analysts to query current rows with `WHERE dbt_valid_to = '9999-12-31'` or execute point-in-time joins:

```sql
-- Point-in-time join: Attributing revenue to customer location & product price at transaction date
SELECT 
    f.order_id,
    f.line_amount,
    c.customer_city,
    p.product_name,
    p.price AS price_at_sale_date
FROM walmart.gold.fact_orders f
JOIN walmart.gold.dim_customer c
  ON f.customer_id = c.customer_id
 AND f.order_timestamp >= c.dbt_valid_from
 AND f.order_timestamp < c.dbt_valid_to
JOIN walmart.gold.dim_products p
  ON f.product_id = p.product_id
 AND f.order_timestamp >= p.dbt_valid_from
 AND f.order_timestamp < p.dbt_valid_to;
```

---

### 5. Orchestration Layer (`airflow_dbt_project`)
- **Role**: Centralized workflow orchestration and automated scheduling.
- **DAG**: [`airflow_dbt_project/dags/orchestrate.py`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/dags/orchestrate.py)
- **Schedule**: `0 11 * * *` (Daily at 11:00 UTC).
- **Execution Pipeline**:
  ```text
  ingest_cdc 
    └──> source_freshness 
          └──> silver_technical 
                └──> silver_technical_test 
                      └──> silver_business 
                            └──> silver_business_test 
                                  └──> gold_ephemeral 
                                        └──> gold_dimensional 
                                              └──> gold_fact
  ```
- **Task Descriptions**:
  1. `ingest_cdc`: Connects to Databricks Workspace via `databricks.sdk` to trigger external CDC ingestion and poll for success.
  2. `source_freshness`: Executes `dbt source freshness` inside the containerized dbt project to verify source data arrival SLAs.
  3. `silver_technical` & `silver_technical_test`: Executes incremental merges for `silver_tech` and runs primary key tests.
  4. `silver_business` & `silver_business_test`: Materializes the `obt` model and enforces referential integrity testing.
  5. `gold_ephemeral`: Compiles and runs ephemeral staging views for downstream models.
  6. `gold_dimensional`: Executes `dbt snapshot` to update SCD Type 2 dimension validity intervals.
  7. `gold_fact`: Materializes the production transactional `fact_orders` table.

---

## 📁 Repository Structure

```text
Walmart_database/
├── .gitignore                            # Multi-tier security, secrets, Airflow & build ignore rules
├── README.md                             # Comprehensive platform engineering documentation
├── Walmart_dataset/                      # Source data assets, relational DDL & ingestion scripts
│   ├── .env.example                      # PostgreSQL / Neon DB connection template
│   ├── data/                             # Raw CSV extracts
│   │   ├── customers.csv                 # Customer identity records
│   │   ├── employees.csv                 # Store associate records
│   │   ├── order_items.csv               # Basket line items
│   │   ├── orders.csv                    # Transaction headers
│   │   ├── products.csv                  # Merchandising SKU catalog
│   │   └── stores.csv                    # Retail store registry
│   ├── ddl/
│   │   └── walmart_schema.sql            # PostgreSQL relational DDL specification
│   └── load_data.py                      # Automated Neon DB schema creation & CSV ingestion
└── airflow_dbt_project/                  # Orchestration & Lakehouse transformation stack
    ├── Dockerfile                        # Airflow custom extended image (dbt-core, Databricks, PostgreSQL)
    ├── docker-compose.yaml               # Apache Airflow 3.3.2 CeleryExecutor cluster (Redis, Postgres)
    ├── requirements.txt                  # Python dependencies for Airflow tasks & dbt-databricks
    ├── .env.example                      # Airflow UID & Fernet Key environment template
    ├── config/                           # Airflow configuration mount directory
    ├── dags/
    │   └── orchestrate.py                # End-to-end orchestration DAG (CDC -> dbt pipeline)
    ├── plugins/                          # Custom Airflow plugins directory
    └── walmart_db/                       # Core dbt transformation project on Databricks Delta Lake
        ├── dbt_project.yml               # dbt project configuration & schema routing
        ├── profiles.sample.yml           # Zero-credential Databricks connection profile template
        ├── macros/
        │   └── custom_schema.sql         # Custom schema resolution macro
        ├── models/
        │   ├── source/
        │   │   └── source.yml            # Bronze Delta Lake source definitions & freshness
        │   ├── silver_tech/              # Incremental technical models with watermark logic
        │   │   ├── customer_tech.sql
        │   │   ├── employee_tech.sql
        │   │   ├── order_items_tech.sql
        │   │   ├── orders_tech.sql
        │   │   ├── product_tech.sql
        │   │   ├── stores_tech.sql
        │   │   └── propeties.yml         # Schema tests & data quality assertions
        │   ├── silver_b/
        │   │   └── obt.sql               # Dynamic Jinja-generated One Big Table (OBT)
        │   └── gold/
        │       ├── ephemeral/            # Ephemeral staging models for snapshot isolation
        │       │   ├── eph_customers.sql
        │       │   ├── eph_employee.sql
        │       │   ├── eph_orders.sql
        │       │   ├── eph_products.sql
        │       │   └── eph_stores.sql
        │       └── fact/
        │           └── fact_orders.sql   # Transactional fact table at order_item grain
        ├── snapshots/                    # SCD Type 2 YAML snapshot definitions
        │   ├── dim_customer.yml
        │   ├── dim_employee.yml
        │   ├── dim_orders.yml
        │   ├── dim_products.yml
        │   └── dim_stores.yml
        └── tests/
            └── test_obt.sql              # OBT foreign key referential integrity test
```

---

## 🔒 Security & Credential Management

This project strictly enforces a **Zero-Credential Policy**:
- **Ignored Files**: All sensitive connection parameters (`profiles.yml`, `.env`, `airflow.cfg`, `.user.yml`, tokens, certificates, and runtime logs) are gitignored in [`.gitignore`](file:///c:/Users/krish/Documents/Code/Walmart_database/.gitignore).
- **Environment Templates**:
  - PostgreSQL / Neon DB: Configure via [`Walmart_dataset/.env.example`](file:///c:/Users/krish/Documents/Code/Walmart_database/Walmart_dataset/.env.example).
  - Apache Airflow: Configure via [`airflow_dbt_project/.env.example`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/.env.example).
  - Databricks dbt Profile: Configure via [`airflow_dbt_project/walmart_db/profiles.sample.yml`](file:///c:/Users/krish/Documents/Code/Walmart_database/airflow_dbt_project/walmart_db/profiles.sample.yml).
- **CI/CD & Secret Injection**: In production and automated runners, inject credentials using environment variables:
  ```bash
  export DBT_DATABRICKS_HOST="dbc-xxxx.cloud.databricks.com"
  export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxx"
  export DBT_DATABRICKS_TOKEN="dapi_your_access_token_here"
  ```
- **Airflow Best Practice**: In production, Databricks tokens and credentials should be stored in Airflow Connections or HashiCorp Vault / AWS Secrets Manager rather than plaintext inside task callables.

---

## 🚀 Operations & Execution Guide

### 1. Prerequisites
- Docker Engine & Docker Compose (v2.0+)
- Python 3.10+
- Active Databricks SQL Warehouse or Unity Catalog compute cluster
- Neon PostgreSQL instance (or local PostgreSQL 14+)

---

### Phase 1: Ingesting Source Data into PostgreSQL (Neon DB)

1. Navigate to the dataset directory and configure your environment:
   ```bash
   cd Walmart_dataset
   cp .env.example .env
   ```
2. Edit `.env` with your Neon PostgreSQL connection string:
   ```env
   POSTGRES_CONN=postgresql://username:password@ep-xyz.neon.tech/neondb?sslmode=require
   ```
3. Install Python dependencies and run the automated loader:
   ```bash
   pip install pandas sqlalchemy psycopg2-binary python-dotenv
   python load_data.py
   ```
4. Verify the output summary confirms that all tables (`stores`, `customers`, `products`, `employees`, `orders`, `order_items`) report `[MATCH]`.

---

### Phase 2: Standalone dbt Execution (Local Development)

1. Navigate to the dbt project:
   ```bash
   cd airflow_dbt_project/walmart_db
   ```
2. Create your local profile from the sample:
   ```bash
   cp profiles.sample.yml profiles.yml
   ```
3. Fill in your Databricks SQL warehouse credentials in `profiles.yml` or set environment variables:
   ```bash
   export DBT_DATABRICKS_HOST="dbc-xxxx.cloud.databricks.com"
   export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxx"
   export DBT_DATABRICKS_TOKEN="dapi_xxxx"
   ```
4. Test connectivity and run the pipeline:
   ```bash
   # Validate connection
   dbt debug

   # Check source data arrival freshness
   dbt source freshness

   # Execute incremental Silver Technical layer
   dbt run --select silver_tech

   # Run Silver Technical data quality tests
   dbt test --select silver_tech

   # Materialize the One Big Table (OBT)
   dbt run --select silver_b

   # Verify referential integrity
   dbt test --select silver_b

   # Capture SCD Type 2 dimension snapshots
   dbt snapshot

   # Build transactional Gold Fact tables
   dbt run --select gold/fact

   # Or run all models, snapshots, and tests in one step
   dbt build
   ```

---

### Phase 3: Full Orchestration with Apache Airflow (Docker Compose)

1. Navigate to the Airflow project directory:
   ```bash
   cd airflow_dbt_project
   ```
2. Generate your Airflow environment file:
   ```bash
   cp .env.example .env
   ```
3. Set your `AIRFLOW_UID` and generate a Fernet Key:
   ```bash
   # On Linux / macOS:
   echo "AIRFLOW_UID=$(id -u)" > .env
   
   # Generate Fernet key using Python:
   python -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())" >> .env
   ```
4. Configure Databricks dbt profile for the Airflow container:
   ```bash
   cp walmart_db/profiles.sample.yml walmart_db/profiles.yml
   ```
5. Build and launch the Airflow container cluster:
   ```bash
   docker compose up -d --build
   ```
6. Verify service health:
   ```bash
   docker compose ps
   ```
7. Access the Airflow Web UI:
   - **URL**: [http://localhost:8080](http://localhost:8080)
   - **Default Username**: `airflow`
   - **Default Password**: `airflow`
8. Trigger the DAG:
   - Navigate to the **`orchestrate`** DAG in the Airflow UI.
   - Unpause the DAG and click **Trigger DAG**.
   - Monitor the execution graph through CDC ingestion, dbt tests, incremental runs, and snapshots.
9. Cluster maintenance and tear-down:
   ```bash
   # View container logs
   docker compose logs -f airflow-scheduler

   # Stop all services
   docker compose down
   ```

---

## 🛡️ Data Governance & Quality Standards

| Testing Layer | Scope / Model | Validation Performed |
| :--- | :--- | :--- |
| **Source Freshness** | `source.yml` | Validates that source Delta tables received updates within defined SLAs before downstream processing |
| **Primary Key Tests** | `silver_tech` | Asserts `unique` and `not_null` constraints across all 6 technical staging models |
| **Domain Constraints** | `product_tech` | Enforces price sanity filters (`where: "price > 0"`) |
| **Referential Integrity** | `test_obt.sql` | Ensures 100% foreign key matching across orders, products, stores, employees, and customers |
| **Audit Traceability** | `silver_tech` & `gold` | Appends `processed_at` timestamps and tracks SCD2 validity with `dbt_valid_from` / `dbt_valid_to` |

---

## 👥 Contributors & Maintainers
Engineered for enterprise data platform analytics. Maintained by the Walmart Data Engineering Team.
