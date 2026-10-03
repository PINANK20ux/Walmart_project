import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as sa

def main():
    base_dir = Path(__file__).resolve().parent
    env_file = base_dir / ".env"
    ddl_file = base_dir / "ddl" / "walmart_schema.sql"
    data_dir = base_dir / "data"

    load_dotenv(env_file)
    conn_str = os.getenv("POSTGRES_CONN")
    if not conn_str:
        print("ERROR: POSTGRES_CONN environment variable not found in .env")
        sys.exit(1)

    print(f"Connecting to Neon DB...")
    engine = sa.create_engine(conn_str)

    # 1. Execute DDL to create tables
    with open(ddl_file, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    # Modify statements to include IF NOT EXISTS just in case
    # Split by semicolon
    statements = [stmt.strip() for stmt in ddl_sql.split(";") if stmt.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            # Replace CREATE TABLE with CREATE TABLE IF NOT EXISTS
            if stmt.upper().startswith("CREATE TABLE"):
                stmt_safe = stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
            else:
                stmt_safe = stmt
            print(f"Executing DDL: {stmt_safe[:50]}...")
            conn.execute(sa.text(stmt_safe))

    print("\n--- DDL Execution Complete ---\n")

    # 2. Ingest CSV data into tables
    # Loading order to respect foreign key concepts if any:
    # stores, customers, products, employees, orders, order_items
    tables = [
        "stores",
        "customers",
        "products",
        "employees",
        "orders",
        "order_items",
    ]

    for table in tables:
        csv_file = data_dir / f"{table}.csv"
        if not csv_file.exists():
            print(f"Warning: {csv_file} does not exist, skipping.")
            continue

        print(f"Reading {csv_file.name}...")
        df = pd.read_csv(csv_file)
        row_count = len(df)
        print(f"Loaded {row_count} rows from CSV. Ingesting into public.{table}...")

        # Check existing row count
        with engine.connect() as conn:
            existing_count = conn.execute(sa.text(f"SELECT COUNT(*) FROM public.{table}")).scalar()

        if existing_count > 0:
            print(f"Table public.{table} already contains {existing_count} rows.")
            if existing_count == row_count:
                print(f"Table public.{table} is already fully populated. Skipping insertion.")
                continue
            else:
                print(f"Count mismatch (existing {existing_count} vs CSV {row_count}).")

        # Ingest using to_sql with multi insert
        df.to_sql(
            name=table,
            con=engine,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )
        print(f"Successfully inserted rows into public.{table}")

    # 3. Validation & Summary
    print("\n=== Validation & Row Counts in Neon DB (public schema) ===")
    with engine.connect() as conn:
        for table in tables:
            actual_count = conn.execute(sa.text(f"SELECT COUNT(*) FROM public.{table}")).scalar()
            csv_file = data_dir / f"{table}.csv"
            csv_count = len(pd.read_csv(csv_file)) if csv_file.exists() else 0
            status = "MATCH" if actual_count == csv_count else "MISMATCH"
            print(f"public.{table}: {actual_count} rows (CSV: {csv_count}) [{status}]")

if __name__ == "__main__":
    main()
