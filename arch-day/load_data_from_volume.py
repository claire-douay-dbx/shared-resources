# Databricks notebook source
# MAGIC %md
# MAGIC # Load CSV Data from Volume into Retail Tables
# MAGIC 
# MAGIC This script loads CSV files from the Unity Catalog volume into the retail data model tables.
# MAGIC 
# MAGIC **Source Volume:** `asda_arch_day.retail.upload_files_to_copy`
# MAGIC 
# MAGIC **Target Schema:** `asda_arch_day.retail`
# MAGIC 
# MAGIC **Files:**
# MAGIC - `date_dimensions.csv` → `date_dimensions` table
# MAGIC - `retail_articles_dataset_short.csv` → `retail_articles` table
# MAGIC - `transactions_dataset.csv` → `transactions` table
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Tables must exist (run `create_retail_tables_notebook.py` first)
# MAGIC - Volume `upload_files_to_copy` must exist with CSV files uploaded

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "asda_arch_day"
SCHEMA = "retail"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/upload_files_to_copy"

# File paths
DATE_DIMENSIONS_CSV = f"{VOLUME_PATH}/date_dimensions.csv"
RETAIL_ARTICLES_CSV = f"{VOLUME_PATH}/retail_articles_dataset_short.csv"
TRANSACTIONS_CSV = f"{VOLUME_PATH}/transactions_dataset.csv"

print(f"Source Volume: {VOLUME_PATH}")
print(f"Target Schema: {CATALOG}.{SCHEMA}")
print("-" * 50)
print(f"Date Dimensions: {DATE_DIMENSIONS_CSV}")
print(f"Retail Articles: {RETAIL_ARTICLES_CSV}")
print(f"Transactions:    {TRANSACTIONS_CSV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Load Functions

# COMMAND ----------

from create_date_dimensions_schema import load_date_dimensions_from_csv
from create_retail_articles_schema import load_retail_articles_from_csv
from create_transactions_schema import load_transactions_with_date_key

print("✓ All load functions imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Date Dimensions

# COMMAND ----------

print("=" * 60)
print("LOADING DATE_DIMENSIONS")
print("=" * 60)

load_date_dimensions_from_csv(
    spark=spark,
    csv_path=DATE_DIMENSIONS_CSV,
    catalog=CATALOG,
    schema=SCHEMA,
    table_name="date_dimensions",
)

print("✓ Date dimensions loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Retail Articles

# COMMAND ----------

print("=" * 60)
print("LOADING RETAIL_ARTICLES")
print("=" * 60)

load_retail_articles_from_csv(
    spark=spark,
    csv_path=RETAIL_ARTICLES_CSV,
    catalog=CATALOG,
    schema=SCHEMA,
    table_name="retail_articles",
)

print("✓ Retail articles loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Transactions

# COMMAND ----------

print("=" * 60)
print("LOADING TRANSACTIONS")
print("=" * 60)

load_transactions_with_date_key(
    spark=spark,
    csv_path=TRANSACTIONS_CSV,
    catalog=CATALOG,
    schema=SCHEMA,
    table_name="transactions",
)

print("✓ Transactions loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Record Counts

# COMMAND ----------

print("=" * 60)
print("RECORD COUNTS SUMMARY")
print("=" * 60)

date_count = spark.table(f"{CATALOG}.{SCHEMA}.date_dimensions").count()
articles_count = spark.table(f"{CATALOG}.{SCHEMA}.retail_articles").count()
transactions_count = spark.table(f"{CATALOG}.{SCHEMA}.transactions").count()

print(f"  date_dimensions:  {date_count:,} records")
print(f"  retail_articles:  {articles_count:,} records")
print(f"  transactions:     {transactions_count:,} records")
print("=" * 60)
print("\n✓ All data loaded successfully!")
