# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Data Model - Table Creation Notebook
# MAGIC 
# MAGIC This notebook creates the retail data model tables in Unity Catalog:
# MAGIC - **date_dimensions** - Date dimension table for time-based analysis
# MAGIC - **retail_articles** - Product master data dimension table
# MAGIC - **transactions** - Sales transactions fact table
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog must be enabled
# MAGIC - User must have CREATE CATALOG, CREATE SCHEMA, and CREATE TABLE permissions
# MAGIC 
# MAGIC ## Table Creation Order
# MAGIC Tables are created in dependency order:
# MAGIC 1. `date_dimensions` (dimension - no foreign keys)
# MAGIC 2. `retail_articles` (dimension - no foreign keys)
# MAGIC 3. `transactions` (fact - references both dimension tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration parameters
CATALOG = "asda_arch_day"
SCHEMA = "retail"

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Schema Creation Functions

# COMMAND ----------

# Import the schema creation modules
# Note: These files should be in the same directory as this notebook

from create_date_dimensions_schema import create_date_dimensions_table
from create_retail_articles_schema import create_retail_articles_table
from create_transactions_schema import create_transactions_table_with_date_key

print("✓ All schema modules imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Date Dimensions Table
# MAGIC 
# MAGIC The date dimension table contains calendar and fiscal date attributes for time-based analysis.
# MAGIC 
# MAGIC **Primary Key:** `date_key` (BIGINT in YYYYMMDD format)

# COMMAND ----------

print("=" * 60)
print("CREATING DATE_DIMENSIONS TABLE")
print("=" * 60)

date_dim_table = create_date_dimensions_table(
    spark=spark,
    catalog=CATALOG,
    schema=SCHEMA,
    table_name="date_dimensions",
)

print(f"\n✓ Table created: {date_dim_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Retail Articles Table
# MAGIC 
# MAGIC The retail articles table contains product master data including pricing, supplier, and distribution information.
# MAGIC 
# MAGIC **Primary Key:** `article_id` (STRING)

# COMMAND ----------

print("=" * 60)
print("CREATING RETAIL_ARTICLES TABLE")
print("=" * 60)

articles_table = create_retail_articles_table(
    spark=spark,
    catalog=CATALOG,
    schema=SCHEMA,
    table_name="retail_articles",
)

print(f"\n✓ Table created: {articles_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Transactions Table
# MAGIC 
# MAGIC The transactions table is the fact table containing sales transaction records.
# MAGIC 
# MAGIC **Primary Key:** `transaction_id` (STRING)
# MAGIC 
# MAGIC **Foreign Keys:**
# MAGIC - `article_id` → `retail_articles.article_id`
# MAGIC - `date_key` → `date_dimensions.date_key`

# COMMAND ----------

print("=" * 60)
print("CREATING TRANSACTIONS TABLE")
print("=" * 60)

transactions_table = create_transactions_table_with_date_key(
    spark=spark,
    catalog=CATALOG,
    schema=SCHEMA,
    table_name="transactions",
)

print(f"\n✓ Table created: {transactions_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Creation

# COMMAND ----------

# List all tables in the schema
print(f"Tables in {CATALOG}.{SCHEMA}:")
print("-" * 40)
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC | Table | Type | Primary Key | Foreign Keys | Tag |
# MAGIC |-------|------|-------------|--------------|-----|
# MAGIC | `date_dimensions` | Dimension | `date_key` | - | asda_articles=silver_articles |
# MAGIC | `retail_articles` | Dimension | `article_id` | - | asda_articles=silver_articles |
# MAGIC | `transactions` | Fact | `transaction_id` | `article_id`, `date_key` | asda_articles=silver_articles |
# MAGIC 
# MAGIC All tables use Delta Lake format with auto-optimization enabled.
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Create a volume in the catalog: `asda_arch_day.retail.upload_files_to_copy`
# MAGIC 2. Upload CSV files to the volume
# MAGIC 3. Run `load_data_from_volume.py` to load the data
