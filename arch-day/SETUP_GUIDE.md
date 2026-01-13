# Retail Data Model Setup Guide

This guide explains how to set up the retail data model in Databricks Unity Catalog.

## Overview

The setup creates a retail analytics data model with three tables:
- **date_dimensions** - Calendar and fiscal date attributes for time-based analysis
- **retail_articles** - Product master data (pricing, supplier, distribution)
- **transactions** - Sales transaction records (fact table)

## Files Included

| File | Purpose |
|------|---------|
| `create_retail_tables_notebook.py` | Creates catalog, schema, and table structures |
| `load_data_from_volume.py` | Loads CSV data into the tables |
| `create_date_dimensions_schema.py` | Schema definition for date_dimensions |
| `create_retail_articles_schema.py` | Schema definition for retail_articles |
| `create_transactions_schema.py` | Schema definition for transactions |
| `date_dimensions.csv` | Date dimension data (182 records) |
| `retail_articles_dataset_short.csv` | Product master data |
| `transactions_dataset.csv` | Transaction records |

---

## Setup Steps

### Step 1: Upload Python Scripts to Databricks Workspace

1. In Databricks, navigate to **Workspace** in the left sidebar
2. Choose or create a folder for your project (e.g., `/Users/your.email@company.com/retail-data-model`)
3. Click **Import** and upload the following Python files:
   - `create_retail_tables_notebook.py`
   - `load_data_from_volume.py`
   - `create_date_dimensions_schema.py`
   - `create_retail_articles_schema.py`
   - `create_transactions_schema.py`

> **Note:** All `.py` files must be in the same workspace folder for imports to work.

---

### Step 2: Run Table Creation Notebook

1. Open `create_retail_tables_notebook.py` in Databricks
2. Attach a cluster with Unity Catalog access
3. Click **Run All** or run each cell sequentially

**What this creates:**
- Catalog: `asda_arch_day`
- Schema: `asda_arch_day.retail`
- Tables:
  - `asda_arch_day.retail.date_dimensions`
  - `asda_arch_day.retail.retail_articles`
  - `asda_arch_day.retail.transactions`

---

### Step 3: Create a Volume

1. Navigate to **Catalog** in the left sidebar
2. Expand `asda_arch_day` → `retail`
3. Click on **Volumes** (or right-click on the schema)
4. Click **Create Volume**
5. Enter the volume name: `upload_files_to_copy`
6. Click **Create**

**Volume path:** `/Volumes/asda_arch_day/retail/upload_files_to_copy`

---

### Step 4: Upload CSV Files to the Volume

1. In **Catalog**, navigate to `asda_arch_day` → `retail` → `upload_files_to_copy`
2. Click **Upload to this volume**
3. Upload the following files:
   - `date_dimensions.csv`
   - `retail_articles_dataset_short.csv`
   - `transactions_dataset.csv`

---

### Step 5: Run Data Loading Notebook

1. Open `load_data_from_volume.py` in Databricks
2. Attach the same cluster used in Step 2
3. Click **Run All**

**What this does:**
- Reads CSV files from the volume
- Loads data into the corresponding tables
- Displays record counts to verify success

---

## Permissions Required

### Minimum Permissions

| Permission | Scope | Required For |
|------------|-------|--------------|
| `CREATE CATALOG` | Metastore | Creating `asda_arch_day` catalog |
| `CREATE SCHEMA` | Catalog | Creating `retail` schema |
| `CREATE TABLE` | Schema | Creating all three tables |
| `CREATE VOLUME` | Schema | Creating the upload volume |
| `WRITE VOLUME` | Volume | Uploading CSV files |
| `USE CATALOG` | Catalog | Accessing the catalog |
| `USE SCHEMA` | Schema | Accessing the schema |

### Granting Permissions (Admin Only)

If users don't have the required permissions, a metastore admin can grant them:

```sql
-- Grant catalog creation (metastore admin only)
GRANT CREATE CATALOG ON METASTORE TO `user@company.com`;

-- Or, if catalog already exists, grant usage and creation within it
GRANT USE CATALOG ON CATALOG asda_arch_day TO `user@company.com`;
GRANT CREATE SCHEMA ON CATALOG asda_arch_day TO `user@company.com`;

-- Grant schema-level permissions
GRANT USE SCHEMA ON SCHEMA asda_arch_day.retail TO `user@company.com`;
GRANT CREATE TABLE ON SCHEMA asda_arch_day.retail TO `user@company.com`;
GRANT CREATE VOLUME ON SCHEMA asda_arch_day.retail TO `user@company.com`;
```

### Alternative: Use Existing Catalog

If you don't have `CREATE CATALOG` permission, modify the configuration in the notebooks to use an existing catalog you have access to:

```python
# In both notebooks, change:
CATALOG = "your_existing_catalog"
SCHEMA = "retail"
```

---

## Data Model

### Entity Relationship

```
┌─────────────────┐         ┌─────────────────┐
│ date_dimensions │         │ retail_articles │
├─────────────────┤         ├─────────────────┤
│ PK: date_key    │◄───┐    │ PK: article_id  │◄───┐
│ full_date       │    │    │ department      │    │
│ calendar_year   │    │    │ category        │    │
│ fiscal_year     │    │    │ brand           │    │
│ retail_season   │    │    │ article_name    │    │
│ ...             │    │    │ ...             │    │
└─────────────────┘    │    └─────────────────┘    │
                       │                           │
                       │    ┌─────────────────┐    │
                       │    │  transactions   │    │
                       │    ├─────────────────┤    │
                       └────│ FK: date_key    │    │
                            │ FK: article_id  │────┘
                            │ PK: transaction_id  │
                            │ quantity        │
                            │ total_amount    │
                            │ ...             │
                            └─────────────────┘
```

### Table Details

| Table | Type | Records | Primary Key | Tags |
|-------|------|---------|-------------|------|
| `date_dimensions` | Dimension | 182 | `date_key` | `asda_articles=silver_articles` |
| `retail_articles` | Dimension | ~10,000 | `article_id` | `asda_articles=silver_articles` |
| `transactions` | Fact | ~100,000 | `transaction_id` | `asda_articles=silver_articles` |

---

## Troubleshooting

### Import Errors
**Error:** `ModuleNotFoundError: No module named 'create_date_dimensions_schema'`

**Solution:** Ensure all Python files are in the same workspace folder as the notebooks.

### Permission Denied
**Error:** `User does not have CREATE on CATALOG`

**Solution:** Contact your Databricks admin to grant permissions, or use an existing catalog you have access to.

### Volume Not Found
**Error:** `Path does not exist: /Volumes/asda_arch_day/retail/upload_files_to_copy`

**Solution:** Ensure the volume was created in Step 3 and the name matches exactly.

### CSV File Not Found
**Error:** `Path does not exist: /Volumes/.../date_dimensions.csv`

**Solution:** Verify all three CSV files were uploaded to the volume in Step 4.

---

## Sample Queries

After setup is complete, test the data model with these queries:

```sql
-- Transactions with article details
SELECT 
    t.transaction_id,
    t.transaction_date,
    t.quantity,
    t.total_amount,
    a.article_name,
    a.department,
    a.category
FROM asda_arch_day.retail.transactions t
JOIN asda_arch_day.retail.retail_articles a 
    ON t.article_id = a.article_id
LIMIT 10;

-- Sales by department and season
SELECT 
    a.department,
    d.retail_season,
    COUNT(*) as transaction_count,
    SUM(t.total_amount) as total_revenue
FROM asda_arch_day.retail.transactions t
JOIN asda_arch_day.retail.retail_articles a ON t.article_id = a.article_id
JOIN asda_arch_day.retail.date_dimensions d ON t.date_key = d.date_key
GROUP BY a.department, d.retail_season
ORDER BY total_revenue DESC;
```

---

## Cleanup (Optional)

To remove all created resources:

```sql
-- Drop tables
DROP TABLE IF EXISTS asda_arch_day.retail.transactions;
DROP TABLE IF EXISTS asda_arch_day.retail.retail_articles;
DROP TABLE IF EXISTS asda_arch_day.retail.date_dimensions;

-- Drop volume
DROP VOLUME IF EXISTS asda_arch_day.retail.upload_files_to_copy;

-- Drop schema (if empty)
DROP SCHEMA IF EXISTS asda_arch_day.retail;

-- Drop catalog (if empty)
DROP CATALOG IF EXISTS asda_arch_day;
```

