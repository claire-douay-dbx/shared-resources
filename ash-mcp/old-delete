# ASDA Data Engineering Standards - SDP SQL

> This document is fetched by the DE Agent via MCP to enforce ASDA's data engineering standards for Spark Declarative Pipelines (SDP).

## 1. Table Naming Convention

### Rule
All table names MUST use `lowercase_snake_case` with layer prefix.

### Layer Prefixes
| Layer | Prefix | Example |
|-------|--------|---------|
| Bronze | `bronze_` | `bronze_transactions` |
| Silver | `silver_` | `silver_articles` |
| Gold | `gold_` | `gold_daily_sales_summary` |

### Valid Examples
- `bronze_transactions`
- `silver_articles`
- `gold_category_performance`
- `gold_retail_analytics`

### Invalid Examples
- `RawTransactions` (PascalCase)
- `BRONZE_TRANSACTIONS` (UPPERCASE)
- `transactions` (missing layer prefix)
- `gold-sales-summary` (kebab-case)

---

## 2. Required Audit Columns

### Rule
Every table MUST include these audit columns as the LAST columns:

| Column Name | Data Type | Description | Required |
|-------------|-----------|-------------|----------|
| `asda_audit_ts` | TIMESTAMP | Processing timestamp | Yes |
| `source_system` | STRING | Origin system identifier | Yes |

### Implementation (SQL)

```sql
SELECT 
  -- ... all other columns ...
  current_timestamp() AS asda_audit_ts,
  'source_system_name' AS source_system
FROM source_table;
```

### Valid source_system Values
| Value | Description |
|-------|-------------|
| `pos` | Point of Sale transactions |
| `ecommerce` | Online orders |
| `product_catalog` | Product master data |
| `date_dimensions` | Date reference data |
| `silver_transformation` | Silver layer processing |
| `gold_aggregation` | Gold layer aggregation |

---

## 3. SDP Table Types

### Rule
Use the appropriate table type for each use case:

| Type | Use Case | Syntax |
|------|----------|--------|
| `STREAMING TABLE` | Incremental data from files (Auto Loader) | `CREATE OR REFRESH STREAMING TABLE` |
| `MATERIALIZED VIEW` | Batch data from existing tables | `CREATE OR REFRESH MATERIALIZED VIEW` |
| `LIVE TABLE` | Reference within pipeline | `LIVE.table_name` |

### When to Use Each Type

**STREAMING TABLE:**
```sql
-- Use for Auto Loader ingestion from files
CREATE OR REFRESH STREAMING TABLE bronze_claims
AS SELECT * FROM STREAM read_files('path/*.json', format => 'json');
```

**MATERIALIZED VIEW:**
```sql
-- Use for reading from existing Delta tables
CREATE OR REFRESH MATERIALIZED VIEW bronze_articles
AS SELECT * FROM source_catalog.schema.table;
```

---

## 4. Data Quality Constraints

### Rule
Apply data quality constraints at Bronze and Silver layers.

### Constraint Actions
| Action | Use Case |
|--------|----------|
| `ON VIOLATION DROP ROW` | Non-critical validation (filter bad data) |
| `ON VIOLATION FAIL UPDATE` | Critical validation (halt pipeline on failure) |

### Bronze Layer (Minimal Filtering)
```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_transactions
COMMENT "Raw sales transactions"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT *
FROM source_table
WHERE transaction_id IS NOT NULL  -- Basic filtering in WHERE clause
  AND article_id IS NOT NULL;
```

### Silver Layer (Full Validation)
```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amounts EXPECT (unit_price >= 0 AND total_amount >= 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated sales transactions"
TBLPROPERTIES ("quality" = "silver")
AS SELECT ...
```

---

## 5. Table Properties

### Rule
Set appropriate table properties for each layer.

### Required Properties
```sql
TBLPROPERTIES (
  "quality" = "bronze|silver|gold",           -- Required: Layer indicator
  "delta.enableChangeDataFeed" = "true"       -- Required for STREAMING TABLEs
)
```

### Optional Properties
```sql
TBLPROPERTIES (
  "delta.enableRowTracking" = "true",         -- Enables incremental MV refresh
  "pipelines.autoOptimize.zOrderCols" = "col1,col2"  -- Z-ordering for performance
)
```

---

## 6. Comments and Documentation

### Rule
Every table MUST have a COMMENT describing its purpose.

### Format
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales_summary
COMMENT "Daily sales summary by store and region - key retail KPIs"
TBLPROPERTIES ("quality" = "gold")
AS SELECT ...
```

### Comment Guidelines
- State what the table contains
- Mention key use cases
- Note any RLS or masking if applicable

---

## 7. Medallion Architecture

### Bronze Layer Standards
- Minimal transformation (just add audit columns)
- Basic NULL filtering only
- Preserve source data structure
- Use `WHERE` clause for filtering (not constraints)

```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_articles
COMMENT "Raw product articles from retail catalog"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  'product_catalog' AS source_system
FROM asda_arch_day.sample_data.retail_articles_dataset
WHERE article_id IS NOT NULL;
```

### Silver Layer Standards
- Full data validation with constraints
- Type conversions and standardization
- Derived fields for analytics
- Data quality flags

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_articles (
  CONSTRAINT valid_article_id EXPECT (article_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_prices EXPECT (base_cost >= 0 AND list_price >= 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated product articles with derived metrics"
TBLPROPERTIES ("quality" = "silver")
AS SELECT 
  TRIM(article_id) AS article_id,
  INITCAP(TRIM(department)) AS department,
  CAST(base_cost AS DOUBLE) AS base_cost,
  CAST(list_price AS DOUBLE) AS list_price,
  -- Derived field
  ROUND(CAST(list_price AS DOUBLE) - CAST(base_cost AS DOUBLE), 2) AS profit_margin,
  -- Data quality flag
  CASE
    WHEN base_cost IS NULL THEN 'MISSING_COST'
    WHEN list_price < base_cost THEN 'NEGATIVE_MARGIN'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
FROM LIVE.bronze_articles;
```

### Gold Layer Standards
- Business aggregations with GROUP BY
- Joins across Silver tables
- KPI calculations
- Performance indicators

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_category_performance
COMMENT "Category performance metrics with profitability"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  t.transaction_date AS sale_date,
  a.department,
  a.category,
  -- Volume metrics
  COUNT(DISTINCT t.transaction_id) AS transaction_count,
  SUM(t.quantity) AS total_units_sold,
  -- Revenue metrics
  ROUND(SUM(t.total_amount), 2) AS gross_revenue,
  -- Profit metrics
  ROUND(SUM(t.net_amount) - SUM(t.quantity * a.base_cost), 2) AS gross_profit,
  -- ASDA audit columns
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system
FROM LIVE.silver_transactions t
INNER JOIN LIVE.silver_articles a ON t.article_id = a.article_id
GROUP BY t.transaction_date, a.department, a.category;
```

---

## 8. SQL Formatting Standards

### Capitalization
- SQL keywords: `UPPERCASE` (SELECT, FROM, WHERE, AS, JOIN)
- Table/column names: `lowercase_snake_case`
- Aliases: short lowercase (`t`, `a`, `d`)

### Column Organization
```sql
SELECT 
  -- Identifiers first
  t.transaction_id,
  t.article_id,
  
  -- Dimensions
  a.department,
  a.category,
  
  -- Metrics
  SUM(t.quantity) AS total_quantity,
  ROUND(SUM(t.total_amount), 2) AS total_revenue,
  
  -- Derived/calculated fields
  CASE WHEN ... END AS status_flag,
  
  -- Audit columns LAST
  current_timestamp() AS asda_audit_ts,
  'source' AS source_system
FROM ...
```

### Aggregations
- Use `ROUND()` for financial calculations
- Use `NULLIF()` to prevent division by zero
- Use `COALESCE()` for NULL handling

```sql
ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS revenue_per_customer
```

---

## 9. Joins

### Rule
Use explicit JOIN syntax with clear aliases.

### Correct Pattern
```sql
FROM LIVE.silver_transactions t
INNER JOIN LIVE.silver_articles a
  ON t.article_id = a.article_id
INNER JOIN LIVE.silver_date_dimensions d
  ON t.transaction_date = d.full_date
```

### Avoid
- Implicit joins (comma-separated FROM)
- Missing aliases
- Joining without LIVE prefix within pipeline

---

## 10. Pipeline Reference Syntax

### Rule
Use `LIVE.table_name` to reference tables within the same pipeline.

### Correct
```sql
FROM LIVE.silver_transactions t
INNER JOIN LIVE.silver_articles a ON t.article_id = a.article_id
```

### Incorrect
```sql
-- DON'T use fully qualified names for pipeline tables
FROM catalog.schema.silver_transactions t
```

### External Tables
```sql
-- DO use fully qualified names for external source tables
FROM asda_arch_day.sample_data.retail_articles_dataset
```

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 2.0 | 2026-01-14 | ASDA Data Platform Team | Updated for SDP SQL standards |
| 1.0 | 2026-01-13 | ASDA Data Platform Team | Initial PySpark standards |
