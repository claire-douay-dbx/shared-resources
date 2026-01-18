# ASDA Data Engineering Standards - SDP SQL

> **For AI Agents:** This document contains complete standards for Spark Declarative Pipelines (SDP). Apply these rules automatically when creating or reviewing pipeline code. No additional instructions are needed.

---

## Quick Reference - How to Apply These Standards

When creating or modifying SDP SQL code, follow this checklist for each layer:

### Bronze Layer Checklist
| Requirement | How to Apply |
|-------------|--------------|
| Table name | `bronze_` prefix + `lowercase_snake_case` (e.g., `bronze_transactions`) |
| Table type | `STREAMING TABLE` for file ingestion, `MATERIALIZED VIEW` for Delta tables |
| Audit columns | Add `current_timestamp() AS asda_audit_ts` and `'source_name' AS source_system` as LAST columns |
| Comment | Add `COMMENT "Raw <entity> data from <source>"` |
| Properties | Add `TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")` |
| Constraints | Use `WHERE` clause for NULL filtering (NOT constraint clauses) |
| Clustering | Add `CLUSTER BY AUTO` for STREAMING TABLEs |

### Silver Layer Checklist
| Requirement | How to Apply |
|-------------|--------------|
| Table name | `silver_` prefix + `lowercase_snake_case` (e.g., `silver_articles`) |
| Table type | `STREAMING TABLE` (for CDC/history) or `MATERIALIZED VIEW` (for batch) |
| Audit columns | Add `current_timestamp() AS asda_audit_ts` and `'silver_transformation' AS source_system` as LAST columns |
| Comment | Add `COMMENT "Cleaned and validated <entity> with derived metrics"` |
| Properties | Add `TBLPROPERTIES ("quality" = "silver", "delta.enableChangeDataFeed" = "true", "delta.enableRowTracking" = "true")` |
| Constraints | Add `CONSTRAINT` clauses with `ON VIOLATION FAIL UPDATE` (critical) or `ON VIOLATION DROP ROW` (non-critical) |
| Clustering | Add `CLUSTER BY AUTO` for STREAMING TABLEs |
| Transformations | Apply `TRIM()`, `CAST()`, `INITCAP()`, add derived fields |
| Data quality flag | Add `data_quality_flag` column using CASE expression |

### Gold Layer Checklist
| Requirement | How to Apply |
|-------------|--------------|
| Table name | `gold_` prefix + `lowercase_snake_case` (e.g., `gold_daily_sales`) |
| Table type | `STREAMING TABLE` (for streaming aggregations) or `MATERIALIZED VIEW` (for batch) |
| Audit columns | Add `current_timestamp() AS asda_audit_ts` and `'gold_aggregation' AS source_system` as LAST columns |
| Comment | Add `COMMENT "Business aggregation for <use case>"` |
| Properties | Add `TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true")` |
| Aggregations | Use `ROUND()` for financials, `NULLIF()` for division, `COALESCE()` for NULLs |
| Joins | Use `INNER JOIN` or `LEFT JOIN` with `LIVE.table_name` syntax |
| Clustering | Add `CLUSTER BY AUTO` for STREAMING TABLEs |

---

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
- `bronze_date_dimensions`
- `silver_articles`
- `silver_transactions`
- `gold_category_performance`
- `gold_retail_analytics`

### Invalid Examples
- `RawTransactions` (PascalCase)
- `BRONZE_TRANSACTIONS` (UPPERCASE)
- `transactions` (missing layer prefix)
- `gold-sales-summary` (kebab-case)
- `goldDailySales` (camelCase)

---

## 2. Required Audit Columns

### Rule
Every table MUST include these audit columns as the **LAST** columns in the SELECT statement:

| Column Name | Data Type | Description | Required |
|-------------|-----------|-------------|----------|
| `asda_audit_ts` | TIMESTAMP | Processing timestamp | Yes |
| `source_system` | STRING | Origin system identifier | Yes |

### Implementation Pattern

```sql
SELECT 
  -- ... all business columns first ...
  
  -- Audit columns LAST
  current_timestamp() AS asda_audit_ts,
  'source_system_name' AS source_system
FROM source_table;
```

### Valid source_system Values by Layer

| Layer | source_system Value | Description |
|-------|---------------------|-------------|
| Bronze | `pos` | Point of Sale transactions |
| Bronze | `ecommerce` | Online orders |
| Bronze | `product_catalog` | Product master data |
| Bronze | `date_dimensions` | Date reference data |
| Bronze | `inventory` | Stock/inventory data |
| Silver | `silver_transformation` | Silver layer processing |
| Gold | `gold_aggregation` | Gold layer aggregation |

---

## 3. SDP Table Types

### Rule
Use the appropriate table type based on the use case:

| Type | When to Use | Syntax |
|------|-------------|--------|
| `STREAMING TABLE` | File ingestion (Auto Loader), CDC, real-time data | `CREATE OR REFRESH STREAMING TABLE` |
| `MATERIALIZED VIEW` | Batch data from existing Delta tables, aggregations | `CREATE OR REFRESH MATERIALIZED VIEW` |
| `LIVE.table_name` | Referencing tables within the same pipeline | `FROM LIVE.bronze_articles` |

### STREAMING TABLE Pattern (File Ingestion)

```sql
-- Use for Auto Loader ingestion from files
CREATE OR REFRESH STREAMING TABLE bronze_transactions
CLUSTER BY AUTO
COMMENT "Raw sales transactions from POS systems"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  'pos' AS source_system
FROM STREAM read_files(
  '${volume_path}/transactions/*.csv',
  format => 'csv',
  header => true
);
```

### MATERIALIZED VIEW Pattern (Delta Tables)

```sql
-- Use for reading from existing Delta tables
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

---

## 4. Data Quality Constraints

### Rule
Apply data quality constraints appropriately for each layer:
- **Bronze**: Use `WHERE` clause filtering only (keep minimal, preserve raw data)
- **Silver**: Use `CONSTRAINT` clauses for validation
- **Gold**: Generally no constraints (data already validated in Silver)

### Constraint Actions

| Action | Use Case | Behavior |
|--------|----------|----------|
| `ON VIOLATION DROP ROW` | Non-critical validation | Silently filters bad rows |
| `ON VIOLATION FAIL UPDATE` | Critical validation | Halts pipeline on failure |

### Bronze Layer - WHERE Clause Only

```sql
CREATE OR REFRESH STREAMING TABLE bronze_transactions
CLUSTER BY AUTO
COMMENT "Raw sales transactions"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT *,
  current_timestamp() AS asda_audit_ts,
  'pos' AS source_system
FROM STREAM read_files('${volume_path}/transactions/*.csv', format => 'csv', header => true)
WHERE transaction_id IS NOT NULL  -- Basic NULL filtering in WHERE
  AND article_id IS NOT NULL;
```

### Silver Layer - Full Validation with Constraints

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  -- Critical fields - halt if invalid
  CONSTRAINT valid_transaction_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_article_id EXPECT (article_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  
  -- Non-critical fields - filter if invalid
  CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amounts EXPECT (unit_price >= 0 AND total_amount >= 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated sales transactions with derived metrics"
TBLPROPERTIES ("quality" = "silver", "delta.enableChangeDataFeed" = "true", "delta.enableRowTracking" = "true")
AS SELECT 
  TRIM(transaction_id) AS transaction_id,
  TRIM(article_id) AS article_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DOUBLE) AS unit_price,
  CAST(total_amount AS DOUBLE) AS total_amount,
  -- Data quality flag
  CASE
    WHEN quantity IS NULL THEN 'MISSING_QUANTITY'
    WHEN unit_price < 0 THEN 'NEGATIVE_PRICE'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
FROM LIVE.bronze_transactions;
```

---

## 5. Table Properties

### Required Properties by Layer

| Layer | Required TBLPROPERTIES |
|-------|------------------------|
| Bronze | `"quality" = "bronze"`, `"delta.enableChangeDataFeed" = "true"` |
| Silver | `"quality" = "silver"`, `"delta.enableChangeDataFeed" = "true"`, `"delta.enableRowTracking" = "true"` |
| Gold | `"quality" = "gold"`, `"delta.enableChangeDataFeed" = "true"` |

### Optional Properties

```sql
TBLPROPERTIES (
  "quality" = "silver",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true",          -- Enables incremental MV refresh
  "pipelines.autoOptimize.zOrderCols" = "article_id,department"  -- Z-ordering
)
```

---

## 6. Comments and Documentation

### Rule
Every table MUST have a `COMMENT` describing its purpose.

### Comment Templates by Layer

| Layer | Template |
|-------|----------|
| Bronze | `COMMENT "Raw <entity> data from <source system>"` |
| Silver | `COMMENT "Cleaned and validated <entity> with derived metrics"` |
| Gold | `COMMENT "<Business metric/aggregation> for <use case>"` |

### Examples

```sql
-- Bronze
COMMENT "Raw sales transactions from POS systems"

-- Silver  
COMMENT "Cleaned and validated product articles with profit margin calculations"

-- Gold
COMMENT "Daily sales summary by store and category for retail analytics dashboard"
```

---

## 7. Medallion Architecture - Complete Patterns

### Bronze Layer Complete Pattern

```sql
-- BRONZE: Minimal transformation, preserve raw data
CREATE OR REFRESH STREAMING TABLE bronze_articles
CLUSTER BY AUTO
COMMENT "Raw product articles from retail catalog"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  'product_catalog' AS source_system
FROM STREAM read_files(
  '${volume_path}/articles/*.csv',
  format => 'csv',
  header => true
)
WHERE article_id IS NOT NULL;  -- Only basic NULL filtering
```

### Silver Layer Complete Pattern

```sql
-- SILVER: Full validation, type conversions, derived fields
CREATE OR REFRESH MATERIALIZED VIEW silver_articles (
  -- Critical constraints
  CONSTRAINT valid_article_id EXPECT (article_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- Non-critical constraints
  CONSTRAINT valid_prices EXPECT (base_cost >= 0 AND list_price >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (status IN ('Active', 'Blocked', 'Pending', 'Discontinued')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated product articles with profit margin calculations"
TBLPROPERTIES (
  "quality" = "silver",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true",
  "pipelines.autoOptimize.zOrderCols" = "article_id,department"
)
AS SELECT 
  -- Identifiers (with trimming)
  TRIM(article_id) AS article_id,
  
  -- Dimensions (with standardization)
  INITCAP(TRIM(department)) AS department,
  INITCAP(TRIM(category)) AS category,
  INITCAP(TRIM(subcategory)) AS subcategory,
  UPPER(TRIM(status)) AS status,
  
  -- Measures (with type casting)
  CAST(base_cost AS DOUBLE) AS base_cost,
  CAST(list_price AS DOUBLE) AS list_price,
  
  -- Derived fields
  ROUND(CAST(list_price AS DOUBLE) - CAST(base_cost AS DOUBLE), 2) AS profit_margin,
  ROUND((CAST(list_price AS DOUBLE) - CAST(base_cost AS DOUBLE)) / 
    NULLIF(CAST(list_price AS DOUBLE), 0) * 100, 2) AS margin_percentage,
  
  -- Data quality flag
  CASE
    WHEN base_cost IS NULL THEN 'MISSING_COST'
    WHEN list_price < base_cost THEN 'NEGATIVE_MARGIN'
    WHEN status IS NULL THEN 'MISSING_STATUS'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  
  -- Audit columns LAST
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
FROM LIVE.bronze_articles;
```

### Gold Layer Complete Pattern

```sql
-- GOLD: Business aggregations with KPIs
CREATE OR REFRESH MATERIALIZED VIEW gold_category_performance
COMMENT "Category performance metrics with profitability for retail analytics"
TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  -- Dimensions
  t.transaction_date AS sale_date,
  a.department,
  a.category,
  a.subcategory,
  t.region,
  t.channel_type,
  
  -- Volume metrics
  COUNT(DISTINCT t.transaction_id) AS transaction_count,
  SUM(t.quantity) AS total_units_sold,
  APPROX_COUNT_DISTINCT(t.customer_id) AS unique_customers,
  
  -- Revenue metrics
  ROUND(SUM(t.total_amount), 2) AS gross_revenue,
  ROUND(SUM(t.net_amount), 2) AS net_revenue,
  ROUND(AVG(t.total_amount), 2) AS avg_transaction_value,
  
  -- Profit metrics
  ROUND(SUM(t.net_amount) - SUM(t.quantity * a.base_cost), 2) AS gross_profit,
  ROUND((SUM(t.net_amount) - SUM(t.quantity * a.base_cost)) / 
    NULLIF(SUM(t.net_amount), 0) * 100, 2) AS profit_margin_pct,
  
  -- Performance indicators
  ROUND(SUM(t.quantity) / NULLIF(COUNT(DISTINCT t.transaction_id), 0), 2) AS units_per_transaction,
  ROUND(SUM(t.net_amount) / NULLIF(APPROX_COUNT_DISTINCT(t.customer_id), 0), 2) AS revenue_per_customer,
  
  -- Audit columns LAST
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system
FROM LIVE.silver_transactions t
INNER JOIN LIVE.silver_articles a
  ON t.article_id = a.article_id
GROUP BY 
  t.transaction_date,
  a.department,
  a.category,
  a.subcategory,
  t.region,
  t.channel_type;
```

---

## 8. SQL Formatting Standards

### Capitalization Rules
| Element | Case | Example |
|---------|------|---------|
| SQL keywords | UPPERCASE | `SELECT`, `FROM`, `WHERE`, `AS`, `JOIN`, `GROUP BY` |
| Table/column names | lowercase_snake_case | `transaction_id`, `bronze_articles` |
| Aliases | short lowercase | `t`, `a`, `d` |
| Functions | UPPERCASE | `ROUND()`, `CAST()`, `COALESCE()` |

### Column Organization Order
1. Identifiers first (primary keys, foreign keys)
2. Dimensions (categories, hierarchies)
3. Measures (quantities, amounts)
4. Derived/calculated fields
5. Data quality flags
6. **Audit columns LAST** (`asda_audit_ts`, `source_system`)

### Aggregation Functions
```sql
-- Use ROUND() for financial calculations
ROUND(SUM(amount), 2) AS total_amount

-- Use NULLIF() to prevent division by zero
ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 2) AS avg_revenue

-- Use COALESCE() for NULL handling
COALESCE(discount_amount, 0) AS discount_amount

-- Use APPROX_COUNT_DISTINCT() for large datasets
APPROX_COUNT_DISTINCT(customer_id) AS unique_customers
```

---

## 9. Joins and References

### Pipeline Table References
Use `LIVE.table_name` to reference tables within the same pipeline:

```sql
-- CORRECT: Use LIVE prefix for pipeline tables
FROM LIVE.silver_transactions t
INNER JOIN LIVE.silver_articles a
  ON t.article_id = a.article_id
INNER JOIN LIVE.silver_date_dimensions d
  ON t.transaction_date = d.full_date
```

### External Table References
Use fully qualified names for tables outside the pipeline:

```sql
-- CORRECT: Full path for external source tables
FROM asda_arch_day.sample_data.retail_articles_dataset

-- CORRECT: Full path for system tables
FROM system.information_schema.column_tags
```

### Join Best Practices
- Always use explicit `JOIN` syntax (not comma-separated FROM)
- Always use table aliases for clarity
- Place join condition on its own line with proper indentation
- Use `INNER JOIN` when matching records are required
- Use `LEFT JOIN` when preserving left table records

---

## 10. STREAMING TABLE vs MATERIALIZED VIEW Decision Guide

| Scenario | Table Type | Reason |
|----------|------------|--------|
| File ingestion (CSV, JSON, Parquet) | `STREAMING TABLE` | Supports Auto Loader |
| CDC/Change Data Capture | `STREAMING TABLE` | Supports streaming reads |
| Reading from existing Delta tables | `MATERIALIZED VIEW` | Batch semantics |
| Batch aggregations | `MATERIALIZED VIEW` | No streaming needed |
| SCD Type 1 or Type 2 tracking | `STREAMING TABLE` with `AUTO CDC` | Requires streaming |
| Join with external system tables | `MATERIALIZED VIEW` | External tables are batch |

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 3.0 | 2026-01-18 | ASDA Data Platform Team | Added agent-ready checklists, aligned with WTW best practices |
| 2.0 | 2026-01-14 | ASDA Data Platform Team | Updated for SDP SQL standards |
| 1.0 | 2026-01-13 | ASDA Data Platform Team | Initial PySpark standards |
