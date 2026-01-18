-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ASDA SDP Pipeline Template
-- MAGIC 
-- MAGIC This template is fetched by the DE Agent via MCP from GitHub.
-- MAGIC 
-- MAGIC **Template enforces:**
-- MAGIC - lowercase_snake_case naming with layer prefixes
-- MAGIC - Required audit columns (asda_audit_ts, source_system)
-- MAGIC - Medallion architecture (Bronze -> Silver -> Gold)
-- MAGIC - Data quality constraints
-- MAGIC - Proper table properties and comments
-- MAGIC - CLUSTER BY AUTO for STREAMING TABLEs
-- MAGIC - Change Data Feed and Row Tracking for incremental refresh
-- MAGIC 
-- MAGIC **Based on:** WTW Health Benefits SDP Pipeline best practices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # BRONZE LAYER - Raw Data Ingestion
-- MAGIC 
-- MAGIC Minimal transformation, preserve source data structure.
-- MAGIC - Use WHERE clause for NULL filtering (not CONSTRAINT clauses)
-- MAGIC - Add CLUSTER BY AUTO for STREAMING TABLEs
-- MAGIC - Enable Change Data Feed for downstream consumption

-- COMMAND ----------

-- =============================================================================
-- BRONZE LAYER TEMPLATE: Streaming Table from files (Auto Loader)
-- =============================================================================
-- Use for: File ingestion (CSV, JSON, Parquet) via Auto Loader
-- Key features: CLUSTER BY AUTO, CDF enabled, metadata capture

CREATE OR REFRESH STREAMING TABLE bronze_<entity_name>
CLUSTER BY AUTO
COMMENT "Raw <entity> data ingested via Auto Loader"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  "${volume_path}/raw_data/<entity>/*.json",
  format => "json"
)
WHERE <primary_key> IS NOT NULL;  -- Basic NULL filtering in WHERE clause

-- COMMAND ----------

-- =============================================================================
-- BRONZE LAYER TEMPLATE: Streaming Table from CSV with schema inference
-- =============================================================================
-- Use for: CSV files with headers

CREATE OR REFRESH STREAMING TABLE bronze_<entity_name>
CLUSTER BY AUTO
COMMENT "Raw <entity> data from CSV files"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  "${volume_path}/raw_data/<entity>/*.csv",
  format => "csv",
  header => true
)
WHERE <primary_key> IS NOT NULL;

-- COMMAND ----------

-- =============================================================================
-- BRONZE LAYER TEMPLATE: Materialized View from existing Delta table
-- =============================================================================
-- Use for: Reading from existing Delta tables (no streaming)

CREATE OR REFRESH MATERIALIZED VIEW bronze_<entity_name>
COMMENT "Raw <entity> data from <source_catalog>.<source_schema>"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system
FROM <source_catalog>.<source_schema>.<source_table>
WHERE <primary_key> IS NOT NULL;

-- COMMAND ----------

-- =============================================================================
-- BRONZE LAYER TEMPLATE: CDC with Quarantine Pattern (Debezium format)
-- =============================================================================
-- Use for: Change Data Capture with proper quarantine for rejected records
-- Key feature: Valid records go to main table, invalid go to quarantine

-- Main CDC table (valid records only)
CREATE OR REFRESH STREAMING TABLE bronze_<entity_name>_cdc
CLUSTER BY AUTO
COMMENT "CDC events - VALID records only (rejected records go to quarantine table)"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  after.*,
  op AS cdc_operation,
  ts_ms AS cdc_timestamp_ms,
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  "${volume_path}/raw_data/<entity>_cdc/*.json",
  format => "json",
  rescuedDataColumn => "_rescued_data"
)
WHERE after.<primary_key> IS NOT NULL
  AND op IN ('c', 'u', 'd', 'r')
  AND _rescued_data IS NULL;

-- Quarantine table (captures ALL rejected records with reason)
CREATE OR REFRESH STREAMING TABLE bronze_<entity_name>_cdc_quarantine
CLUSTER BY AUTO
COMMENT "Quarantine for rejected CDC records - captures reason for review/reprocessing"
TBLPROPERTIES ("quality" = "quarantine", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  after.*,
  op AS cdc_operation,
  ts_ms AS cdc_timestamp_ms,
  _rescued_data,
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system,
  _metadata.file_path AS _source_file,
  CASE 
    WHEN after.<primary_key> IS NULL THEN 'NULL_PRIMARY_KEY'
    WHEN op NOT IN ('c', 'u', 'd', 'r') THEN 'INVALID_CDC_OPERATION'
    WHEN _rescued_data IS NOT NULL THEN 'SCHEMA_MISMATCH'
    ELSE 'UNKNOWN'
  END AS quarantine_reason,
  current_timestamp() AS quarantined_at
FROM STREAM read_files(
  "${volume_path}/raw_data/<entity>_cdc/*.json",
  format => "json",
  rescuedDataColumn => "_rescued_data"
)
WHERE after.<primary_key> IS NULL
   OR op NOT IN ('c', 'u', 'd', 'r')
   OR _rescued_data IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SILVER LAYER - Data Cleansing & Validation
-- MAGIC 
-- MAGIC Full validation, type conversions, derived fields.
-- MAGIC - Use CONSTRAINT clauses for validation
-- MAGIC - Enable Row Tracking for incremental MV refresh
-- MAGIC - Add data_quality_flag column

-- COMMAND ----------

-- =============================================================================
-- SILVER LAYER TEMPLATE: Materialized View with validation
-- =============================================================================
-- Use for: Batch transformations from Bronze
-- Key features: Constraints, Row Tracking, derived fields, data quality flag

CREATE OR REFRESH MATERIALIZED VIEW silver_<entity_name> (
  -- Critical constraints (halt pipeline on failure)
  CONSTRAINT valid_<pk> EXPECT (<primary_key> IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- Non-critical constraints (filter bad rows)
  CONSTRAINT valid_amounts EXPECT (<amount_field> >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status EXPECT (<status_field> IN ('Active', 'Inactive', 'Pending')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated <entity> with derived metrics"
TBLPROPERTIES (
  "quality" = "silver",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true",
  "pipelines.autoOptimize.zOrderCols" = "<primary_key>,<partition_key>"
)
AS SELECT 
  -- ===========================================
  -- IDENTIFIERS (with trimming)
  -- ===========================================
  TRIM(<primary_key>) AS <primary_key>,
  TRIM(<foreign_key>) AS <foreign_key>,
  
  -- ===========================================
  -- DIMENSIONS (with standardization)
  -- ===========================================
  INITCAP(TRIM(<category_field>)) AS <category_field>,
  UPPER(TRIM(<status_field>)) AS <status_field>,
  
  -- ===========================================
  -- DATES (with casting)
  -- ===========================================
  CAST(<date_field> AS DATE) AS <date_field>,
  CAST(<timestamp_field> AS TIMESTAMP) AS <timestamp_field>,
  
  -- ===========================================
  -- AMOUNTS (with type conversions)
  -- ===========================================
  CAST(<amount_field> AS DOUBLE) AS <amount_field>,
  CAST(<quantity_field> AS BIGINT) AS <quantity_field>,
  
  -- ===========================================
  -- DERIVED FIELDS
  -- ===========================================
  ROUND(CAST(<price> AS DOUBLE) - CAST(<cost> AS DOUBLE), 2) AS profit_margin,
  ROUND((CAST(<price> AS DOUBLE) - CAST(<cost> AS DOUBLE)) / 
    NULLIF(CAST(<price> AS DOUBLE), 0) * 100, 2) AS margin_percentage,
  
  -- Boolean flags
  CASE WHEN <condition> THEN TRUE ELSE FALSE END AS is_<flag_name>,
  
  -- ===========================================
  -- DATA QUALITY FLAG
  -- ===========================================
  CASE
    WHEN <field1> IS NULL THEN 'MISSING_<FIELD1>'
    WHEN <field2> < 0 THEN 'NEGATIVE_<FIELD2>'
    WHEN <field3> NOT IN ('Valid', 'Values') THEN 'INVALID_<FIELD3>'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  
  -- ===========================================
  -- ASDA AUDIT COLUMNS (Always last!)
  -- ===========================================
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
  
FROM LIVE.bronze_<entity_name>;

-- COMMAND ----------

-- =============================================================================
-- SILVER LAYER TEMPLATE: Streaming Table with explicit schema
-- =============================================================================
-- Use for: Streaming transformations, explicit column definitions with comments
-- Key features: Column-level comments, explicit NOT NULL, streaming from Bronze

CREATE OR REFRESH STREAMING TABLE silver_<entity_name> (
  <primary_key> STRING NOT NULL COMMENT "Primary key identifier",
  <column1> STRING COMMENT "Description of column1",
  <amount_field> DOUBLE COMMENT "Amount in dollars",
  <date_field> DATE COMMENT "Date of record",
  -- Derived fields
  <derived_field> DOUBLE COMMENT "Calculated from <formula>",
  is_<flag> BOOLEAN COMMENT "Flag indicating <condition>",
  data_quality_flag STRING COMMENT "Data quality indicator",
  asda_audit_ts TIMESTAMP COMMENT "Pipeline processing timestamp",
  source_system STRING COMMENT "Origin system identifier",
  -- Constraints
  CONSTRAINT valid_<pk> EXPECT (<primary_key> IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_amounts EXPECT (<amount_field> >= 0) ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT "Cleaned and validated <entity> with derived metrics"
TBLPROPERTIES (
  "quality" = "silver",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true"
)
AS SELECT 
  TRIM(<primary_key>) AS <primary_key>,
  TRIM(<column1>) AS <column1>,
  CAST(<amount_field> AS DOUBLE) AS <amount_field>,
  CAST(<date_field> AS DATE) AS <date_field>,
  -- Derived fields
  ROUND(CAST(<numerator> AS DOUBLE) / NULLIF(CAST(<denominator> AS DOUBLE), 0), 2) AS <derived_field>,
  CASE WHEN <condition> THEN TRUE ELSE FALSE END AS is_<flag>,
  CASE
    WHEN <field1> IS NULL THEN 'MISSING_<FIELD1>'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
FROM stream(LIVE.bronze_<entity_name>);

-- COMMAND ----------

-- =============================================================================
-- SILVER LAYER TEMPLATE: SCD Type 2 with AUTO CDC
-- =============================================================================
-- Use for: Maintaining full history with slowly changing dimensions
-- Key features: AUTO CDC flow, SCD Type 2 tracking, KEYS and SEQUENCE BY

-- Step 1: Intermediate transformation table
CREATE OR REFRESH STREAMING TABLE silver_<entity_name>_transformed (
  CONSTRAINT valid_amounts EXPECT (<amount_field> >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT critical_fields EXPECT (<primary_key> IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Intermediate: Transformed <entity> with type conversions"
TBLPROPERTIES ("quality" = "silver_intermediate")
AS SELECT 
  <primary_key>,
  CAST(<amount_field> AS DOUBLE) AS <amount_field>,
  CAST(<date_field> AS DATE) AS <date_field>,
  -- Include CDC metadata for sequencing
  CAST(FROM_UNIXTIME(cdc_timestamp_ms / 1000) AS TIMESTAMP) AS cdc_timestamp,
  cdc_operation,
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
FROM stream(LIVE.bronze_<entity_name>_cdc)
WHERE cdc_operation != 'd';  -- Exclude deletes (handle separately if needed)

-- Step 2: Apply AUTO CDC with SCD Type 2 tracking
CREATE OR REFRESH STREAMING TABLE silver_<entity_name> (
  CONSTRAINT valid_amounts_final EXPECT (<amount_field> >= 0) ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT "Cleaned <entity> with full history (SCD Type 2)"
TBLPROPERTIES (
  "quality" = "silver",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true"
);

CREATE FLOW silver_<entity_name>_cdc_flow
AS AUTO CDC INTO LIVE.silver_<entity_name>
FROM stream(LIVE.silver_<entity_name>_transformed)
KEYS (<primary_key>)
SEQUENCE BY cdc_timestamp
COLUMNS * EXCEPT (cdc_operation, cdc_timestamp)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # GOLD LAYER - Business Aggregations
-- MAGIC 
-- MAGIC Aggregations, joins, KPIs for business analytics.
-- MAGIC - Use ROUND() for financial calculations
-- MAGIC - Use NULLIF() to prevent division by zero
-- MAGIC - Use APPROX_COUNT_DISTINCT() for large datasets

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: Streaming Aggregation
-- =============================================================================
-- Use for: Real-time aggregations from streaming Silver tables
-- Key features: CLUSTER BY AUTO, streaming from Silver

CREATE OR REFRESH STREAMING TABLE gold_<metric_name>_summary
CLUSTER BY AUTO
COMMENT "<Description of business metrics>"
TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  -- ===========================================
  -- DIMENSIONS (GROUP BY columns)
  -- ===========================================
  CAST(<date_field> AS DATE) AS date,
  <dimension1>,
  <dimension2>,
  
  -- ===========================================
  -- VOLUME METRICS
  -- ===========================================
  APPROX_COUNT_DISTINCT(<entity_id>) AS unique_<entities>,
  COUNT(*) AS <entity>_count,
  SUM(CASE WHEN <status> = 'Active' THEN 1 ELSE 0 END) AS active_<entity>_count,
  SUM(<quantity_field>) AS total_<quantity>,
  
  -- ===========================================
  -- REVENUE METRICS
  -- ===========================================
  ROUND(SUM(<amount_field>), 2) AS total_<amount>,
  ROUND(AVG(<amount_field>), 2) AS avg_<amount>,
  
  -- ===========================================
  -- CALCULATED RATIOS
  -- ===========================================
  ROUND(SUM(<amount_field>) / NULLIF(APPROX_COUNT_DISTINCT(<entity_id>), 0), 2) AS <amount>_per_<entity>,
  ROUND(COUNT(*) / NULLIF(APPROX_COUNT_DISTINCT(<entity_id>), 0), 2) AS <records>_per_<entity>,
  
  -- ===========================================
  -- PERCENTAGE METRICS
  -- ===========================================
  ROUND(SUM(CASE WHEN <condition> THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS <condition>_rate_pct,
  ROUND(SUM(CASE WHEN data_quality_flag = 'CLEAN' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS clean_data_rate_pct,
  
  -- ===========================================
  -- ASDA AUDIT COLUMNS (Always last!)
  -- ===========================================
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system
  
FROM stream(LIVE.silver_<entity_name>)
GROUP BY 
  CAST(<date_field> AS DATE),
  <dimension1>,
  <dimension2>;

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: Materialized View with Multi-table Join
-- =============================================================================
-- Use for: Batch analytics combining multiple Silver tables
-- Key features: INNER JOIN with LIVE prefix, profit calculations

CREATE OR REFRESH MATERIALIZED VIEW gold_<combined_view_name>
COMMENT "<Description of combined analytics>"
TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  -- ===========================================
  -- DIMENSIONS FROM MULTIPLE TABLES
  -- ===========================================
  t.<date_field> AS date,
  d.<date_dimension1>,
  d.<date_dimension2>,
  a.<product_dimension>,
  a.<category>,
  t.<region>,
  
  -- ===========================================
  -- VOLUME METRICS
  -- ===========================================
  COUNT(DISTINCT t.<transaction_id>) AS transaction_count,
  SUM(t.<quantity>) AS total_units_sold,
  APPROX_COUNT_DISTINCT(t.<customer_id>) AS unique_customers,
  
  -- ===========================================
  -- REVENUE METRICS
  -- ===========================================
  ROUND(SUM(t.<revenue>), 2) AS gross_revenue,
  ROUND(AVG(t.<revenue>), 2) AS avg_transaction_value,
  
  -- ===========================================
  -- PROFIT/COST CALCULATIONS
  -- ===========================================
  ROUND(SUM(t.<quantity> * a.<cost>), 2) AS total_cost,
  ROUND(SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>), 2) AS gross_profit,
  ROUND((SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>)) / 
    NULLIF(SUM(t.<revenue>), 0) * 100, 2) AS profit_margin_pct,
  
  -- ===========================================
  -- PERFORMANCE INDICATORS
  -- ===========================================
  CASE
    WHEN SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>) < 0 THEN 'LOSS'
    WHEN (SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>)) / NULLIF(SUM(t.<revenue>), 0) * 100 < 10 THEN 'LOW_MARGIN'
    WHEN (SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>)) / NULLIF(SUM(t.<revenue>), 0) * 100 >= 30 THEN 'HIGH_MARGIN'
    ELSE 'NORMAL'
  END AS performance_indicator,
  
  -- ===========================================
  -- ASDA AUDIT COLUMNS (Always last!)
  -- ===========================================
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system

FROM LIVE.silver_<transaction_entity> t
INNER JOIN LIVE.silver_<product_entity> a
  ON t.<product_key> = a.<product_key>
INNER JOIN LIVE.silver_<date_entity> d
  ON t.<date_key> = d.<date_key>
GROUP BY 
  t.<date_field>,
  d.<date_dimension1>,
  d.<date_dimension2>,
  a.<product_dimension>,
  a.<category>,
  t.<region>;

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: SCD Type 1 (Current State Only)
-- =============================================================================
-- Use for: Latest snapshot of entity state
-- Key features: AUTO CDC with SCD TYPE 1

CREATE OR REFRESH STREAMING TABLE gold_<entity>_current (
  CONSTRAINT valid_<pk> EXPECT (<primary_key> IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY AUTO
COMMENT "Current state of <entity> (SCD Type 1) - latest snapshot"
TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true");

CREATE FLOW gold_<entity>_current_flow
AS AUTO CDC INTO LIVE.gold_<entity>_current
FROM stream(LIVE.silver_<entity>)
KEYS (<primary_key>)
SEQUENCE BY asda_audit_ts
COLUMNS * EXCEPT (__START_AT, __END_AT)
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: SCD Type 2 (Full History)
-- =============================================================================
-- Use for: Complete audit trail of all changes
-- Key features: AUTO CDC with SCD TYPE 2

CREATE OR REFRESH STREAMING TABLE gold_<entity>_history (
  CONSTRAINT valid_<pk> EXPECT (<primary_key> IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY AUTO
COMMENT "Historical <entity> audit trail (SCD Type 2) - all changes tracked"
TBLPROPERTIES ("quality" = "gold", "delta.enableChangeDataFeed" = "true");

CREATE FLOW gold_<entity>_history_flow
AS AUTO CDC INTO LIVE.gold_<entity>_history
FROM stream(LIVE.silver_<entity>)
KEYS (<primary_key>)
SEQUENCE BY asda_audit_ts
COLUMNS * EXCEPT (__START_AT, __END_AT)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: Materialized View from System Tables
-- =============================================================================
-- Use for: Reading from event logs, information_schema, system tables
-- Key features: Full catalog path, no LIVE prefix for external tables

CREATE OR REFRESH MATERIALIZED VIEW gold_pipeline_performance
COMMENT "Pipeline performance metrics from event log"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  CAST(timestamp AS DATE) AS date,
  origin.pipeline_name AS pipeline_name,
  origin.flow_name AS flow_name,
  origin.dataset_name AS dataset_name,
  
  -- Run metrics
  COUNT(*) AS event_count,
  COUNT(DISTINCT origin.update_id) AS update_count,
  
  -- Duration analysis
  ROUND(AVG(CASE WHEN event_type = 'flow_progress' 
    THEN CAST(details:duration_ms AS DOUBLE) / 60000 END), 2) AS avg_duration_minutes,
  
  -- Error tracking
  SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
  
  -- Health status
  CASE
    WHEN SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) = 0 THEN 'GREEN'
    WHEN SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) <= 2 THEN 'YELLOW'
    ELSE 'RED'
  END AS health_status,
  
  current_timestamp() AS asda_audit_ts,
  'event_log' AS source_system
FROM ${catalog}.${schema}.<pipeline_name>_event_log
WHERE timestamp >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY CAST(timestamp AS DATE), origin.pipeline_name, origin.flow_name, origin.dataset_name;

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: Array Handling with EXPLODE and COLLECT_SET
-- =============================================================================
-- Use for: Working with array columns (diagnosis codes, product IDs, etc.)
-- Key features: EXPLODE for flattening, COLLECT_SET for unique values

CREATE OR REFRESH MATERIALIZED VIEW gold_<entity>_by_<array_element>
COMMENT "<Entity> analytics by <array_element>"
TBLPROPERTIES ("quality" = "gold")
AS 
WITH exploded AS (
  SELECT 
    <primary_key>,
    <dimension>,
    <amount>,
    EXPLODE(<array_column>) AS <element_name>
  FROM LIVE.silver_<entity>
  WHERE <array_column> IS NOT NULL
)
SELECT 
  <element_name>,
  <dimension>,
  COUNT(DISTINCT <primary_key>) AS <entity>_count,
  ROUND(SUM(<amount>), 2) AS total_<amount>,
  ROUND(AVG(<amount>), 2) AS avg_<amount>
  -- Note: Avoid current_timestamp() in MVs for incremental refresh
FROM exploded
GROUP BY <element_name>, <dimension>;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Summary Template
-- MAGIC 
-- MAGIC Update this section based on your actual pipeline:
-- MAGIC 
-- MAGIC **Bronze Layer (X STREAMING tables):**
-- MAGIC - bronze_<entity1> - Raw data from <source>
-- MAGIC - bronze_<entity2> - Raw data from <source>
-- MAGIC - bronze_<entity>_quarantine - Rejected records with reason
-- MAGIC 
-- MAGIC **Silver Layer (X tables with Row Tracking):**
-- MAGIC - silver_<entity1> - Cleaned and validated with DQ constraints
-- MAGIC - silver_<entity2> - Cleaned and validated with DQ constraints
-- MAGIC 
-- MAGIC **Gold Layer (X STREAMING tables + Y MATERIALIZED VIEWS):**
-- MAGIC - gold_<summary1> - Daily aggregations
-- MAGIC - gold_<analytics> - Multi-table join analytics
-- MAGIC - gold_<entity>_current - SCD Type 1 current state
-- MAGIC - gold_<entity>_history - SCD Type 2 full history
-- MAGIC 
-- MAGIC **ASDA Standards Applied:**
-- MAGIC - All tables use lowercase_snake_case naming with layer prefix
-- MAGIC - All tables have asda_audit_ts and source_system as last columns
-- MAGIC - Bronze: CLUSTER BY AUTO, CDF enabled, WHERE clause filtering
-- MAGIC - Silver: CONSTRAINT clauses, Row Tracking, data_quality_flag
-- MAGIC - Gold: CDF enabled, ROUND() for financials, NULLIF() for division
-- MAGIC - Proper Medallion architecture (Bronze -> Silver -> Gold)
