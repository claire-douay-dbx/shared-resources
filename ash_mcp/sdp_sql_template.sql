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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # BRONZE LAYER - Raw Data Ingestion
-- MAGIC 
-- MAGIC Minimal transformation, preserve source data structure

-- COMMAND ----------

-- =============================================================================
-- BRONZE LAYER TEMPLATE: Materialized View from existing table
-- =============================================================================
-- Use this pattern when reading from existing Delta tables

CREATE OR REFRESH MATERIALIZED VIEW bronze_<entity_name>
COMMENT "<Description of raw data source>"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
  -- All source columns
  *,
  -- ASDA audit columns (REQUIRED)
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system
FROM <source_catalog>.<source_schema>.<source_table>
WHERE <primary_key> IS NOT NULL;  -- Basic NULL filtering only

-- COMMAND ----------

-- =============================================================================
-- BRONZE LAYER TEMPLATE: Streaming Table from files (Auto Loader)
-- =============================================================================
-- Use this pattern when ingesting from files

CREATE OR REFRESH STREAMING TABLE bronze_<entity_name> (
  CONSTRAINT valid_pk EXPECT (<primary_key> IS NOT NULL) ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT "<Description of raw data source>"
TBLPROPERTIES ("quality" = "bronze", "delta.enableChangeDataFeed" = "true")
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  '<source_system>' AS source_system,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  "${data_path}/raw_data/<entity>/*.json",
  format => "json"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SILVER LAYER - Data Cleansing & Validation
-- MAGIC 
-- MAGIC Full validation, type conversions, derived fields

-- COMMAND ----------

-- =============================================================================
-- SILVER LAYER TEMPLATE
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_<entity_name> (
  -- Critical constraints (halt pipeline on failure)
  CONSTRAINT valid_pk EXPECT (<primary_key> IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- Non-critical constraints (filter bad rows)
  CONSTRAINT valid_amounts EXPECT (<amount_field> >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dates EXPECT (<date_field> IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated <entity> with derived metrics"
TBLPROPERTIES ("quality" = "silver")
AS SELECT 
  -- ===========================================
  -- IDENTIFIERS (Primary keys, foreign keys)
  -- ===========================================
  TRIM(<primary_key>) AS <primary_key>,
  TRIM(<foreign_key>) AS <foreign_key>,
  
  -- ===========================================
  -- DIMENSIONS (Categorical fields)
  -- ===========================================
  INITCAP(TRIM(<category_field>)) AS <category_field>,
  TRIM(<status_field>) AS <status_field>,
  
  -- ===========================================
  -- DATES
  -- ===========================================
  <date_field>,
  
  -- ===========================================
  -- AMOUNTS (With type conversions)
  -- ===========================================
  CAST(<amount_field> AS DOUBLE) AS <amount_field>,
  CAST(<quantity_field> AS BIGINT) AS <quantity_field>,
  
  -- ===========================================
  -- DERIVED FIELDS
  -- ===========================================
  -- Example: Net amount after discount
  ROUND(CAST(<total_amount> AS DOUBLE) * (1 - COALESCE(CAST(<discount_pct> AS DOUBLE), 0) / 100), 2) AS net_amount,
  
  -- Example: Boolean flag
  CASE WHEN <condition> THEN TRUE ELSE FALSE END AS is_<flag_name>,
  
  -- Example: Calculated percentage
  CASE 
    WHEN CAST(<denominator> AS DOUBLE) > 0 
    THEN ROUND(CAST(<numerator> AS DOUBLE) / CAST(<denominator> AS DOUBLE) * 100, 2)
    ELSE 0
  END AS <metric>_pct,
  
  -- ===========================================
  -- DATA QUALITY FLAG
  -- ===========================================
  CASE
    WHEN <field1> IS NULL THEN 'MISSING_<FIELD1>'
    WHEN <field2> < 0 THEN 'NEGATIVE_<FIELD2>'
    WHEN <field3> > 100 THEN 'INVALID_<FIELD3>'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  
  -- ===========================================
  -- ASDA AUDIT COLUMNS (Always last!)
  -- ===========================================
  current_timestamp() AS asda_audit_ts,
  'silver_transformation' AS source_system
  
FROM LIVE.bronze_<entity_name>
WHERE <filter_condition>;  -- Additional filtering if needed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # GOLD LAYER - Business Aggregations
-- MAGIC 
-- MAGIC Aggregations, joins, KPIs

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: Aggregation
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_<metric_name>_summary
COMMENT "<Description of business metrics>"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  -- ===========================================
  -- DIMENSIONS (GROUP BY columns)
  -- ===========================================
  t.<date_field> AS <date_alias>,
  t.<dimension1>,
  t.<dimension2>,
  
  -- ===========================================
  -- VOLUME METRICS
  -- ===========================================
  COUNT(DISTINCT t.<primary_key>) AS <entity>_count,
  COUNT(DISTINCT t.<secondary_key>) AS unique_<secondary_entity>s,
  SUM(t.<quantity_field>) AS total_<quantity>,
  
  -- ===========================================
  -- REVENUE METRICS
  -- ===========================================
  ROUND(SUM(t.<amount_field>), 2) AS total_<amount>,
  ROUND(AVG(t.<amount_field>), 2) AS avg_<amount>,
  
  -- ===========================================
  -- CALCULATED RATIOS
  -- ===========================================
  ROUND(SUM(t.<amount_field>) / NULLIF(COUNT(DISTINCT t.<entity_key>), 0), 2) AS <amount>_per_<entity>,
  
  -- ===========================================
  -- PERCENTAGE METRICS
  -- ===========================================
  ROUND(SUM(CASE WHEN t.<condition> THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS <condition>_rate_pct,
  
  -- ===========================================
  -- ASDA AUDIT COLUMNS
  -- ===========================================
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system
  
FROM LIVE.silver_<entity_name> t
GROUP BY 
  t.<date_field>,
  t.<dimension1>,
  t.<dimension2>;

-- COMMAND ----------

-- =============================================================================
-- GOLD LAYER TEMPLATE: Multi-table Join
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_<combined_view_name>
COMMENT "<Description of combined analytics>"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  -- ===========================================
  -- DIMENSIONS FROM MULTIPLE TABLES
  -- ===========================================
  d.<date_field>,
  d.<date_dimension1>,
  d.<date_dimension2>,
  
  t.<entity_dimension>,
  
  a.<product_dimension>,
  a.<category>,
  
  -- ===========================================
  -- AGGREGATED METRICS
  -- ===========================================
  COUNT(DISTINCT t.<primary_key>) AS <entity>_count,
  ROUND(SUM(t.<amount>), 2) AS total_<amount>,
  
  -- ===========================================
  -- PROFIT/COST CALCULATIONS
  -- ===========================================
  ROUND(SUM(t.<quantity> * a.<cost>), 2) AS total_cost,
  ROUND(SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>), 2) AS gross_profit,
  CASE 
    WHEN SUM(t.<revenue>) > 0 
    THEN ROUND((SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>)) / SUM(t.<revenue>) * 100, 2)
    ELSE 0
  END AS profit_margin_pct,
  
  -- ===========================================
  -- PERFORMANCE INDICATORS
  -- ===========================================
  CASE
    WHEN SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>) < 0 THEN 'LOSS'
    WHEN (SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>)) / NULLIF(SUM(t.<revenue>), 0) * 100 < 10 THEN 'LOW_MARGIN'
    WHEN (SUM(t.<revenue>) - SUM(t.<quantity> * a.<cost>)) / NULLIF(SUM(t.<revenue>), 0) * 100 >= 30 THEN 'HIGH_MARGIN'
    ELSE 'NORMAL'
  END AS performance_indicator

FROM LIVE.silver_<transaction_entity> t
INNER JOIN LIVE.silver_<product_entity> a
  ON t.<product_key> = a.<product_key>
INNER JOIN LIVE.silver_<date_entity> d
  ON t.<date_key> = d.<date_key>
GROUP BY 
  d.<date_field>,
  d.<date_dimension1>,
  d.<date_dimension2>,
  t.<entity_dimension>,
  a.<product_dimension>,
  a.<category>;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Summary Template
-- MAGIC 
-- MAGIC Update this section based on your actual pipeline:
-- MAGIC 
-- MAGIC **Bronze Layer:**
-- MAGIC - bronze_<entity1> - <description>
-- MAGIC - bronze_<entity2> - <description>
-- MAGIC 
-- MAGIC **Silver Layer:**
-- MAGIC - silver_<entity1> - <description>
-- MAGIC - silver_<entity2> - <description>
-- MAGIC 
-- MAGIC **Gold Layer:**
-- MAGIC - gold_<summary1> - <description>
-- MAGIC - gold_<analytics> - <description>
-- MAGIC 
-- MAGIC **ASDA Standards Applied:**
-- MAGIC - All tables use lowercase_snake_case naming
-- MAGIC - All tables have asda_audit_ts and source_system columns
-- MAGIC - Data quality constraints at Silver layer
-- MAGIC - Proper medallion architecture (Bronze -> Silver -> Gold)
