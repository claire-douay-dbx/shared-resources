# Sensitive Data Policy - Financial Services PII Handling

> **For AI Agents:** This document contains PII detection patterns and handling requirements for financial services data. Apply these rules when generating or reviewing code that processes customer data.

---

## 1. PII Detection Patterns - Financial Services

### Column Name Patterns
The DE Agent should automatically identify columns containing PII based on these naming patterns:

| Column Pattern | PII Type | Risk Level | Example Columns |
|----------------|----------|------------|-----------------|
| `customer_id`, `account_id`, `member_id` | IDENTIFIER | LOW | `customer_id`, `primary_account_id` |
| `email`, `email_address`, `e_mail` | EMAIL | HIGH | `email`, `contact_email` |
| `phone`, `mobile`, `telephone` | PHONE | HIGH | `phone_number`, `mobile_phone` |
| `first_name`, `last_name`, `full_name` | NAME | MEDIUM | `first_name`, `customer_name` |
| `ssn`, `social_security`, `national_id`, `tax_id` | SSN | CRITICAL | `ssn`, `national_insurance_number` |
| `dob`, `date_of_birth`, `birth_date` | DOB | MEDIUM | `date_of_birth`, `dob` |
| `address`, `street`, `postal_code`, `zip` | ADDRESS | MEDIUM | `street_address`, `billing_address` |
| `account_number`, `iban`, `sort_code` | ACCOUNT | HIGH | `account_number`, `iban` |
| `credit_card`, `card_number`, `pan`, `cvv` | PAYMENT | CRITICAL | `card_number`, `credit_card_number` |
| `credit_score`, `income`, `salary` | FINANCIAL | HIGH | `credit_score`, `annual_income` |
| `ip_address`, `device_id` | DEVICE | LOW | `ip_address`, `device_fingerprint` |

---

## 2. Financial Services Specific Regulations

### Regulatory Requirements

| Regulation | Jurisdiction | Key Requirements |
|------------|--------------|------------------|
| GDPR | EU/UK | Right to access, erasure, data minimization |
| FCA/PRA | UK | Regulatory reporting, data retention |
| PCI DSS | Global | Payment card data protection |
| SOX | US | Financial reporting controls |
| AML/KYC | Global | Customer identity verification |

### Data Retention Requirements

| Data Type | Minimum Retention | Maximum Retention |
|-----------|-------------------|-------------------|
| Transaction Records | 7 years | 10 years |
| Account Opening Docs | 6 years | 10 years |
| KYC Documents | 5 years after account closure | 7 years |
| Customer Complaints | 5 years | 7 years |
| Marketing Consent | Active until withdrawal | 2 years after withdrawal |

---

## 3. PII Handling Actions by Risk Level

| Risk Level | Required Action | Implementation |
|------------|-----------------|----------------|
| CRITICAL | **Block/Tokenize** | Never store raw; use tokenization or encryption |
| HIGH | **Mask/Hash** | Apply column masking; restrict access to authorized roles |
| MEDIUM | **Restrict Access** | Apply row-level security or column masks |
| LOW | **Audit Only** | Include audit columns; log all access |

---

## 4. Implementation Patterns

### Bronze Layer - Pass Through with PII Markers

At Bronze layer, pass PII through but add comments and properties for visibility:

```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_customers
COMMENT "Raw customer data - CONTAINS PII: name, email, phone, address, income"
TBLPROPERTIES (
  "quality" = "bronze",
  "contains_pii" = "true",
  "pii_columns" = "first_name,last_name,email,phone,date_of_birth,street_address,annual_income,credit_score"
)
AS SELECT 
  customer_id,           -- [PII: IDENTIFIER - LOW]
  first_name,            -- [PII: NAME - MEDIUM]
  last_name,             -- [PII: NAME - MEDIUM]
  email,                 -- [PII: EMAIL - HIGH]
  phone,                 -- [PII: PHONE - HIGH]
  date_of_birth,         -- [PII: DOB - MEDIUM]
  street_address,        -- [PII: ADDRESS - MEDIUM]
  annual_income,         -- [PII: FINANCIAL - HIGH]
  credit_score,          -- [PII: FINANCIAL - HIGH]
  *,
  current_timestamp() AS audit_timestamp,
  'crm_system' AS source_system
FROM STREAM read_files('${volume_path}/customers/*.json', format => 'json');
```

### Silver Layer - Apply Masking for HIGH/CRITICAL PII

At Silver layer, apply masking functions for sensitive data:

```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_customers
COMMENT "Cleaned customer data with PII masking applied"
TBLPROPERTIES (
  "quality" = "silver",
  "pii_masked" = "true"
)
AS SELECT 
  customer_id,
  
  -- Names can pass through (MEDIUM risk) - mask at query time with UC
  INITCAP(TRIM(first_name)) AS first_name,
  INITCAP(TRIM(last_name)) AS last_name,
  
  -- Hash email for matching without exposing raw value
  SHA2(LOWER(TRIM(email)), 256) AS email_hash,
  CONCAT(SUBSTR(email, 1, 3), '***@', SPLIT_PART(email, '@', 2)) AS email_masked,
  
  -- Mask phone - show last 4 digits only
  CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4)) AS phone_masked,
  
  -- Age instead of DOB (derived, less sensitive)
  FLOOR(DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365) AS age,
  
  -- Location without full address
  INITCAP(TRIM(city)) AS city,
  TRIM(region) AS region,
  
  -- Income tier instead of exact income
  CASE
    WHEN annual_income >= 250000 THEN 'High Income'
    WHEN annual_income >= 100000 THEN 'Upper Middle'
    WHEN annual_income >= 50000 THEN 'Middle'
    ELSE 'Lower Middle'
  END AS income_tier,
  
  -- Credit tier instead of exact score
  CASE
    WHEN credit_score >= 750 THEN 'Excellent'
    WHEN credit_score >= 700 THEN 'Good'
    WHEN credit_score >= 650 THEN 'Fair'
    ELSE 'Poor'
  END AS credit_tier,
  
  current_timestamp() AS audit_timestamp,
  'silver_transformation' AS source_system
FROM LIVE.bronze_customers;
```

### Gold Layer - Aggregated Only (No Individual PII)

At Gold layer, only include aggregated data - no individual PII:

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_segments
COMMENT "Customer segment analytics - NO PII"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  region,                 -- Geographic only, no address
  income_tier,            -- Tier only, no exact values
  credit_tier,            -- Tier only, no exact scores
  customer_segment,
  
  COUNT(DISTINCT customer_id) AS customer_count,
  ROUND(AVG(age), 1) AS avg_age,
  ROUND(AVG(tenure_years), 1) AS avg_tenure,
  
  current_timestamp() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM LIVE.silver_customers
GROUP BY region, income_tier, credit_tier, customer_segment;
```

---

## 5. Unity Catalog Column Masking

For dynamic masking based on user permissions:

### Create Masking Functions

```sql
-- Email masking
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii_full_access') THEN email
  WHEN is_member('pii_partial_access') THEN CONCAT(SUBSTR(email, 1, 3), '***@', SPLIT_PART(email, '@', 2))
  ELSE '***@***'
END;

-- Phone masking
CREATE OR REPLACE FUNCTION mask_phone(phone STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii_full_access') THEN phone
  ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4))
END;

-- Income masking
CREATE OR REPLACE FUNCTION mask_income(income DOUBLE)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii_full_access') THEN CAST(income AS STRING)
  WHEN income >= 250000 THEN 'High Income (250K+)'
  WHEN income >= 100000 THEN 'Upper Middle (100K-250K)'
  WHEN income >= 50000 THEN 'Middle (50K-100K)'
  ELSE 'Lower Middle (<50K)'
END;

-- Credit score masking
CREATE OR REPLACE FUNCTION mask_credit_score(score INT)
RETURNS STRING
RETURN CASE 
  WHEN is_member('pii_full_access') THEN CAST(score AS STRING)
  WHEN score >= 750 THEN 'Excellent (750+)'
  WHEN score >= 700 THEN 'Good (700-749)'
  WHEN score >= 650 THEN 'Fair (650-699)'
  ELSE 'Poor (<650)'
END;
```

### Apply Masks to Columns

```sql
-- Apply masks to silver_customers table
ALTER TABLE silver_customers ALTER COLUMN email SET MASK mask_email;
ALTER TABLE silver_customers ALTER COLUMN phone SET MASK mask_phone;
ALTER TABLE silver_customers ALTER COLUMN annual_income SET MASK mask_income;
ALTER TABLE silver_customers ALTER COLUMN credit_score SET MASK mask_credit_score;
```

---

## 6. Row-Level Security for Financial Data

### Regional Access Control

```sql
-- Create row filter for regional access
CREATE OR REPLACE FUNCTION customer_region_filter()
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('all_regions') THEN TRUE
  WHEN is_member('uk_team') AND region IN ('North', 'South', 'London', 'Scotland', 'Wales') THEN TRUE
  WHEN is_member('ireland_team') AND region = 'Northern Ireland' THEN TRUE
  ELSE FALSE
END;

-- Apply to customer tables
ALTER TABLE silver_customers SET ROW FILTER customer_region_filter ON ();
ALTER TABLE gold_customer_segments SET ROW FILTER customer_region_filter ON ();
```

### Segment-Based Access Control

```sql
-- Create row filter for customer segment access
CREATE OR REPLACE FUNCTION customer_segment_filter()
RETURNS BOOLEAN
RETURN CASE
  WHEN is_member('hnw_advisors') THEN TRUE  -- Full access
  WHEN is_member('retail_team') AND customer_segment IN ('Mass Market', 'Mass Affluent') THEN TRUE
  WHEN is_member('business_team') AND customer_segment = 'Small Business' THEN TRUE
  ELSE FALSE
END;

ALTER TABLE silver_customers SET ROW FILTER customer_segment_filter ON ();
```

---

## 7. Account Number Protection

Account numbers require special handling:

```sql
-- Never expose full account numbers in Silver/Gold
-- Use last 4 digits or hash

SELECT 
  account_id,
  -- Masked account number
  CONCAT('****', RIGHT(account_number, 4)) AS account_number_masked,
  -- Hash for joining
  SHA2(account_number, 256) AS account_number_hash,
  ...
FROM LIVE.bronze_accounts;
```

---

## 8. Audit Logging for Financial Data Access

All tables containing PII should log access:

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_pii_access_audit
COMMENT "Audit log for PII table access"
TBLPROPERTIES ("quality" = "audit")
AS SELECT 
  CAST(event_time AS DATE) AS access_date,
  user_identity.email AS accessed_by,
  request_params.full_name_arg AS table_accessed,
  action_name AS operation_type,
  source_ip_address AS client_ip,
  
  COUNT(*) AS access_count
FROM system.access.audit
WHERE action_name IN ('commandSubmit', 'getTable', 'selectFromTable')
  AND request_params.full_name_arg LIKE '%customer%'
GROUP BY 
  CAST(event_time AS DATE),
  user_identity.email,
  request_params.full_name_arg,
  action_name,
  source_ip_address;
```

---

## 9. GDPR Compliance for Financial Services

### Right to Access (Subject Access Request)

```sql
-- Export all data for a specific customer
CREATE OR REPLACE FUNCTION export_customer_data(customer_id_param STRING)
RETURNS TABLE
RETURN SELECT * FROM (
  SELECT 'customer' AS data_type, to_json(struct(*)) AS data 
  FROM silver_customers WHERE customer_id = customer_id_param
  UNION ALL
  SELECT 'accounts' AS data_type, to_json(struct(*)) AS data 
  FROM silver_accounts WHERE customer_id = customer_id_param
  UNION ALL
  SELECT 'transactions' AS data_type, to_json(struct(*)) AS data 
  FROM silver_transactions t
  JOIN silver_accounts a ON t.account_id = a.account_id
  WHERE a.customer_id = customer_id_param
);
```

### Right to Erasure (Right to be Forgotten)

```sql
-- Anonymize customer data (soft delete pattern)
UPDATE silver_customers
SET 
  first_name = 'ANONYMIZED',
  last_name = 'ANONYMIZED',
  email = CONCAT('deleted_', customer_id, '@anonymized.local'),
  phone = 'ANONYMIZED',
  street_address = 'ANONYMIZED',
  date_of_birth = NULL,
  annual_income = NULL,
  credit_score = NULL,
  status = 'DELETED',
  is_anonymized = TRUE,
  anonymized_at = current_timestamp()
WHERE customer_id = '<customer_id_to_delete>';
```

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02 | Databricks Field Engineering | Generic financial services PII handling template |
