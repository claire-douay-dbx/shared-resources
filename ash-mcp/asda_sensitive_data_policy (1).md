# ASDA Sensitive Data Policy - PII Handling Standards

> **For AI Agents:** Apply these rules when creating tables that contain personally identifiable information (PII) or other sensitive data. These standards ensure GDPR compliance and enable Unity Catalog's data classification features.

---

## Quick Reference - PII Handling Checklist

| Requirement | How to Apply |
|-------------|--------------|
| Table label | Add `TBLPROPERTIES ("contains_pii" = "true")` |
| Table comment | Include `[CONTAINS PII]` prefix in COMMENT |
| Column comment | Add `[PII: <type>]` prefix to sensitive column comments |
| Column tags | Apply Unity Catalog tags (e.g., `class.email_address`) |

---

## 1. PII Classification Types

### Standard PII Types

| PII Type | Tag | Risk Level | Examples |
|----------|-----|------------|----------|
| `SSN` | `class.ssn` | CRITICAL | Social Security Number, National Insurance Number |
| `CREDIT_CARD` | `class.credit_card` | CRITICAL | Card numbers, CVV |
| `BANK_ACCOUNT` | `class.bank_account` | CRITICAL | Account numbers, sort codes |
| `EMAIL` | `class.email_address` | HIGH | Email addresses |
| `PHONE` | `class.phone_number` | HIGH | Phone numbers, mobile numbers |
| `DOB` | `class.date_of_birth` | HIGH | Date of birth |
| `NAME` | `class.name` | MEDIUM | First name, last name, full name |
| `ADDRESS` | `class.address` | MEDIUM | Street address, postcode |
| `LOCATION` | `class.location` | MEDIUM | GPS coordinates, store location |
| `IP_ADDRESS` | `class.ip_address` | MEDIUM | IP addresses |
| `CUSTOMER_ID` | `class.customer_id` | LOW | Internal customer identifiers |

### PII Detection Patterns

**For AI Agents:** When creating or reviewing tables, scan column names for these patterns to automatically identify PII:

| Column Name Contains | PII Type | Risk | Action |
|---------------------|----------|------|--------|
| `customer_id`, `member_id`, `user_id`, `account_id`, `loyalty_id` | CUSTOMER_ID | LOW | Mark with `[PII: CUSTOMER_ID]` |
| `email`, `email_address`, `e_mail` | EMAIL | HIGH | Mark with `[PII: EMAIL]` |
| `phone`, `mobile`, `telephone`, `tel_`, `contact_number` | PHONE | HIGH | Mark with `[PII: PHONE]` |
| `ssn`, `national_insurance`, `ni_number`, `social_security` | SSN | CRITICAL | Mark with `[PII: SSN]` |
| `first_name`, `last_name`, `full_name`, `customer_name`, `member_name` | NAME | MEDIUM | Mark with `[PII: NAME]` |
| `address`, `street`, `postcode`, `zip_code`, `postal_code`, `city` | ADDRESS | MEDIUM | Mark with `[PII: ADDRESS]` |
| `dob`, `date_of_birth`, `birth_date`, `birthdate` | DOB | HIGH | Mark with `[PII: DOB]` |
| `card_number`, `credit_card`, `payment_card`, `cvv` | CREDIT_CARD | CRITICAL | Mark with `[PII: CREDIT_CARD]` |
| `bank_account`, `sort_code`, `iban`, `account_number` | BANK_ACCOUNT | CRITICAL | Mark with `[PII: BANK_ACCOUNT]` |
| `ip_address`, `ip_addr`, `client_ip` | IP_ADDRESS | MEDIUM | Mark with `[PII: IP_ADDRESS]` |
| `latitude`, `longitude`, `lat_long`, `gps_` | LOCATION | MEDIUM | Mark with `[PII: LOCATION]` |

**Example:** If you see a column named `customer_id` in a transactions table, automatically:
1. Add `[PII: CUSTOMER_ID]` to the column COMMENT
2. Add `"contains_pii" = "true"` to TBLPROPERTIES
3. Add `CUSTOMER_ID` to the `"pii_types"` property

---

## 2. Table-Level PII Marking

### TBLPROPERTIES for PII Tables

```sql
TBLPROPERTIES (
  "quality" = "silver",
  "contains_pii" = "true",                    -- REQUIRED for PII tables
  "pii_types" = "EMAIL,PHONE,NAME,ADDRESS",   -- List of PII types present
  "data_classification" = "CONFIDENTIAL",     -- CONFIDENTIAL, INTERNAL, PUBLIC
  "retention_days" = "365",                   -- Data retention policy
  "delta.enableChangeDataFeed" = "true"
)
```

### Table COMMENT Pattern

```sql
-- Pattern: [CONTAINS PII] <description> - <PII summary>
COMMENT "[CONTAINS PII] Customer profile data with contact details - includes NAME, EMAIL, PHONE, ADDRESS"
```

### Complete Bronze Example (with PII)

```sql
CREATE OR REFRESH STREAMING TABLE bronze_customers
CLUSTER BY AUTO
COMMENT "[CONTAINS PII] Raw customer data from loyalty system - includes NAME, EMAIL, PHONE, ADDRESS, DOB"
TBLPROPERTIES (
  "quality" = "bronze",
  "contains_pii" = "true",
  "pii_types" = "NAME,EMAIL,PHONE,ADDRESS,DOB",
  "data_classification" = "CONFIDENTIAL",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT 
  *,
  current_timestamp() AS asda_audit_ts,
  'loyalty_system' AS source_system
FROM STREAM read_files('${volume_path}/customers/*.json', format => 'json')
WHERE customer_id IS NOT NULL;
```

---

## 3. Column-Level PII Marking

### Column COMMENT Pattern

```sql
-- Pattern: [PII: <TYPE>] <description>
first_name STRING COMMENT "[PII: NAME] Customer first name",
email STRING COMMENT "[PII: EMAIL] Customer email address for communications",
phone STRING COMMENT "[PII: PHONE] Primary contact phone number",
date_of_birth DATE COMMENT "[PII: DOB] Customer date of birth for age verification"
```

### Complete Silver Example (with PII columns)

```sql
CREATE OR REFRESH STREAMING TABLE silver_customers (
  -- Identifiers
  customer_id STRING NOT NULL COMMENT "Unique customer identifier",
  loyalty_card_number STRING COMMENT "[PII: CUSTOMER_ID] Loyalty program card number",
  
  -- PII Fields
  first_name STRING COMMENT "[PII: NAME] Customer first name",
  last_name STRING COMMENT "[PII: NAME] Customer last name",
  email STRING COMMENT "[PII: EMAIL] Customer email address",
  phone STRING COMMENT "[PII: PHONE] Primary contact phone number",
  date_of_birth DATE COMMENT "[PII: DOB] Customer date of birth",
  
  -- Address (PII)
  address_line1 STRING COMMENT "[PII: ADDRESS] Street address line 1",
  address_line2 STRING COMMENT "[PII: ADDRESS] Street address line 2",
  city STRING COMMENT "[PII: ADDRESS] City",
  postcode STRING COMMENT "[PII: ADDRESS] UK postcode",
  
  -- Non-PII Fields
  registration_date DATE COMMENT "Date customer registered",
  preferred_store STRING COMMENT "Preferred store location code",
  marketing_opt_in BOOLEAN COMMENT "Marketing consent flag",
  
  -- Derived fields
  age_group STRING COMMENT "Age group bucket (derived from DOB)",
  customer_segment STRING COMMENT "Customer segment classification",
  data_quality_flag STRING COMMENT "Data quality indicator",
  
  -- Audit columns
  asda_audit_ts TIMESTAMP COMMENT "Pipeline processing timestamp",
  source_system STRING COMMENT "Source system identifier",
  
  -- Constraints
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email EXPECT (email LIKE '%@%') ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT "[CONTAINS PII] Validated customer profiles - includes NAME, EMAIL, PHONE, ADDRESS, DOB"
TBLPROPERTIES (
  "quality" = "silver",
  "contains_pii" = "true",
  "pii_types" = "NAME,EMAIL,PHONE,ADDRESS,DOB,CUSTOMER_ID",
  "data_classification" = "CONFIDENTIAL",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true"
);
```

---

## 4. Gold Layer PII Handling

### Option A: Aggregate Away PII (Recommended)

```sql
-- Gold tables should aggregate to remove PII where possible
CREATE OR REFRESH MATERIALIZED VIEW gold_customer_demographics
COMMENT "Customer demographics summary - NO PII (aggregated)"
TBLPROPERTIES (
  "quality" = "gold",
  "contains_pii" = "false",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT 
  region,
  age_group,
  customer_segment,
  COUNT(DISTINCT customer_id) AS customer_count,
  ROUND(AVG(total_spend), 2) AS avg_spend,
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system
FROM LIVE.silver_customers c
JOIN LIVE.silver_transactions t ON c.customer_id = t.customer_id
GROUP BY region, age_group, customer_segment;
```

### Option B: Gold with PII (for authorized use cases)

```sql
-- When PII must be retained in Gold, mark clearly
CREATE OR REFRESH STREAMING TABLE gold_customer_360
CLUSTER BY AUTO
COMMENT "[CONTAINS PII] Complete customer view for CRM - includes NAME, EMAIL, PHONE - AUTHORIZED ACCESS ONLY"
TBLPROPERTIES (
  "quality" = "gold",
  "contains_pii" = "true",
  "pii_types" = "NAME,EMAIL,PHONE",
  "data_classification" = "CONFIDENTIAL",
  "authorized_roles" = "CRM_ADMIN,CUSTOMER_SERVICE",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT 
  customer_id,
  -- PII fields (mark in SELECT for visibility)
  first_name,      -- [PII: NAME]
  last_name,       -- [PII: NAME]
  email,           -- [PII: EMAIL]
  phone,           -- [PII: PHONE]
  -- Non-PII aggregations
  total_orders,
  total_spend,
  avg_order_value,
  customer_segment,
  current_timestamp() AS asda_audit_ts,
  'gold_aggregation' AS source_system
FROM LIVE.silver_customers c
JOIN LIVE.silver_order_summary o ON c.customer_id = o.customer_id;
```

---

## 5. Unity Catalog Tags (Post-Deployment)

After tables are created, apply Unity Catalog classification tags:

```sql
-- Apply column-level tags for PII
ALTER TABLE silver_customers ALTER COLUMN first_name SET TAGS ('class.name');
ALTER TABLE silver_customers ALTER COLUMN last_name SET TAGS ('class.name');
ALTER TABLE silver_customers ALTER COLUMN email SET TAGS ('class.email_address');
ALTER TABLE silver_customers ALTER COLUMN phone SET TAGS ('class.phone_number');
ALTER TABLE silver_customers ALTER COLUMN date_of_birth SET TAGS ('class.date_of_birth');
ALTER TABLE silver_customers ALTER COLUMN address_line1 SET TAGS ('class.address');
ALTER TABLE silver_customers ALTER COLUMN postcode SET TAGS ('class.address');

-- Apply table-level tag
ALTER TABLE silver_customers SET TAGS ('pii_table' = 'true');
```

---

## 6. Column Masking Recommendations

| PII Type | Masking Function | Example Output |
|----------|------------------|----------------|
| SSN | `mask_ssn()` | `***-**-1234` |
| EMAIL | `mask_email()` | `j***@***.com` |
| PHONE | `mask_phone()` | `***-***-1234` |
| DOB | `mask_date_of_birth()` | `****-**-15` |
| NAME | `mask_name()` | `J*** S***` |
| ADDRESS | `mask_address()` | `*** High Street` |
| CREDIT_CARD | `mask_credit_card()` | `****-****-****-1234` |

### Applying Masks via Row-Level Security

```sql
-- Create masking function
CREATE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN IS_ACCOUNT_GROUP_MEMBER('PII_VIEWERS') THEN email
  ELSE CONCAT(SUBSTRING(email, 1, 1), '***@***.com')
END;

-- Apply to column
ALTER TABLE silver_customers 
ALTER COLUMN email SET MASK mask_email;
```

---

## 7. Data Classification Matrix

| Layer | Contains PII? | data_classification | Access Control |
|-------|---------------|---------------------|----------------|
| Bronze | Often | CONFIDENTIAL | Data Engineers only |
| Silver | Often | CONFIDENTIAL | Data Engineers + Analysts (masked) |
| Gold (aggregated) | No | INTERNAL | Business Users |
| Gold (detailed) | Sometimes | CONFIDENTIAL | Authorized roles only |

---

## 8. GDPR Compliance Checklist

When creating tables with PII, ensure:

- [ ] Table has `"contains_pii" = "true"` property
- [ ] Table COMMENT includes `[CONTAINS PII]` prefix
- [ ] All PII columns have `[PII: TYPE]` in COMMENT
- [ ] `pii_types` property lists all PII types present
- [ ] `data_classification` property is set
- [ ] Consider `retention_days` for data lifecycle
- [ ] Document `authorized_roles` for access control
- [ ] Plan for column masking post-deployment
- [ ] Consider aggregating away PII in Gold layer

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.1 | 2026-01-19 | ASDA Data Platform Team | Added PII Detection Patterns for auto-identification |
| 1.0 | 2026-01-18 | ASDA Data Platform Team | Initial sensitive data policy |
