# Metadata Approval Workflow

## Overview

The Unity Catalog Metadata Editor uses a **batch-driven approval workflow** that allows metadata changes to be reviewed and approved before being synced to Unity Catalog tables via a separate workflow.

## Workflow Diagram

```
User A (Suggester)          Staging Table           User B (Approver)
      |                           |                         |
      |--1. Browse tables-------->|                         |
      |--2. Edit metadata-------->|                         |
      |--3. Submit changes------->|                         |
      |                           |<--4. View pending-------|
      |                           |<--5. Review changes-----|
      |                           |<--6. Approve/Reject-----|
      |                           |                         |
      |                     (status = 'approved')           |
      |                           |                         |
      |                           |                         |
                         [Separate Sync Workflow]
                                  |
                                  |--7. Push to UC--------->|
                                  |--8. Mark synced-------->|
```

**Key Difference:** Approval only updates the status in the staging table. A separate sync workflow pushes approved changes to Unity Catalog.

## Roles

### User A - Suggester (Browse & Suggest Mode)
- **Requirements:**
  - OAuth login (identity captured from headers)
  - No specific database permissions needed (Service Principal handles all queries)

- **Actions:**
  1. Browse Unity Catalog tables
  2. View current metadata (with status indicators)
  3. Suggest changes to descriptions
  4. Submit changes for approval

### User B - Approver (Review & Approve Mode)
- **Requirements:**
  - OAuth login (identity captured from headers)
  - Email listed in `reviewers_config.yaml` approvers list
  - No specific database permissions needed (Service Principal handles all operations)

- **Actions:**
  1. View tables with pending changes
  2. Review proposed changes
  3. Compare current vs. proposed values
  4. Approve or reject changes
  5. **Approved changes remain in staging table** until synced

## Status Indicators

The app shows visual indicators to help users understand the current state:

| Indicator | Meaning |
|-----------|---------|
| (none) | Showing UC value, no pending changes |
| ⏳ Pending | A suggestion is awaiting review |
| ✅ Approved | Approved but not yet synced to UC |

**Auto-detection:** The app automatically detects when an approved suggestion has been synced by comparing the suggestion value with the current UC value.

## Staging Table

### Location
- **Catalog:** Configurable via `STAGING_CATALOG` env var (default: `asda_metadata_rampup`)
- **Schema:** Configurable via `STAGING_SCHEMA` env var (default: `metadata_population`)
- **Table:** `metadata_suggestions`

### Schema
```sql
CREATE TABLE metadata_suggestions (
    suggestion_date TIMESTAMP,     -- Submission timestamp
    suggestor_id STRING,           -- Username/email of suggester
    catalog STRING,                -- Target catalog
    `schema` STRING,               -- Target schema
    asset STRING,                  -- Target table/view name
    asset_type STRING,             -- Type: table, view, materialized view
    suggestion_format STRING,      -- Type: table description, column description
    suggestion_on STRING,          -- Target: table name or column name
    suggestion STRING,             -- Proposed value
    suggestion_type STRING,        -- Type: add new, modify, delete
    status STRING,                 -- Status: pending, approved, rejected
    reviewer_id STRING,            -- Username of reviewer (NULL for pending)
    review_date TIMESTAMP,         -- Review timestamp (NULL for pending)
    reviewer_comments STRING,      -- Reviewer's comments (optional)
    change_id STRING,              -- Unique identifier for each change
    synced_to_uc BOOLEAN           -- Flag: TRUE when synced to UC
)
```

## Configuration

### Environment Variables

Add these to your `app.yaml` for Databricks Apps:

```yaml
env:
  - name: "DATABRICKS_WAREHOUSE_ID"
    value: "your-warehouse-id"
  - name: "STAGING_CATALOG"
    value: "asda_metadata_rampup"
  - name: "STAGING_SCHEMA"
    value: "metadata_population"
  - name: "ALLOWED_CATALOGS"
    value: "my_catalog,production_catalog"
  - name: "ALLOWED_SCHEMAS"
    value: ""
```

### Approver Configuration

Approvers are configured via `reviewers_config.yaml`:

```yaml
# Approver Configuration
# 
# Approvers can view pending changes, add comments, and approve/reject suggestions.
# Users not in this list can only use "Browse & Suggest" mode.

approvers:
  - admin@company.com
  - data-steward@company.com
```

## Usage Guide

### For Suggesters

1. **Launch the app** and select "Browse & Suggest" mode

2. **Browse and select a table:**
   - Choose catalog from dropdown
   - Choose schema from dropdown
   - Choose table/view from dropdown
   - Click "Load Metadata"

3. **View current metadata:**
   - Table description (with status indicator if applicable)
   - Column descriptions (with status indicators)
   - ⏳ = pending change exists
   - ✅ = approved change pending sync

4. **Propose changes:**
   - Click "Edit Metadata"
   - Modify descriptions
   - **Note:** If an item already has a pending change, you cannot submit another until it's reviewed

5. **Submit for approval:**
   - Click "Submit Changes for Approval"
   - Changes are saved to staging table with status `pending`

### For Approvers

1. **Launch the app** and select "Review & Approve" mode

2. **View pending changes:**
   - Sidebar shows count of tables with pending changes
   - See total number of changes across all tables

3. **Select a table to review:**
   - Click on a table name from the sidebar
   - Shows all pending changes for that table

4. **Review individual changes:**
   - Each change shows:
     - Change type (table/column description)
     - Current UC value vs. Proposed value
     - Submitter and timestamp

5. **Take action:**
   - **Approve:** Status updated to `approved` (remains in staging table)
   - **Reject:** Status updated to `rejected`

6. **Bulk actions (optional):**
   - "Approve All Changes" - Approve all changes for the table
   - "Reject All Changes" - Reject all changes for the table

## Sync Workflow

A separate workflow is needed to push approved changes to Unity Catalog. This can be:
- A Databricks job running on a schedule
- A notebook triggered manually
- A workflow triggered by an external system

### Example Sync Workflow (SQL)

```sql
-- 1. Get approved suggestions not yet synced
CREATE OR REPLACE TEMP VIEW pending_sync AS
SELECT * FROM asda_metadata_rampup.metadata_population.metadata_suggestions
WHERE status = 'approved' 
  AND (synced_to_uc IS NULL OR synced_to_uc = FALSE);

-- 2. For each row, apply the change to UC
-- (This requires iteration - use a notebook or stored procedure)

-- 3. Mark as synced
UPDATE asda_metadata_rampup.metadata_population.metadata_suggestions
SET synced_to_uc = TRUE
WHERE status = 'approved' 
  AND (synced_to_uc IS NULL OR synced_to_uc = FALSE);
```

### Example Sync Workflow (Python/PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Get approved but not synced suggestions
pending = spark.sql("""
    SELECT * FROM asda_metadata_rampup.metadata_population.metadata_suggestions
    WHERE status = 'approved' 
      AND (synced_to_uc IS NULL OR synced_to_uc = FALSE)
""").collect()

# Apply each change
for row in pending:
    if row.suggestion_format == 'table description':
        full_name = f"{row.catalog}.{row.schema}.{row.asset}"
        comment = row.suggestion.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
    elif row.suggestion_format == 'column description':
        full_name = f"{row.catalog}.{row.schema}.{row.asset}"
        column = row.suggestion_on
        comment = row.suggestion.replace("'", "''")
        spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN `{column}` COMMENT '{comment}'")

# Mark all as synced (bulk update)
spark.sql("""
    UPDATE asda_metadata_rampup.metadata_population.metadata_suggestions
    SET synced_to_uc = TRUE
    WHERE status = 'approved' 
      AND (synced_to_uc IS NULL OR synced_to_uc = FALSE)
""")
```

### Auto-Detection

Even without explicitly setting `synced_to_uc = TRUE`, the app will auto-detect synced suggestions by comparing the suggestion value with the current UC value. If they match, the suggestion is treated as synced.

## Change Types

### Table Description
- **Type:** `table description`
- **Description:** Changes to table/view description
- **Applied via:** `COMMENT ON TABLE` (or `COMMENT ON VIEW` for views)

### Column Description
- **Type:** `column description`
- **Description:** Changes to column description
- **Applied via:** `ALTER TABLE ... ALTER COLUMN ... COMMENT` (or `COMMENT ON COLUMN` for views)

### Suggestion Types
- **add new:** Adding metadata where none existed
- **modify:** Changing existing metadata
- **delete:** Removing existing metadata (setting to empty)

## Permissions Setup

All database operations are performed by the **Service Principal**. Users do not need individual database permissions.

### Grant Permissions to App Service Principal

```sql
-- Read access to browse catalogs, schemas, and tables
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-id>`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<service-principal-id>`;

-- Staging table full access
GRANT ALL PRIVILEGES ON CATALOG asda_metadata_rampup TO `<service-principal-id>`;
GRANT ALL PRIVILEGES ON SCHEMA asda_metadata_rampup.metadata_population TO `<service-principal-id>`;
```

### Grant Permissions to Sync Workflow

The sync workflow needs MODIFY permissions:

```sql
-- Modify permissions for the sync workflow to update UC metadata
GRANT MODIFY ON CATALOG <catalog_name> TO `<sync-workflow-principal>`;
-- Or grant on specific schemas:
GRANT MODIFY ON SCHEMA <catalog>.<schema> TO `<sync-workflow-principal>`;
```

## Troubleshooting

### Issue: "Staging table not found"
**Solution:** The app auto-creates the staging table on first launch. If this fails:
- Check that the service principal has CREATE permissions on the staging catalog/schema
- Manually create the staging table using the schema above

### Issue: "Failed to submit changes"
**Solution:** 
- Verify the service principal has INSERT permissions on staging table
- Check that STAGING_CATALOG and STAGING_SCHEMA exist

### Issue: "Approved changes not showing in UC"
**Solution:**
- Remember: Approval doesn't write to UC - you need a sync workflow
- Check that your sync workflow is running
- Verify the sync workflow has MODIFY permissions

### Issue: "No pending changes showing up"
**Solution:**
- Verify the service principal has SELECT permissions on staging table
- Check that changes were successfully submitted

## Query Staging Table Directly

```sql
-- View all pending changes
SELECT * 
FROM asda_metadata_rampup.metadata_population.metadata_suggestions 
WHERE status = 'pending'
ORDER BY suggestion_date DESC;

-- View approved but not synced
SELECT * 
FROM asda_metadata_rampup.metadata_population.metadata_suggestions 
WHERE status = 'approved'
  AND (synced_to_uc IS NULL OR synced_to_uc = FALSE);

-- View approval history
SELECT suggestor_id, reviewer_id, status, synced_to_uc,
       COUNT(*) as change_count
FROM asda_metadata_rampup.metadata_population.metadata_suggestions 
GROUP BY suggestor_id, reviewer_id, status, synced_to_uc;
```

## Cleanup Old Records

```sql
-- Delete synced approved changes older than 30 days
DELETE FROM asda_metadata_rampup.metadata_population.metadata_suggestions
WHERE status = 'approved'
  AND synced_to_uc = TRUE
  AND review_date < CURRENT_TIMESTAMP - INTERVAL 30 DAYS;

-- Delete rejected changes older than 30 days
DELETE FROM asda_metadata_rampup.metadata_population.metadata_suggestions
WHERE status = 'rejected'
  AND review_date < CURRENT_TIMESTAMP - INTERVAL 30 DAYS;
```

## Granular Change Blocking

The approval workflow uses **granular blocking** to prevent conflicting changes:

- Blocking is done at the **table description / column description level**
- If there's a pending change on a specific metadata item, only that item is blocked
- Users can still suggest changes to other metadata items on the same table

## Best Practices

1. **Schedule sync workflow:** Run the sync during off-peak hours
2. **Regular Reviews:** Approvers should check for pending changes regularly
3. **Clear Descriptions:** Suggestors should provide meaningful metadata
4. **Audit Trail:** Keep staging table records for audit purposes
5. **Monitor sync status:** Check for approved but not-synced records

## Security Considerations

- **Separation of Duties:** Suggesting != Approving != Syncing
- **Audit Trail:** All changes tracked with user and timestamp
- **Batch Control:** Sync workflow can be controlled/scheduled
- **Authentication:** Uses Databricks OAuth for user identification
