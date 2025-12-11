# Implementation Guide

This guide provides a comprehensive walkthrough for implementing the Unity Catalog Metadata Editor in your Databricks workspace. Follow these steps in order to ensure a successful deployment.

> ⚠️ **Reminder:** Please review the [disclaimers in the README](README.md) before proceeding. This code is provided as a reference implementation and requires proper evaluation and testing before production use.

---

## 📚 Documentation Reading Order

Before implementing, familiarize yourself with the repository by reading the documentation in this order:

1. **[README.md](README.md)** - Start here for an overview of the app's features, architecture, and high-level deployment steps
2. **[PERMISSIONS_MODEL.md](PERMISSIONS_MODEL.md)** - Understand the authentication and authorization model, including OAuth setup and service principal requirements
3. **[APPROVAL_WORKFLOW.md](APPROVAL_WORKFLOW.md)** - Deep dive into the batch-driven approval workflow, staging table schema, and sync process
4. **[QUICKSTART.md](QUICKSTART.md)** - Quick reference for deployment commands and troubleshooting

---

## 🛠️ Implementation Steps

### Step 1: Configure Allowed Approvers

**What:** Define which users can approve metadata change suggestions.

**Where to edit:** `reviewers_config.yaml`

```yaml
# Approver Configuration
# Users not in this list can only use "Browse & Suggest" mode.

approvers:
  - data-steward@yourcompany.com
  - metadata-admin@yourcompany.com
  - governance-team@yourcompany.com
```

**Notes:**
- Use the exact email addresses that users will authenticate with via OAuth
- You can add as many approvers as needed
- Only approvers can see and access the "Review & Approve" mode in the app

---

### Step 2: Configure Allowed Catalogs

**What:** Define which Unity Catalog catalogs the app can browse and display in the UI.

**Where to edit:** `app.yaml`

```yaml
env:
  - name: "DATABRICKS_WAREHOUSE_ID"
    value: "your_warehouse_id_here"

  - name: "ALLOWED_CATALOGS"
    value: "catalog1,catalog2,catalog3"

  - name: "ALLOWED_SCHEMAS"
    value: ""  # Optional: "catalog1:schema1,schema2;catalog2:schema3"
```

**Configuration options:**
- `DATABRICKS_WAREHOUSE_ID`: Your SQL Warehouse ID (required)
- `ALLOWED_CATALOGS`: Comma-separated list of catalog names users can browse
- `ALLOWED_SCHEMAS`: Optional schema filtering per catalog (format: `catalog:schema1,schema2;catalog2:schema3`)

**Important:** The same catalog list should be configured in the KPIs notebook (Step 4) to ensure consistency.

---

### Step 3: Create the Metadata Suggestions Table

**What:** Create a dedicated catalog/schema for storing metadata suggestions and KPIs cache.

**Recommendation:** Create a dedicated catalog for this app's operational data.

```sql
-- Option A: Create a dedicated catalog (recommended)
CREATE CATALOG IF NOT EXISTS metadata_management;
CREATE SCHEMA IF NOT EXISTS metadata_management.app_data;

-- Option B: Use an existing catalog with a dedicated schema
CREATE SCHEMA IF NOT EXISTS your_catalog.metadata_app;
```

**Configure the staging table location** in `app.yaml`:

```yaml
env:
  # ... other env vars ...
  
  - name: "STAGING_CATALOG"
    value: "metadata_management"  # Or your chosen catalog
    
  - name: "STAGING_SCHEMA"
    value: "app_data"  # Or your chosen schema
```

**Note:** You can also set these in `app.py` (lines 173-175) if you prefer to hardcode them:

```python
STAGING_CATALOG = os.getenv('STAGING_CATALOG', 'your_catalog_name')
STAGING_SCHEMA = os.getenv('STAGING_SCHEMA', 'your_schema_name')
STAGING_TABLE = 'metadata_suggestions'
```

The app will automatically create the `metadata_suggestions` table on first launch.

---

### Step 4: Set Up the KPIs Cache Notebook

**What:** Configure and deploy the notebook that computes metadata health KPIs.

**File:** `metadata_kpis cache.ipynb`

#### 4.1 Upload the Notebook

1. Navigate to your Databricks workspace
2. Go to **Workspace** → choose an appropriate folder (e.g., `/Shared/metadata-app/` or your team's folder)
3. Upload `metadata_kpis cache.ipynb`

#### 4.2 Configure the Notebook

Edit **Cell 1** (Configuration cell) to match your setup:

```python
# Configuration
STAGING_CATALOG = "metadata_management"  # Match your staging catalog
STAGING_SCHEMA = "app_data"              # Match your staging schema
CACHE_TABLE = "metadata_kpis"
SUGGESTIONS_TABLE = "metadata_suggestions"

# Catalogs to include in KPI computation - MUST match ALLOWED_CATALOGS in app.yaml
ALLOWED_CATALOGS = [
    "catalog1",
    "catalog2",
    "catalog3"
]
```

#### 4.3 Test the Notebook

1. Attach the notebook to a cluster with access to your catalogs
2. Run all cells to verify the KPIs are computed correctly
3. Check the output table: `SELECT * FROM {STAGING_CATALOG}.{STAGING_SCHEMA}.metadata_kpis`

#### 4.4 Set Up a Refresh Schedule

1. Go to **Workflows** → **Jobs** → **Create Job**
2. Configure the job:
   - **Task name:** `refresh-metadata-kpis`
   - **Type:** Notebook
   - **Source:** Select your uploaded notebook
   - **Cluster:** A small cluster is sufficient (1-2 workers)
3. Add a **Schedule**:
   - **Recommended:** Hourly during business hours, or daily at a specific time
   - Example: Every hour from 8 AM to 6 PM on weekdays
4. Save and enable the job

---

### Step 5: Grant Permissions

**What:** Configure permissions for the app's service principal.

See [PERMISSIONS_MODEL.md](PERMISSIONS_MODEL.md) for detailed permission requirements.

**Quick reference:**

```sql
-- Replace <app-service-principal> with your app's service principal ID

-- 1. Warehouse access
GRANT CAN_USE ON WAREHOUSE `your_warehouse_id` TO `<app-service-principal>`;

-- 2. Browse permissions on catalogs users will access
GRANT USE CATALOG ON CATALOG catalog1 TO `<app-service-principal>`;
GRANT USE SCHEMA ON SCHEMA catalog1.* TO `<app-service-principal>`;
GRANT SELECT ON SCHEMA catalog1.* TO `<app-service-principal>`;

-- 3. Full access to staging schema
GRANT ALL PRIVILEGES ON CATALOG metadata_management TO `<app-service-principal>`;
GRANT ALL PRIVILEGES ON SCHEMA metadata_management.app_data TO `<app-service-principal>`;
```

---

### Step 6: Deploy the App

```bash
# Deploy using Databricks CLI
databricks apps deploy metadata-writeback-app --source-directory .

# Check logs
databricks apps logs metadata-writeback-app --follow
```

---

## ⚠️ What's NOT Included in This Repo

### Sync Workflow (Required)

This repository does **not** include the workflow that pushes approved metadata changes to Unity Catalog. You need to create this separately.

**Why:** The sync workflow requires `MODIFY` permissions on your catalogs, which should be carefully controlled and potentially run by a different service principal than the app.

**How to set it up:**

1. **Create a notebook** with the sync logic (see [APPROVAL_WORKFLOW.md](APPROVAL_WORKFLOW.md) for example code):

```python
# Example sync workflow
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Get approved but not synced suggestions
pending = spark.sql("""
    SELECT * FROM metadata_management.app_data.metadata_suggestions
    WHERE status = 'approved' 
      AND (synced_to_uc IS NULL OR synced_to_uc = FALSE)
""").collect()

# Apply each change
for row in pending:
    full_name = f"{row.catalog}.{row.schema}.{row.asset}"
    
    if row.suggestion_format == 'table description':
        comment = row.suggestion.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
    
    elif row.suggestion_format == 'column description':
        column = row.suggestion_on
        comment = row.suggestion.replace("'", "''")
        spark.sql(f"ALTER TABLE {full_name} ALTER COLUMN `{column}` COMMENT '{comment}'")

# Mark all as synced
spark.sql("""
    UPDATE metadata_management.app_data.metadata_suggestions
    SET synced_to_uc = TRUE
    WHERE status = 'approved' 
      AND (synced_to_uc IS NULL OR synced_to_uc = FALSE)
""")
```

2. **Create a Databricks Job** to run this notebook:
   - Schedule: As needed (e.g., hourly, or triggered manually)
   - Permissions: The cluster/service principal needs `MODIFY` on target catalogs

3. **Grant MODIFY permissions** to the sync workflow's service principal:

```sql
GRANT MODIFY ON CATALOG catalog1 TO `<sync-workflow-principal>`;
GRANT MODIFY ON CATALOG catalog2 TO `<sync-workflow-principal>`;
```

---

## 💻 Local Development & Testing

### Setting Up a Local Environment

1. **Create a virtual environment:**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**

```bash
pip install -r requirements.txt
```

3. **Create a `.env` file** for local configuration:

```bash
# .env file - NEVER commit this file!

# Databricks connection
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi_your_personal_access_token_here
DATABRICKS_WAREHOUSE_ID=your_warehouse_id

# App configuration
ALLOWED_CATALOGS=catalog1,catalog2,catalog3
ALLOWED_SCHEMAS=
STAGING_CATALOG=metadata_management
STAGING_SCHEMA=app_data

# For local testing without OAuth
# (The app will use service principal auth instead of OAuth)
```

4. **Run the app locally:**

```bash
streamlit run app.py
```

### Security: Protecting Sensitive Files

**Critical:** Ensure sensitive files are NOT pushed to version control.

The `.gitignore` file should include:

```gitignore
# Environment files - NEVER commit these!
.env
.env.local
.env.*.local

# Streamlit secrets
.streamlit/secrets.toml

# Python virtual environment
venv/
env/
.venv/

# IDE and OS files
.vscode/
.idea/
.DS_Store
```

**Before committing, verify:**

```bash
# Check what files will be committed
git status

# Ensure no .env files are staged
git diff --cached --name-only | grep -E "\.env|secrets"
```

**If you accidentally committed secrets:**
1. Immediately rotate any exposed tokens/credentials
2. Use `git filter-branch` or BFG Repo-Cleaner to remove from history
3. Force push the cleaned history (coordinate with team)

---

## 🔧 Troubleshooting Development & Testing

### Common Issues and Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| `No catalogs available` | Service principal missing `USE CATALOG` | Grant `USE CATALOG` permission |
| `Query failed` | Missing `SELECT` permissions | Grant `SELECT` on schemas/tables |
| `OAuth token not found` | Running locally without OAuth | Set `DATABRICKS_TOKEN` in `.env` |
| `Staging table not found` | First run or missing permissions | Check `STAGING_CATALOG`/`STAGING_SCHEMA` config and permissions |
| `KPIs not loading` | Cache table empty or missing | Run the KPIs notebook manually |
| Can't see Review mode | User not in approvers list | Add email to `reviewers_config.yaml` |

### Testing Checklist

Before deploying to production:

- [ ] **Permissions:** Verify service principal can access all configured catalogs
- [ ] **Staging table:** Confirm the suggestions table was created successfully
- [ ] **KPIs cache:** Run the KPIs notebook and verify data appears
- [ ] **OAuth:** Test login with different user accounts
- [ ] **Approvers:** Confirm approvers can see the Review mode
- [ ] **Suggestions:** Submit a test suggestion and verify it appears in the staging table
- [ ] **Sync workflow:** Test the sync workflow in a non-production catalog first

### Viewing Logs

```bash
# Deployed app logs
databricks apps logs metadata-writeback-app --follow

# Local development - Streamlit logs appear in terminal
streamlit run app.py 2>&1 | tee app.log
```

### Debugging SQL Queries

Test SQL queries directly in the SQL Editor:

```sql
-- Check staging table contents
SELECT * FROM metadata_management.app_data.metadata_suggestions
ORDER BY suggestion_date DESC
LIMIT 10;

-- Check KPIs cache
SELECT * FROM metadata_management.app_data.metadata_kpis;

-- Verify catalog access
SHOW CATALOGS;
SHOW SCHEMAS IN catalog1;
SHOW TABLES IN catalog1.schema1;
```

---

## 📖 Additional Resources

### Databricks Documentation

- **Databricks Apps Development:** [https://docs.databricks.com/dev-tools/databricks-apps/](https://docs.databricks.com/dev-tools/databricks-apps/)
- **Databricks Apps Streamlit Cookbook:** [https://apps-cookbook.dev/docs/category/streamlit](https://apps-cookbook.dev/docs/category/streamlit)
- **Unity Catalog Documentation:** [https://docs.databricks.com/data-governance/unity-catalog/](https://docs.databricks.com/data-governance/unity-catalog/)
- **Databricks CLI:** [https://docs.databricks.com/dev-tools/cli/](https://docs.databricks.com/dev-tools/cli/)

### Streamlit Documentation

- **Streamlit Docs:** [https://docs.streamlit.io](https://docs.streamlit.io)
- **Streamlit Components:** [https://streamlit.io/components](https://streamlit.io/components)

---

## 📋 Implementation Checklist

Use this checklist to track your implementation progress:

- [ ] Read all documentation (README, PERMISSIONS_MODEL, APPROVAL_WORKFLOW, QUICKSTART)
- [ ] Configure approvers in `reviewers_config.yaml`
- [ ] Configure allowed catalogs in `app.yaml`
- [ ] Create dedicated catalog/schema for staging tables
- [ ] Update staging catalog/schema configuration
- [ ] Upload KPIs notebook to workspace
- [ ] Configure KPIs notebook with your catalogs
- [ ] Test KPIs notebook manually
- [ ] Create scheduled job for KPIs refresh
- [ ] Grant permissions to app service principal
- [ ] Deploy the app
- [ ] Test the deployed app
- [ ] Create sync workflow notebook
- [ ] Create scheduled job for sync workflow
- [ ] Grant MODIFY permissions to sync workflow
- [ ] Test end-to-end workflow
- [ ] Document any customizations for your team

