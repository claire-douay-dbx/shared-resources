# 🚀 Quick Start Guide

> ⚠️ **Before proceeding**, please read the [important disclaimers in the README](README.md) regarding production deployment. This code requires proper evaluation, adaptation, and testing before use in any production environment.
>
> 📖 **New to this repo?** Start with the [Implementation Guide](IMPLEMENTATION_GUIDE.md) for a step-by-step walkthrough of how to set up and deploy this solution.

---

## Deploy to Databricks

### 1. Update app.yaml

Edit `app.yaml` and configure your warehouse:
```yaml
env:
  - name: "DATABRICKS_WAREHOUSE_ID"
    value: "your_actual_warehouse_id"
  - name: "ALLOWED_CATALOGS"
    value: "my_catalog,production_catalog"
```

### 2. Configure Approvers

Edit `reviewers_config.yaml`:
```yaml
approvers:
  - approver1@company.com
  - data-steward@company.com
```

### 3. Grant Service Principal Permissions

```sql
-- Grant warehouse access to your app service principal
GRANT CAN_USE ON WAREHOUSE `your_warehouse_id` TO `app-xxxxx`;

-- Grant catalog permissions
GRANT USE CATALOG ON CATALOG main TO `app-xxxxx`;
GRANT USE SCHEMA ON SCHEMA main.default TO `app-xxxxx`;
GRANT SELECT ON SCHEMA main.default TO `app-xxxxx`;

-- Grant permissions on staging schema
GRANT ALL PRIVILEGES ON SCHEMA asda_metadata_rampup.metadata_population TO `app-xxxxx`;
```

### 4. Deploy

```bash
databricks apps deploy metadata-writeback-app --source-directory .
```

### 5. Check Logs

```bash
databricks apps logs metadata-writeback-app --follow
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "No catalogs available" | Check SP has USE CATALOG permission |
| "App Not Available" | Check warehouse ID in app.yaml |
| "403 Forbidden" | Grant catalog/schema permissions to service principal |
| Can't see Review mode | Add your email to `reviewers_config.yaml` approvers list |

---

## File Guide

### Core Files:
- **`app.py`** - Main application (uses OAuth)
- **`app.yaml`** - Databricks app configuration
- **`requirements.txt`** - Python dependencies
- **`reviewers_config.yaml`** - Approver configuration

### Documentation:
- **`README.md`** - General overview
- **`APPROVAL_WORKFLOW.md`** - Approval workflow and sync process
- **`PERMISSIONS_MODEL.md`** - Permissions and authentication details

---

## Quick Commands Cheat Sheet

```bash
# Databricks deployment
databricks apps deploy metadata-writeback-app --source-directory .
databricks apps list
databricks apps get metadata-writeback-app
databricks apps logs metadata-writeback-app --follow
databricks apps delete metadata-writeback-app

# Testing SQL connection
databricks sql execute --warehouse-id YOUR_ID --query "SHOW CATALOGS"
```

---

## Workflow Overview

1. ✅ **Configure** - Set warehouse ID and approvers
2. ✅ **Grant permissions** - Service principal needs access
3. ✅ **Deploy to Databricks** - App runs with OAuth
4. ✅ **Share with users** - They need catalog permissions
5. ✅ **Set up sync workflow** - Push approved changes to UC

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Suggestors    │────▶│  Staging Table   │────▶│    Approvers    │
│  (Submit ideas) │     │ (metadata_sugg.) │     │ (Review/Approve)│
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                │
                                │ Sync Workflow
                                ▼
                        ┌──────────────────┐
                        │  Unity Catalog   │
                        │ (Table Metadata) │
                        └──────────────────┘
```

**Key Point:** Approved changes stay in the staging table until a separate sync workflow pushes them to UC.

---

## Support

- **Permissions issues**: Check `PERMISSIONS_MODEL.md`
- **Workflow questions**: Check `APPROVAL_WORKFLOW.md`
- **Databricks Apps**: https://docs.databricks.com/dev-tools/databricks-apps/
- **Unity Catalog**: https://docs.databricks.com/data-governance/unity-catalog/
