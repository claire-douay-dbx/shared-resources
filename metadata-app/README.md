# Unity Catalog Metadata Editor

> ⚠️ **IMPORTANT DISCLAIMER - PLEASE READ BEFORE USE**
>
> **This is NOT an official Databricks feature or Solution Accelerator.** This repository is provided as an **example application** and **reference implementation** for building a metadata management workflow in Databricks. It is intended to demonstrate how any organization can implement similar functionality, but **is not supported by Databricks' official support resources**—this includes bug fixes, implementation support, and ongoing maintenance.
>
> **DO NOT deploy this code directly into production environments** without:
> - Thoroughly reviewing and understanding all code, workflows, and configurations
> - Adapting the solution to your organization's specific requirements, security policies, and governance standards
> - Conducting comprehensive testing in a non-production environment
> - Performing security reviews and ensuring compliance with your organization's data handling policies
> - Validating all permission models and access controls for your specific use case
>
> **Databricks cannot be held responsible for any issues, data loss, security vulnerabilities, or other problems that may arise from implementing this code in production environments.** This includes but is not limited to: incorrect metadata changes, permission misconfigurations, workflow failures, or any unintended consequences of deployment.
>
> By using this code, you acknowledge that you are responsible for proper evaluation, adaptation, and testing before any production deployment.

---

A Streamlit-based Databricks App that enables business users to view and suggest metadata changes for Unity Catalog tables and views with a batch-driven approval workflow.

## 📖 Getting Started

New to this repository? Follow these steps:

1. **Read this README** for an overview of features and architecture
2. **Review the [Implementation Guide](IMPLEMENTATION_GUIDE.md)** for step-by-step deployment instructions
3. **Check [QUICKSTART.md](QUICKSTART.md)** for quick deployment commands

---

## Features

- 🔍 **Browse** catalogs, schemas, and tables/views
- 📝 **View** table descriptions and column descriptions
- ✏️ **Suggest** description changes with an intuitive interface
- ✅ **Approval Workflow** - Two-step process: suggest changes → review & approve
- 🔄 **Batch Sync** - Approved changes are synced to UC via a separate workflow
- 🔐 **OAuth support** for user authentication

## Workflow Modes

### Browse & Suggest Mode (Suggestors)
- Browse and view Unity Catalog metadata
- See current metadata with status indicators (⏳ pending, ✅ approved)
- Suggest changes to descriptions
- Submit changes for approval (queued in staging table)

### Review & Approve Mode (Approvers)
- View all tables with pending changes
- Review proposed changes side-by-side with current values
- Approve or reject individual changes or bulk approve/reject
- **Approved changes remain in staging table** until synced by a separate workflow

📖 **See [APPROVAL_WORKFLOW.md](APPROVAL_WORKFLOW.md) for detailed workflow documentation**

## Batch-Driven Architecture

This app uses a **batch-driven workflow** rather than real-time writeback:

1. **Suggestors** submit changes → stored in staging table with status `pending`
2. **Approvers** review and approve → status updated to `approved`
3. **Separate sync workflow** pushes approved changes to Unity Catalog
4. **App auto-detects sync** by comparing values with UC

This approach provides:
- ✅ Better auditability and control
- ✅ Bulk operations (no row-by-row processing)
- ✅ Separation between approval and execution
- ✅ Ability to schedule syncs during maintenance windows

## Metadata Types Supported

1. **Table Descriptions** - Add comments to describe table purpose and content
2. **Column Descriptions** - Document what each column contains

## Permissions Required

### User Requirements
- OAuth login to the Databricks App (for identity tracking)
- No specific database permissions needed
- Approvers must be listed in `reviewers_config.yaml`

### Service Principal Permissions (All Operations)
- `USE CATALOG` and `USE SCHEMA` permissions on target catalogs/schemas
- `SELECT` on tables/views to browse metadata
- `MODIFY` on tables/views (for the sync workflow to update metadata)
- `ALL PRIVILEGES` on the staging schema for the suggestion table
- Access to a SQL Warehouse

## Deployment

### Prerequisites
1. A Databricks workspace with Apps enabled
2. A SQL Warehouse (Serverless recommended)
3. Unity Catalog enabled

### Deploy to Databricks

1. **Upload files to Databricks workspace or Git integration**

2. **Configure app.yaml**
   - Update `DATABRICKS_WAREHOUSE_ID` to point to your SQL Warehouse
   - Add `ALLOWED_CATALOGS` to restrict which catalogs appear in the dropdown
   - Optionally add `ALLOWED_SCHEMAS` to restrict schemas per catalog
   - Example:
     ```yaml
     env:
       - name: "DATABRICKS_WAREHOUSE_ID"
         value: "your-warehouse-id"
       - name: "ALLOWED_CATALOGS"
         value: "my_catalog,production_catalog"
       - name: "ALLOWED_SCHEMAS"
         value: ""  # Optional: "catalog1:schema1,schema2;catalog2:schema3"
     ```

3. **Configure approvers in `reviewers_config.yaml`**
   ```yaml
   approvers:
     - approver1@company.com
     - approver2@company.com
   ```

4. **Deploy using Databricks CLI or UI**
   ```bash
   databricks apps deploy <app-name> --source-directory /path/to/metadata-writeback
   ```

5. **Grant App Permissions**
   ```sql
   -- Grant the app service principal permissions on catalogs/schemas
   GRANT USE CATALOG ON CATALOG <catalog_name> TO `<app_service_principal>`;
   GRANT USE SCHEMA ON SCHEMA <catalog_name>.<schema_name> TO `<app_service_principal>`;
   GRANT SELECT ON SCHEMA <catalog_name>.<schema_name> TO `<app_service_principal>`;
   
   -- Grant permissions on staging schema
   GRANT ALL PRIVILEGES ON SCHEMA asda_metadata_rampup.metadata_population TO `<app_service_principal>`;
   ```

6. **Set up the sync workflow** (see APPROVAL_WORKFLOW.md for details)

## Usage Flow

### Suggester Flow
1. **Log in** - Users authenticate via Databricks OAuth
2. **Select mode** - Choose "Browse & Suggest" from the sidebar
3. **Select table/view** - Use sidebar to browse catalogs → schemas → tables
4. **Load metadata** - Click "Load Metadata" to view current metadata
5. **Check status** - See ⏳ pending or ✅ approved indicators
6. **Edit metadata** - Click "Edit Metadata" to enter edit mode
7. **Make changes** - Update descriptions using text boxes
8. **Submit** - Click "Submit Changes for Approval" to queue changes

### Approver Flow
1. **Log in** - Users authenticate via Databricks OAuth
2. **Select mode** - Choose "Review & Approve" from the sidebar
3. **View pending** - See list of tables with pending changes
4. **Select table** - Click on a table to review its pending changes
5. **Review changes** - Compare current vs. proposed values
6. **Take action** - Approve or reject changes (or use bulk actions)
7. **Changes queued** - Approved changes wait for sync workflow

## SQL Commands Used

The sync workflow uses Unity Catalog SQL commands:

- **Read metadata**: `DESCRIBE TABLE EXTENDED`, `SHOW TABLES`
- **Update table description**: `COMMENT ON TABLE ... IS '...'`
- **Update column description**: `ALTER TABLE ... ALTER COLUMN ... COMMENT '...'`

## Troubleshooting

### "No catalogs available"
- Check that the service principal has `USE CATALOG` permission
- Verify the SQL Warehouse is running

### "Query failed" errors
- Ensure the service principal has `SELECT` permissions
- Check that the SQL Warehouse has connectivity

### OAuth token not found
- Ensure the app is deployed with OAuth enabled
- Check that users are authenticated before accessing the app

## References

- [Databricks Apps Streamlit Cookbook](https://apps-cookbook.dev/docs/category/streamlit)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Streamlit Documentation](https://docs.streamlit.io)

## License

This example application is provided as-is. It is not an official Databricks product and is not supported by Databricks' official support channels.
