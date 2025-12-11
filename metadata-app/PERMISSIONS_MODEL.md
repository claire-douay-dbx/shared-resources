# Permissions Model - Service Principal Authentication

## Overview

This app uses **Service Principal (SP) authentication** for all database operations. User information is captured from OAuth headers for tracking purposes (suggestor_id, reviewer_id) but all queries execute using the Service Principal's credentials.

## Simplified Authentication Model

- **All database operations** use Service Principal credentials
- **User tracking**: Username is captured from OAuth headers for audit trail
- **Approval only updates status** - does not write to Unity Catalog
- **Sync workflow** (separate) pushes approved changes to UC
- **No OBO complexity**: Simpler deployment and permission management

```python
# All operations use Service Principal
def execute_query(query: str):
    connection_params = {
        "server_hostname": cfg.host,
        "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
        "credentials_provider": lambda: cfg.authenticate  # SP credentials
    }
    # ...
```

## Permission Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      APP PERMISSIONS                        │
│              (Service Principal Credentials)                │
└─────────────────────────────────────────────────────────────┘
            │
            ├─→ Read UC Metadata (BROWSE) ··········· SP
            │   • Uses SP's SELECT permissions
            │   • Shows all tables SP has access to
            │
            ├─→ Read/Write Suggestion Table ········· SP
            │   • INSERT suggestions with user's email
            │   • UPDATE status, reviewer_id, comments
            │   • Username captured from OAuth headers
            │
            └─→ Access SQL Warehouse ················ SP
                • All queries route through warehouse
                • App credentials handle connection


┌─────────────────────────────────────────────────────────────┐
│                  SYNC WORKFLOW (Separate)                    │
│              (Service Principal or Job Principal)           │
└─────────────────────────────────────────────────────────────┘
            │
            └─→ Write Metadata to UC ················ SP
                • Applies approved changes to tables
                • Requires MODIFY on target tables
                • Runs on schedule or manually triggered


┌─────────────────────────────────────────────────────────────┐
│                      USER IDENTITY                          │
│              (OAuth Headers - Display Only)                 │
└─────────────────────────────────────────────────────────────┘
            │
            └─→ Username Tracking ···················· -
                • Captured from X-Forwarded-* headers
                • Stored as suggestor_id / reviewer_id
                • Used for audit trail only
```

## Detailed Permission Breakdown

### 1. App Database Operations - **Service Principal (SP)**

**Operations:**
- `get_catalogs()` - List catalogs
- `get_schemas()` - List schemas in catalog
- `get_tables()` - List tables in schema
- `get_table_metadata()` - Read table/column descriptions + suggestion status
- `submit_metadata_change()` - Submit suggestions (bulk INSERT)
- `get_user_suggestions()` - View suggestion history
- `get_pending_changes()` - View pending reviews
- `approve_change()` / `reject_change()` - Update suggestion status (bulk UPDATE)

**Note:** The app does NOT write metadata directly to Unity Catalog. Approval only updates the status in the staging table.

**SQL Permissions Required (App Service Principal):**
```sql
-- SP needs read access to browse catalogs/schemas/tables:
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-id>`;
GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `<service-principal-id>`;

-- SP needs full access to staging schema (for metadata_suggestions table):
GRANT ALL PRIVILEGES ON SCHEMA asda_metadata_rampup.metadata_population TO `<service-principal-id>`;
```

### 2. Sync Workflow Operations - **Separate Principal**

The sync workflow (job/notebook) that pushes approved changes to UC needs:

```sql
-- MODIFY permissions to update metadata:
GRANT MODIFY ON SCHEMA <catalog>.<schema> TO `<sync-workflow-principal>`;

-- Or at catalog level:
GRANT MODIFY ON CATALOG <catalog_name> TO `<sync-workflow-principal>`;

-- Also needs access to staging table:
GRANT SELECT, MODIFY ON TABLE asda_metadata_rampup.metadata_population.metadata_suggestions 
TO `<sync-workflow-principal>`;
```

### 3. User Identity Tracking - **OAuth Headers**

**How it Works:**
- User's identity is captured from Databricks OAuth headers
- Username stored as `suggestor_id` or `reviewer_id` in the suggestion table
- Provides audit trail without requiring user-level database permissions

**Code Implementation:**
```python
def get_user_info():
    """Extract user information from OAuth headers."""
    user_info = {
        'token': None,
        'email': None,
        'username': None,
        'user': None,
        'display_name': None
    }
    
    if hasattr(st, 'context') and hasattr(st.context, 'headers'):
        headers = st.context.headers
        user_info['token'] = headers.get('X-Forwarded-Access-Token')
        user_info['email'] = headers.get('X-Forwarded-Email')
        user_info['username'] = headers.get('X-Forwarded-Preferred-Username')
        user_info['user'] = headers.get('X-Forwarded-User')
    
    return user_info
```

## Permission Flow Examples

### Example 1: User A Submits Suggestion

```
1. User A logs in → OAuth provides identity via headers
2. User A browses catalogs → execute_query() [SP]
   ✓ SP queries system.information_schema for available catalogs
3. User A selects table → get_table_metadata() [SP]
   ✓ SP fetches metadata from DESCRIBE TABLE
   ✓ SP fetches suggestion status from staging table
4. User A edits metadata → Form captures changes
5. User A submits → submit_metadata_changes_bulk(username) [SP]
   ✓ Bulk INSERT to suggestion table with User A's email
   ✓ Status = 'pending'
```

### Example 2: User B Reviews and Approves

```
1. User B logs in → OAuth provides identity via headers
2. User B views pending → get_pending_changes() [SP]
   ✓ SP queries suggestion table for pending changes
3. User B reviews change → Shows current metadata from UC [SP]
4. User B adds comment → Text input
5. User B clicks Approve → approve_changes_bulk(username) [SP]
   ✓ Bulk UPDATE suggestion table: status = 'approved'
   ✓ User B's email stored as reviewer_id
   ✓ Changes remain in staging table (NOT written to UC)
```

### Example 3: Sync Workflow Pushes to UC

```
1. Scheduled job runs
2. Query approved, not-synced suggestions [Job SP]
   ✓ SELECT WHERE status = 'approved' AND synced_to_uc IS FALSE
3. For each suggestion, apply to UC [Job SP]
   ✓ COMMENT ON TABLE / ALTER TABLE ... COMMENT
4. Mark as synced [Job SP]
   ✓ UPDATE synced_to_uc = TRUE
```

## Security Considerations

### ✅ Secure Practices

1. **Identity Tracking:** OAuth headers capture user identity for audit trail
2. **Centralized Control:** SP controls all database operations
3. **Batch Sync:** UC writes happen in controlled sync workflow
4. **Audit Trail:** All actions tracked with user emails
5. **Separation of Duties:** Suggesting != Approving != Syncing

### 🔒 What This Prevents

- **Users bypassing reviews:** Suggestions go through approval workflow
- **Unauthorized metadata changes:** Only sync workflow writes to UC
- **Uncontrolled writes:** Sync can be scheduled during maintenance windows

### ⚠️ Considerations

- **All users see same data:** Users see all tables the SP has access to
- **Trust model:** Users are trusted to submit suggestions for appropriate tables
- **Sync timing:** Approved changes don't appear in UC until sync runs

## Approver Configuration

The app uses `reviewers_config.yaml` to control who can access Review & Approve mode:

### Configuration File: `reviewers_config.yaml`

```yaml
# Approver Configuration
# 
# Approvers can view pending changes, add comments, and approve/reject suggestions.
# Users not in this list can only use "Browse & Suggest" mode.

approvers:
  - admin@company.com
  - data-steward@company.com
  - claire.douay@databricks.com
```

### How It Works

1. User logs in → OAuth provides identity via headers
2. App loads `reviewers_config.yaml` at startup
3. App checks if user's email is in `approvers` list
4. Access granted based on role:

```
Regular User     → Browse & Suggest mode only
Approver         → Review & Approve mode (full access)
```

### UI Behavior

#### For Regular Users:
```
🎯 Mode Selection
● Browse & Suggest

ℹ️ About Review & Approve
  Review & Approve mode is only available to authorized users.
  Contact your administrator to be added to reviewers_config.yaml.
```

#### For Approvers:
```
🎯 Mode Selection
● Browse & Suggest
○ Review & Approve

🔑 Your role: Approver
```
- Full access to view, comment, approve, reject changes
- Can use bulk actions

## Troubleshooting

### Issue: "No catalogs available"
**Cause:** Service Principal doesn't have USE CATALOG permissions
**Solution:** 
```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-id>`;
```

### Issue: "Approved changes not appearing in UC"
**Cause:** This is expected - you need a sync workflow
**Solution:**
- Set up a sync workflow (job/notebook) to push approved changes
- See APPROVAL_WORKFLOW.md for sync workflow examples

### Issue: "Failed to create staging table"
**Cause:** Service Principal doesn't have CREATE permissions
**Solution:**
```sql
GRANT ALL PRIVILEGES ON SCHEMA asda_metadata_rampup.metadata_population TO `<service-principal-id>`;
```

### Issue: "Query failed" errors
**Cause:** Service Principal doesn't have SELECT permissions
**Solution:**
```sql
GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `<service-principal-id>`;
```

## Setup Checklist

### For App Deployment:

- [ ] Create Service Principal for the app
- [ ] Grant SP access to SQL Warehouse
- [ ] Grant SP USE CATALOG + USE SCHEMA + SELECT on all target catalogs/schemas
- [ ] Grant SP ALL PRIVILEGES on staging schema (for suggestion table)
- [ ] Set DATABRICKS_WAREHOUSE_ID in app.yaml
- [ ] Configure `reviewers_config.yaml` with approvers
- [ ] Deploy app with OAuth enabled

### For Sync Workflow:

- [ ] Create job/notebook for sync workflow
- [ ] Grant sync principal MODIFY on target catalogs/schemas
- [ ] Grant sync principal SELECT + MODIFY on staging table
- [ ] Schedule sync workflow (e.g., every hour, daily)

### Testing Permissions:

```sql
-- Test App SP can browse catalogs:
SELECT * FROM system.information_schema.catalogs;

-- Test App SP can read metadata:
DESCRIBE TABLE EXTENDED <catalog>.<schema>.<table>;

-- Test App SP can access staging table:
SELECT * FROM asda_metadata_rampup.metadata_population.metadata_suggestions LIMIT 1;

-- Test Sync SP can modify metadata:
COMMENT ON TABLE <catalog>.<schema>.<table> IS 'Test description';
```

## Summary

| Operation | Auth Method | Who |
|-----------|-------------|-----|
| Browse UC Metadata | **SP** | App Service Principal |
| View Suggestions | **SP** | App Service Principal |
| Submit Suggestions | **SP** | App (with user identity from OAuth) |
| View Pending Changes | **SP** | App Service Principal |
| Approve/Reject | **SP** | App (with reviewer identity from OAuth) |
| **Write to UC** | **SP** | **Sync Workflow (separate)** |
| Warehouse Access | **SP** | App Service Principal |

This model provides:
- **Simplicity** - Single set of permissions for app (no MODIFY needed)
- **Control** - UC writes happen in controlled sync workflow
- **Auditability** - User identities tracked via OAuth headers
- **Batch Processing** - Bulk operations, not row-by-row
