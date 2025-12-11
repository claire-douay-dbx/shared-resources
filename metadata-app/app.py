import os
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
import json

# Page configuration
st.set_page_config(
    page_title="ASDA Metadata Editor",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for ASDA branding
st.markdown("""
<style>
    /* Import ASDA-like font */
    @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600;700&display=swap');
    
    /* Apply font to main content areas only */
    .main .block-container {
        font-family: 'Open Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* ASDA Logo styling */
    .asda-logo-container {
        text-align: center;
        background: #78BE20;
        padding: 1.5rem 0;
        margin: -1rem -1rem 2rem -1rem;
        border-radius: 0 0 12px 12px;
    }
    
    .asda-logo {
        max-width: 200px;
        height: auto;
    }
    
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #78BE20;
        margin-bottom: 0.75rem;
    }
    
    .sub-header {
        font-size: 1rem;
        color: #555;
        margin-bottom: 1.5rem;
        margin-top: 0.5rem;
        font-weight: 400;
        line-height: 1.65;
    }
    
    /* ASDA-themed buttons */
    .stButton > button {
        background-color: #78BE20;
        color: white;
        border: none;
        font-weight: 600;
        transition: background-color 0.3s;
    }
    
    .stButton > button:hover {
        background-color: #5A8D15;
        color: white;
    }
    
    .stButton > button:disabled {
        background-color: #cccccc;
        color: #666666;
    }
    
    /* Sidebar styling - light green-grey background */
    [data-testid="stSidebar"] {
        background-color: #f5f9f5;
    }
    
    /* Success messages */
    .stSuccess {
        background-color: #e8f5e9;
        border-left: 4px solid #78BE20;
    }
    
    /* Warning messages */
    .stWarning {
        background-color: #fff9e6;
        border-left: 4px solid #FFC107;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        font-weight: 600;
        color: #5A8D15;
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: #78BE20;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #f5f9f5;
        border-radius: 4px 4px 0 0;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #78BE20;
        color: white;
    }
    
    /* Mobile Responsive */
    @media (max-width: 768px) {
        .asda-logo {
            max-width: 150px;
        }
        .main-header {
            font-size: 2rem;
        }
        .sub-header {
            font-size: 0.95rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Initialize Databricks config
try:
    cfg = Config()
except Exception as e:
    st.error(f"❌ Failed to initialize Databricks config: {str(e)}")
    st.info("Please ensure the app is deployed correctly with proper service principal credentials.")
    st.stop()

# Validate warehouse ID is configured
warehouse_id_check = os.getenv('DATABRICKS_WAREHOUSE_ID')
if not warehouse_id_check:
    st.error("❌ DATABRICKS_WAREHOUSE_ID environment variable is not set")
    st.info("Please configure the SQL Warehouse ID in app.yaml")
    st.code("""
env:
  - name: "DATABRICKS_WAREHOUSE_ID"
    value: "your-warehouse-id-here"
""", language="yaml")
    st.stop()

# Initialize session state
if 'selected_catalog' not in st.session_state:
    st.session_state.selected_catalog = None
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = None
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'metadata' not in st.session_state:
    st.session_state.metadata = None
if 'current_view' not in st.session_state:
    st.session_state.current_view = 'browse'  # browse, approve
if 'approval_selected_table' not in st.session_state:
    st.session_state.approval_selected_table = None
if 'show_detailed_suggestions' not in st.session_state:
    st.session_state.show_detailed_suggestions = False

# Configuration for staging table
STAGING_CATALOG = os.getenv('STAGING_CATALOG', 'asda_metadata_rampup')
STAGING_SCHEMA = os.getenv('STAGING_SCHEMA', 'metadata_population')
STAGING_TABLE = 'metadata_suggestions'

# Configuration for reviewers/approvers
# Can be set via:
# 1. reviewers_config.yaml file (preferred)
# 2. APPROVED_REVIEWERS env var (fallback, comma-separated list)
APPROVED_REVIEWERS_ENV = os.getenv('APPROVED_REVIEWERS', '')

# Load reviewer config from YAML file
def load_reviewers_config():
    """
    Load reviewer and approver lists from reviewers_config.yaml.
    Falls back to APPROVED_REVIEWERS env var if file not found.
    
    Returns:
        dict with 'reviewers' and 'approvers' sets (lowercase emails)
    """
    import yaml
    
    config = {
        'reviewers': set(),
        'approvers': set()
    }
    
    # Try to load from YAML file first
    config_paths = [
        'reviewers_config.yaml',
        '/app/reviewers_config.yaml',  # Databricks Apps path
        os.path.join(os.path.dirname(__file__), 'reviewers_config.yaml')
    ]
    
    for config_path in config_paths:
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    yaml_config = yaml.safe_load(f)
                    if yaml_config:
                        # Load reviewers
                        if yaml_config.get('reviewers'):
                            for email in yaml_config['reviewers']:
                                if email:
                                    config['reviewers'].add(email.strip().lower())
                        # Load approvers
                        if yaml_config.get('approvers'):
                            for email in yaml_config['approvers']:
                                if email:
                                    config['approvers'].add(email.strip().lower())
                        return config
        except Exception:
            continue
    
    # Fallback to environment variable (all get approver permissions for backward compatibility)
    if APPROVED_REVIEWERS_ENV:
        for email in APPROVED_REVIEWERS_ENV.split(','):
            email = email.strip().lower()
            if email:
                config['reviewers'].add(email)
                config['approvers'].add(email)
    
    return config

# Load config at startup
REVIEWERS_CONFIG = load_reviewers_config()

# Configuration for allowed catalogs (comma-separated list) - REQUIRED
# Only these catalogs will appear in the dropdown
# Example: "my_catalog,production_catalog,dev_catalog"
ALLOWED_CATALOGS = os.getenv('ALLOWED_CATALOGS', '')

# Configuration for allowed schemas per catalog (optional)
# Format: "catalog1:schema1,schema2;catalog2:schema3,schema4"
# If not set for a catalog, all schemas in that catalog will be shown
# Example: "prod_catalog:sales,marketing;dev_catalog:sandbox"
ALLOWED_SCHEMAS = os.getenv('ALLOWED_SCHEMAS', '')


def get_reviewers() -> set:
    """
    Get the set of users who can review (view pending changes).
    Includes both reviewers and approvers.
    Returns a set of lowercase email addresses for case-insensitive matching.
    """
    # Approvers automatically have reviewer permissions
    return REVIEWERS_CONFIG['reviewers'] | REVIEWERS_CONFIG['approvers']


def get_approvers() -> set:
    """
    Get the set of users who can approve/reject changes.
    Returns a set of lowercase email addresses for case-insensitive matching.
    """
    return REVIEWERS_CONFIG['approvers']


def _check_user_in_list(user_info: dict, user_list: set) -> bool:
    """
    Check if user is in a given list (by email, username, or user field).
    
    Args:
        user_info: Dictionary containing user information from OAuth headers
        user_list: Set of lowercase emails/usernames to check against
    
    Returns:
        True if user is in the list, False otherwise
    """
    if not user_list:
        return False
    
    # Check email
    if user_info.get('email'):
        if user_info['email'].lower() in user_list:
            return True
    
    # Check username (often the same as email)
    if user_info.get('username'):
        if user_info['username'].lower() in user_list:
            return True
    
    # Check user field
    if user_info.get('user'):
        if user_info['user'].lower() in user_list:
            return True
    
    return False


def is_reviewer(user_info: dict) -> bool:
    """
    Check if the current user can review (view pending changes).
    Reviewers can see pending changes but may not be able to approve/reject.
    
    Args:
        user_info: Dictionary containing user information from OAuth headers
    
    Returns:
        True if user is a reviewer or approver, False otherwise
    """
    return _check_user_in_list(user_info, get_reviewers())


def is_approver(user_info: dict) -> bool:
    """
    Check if the current user can approve/reject changes.
    Approvers have full review + approve capabilities.
    
    Args:
        user_info: Dictionary containing user information from OAuth headers
    
    Returns:
        True if user is an approver, False otherwise
    """
    return _check_user_in_list(user_info, get_approvers())


# Backward compatibility alias
def is_approved_reviewer(user_info: dict) -> bool:
    """Backward compatibility: checks if user can access Review mode."""
    return is_reviewer(user_info)


def get_user_info():
    """
    Extract user information from Streamlit headers.
    Returns dict with user details and access token if available.
    """
    try:
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
        
        # If we have a user access token, get more details from Databricks API
        if user_info['token']:
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient(token=user_info['token'], auth_type="pat")
                current_user = w.current_user.me()
                user_info['display_name'] = current_user.display_name
                user_info['user_id'] = current_user.id
                # Use email from API if available, fallback to header
                if current_user.user_name:
                    user_info['username'] = current_user.user_name
            except:
                pass  # If API call fails, we still have header info
        
        return user_info
    except Exception as e:
        return {
            'token': None,
            'email': None,
            'username': None,
            'user': None,
            'display_name': None
        }


def get_warehouse_id():
    """Get warehouse ID from config or environment."""
    if hasattr(cfg, 'warehouse_id') and cfg.warehouse_id:
        return cfg.warehouse_id
    return os.getenv('DATABRICKS_WAREHOUSE_ID')


def execute_query(query: str):
    """Execute a SQL query using Service Principal credentials and return the result as a pandas DataFrame."""
    try:
        # Validate warehouse_id exists
        warehouse_id = get_warehouse_id()
        if not warehouse_id:
            st.error("❌ DATABRICKS_WAREHOUSE_ID is not configured properly")
            return pd.DataFrame()
        
        connection_params = {
            "server_hostname": cfg.host,
            "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
            "credentials_provider": lambda: cfg.authenticate
        }
        
        with sql.connect(**connection_params) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall_arrow().to_pandas()
                return result
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        st.caption("Check that the SQL Warehouse is running and the Service Principal has proper permissions.")
        return pd.DataFrame()


def execute_command(command: str):
    """Execute a SQL command using Service Principal credentials (no return data expected)."""
    try:
        # Validate warehouse_id exists
        warehouse_id = get_warehouse_id()
        if not warehouse_id:
            st.error("❌ DATABRICKS_WAREHOUSE_ID is not configured properly")
            return False
        
        connection_params = {
            "server_hostname": cfg.host,
            "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
            "credentials_provider": lambda: cfg.authenticate
        }
        
        with sql.connect(**connection_params) as connection:
            with connection.cursor() as cursor:
                cursor.execute(command)
        return True
    except Exception as e:
        st.error(f"Command failed: {str(e)}")
        st.caption("The Service Principal may not have MODIFY permissions on this table/view.")
        return False


def execute_commands_batch(commands: list) -> dict:
    """
    Execute multiple SQL commands in a single connection session (bulk operation).
    
    This follows Databricks best practices:
    - Single connection session for all commands (minimizes connection overhead)
    - No per-command connection open/close
    - Returns structured results for bulk status reporting
    
    Note: For Unity Catalog metadata updates (COMMENT ON statements), batch execution
    is not natively supported, but using a single session reduces connection overhead.
    
    Args:
        commands: List of dicts with 'id' and 'sql' keys
    
    Returns:
        dict with 'success_ids' and 'failed_ids' lists
    """
    success_ids = []
    failed_ids = []
    
    if not commands:
        return {'success_ids': success_ids, 'failed_ids': failed_ids}
    
    try:
        # Validate warehouse_id exists
        warehouse_id = get_warehouse_id()
        if not warehouse_id:
            # All commands fail if no warehouse
            return {'success_ids': [], 'failed_ids': [cmd['id'] for cmd in commands]}
        
        connection_params = {
            "server_hostname": cfg.host,
            "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
            "credentials_provider": lambda: cfg.authenticate
        }
        
        # Execute all commands in a single connection session
        with sql.connect(**connection_params) as connection:
            with connection.cursor() as cursor:
                for cmd in commands:
                    try:
                        cursor.execute(cmd['sql'])
                        success_ids.append(cmd['id'])
                    except Exception as e:
                        # Log individual command failures but continue with others
                        failed_ids.append(cmd['id'])
        
        return {'success_ids': success_ids, 'failed_ids': failed_ids}
    
    except Exception as e:
        # Connection-level failure - all commands fail
        st.error(f"Batch command execution failed: {str(e)}")
        return {'success_ids': [], 'failed_ids': [cmd['id'] for cmd in commands]}


def ensure_staging_table_exists():
    """Create staging table if it doesn't exist and ensure schema is up-to-date."""
    try:
        # Create catalog if not exists
        execute_command(f"CREATE CATALOG IF NOT EXISTS {STAGING_CATALOG}")
        
        # Create schema if not exists
        execute_command(f"CREATE SCHEMA IF NOT EXISTS {STAGING_CATALOG}.{STAGING_SCHEMA}")
        
        # Create staging table if not exists
        staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {staging_full_name} (
            suggestion_date TIMESTAMP,
            suggestor_id STRING,
            catalog STRING,
            `schema` STRING,
            asset STRING,
            asset_type STRING,
            suggestion_format STRING,
            suggestion_on STRING,
            suggestion STRING,
            suggestion_type STRING,
            status STRING,
            reviewer_id STRING,
            review_date TIMESTAMP,
            reviewer_comments STRING,
            change_id STRING,
            synced_to_uc BOOLEAN
        )
        """
        execute_command(create_table_sql)
        
        # Migration: Handle schema updates for existing tables
        try:
            check_column_query = f"""
            SELECT column_name, data_type
            FROM {STAGING_CATALOG}.information_schema.columns 
            WHERE table_schema = '{STAGING_SCHEMA}' 
            AND table_name = '{STAGING_TABLE}'
            """
            result = execute_query(check_column_query)
            
            existing_columns = set(result['column_name'].tolist()) if not result.empty else set()
            
            # Migration: Add synced_to_uc column if it doesn't exist
            if 'synced_to_uc' not in existing_columns:
                alter_sql = f"ALTER TABLE {staging_full_name} ADD COLUMN synced_to_uc BOOLEAN"
                execute_command(alter_sql)
            
            # Migration: Remove old_value column if it exists
            if 'old_value' in existing_columns:
                alter_table_sql = f"ALTER TABLE {staging_full_name} DROP COLUMN old_value"
                execute_command(alter_table_sql)
                
        except Exception as e:
            # If migrations fail, it's not critical - table still works
            pass
        
        return True
    except Exception as e:
        st.error(f"Failed to create staging table: {str(e)}")
        return False


def determine_suggestion_type(old_value: str, new_value: str) -> str:
    """Determine if suggestion is add new, modify, or delete."""
    if not old_value or old_value.strip() == '':
        return 'add new'
    elif not new_value or new_value.strip() == '':
        return 'delete'
    else:
        return 'modify'


def get_asset_type(catalog: str, schema: str, table: str) -> str:
    """Get the asset type (table, view, materialized view)."""
    try:
        query = f"""
        SELECT table_type 
        FROM {catalog}.information_schema.tables 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        """
        result = execute_query(query)
        if not result.empty and 'table_type' in result.columns:
            table_type = result.iloc[0]['table_type']
            if table_type == 'MANAGED' or table_type == 'EXTERNAL':
                return 'table'
            elif table_type == 'VIEW':
                return 'view'
            elif table_type == 'MATERIALIZED_VIEW':
                return 'materialized view'
        return 'table'  # default
    except:
        return 'table'  # default fallback


def check_table_has_pending_changes(catalog: str, schema: str, table: str) -> bool:
    """Check if a table has any pending change requests."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    query = f"""
    SELECT COUNT(*) as pending_count
    FROM {staging_full_name}
    WHERE status = 'pending'
      AND catalog = '{catalog}'
      AND `schema` = '{schema}'
      AND asset = '{table}'
    """
    result = execute_query(query)
    if not result.empty and 'pending_count' in result.columns:
        return int(result.iloc[0]['pending_count']) > 0
    return False


def check_specific_metadata_has_pending_change(catalog: str, schema: str, table: str, 
                                                 suggestion_format: str, suggestion_on: str) -> bool:
    """
    Check if a specific metadata item has a pending change.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        suggestion_format: Type of metadata (table description, column description)
        suggestion_on: The target (table name for table-level, column name for column-level)
    
    Returns:
        True if there's a pending change for this specific metadata item, False otherwise
    """
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    query = f"""
    SELECT COUNT(*) as pending_count
    FROM {staging_full_name}
    WHERE status = 'pending'
      AND catalog = '{catalog}'
      AND `schema` = '{schema}'
      AND asset = '{table}'
      AND suggestion_format = '{suggestion_format}'
      AND suggestion_on = '{suggestion_on}'
    """
    result = execute_query(query)
    if not result.empty and 'pending_count' in result.columns:
        return int(result.iloc[0]['pending_count']) > 0
    return False


def submit_metadata_change(catalog: str, schema: str, table: str, change_type: str, 
                          column_name: str, old_value: str, new_value: str, username: str):
    """
    Submit a metadata change to the staging table.
    
    Returns:
        dict with keys:
            - success (bool): Whether the submission succeeded
            - target (str): What was being changed (table description or column name)
            - reason (str): Reason for failure if failed, None if success
    """
    import uuid
    from datetime import datetime
    
    # Map change_type to suggestion_format
    format_mapping = {
        'table_comment': 'table description',
        'column_comment': 'column description'
    }
    suggestion_format = format_mapping.get(change_type, change_type)
    
    # Determine suggestion_on (table name or column name)
    suggestion_on = column_name if column_name else table
    target_display = f"column `{column_name}`" if column_name else "table description"
    
    # Check if this specific metadata item already has pending changes
    if check_specific_metadata_has_pending_change(catalog, schema, table, suggestion_format, suggestion_on):
        return {
            'success': False,
            'target': target_display,
            'reason': 'already has a pending change awaiting review'
        }
    
    change_id = str(uuid.uuid4())
    suggestion_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Escape single quotes
    new_value = new_value.replace("'", "''")
    username = username.replace("'", "''")
    
    # Determine suggestion type (add new, modify, delete)
    suggestion_type = determine_suggestion_type(old_value, new_value)
    
    # Get asset type
    asset_type = get_asset_type(catalog, schema, table)
    
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    insert_sql = f"""
    INSERT INTO {staging_full_name} 
    (suggestion_date, suggestor_id, catalog, `schema`, asset, asset_type, 
     suggestion_format, suggestion_on, suggestion, suggestion_type, status, 
     reviewer_id, review_date, reviewer_comments, change_id)
    VALUES (
        TIMESTAMP'{suggestion_timestamp}', '{username}', '{catalog}', '{schema}', '{table}', 
        '{asset_type}', '{suggestion_format}', '{suggestion_on}', '{new_value}', 
        '{suggestion_type}', 'pending', NULL, NULL, NULL, '{change_id}'
    )
    """
    if execute_command(insert_sql):
        return {
            'success': True,
            'target': target_display,
            'reason': None
        }
    else:
        return {
            'success': False,
            'target': target_display,
            'reason': 'database error during submission'
        }


def check_pending_changes_bulk(catalog: str, schema: str, table: str, 
                                changes: list) -> dict:
    """
    Check which metadata items already have pending changes.
    Uses a single bulk query instead of per-item checks.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        changes: List of dicts with 'suggestion_format' and 'suggestion_on' keys
    
    Returns:
        dict mapping (suggestion_format, suggestion_on) -> bool (True if pending exists)
    """
    if not changes:
        return {}
    
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    # Build a single query to check all items at once
    conditions = []
    for change in changes:
        sf = change['suggestion_format'].replace("'", "''")
        so = change['suggestion_on'].replace("'", "''")
        conditions.append(f"(suggestion_format = '{sf}' AND suggestion_on = '{so}')")
    
    where_conditions = " OR ".join(conditions)
    
    query = f"""
    SELECT suggestion_format, suggestion_on
    FROM {staging_full_name}
    WHERE status = 'pending'
      AND catalog = '{catalog}'
      AND `schema` = '{schema}'
      AND asset = '{table}'
      AND ({where_conditions})
    """
    
    result = execute_query(query)
    
    # Build result dict
    pending_items = {}
    if not result.empty:
        for _, row in result.iterrows():
            key = (row['suggestion_format'], row['suggestion_on'])
            pending_items[key] = True
    
    return pending_items


def submit_metadata_changes_bulk(catalog: str, schema: str, table: str, 
                                  changes: list, username: str) -> dict:
    """
    Submit multiple metadata changes in a single bulk INSERT operation.
    
    This follows Databricks best practices:
    - Single INSERT statement with multiple VALUES rows
    - No per-row processing or loops for database operations
    - All validation done upfront in bulk
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        changes: List of dicts with keys: change_type, column_name, old_value, new_value
        username: Username of the submitter
    
    Returns:
        dict with keys:
            - successful (list): List of successfully submitted change targets
            - failed (list): List of dicts with 'target' and 'reason' for failures
    """
    import uuid
    from datetime import datetime
    
    if not changes:
        return {'successful': [], 'failed': []}
    
    # Map change_type to suggestion_format
    format_mapping = {
        'table_comment': 'table description',
        'column_comment': 'column description'
    }
    
    # Prepare all changes with their metadata
    prepared_changes = []
    for change in changes:
        suggestion_format = format_mapping.get(change['change_type'], change['change_type'])
        suggestion_on = change['column_name'] if change['column_name'] else table
        target_display = f"column `{change['column_name']}`" if change['column_name'] else "table description"
        
        prepared_changes.append({
            'suggestion_format': suggestion_format,
            'suggestion_on': suggestion_on,
            'target_display': target_display,
            'old_value': change['old_value'],
            'new_value': change['new_value']
        })
    
    # Bulk check for pending changes (single query)
    pending_items = check_pending_changes_bulk(catalog, schema, table, prepared_changes)
    
    # Separate changes into those that can be submitted vs blocked
    successful = []
    failed = []
    to_insert = []
    
    # Get asset type once (not per-change)
    asset_type = get_asset_type(catalog, schema, table)
    suggestion_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    escaped_username = username.replace("'", "''")
    
    for change in prepared_changes:
        key = (change['suggestion_format'], change['suggestion_on'])
        
        if pending_items.get(key):
            failed.append({
                'target': change['target_display'],
                'reason': 'already has a pending change awaiting review'
            })
        else:
            # Prepare for bulk insert
            change_id = str(uuid.uuid4())
            suggestion_type = determine_suggestion_type(change['old_value'], change['new_value'])
            escaped_value = change['new_value'].replace("'", "''")
            escaped_format = change['suggestion_format'].replace("'", "''")
            escaped_on = change['suggestion_on'].replace("'", "''")
            
            to_insert.append({
                'change_id': change_id,
                'suggestion_format': escaped_format,
                'suggestion_on': escaped_on,
                'suggestion': escaped_value,
                'suggestion_type': suggestion_type,
                'target_display': change['target_display']
            })
    
    # Bulk INSERT all valid changes in a single statement
    if to_insert:
        staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
        
        # Build VALUES clause with all rows
        values_rows = []
        for item in to_insert:
            values_rows.append(f"""(
                TIMESTAMP'{suggestion_timestamp}', '{escaped_username}', '{catalog}', '{schema}', '{table}', 
                '{asset_type}', '{item['suggestion_format']}', '{item['suggestion_on']}', '{item['suggestion']}', 
                '{item['suggestion_type']}', 'pending', NULL, NULL, NULL, '{item['change_id']}'
            )""")
        
        values_clause = ",\n".join(values_rows)
        
        bulk_insert_sql = f"""
        INSERT INTO {staging_full_name} 
        (suggestion_date, suggestor_id, catalog, `schema`, asset, asset_type, 
         suggestion_format, suggestion_on, suggestion, suggestion_type, status, 
         reviewer_id, review_date, reviewer_comments, change_id)
        VALUES {values_clause}
        """
        
        if execute_command(bulk_insert_sql):
            # All inserts succeeded
            successful = [item['target_display'] for item in to_insert]
        else:
            # Bulk insert failed - mark all as failed
            for item in to_insert:
                failed.append({
                    'target': item['target_display'],
                    'reason': 'database error during bulk submission'
                })
    
    return {'successful': successful, 'failed': failed}


def get_pending_changes():
    """Get all pending metadata changes grouped by table."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    query = f"""
    SELECT DISTINCT catalog, `schema`, asset, 
           COUNT(*) as change_count,
           MIN(suggestion_date) as first_submitted
    FROM {staging_full_name}
    WHERE status = 'pending'
    GROUP BY catalog, `schema`, asset
    ORDER BY first_submitted DESC
    """
    return execute_query(query)


def get_user_suggestions_kpis(username: str, status_filter: str = None):
    """Get summary KPIs for user suggestions (counts only - fast query)."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    # Extract username part (before @) to match both with and without email format
    username_escaped = username.replace(chr(39), chr(39)+chr(39))
    username_part = username.split('@')[0].replace(chr(39), chr(39)+chr(39))
    
    # Match both full username and username with any email domain
    where_clause = f"WHERE (suggestor_id = '{username_escaped}' OR suggestor_id LIKE '{username_part}@%' OR suggestor_id = '{username_part}')"
    if status_filter and status_filter != 'all':
        where_clause += f" AND status = '{status_filter}'"
    
    query = f"""
    SELECT 
        COUNT(*) as total_suggestions,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_count,
        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_count,
        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected_count
    FROM {staging_full_name}
    {where_clause}
    """
    return execute_query(query)


def get_user_suggestions(username: str, status_filter: str = None):
    """Get all suggestions made by a specific user."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    # Extract username part (before @) to match both with and without email format
    username_escaped = username.replace(chr(39), chr(39)+chr(39))
    username_part = username.split('@')[0].replace(chr(39), chr(39)+chr(39))
    
    # Match both full username and username with any email domain
    where_clause = f"WHERE (suggestor_id = '{username_escaped}' OR suggestor_id LIKE '{username_part}@%' OR suggestor_id = '{username_part}')"
    if status_filter and status_filter != 'all':
        where_clause += f" AND status = '{status_filter}'"
    
    query = f"""
    SELECT 
        suggestion_date,
        catalog,
        `schema`,
        asset,
        asset_type,
        suggestion_format,
        suggestion_on,
        suggestion,
        suggestion_type,
        status,
        reviewer_id,
        review_date,
        reviewer_comments,
        change_id
    FROM {staging_full_name}
    {where_clause}
    ORDER BY suggestion_date DESC, review_date DESC
    """
    return execute_query(query)


def get_reviewer_history_kpis(username: str, status_filter: str = None):
    """Get summary KPIs for reviews done by a reviewer (counts only - fast query)."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    # Extract username part (before @) to match both with and without email format
    username_escaped = username.replace(chr(39), chr(39)+chr(39))
    username_part = username.split('@')[0].replace(chr(39), chr(39)+chr(39))
    
    # Match both full username and username with any email domain
    where_clause = f"WHERE reviewer_id IS NOT NULL AND (reviewer_id = '{username_escaped}' OR reviewer_id LIKE '{username_part}@%' OR reviewer_id = '{username_part}')"
    if status_filter and status_filter != 'all':
        where_clause += f" AND status = '{status_filter}'"
    
    query = f"""
    SELECT 
        COUNT(*) as total_reviews,
        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_count,
        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) as rejected_count
    FROM {staging_full_name}
    {where_clause}
    """
    return execute_query(query)


def get_reviewer_history(username: str, status_filter: str = None):
    """Get all reviews done by a specific reviewer."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    # Extract username part (before @) to match both with and without email format
    username_escaped = username.replace(chr(39), chr(39)+chr(39))
    username_part = username.split('@')[0].replace(chr(39), chr(39)+chr(39))
    
    # Match both full username and username with any email domain
    where_clause = f"WHERE reviewer_id IS NOT NULL AND (reviewer_id = '{username_escaped}' OR reviewer_id LIKE '{username_part}@%' OR reviewer_id = '{username_part}')"
    if status_filter and status_filter != 'all':
        where_clause += f" AND status = '{status_filter}'"
    
    query = f"""
    SELECT 
        suggestion_date,
        suggestor_id,
        catalog,
        `schema`,
        asset,
        asset_type,
        suggestion_format,
        suggestion_on,
        suggestion,
        suggestion_type,
        status,
        reviewer_id,
        review_date,
        reviewer_comments,
        change_id
    FROM {staging_full_name}
    {where_clause}
    ORDER BY review_date DESC, suggestion_date DESC
    """
    return execute_query(query)


def check_reviewer_permissions(user_info: dict) -> bool:
    """
    Check if the current user is an approved reviewer based on the APPROVED_REVIEWERS config.
    
    Args:
        user_info: Dictionary containing user information from OAuth headers
    
    Returns:
        True if user is in the approved reviewers list, False otherwise
    """
    return is_approved_reviewer(user_info)


def get_current_metadata_value(catalog: str, schema: str, table: str, 
                               suggestion_format: str, suggestion_on: str) -> str:
    """
    Fetch the current metadata value from Unity Catalog system tables.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table/view name
        suggestion_format: Type of metadata (table description, column description)
        suggestion_on: The target (table name or column name)
    
    Returns:
        Current metadata value from UC, or empty string if not set
    """
    try:
        if suggestion_format == 'table description':
            # Get table comment from information_schema
            query = f"""
            SELECT comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
            """
            result = execute_query(query)
            if not result.empty and 'comment' in result.columns:
                comment = result.iloc[0]['comment']
                return comment if pd.notna(comment) else ''
            return ''
        
        elif suggestion_format == 'column description':
            # Get column comment from columns system table
            query = f"""
            SELECT comment
            FROM {catalog}.information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
              AND column_name = '{suggestion_on}'
            """
            result = execute_query(query)
            if not result.empty and 'comment' in result.columns:
                comment = result.iloc[0]['comment']
                return comment if pd.notna(comment) else ''
            return ''
        
        return ''
    
    except Exception as e:
        # If we can't fetch the current value, return empty string
        return ''


def get_changes_for_table(catalog: str, schema: str, table: str):
    """Get all pending changes for a specific table."""
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    query = f"""
    SELECT change_id, suggestion_format, suggestion_on, suggestion, 
           suggestion_type, suggestor_id, suggestion_date, asset_type,
           catalog, `schema`, asset
    FROM {staging_full_name}
    WHERE status = 'pending'
      AND catalog = '{catalog}'
      AND `schema` = '{schema}'
      AND asset = '{table}'
    ORDER BY 
        CASE 
            WHEN suggestion_format = 'table description' THEN 1
            WHEN suggestion_format = 'column description' THEN 2
            ELSE 3
        END,
        suggestion_on ASC,
        suggestion_date ASC
    """
    return execute_query(query)


def approve_change(change_id: str, username: str, comments: str = ""):
    """Approve a pending change."""
    from datetime import datetime
    
    review_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    username = username.replace("'", "''")
    comments = comments.replace("'", "''")
    
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    update_sql = f"""
    UPDATE {staging_full_name}
    SET status = 'approved', 
        reviewer_id = '{username}',
        review_date = TIMESTAMP'{review_timestamp}',
        reviewer_comments = '{comments}'
    WHERE change_id = '{change_id}'
    """
    return execute_command(update_sql)


def reject_change(change_id: str, username: str, comments: str = ""):
    """Reject a pending change."""
    from datetime import datetime
    
    review_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    username = username.replace("'", "''")
    comments = comments.replace("'", "''")
    
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    update_sql = f"""
    UPDATE {staging_full_name}
    SET status = 'rejected', 
        reviewer_id = '{username}',
        review_date = TIMESTAMP'{review_timestamp}',
        reviewer_comments = '{comments}'
    WHERE change_id = '{change_id}'
    """
    return execute_command(update_sql)


def update_changes_bulk(change_ids: list, status: str, username: str, comments: str = "") -> bool:
    """
    Update multiple changes in a single bulk UPDATE operation.
    
    This follows Databricks best practices:
    - Single UPDATE statement with IN clause for multiple IDs
    - No per-row processing or loops for database operations
    
    Args:
        change_ids: List of change_id strings to update
        status: New status ('approved' or 'rejected')
        username: Username of the reviewer
        comments: Optional reviewer comments (applied to all changes)
    
    Returns:
        bool: True if bulk update succeeded, False otherwise
    """
    from datetime import datetime
    
    if not change_ids:
        return True
    
    review_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    escaped_username = username.replace("'", "''")
    escaped_comments = comments.replace("'", "''")
    
    # Build IN clause with all change_ids
    escaped_ids = [f"'{cid.replace(chr(39), chr(39)+chr(39))}'" for cid in change_ids]
    ids_clause = ", ".join(escaped_ids)
    
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    bulk_update_sql = f"""
    UPDATE {staging_full_name}
    SET status = '{status}', 
        reviewer_id = '{escaped_username}',
        review_date = TIMESTAMP'{review_timestamp}',
        reviewer_comments = '{escaped_comments}'
    WHERE change_id IN ({ids_clause})
    """
    
    return execute_command(bulk_update_sql)


def approve_changes_bulk(change_ids: list, username: str, comments: str = "") -> bool:
    """
    Approve multiple pending changes in a single bulk UPDATE operation.
    
    Args:
        change_ids: List of change_id strings to approve
        username: Username of the reviewer
        comments: Optional reviewer comments
    
    Returns:
        bool: True if bulk approval succeeded, False otherwise
    """
    return update_changes_bulk(change_ids, 'approved', username, comments)


def reject_changes_bulk(change_ids: list, username: str, comments: str = "") -> bool:
    """
    Reject multiple pending changes in a single bulk UPDATE operation.
    
    Args:
        change_ids: List of change_id strings to reject
        username: Username of the reviewer
        comments: Optional reviewer comments
    
    Returns:
        bool: True if bulk rejection succeeded, False otherwise
    """
    return update_changes_bulk(change_ids, 'rejected', username, comments)


def apply_approved_changes_bulk(changes_df: pd.DataFrame) -> dict:
    """
    Apply multiple approved changes to Unity Catalog in a single connection session.
    
    This follows Databricks best practices:
    - Single connection session for all COMMENT ON statements (minimizes overhead)
    - Pre-fetch all object types in bulk to minimize lookups
    - Build all SQL commands upfront, then execute in one batch
    - Returns structured results for bulk status reporting
    
    Note: Unity Catalog's COMMENT ON statements don't support true batch syntax
    (like INSERT with multiple VALUES), but executing all statements in a single
    connection session significantly reduces connection overhead vs. per-row connections.
    
    Args:
        changes_df: DataFrame with pending changes to apply
    
    Returns:
        dict with 'success_ids' and 'failed_ids' lists
    """
    if changes_df.empty:
        return {'success_ids': [], 'failed_ids': []}
    
    # Step 1: Pre-fetch all object types in bulk (single query per unique table)
    # This minimizes database round-trips for type lookups
    object_types_cache = {}
    unique_tables = changes_df[['catalog', 'schema', 'asset']].drop_duplicates()
    
    for _, row in unique_tables.iterrows():
        key = f"{row['catalog']}.{row['schema']}.{row['asset']}"
        object_types_cache[key] = get_object_type(
            row['catalog'], row['schema'], row['asset']
        )
    
    # Step 2: Build all SQL commands upfront (no database calls in this loop)
    commands_to_execute = []
    
    for _, change in changes_df.iterrows():
        catalog = change['catalog']
        schema_name = change['schema']
        table = change['asset']
        suggestion_format = change['suggestion_format']
        suggestion_on = change['suggestion_on']
        suggestion = change['suggestion']
        suggestion_type = change['suggestion_type']
        change_id = change['change_id']
        
        # Handle delete type - clear the metadata
        if suggestion_type == 'delete':
            suggestion = ''
        
        # Escape single quotes in suggestion
        escaped_suggestion = suggestion.replace("'", "''")
        
        # Get cached object type
        key = f"{catalog}.{schema_name}.{table}"
        object_type = object_types_cache.get(key, 'TABLE')
        full_name = f"{catalog}.{schema_name}.{table}"
        
        # Build the appropriate SQL command
        sql_command = None
        if suggestion_format == 'table description':
            if object_type == 'VIEW':
                sql_command = f"COMMENT ON VIEW {full_name} IS '{escaped_suggestion}'"
            elif object_type == 'MATERIALIZED VIEW':
                sql_command = f"COMMENT ON MATERIALIZED VIEW {full_name} IS '{escaped_suggestion}'"
            else:
                sql_command = f"COMMENT ON TABLE {full_name} IS '{escaped_suggestion}'"
        elif suggestion_format == 'column description':
            if object_type in ('VIEW', 'MATERIALIZED VIEW'):
                sql_command = f"COMMENT ON COLUMN {full_name}.`{suggestion_on}` IS '{escaped_suggestion}'"
            else:
                sql_command = f"ALTER TABLE {full_name} ALTER COLUMN `{suggestion_on}` COMMENT '{escaped_suggestion}'"
        
        if sql_command:
            commands_to_execute.append({
                'id': change_id,
                'sql': sql_command
            })
    
    # Step 3: Execute all commands in a single connection session (bulk operation)
    return execute_commands_batch(commands_to_execute)


def apply_approved_change(change_row):
    """
    Apply an approved change to Unity Catalog using Service Principal credentials.
    """
    catalog = change_row['catalog']
    schema_name = change_row['schema']
    table = change_row['asset']
    suggestion_format = change_row['suggestion_format']
    suggestion_on = change_row['suggestion_on']
    suggestion = change_row['suggestion']
    suggestion_type = change_row['suggestion_type']
    
    # Handle delete type - clear the metadata
    if suggestion_type == 'delete':
        suggestion = ''
    
    # Apply change using Service Principal credentials
    if suggestion_format == 'table description':
        return update_table_comment(catalog, schema_name, table, suggestion)
    elif suggestion_format == 'column description':
        # suggestion_on contains the column name
        return update_column_comment(catalog, schema_name, table, suggestion_on, suggestion)
    
    return False


def get_allowed_catalogs() -> list:
    """
    Get the list of allowed catalogs from configuration.
    Returns an empty list if not configured.
    """
    if not ALLOWED_CATALOGS:
        return []
    
    # Parse comma-separated list
    catalogs = []
    for catalog in ALLOWED_CATALOGS.split(','):
        catalog = catalog.strip()
        if catalog:
            catalogs.append(catalog)
    return catalogs


def get_allowed_schemas_for_catalog(catalog: str) -> list:
    """
    Get the list of allowed schemas for a specific catalog from configuration.
    Returns an empty list if not configured (meaning query for schemas).
    
    Format: "catalog1:schema1,schema2;catalog2:schema3,schema4"
    """
    if not ALLOWED_SCHEMAS:
        return []
    
    # Parse the format: "catalog1:schema1,schema2;catalog2:schema3,schema4"
    for catalog_config in ALLOWED_SCHEMAS.split(';'):
        catalog_config = catalog_config.strip()
        if ':' in catalog_config:
            config_catalog, schemas_str = catalog_config.split(':', 1)
            if config_catalog.strip().lower() == catalog.lower():
                schemas = []
                for schema in schemas_str.split(','):
                    schema = schema.strip()
                    if schema:
                        schemas.append(schema)
                return schemas
    return []


def get_catalogs():
    """
    Get list of catalogs for the dropdown from ALLOWED_CATALOGS configuration.
    
    ALLOWED_CATALOGS must be configured - this prevents showing all catalogs.
    """
    allowed = get_allowed_catalogs()
    if not allowed:
        st.warning("⚠️ ALLOWED_CATALOGS is not configured. Please set this environment variable.")
        return []
    
    return sorted(allowed)


def get_schemas(catalog: str):
    """
    Get list of schemas in a catalog.
    
    If ALLOWED_SCHEMAS is configured for this catalog, uses that list.
    Otherwise, queries information_schema for available schemas.
    """
    try:
        # Check if schemas are configured for this catalog
        allowed_schemas = get_allowed_schemas_for_catalog(catalog)
        if allowed_schemas:
            return sorted(allowed_schemas)
        
        # Otherwise, query for schemas in this catalog (excluding internal ones)
        query = f"""
        SELECT DISTINCT schema_name
        FROM {catalog}.information_schema.schemata
        WHERE schema_name NOT LIKE '!_!_%' ESCAPE '!'
          AND schema_name != 'information_schema'
        ORDER BY schema_name
        """
        df = execute_query(query)
        
        if df.empty:
            return []
        
        return df['schema_name'].tolist()
        
    except Exception as e:
        st.error(f"Error loading schemas for {catalog}: {str(e)}")
        return []


def get_tables(catalog: str, schema: str):
    """
    Get list of tables and views in a schema.
    
    Queries the catalog's information_schema for tables and views.
    """
    try:
        query = f"""
        SELECT table_name, table_type
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY table_name
        """
        df = execute_query(query)
        
        if df.empty:
            return []
        
        # Format results to match expected structure
        tables = []
        for _, row in df.iterrows():
            tables.append({
                'tableName': row['table_name'],
                'tableType': row.get('table_type', 'TABLE'),
                'isTemporary': False
            })
        
        return tables
        
    except Exception as e:
        st.error(f"Error loading tables for {catalog}.{schema}: {str(e)}")
        return []


def get_suggestions_for_table(catalog: str, schema: str, table: str) -> dict:
    """
    Get all suggestions (pending, approved, rejected) for a specific table.
    
    Returns a dict mapping (suggestion_format, suggestion_on) -> dict with:
        - status: 'pending', 'approved', or 'rejected'
        - suggestion: the suggested value
        - suggestor_id: who made the suggestion
        - suggestion_date: when it was made
        - suggestion_type: 'add new', 'modify', or 'delete'
        - synced_to_uc: boolean flag indicating if synced (from staging table)
    
    Only returns the most recent suggestion per (format, target) combination.
    Priority: pending > approved > rejected (for display purposes)
    """
    staging_full_name = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.{STAGING_TABLE}"
    
    # Get all suggestions for this table, ordered by priority (pending first) and recency
    # Include synced_to_uc flag if it exists (with COALESCE for backward compatibility)
    query = f"""
    SELECT suggestion_format, suggestion_on, suggestion, suggestion_type, 
           status, suggestor_id, suggestion_date,
           COALESCE(synced_to_uc, FALSE) as synced_to_uc,
           ROW_NUMBER() OVER (
               PARTITION BY suggestion_format, suggestion_on 
               ORDER BY 
                   CASE status 
                       WHEN 'pending' THEN 1 
                       WHEN 'approved' THEN 2 
                       WHEN 'rejected' THEN 3 
                   END,
                   suggestion_date DESC
           ) as rn
    FROM {staging_full_name}
    WHERE catalog = '{catalog}'
      AND `schema` = '{schema}'
      AND asset = '{table}'
    """
    
    result = execute_query(query)
    
    suggestions = {}
    if not result.empty:
        # Filter to only the first (highest priority) suggestion per target
        for _, row in result.iterrows():
            if row['rn'] == 1:  # Only take the highest priority suggestion
                key = (row['suggestion_format'], row['suggestion_on'])
                suggestions[key] = {
                    'status': row['status'],
                    'suggestion': row['suggestion'] if pd.notna(row['suggestion']) else '',
                    'suggestor_id': row['suggestor_id'],
                    'suggestion_date': row['suggestion_date'],
                    'suggestion_type': row['suggestion_type'],
                    'synced_to_uc': bool(row['synced_to_uc']) if pd.notna(row['synced_to_uc']) else False
                }
    
    return suggestions


def _normalize_for_comparison(value: str) -> str:
    """Normalize a string value for comparison (strip whitespace, handle None)."""
    if value is None:
        return ''
    return str(value).strip()


def _is_suggestion_synced(suggestion: dict, uc_value: str) -> bool:
    """
    Check if an approved suggestion has been synced to UC.
    
    A suggestion is considered synced if:
    1. The synced_to_uc flag is explicitly set to TRUE, OR
    2. The suggestion value matches the current UC value (auto-detection)
    
    This dual approach allows:
    - External workflows to explicitly mark syncs
    - Automatic detection if the flag wasn't set
    """
    # If explicitly marked as synced
    if suggestion.get('synced_to_uc', False):
        return True
    
    # Auto-detect by comparing values (normalized for whitespace differences)
    suggestion_value = _normalize_for_comparison(suggestion.get('suggestion', ''))
    uc_normalized = _normalize_for_comparison(uc_value)
    
    return suggestion_value == uc_normalized


def get_table_metadata(catalog: str, schema: str, table: str):
    """
    Get detailed metadata for a table/view, merging UC data with approved suggestions.
    
    Display logic:
    - If approved suggestion exists AND synced to UC → show UC value (no indicator)
    - If approved suggestion exists AND NOT synced → show suggestion value + approved indicator
    - If pending suggestion exists → show UC value + pending indicator
    - If rejected/no suggestion → show UC value (no indicator)
    
    Sync detection: A suggestion is considered synced if the synced_to_uc flag is TRUE,
    OR if the suggestion value matches the current UC value.
    """
    full_name = f"{catalog}.{schema}.{table}"
    
    # Step 1: Get base metadata from Unity Catalog
    describe_query = f"DESCRIBE TABLE EXTENDED {full_name}"
    describe_df = execute_query(describe_query)
    
    # Parse table comment from UC
    uc_table_comment = ""
    if not describe_df.empty:
        comment_rows = describe_df[describe_df['col_name'] == 'Comment']
        if not comment_rows.empty:
            uc_table_comment = comment_rows.iloc[0]['data_type']
    
    # Get column information from UC
    columns_query = f"DESCRIBE {full_name}"
    columns_df = execute_query(columns_query)
    
    uc_columns = []
    if not columns_df.empty:
        for _, row in columns_df.iterrows():
            col_name = row['col_name']
            if col_name.startswith('#') or col_name == '':
                continue
            
            uc_columns.append({
                'name': col_name,
                'type': row['data_type'],
                'comment': row.get('comment', '') if pd.notna(row.get('comment', '')) else ''
            })
    
    # Step 2: Get suggestions from staging table (single bulk query)
    suggestions = get_suggestions_for_table(catalog, schema, table)
    
    # Step 3: Apply display logic - merge UC data with suggestions
    
    # Table description logic
    table_desc_key = ('table description', table)
    table_suggestion = suggestions.get(table_desc_key)
    
    if table_suggestion and table_suggestion['status'] == 'approved':
        # Check if the approved suggestion has been synced to UC
        if _is_suggestion_synced(table_suggestion, uc_table_comment):
            # Synced → show UC value (which now matches the suggestion)
            table_comment = uc_table_comment
            table_status = None  # No indicator needed, it's in sync
        else:
            # Not synced → show suggestion value with approved indicator
            table_comment = table_suggestion['suggestion']
            table_status = 'approved'
    elif table_suggestion and table_suggestion['status'] == 'pending':
        # Pending suggestion exists → show UC value + pending indicator
        table_comment = uc_table_comment
        table_status = 'pending'
    else:
        # No suggestion or rejected → show UC value
        table_comment = uc_table_comment
        table_status = None
    
    # Column descriptions logic
    columns = []
    for col in uc_columns:
        col_key = ('column description', col['name'])
        col_suggestion = suggestions.get(col_key)
        
        if col_suggestion and col_suggestion['status'] == 'approved':
            # Check if the approved suggestion has been synced to UC
            if _is_suggestion_synced(col_suggestion, col['comment']):
                # Synced → show UC value (which now matches the suggestion)
                col_comment = col['comment']
                col_status = None  # No indicator needed, it's in sync
            else:
                # Not synced → show suggestion value with approved indicator
                col_comment = col_suggestion['suggestion']
                col_status = 'approved'
        elif col_suggestion and col_suggestion['status'] == 'pending':
            # Pending suggestion exists → show UC value + pending indicator
            col_comment = col['comment']
            col_status = 'pending'
        else:
            # No suggestion or rejected → show UC value
            col_comment = col['comment']
            col_status = None
        
        columns.append({
            'name': col['name'],
            'type': col['type'],
            'comment': col_comment,
            'status': col_status  # 'pending', 'approved', or None
        })
    
    return {
        'table_name': table,
        'table_comment': table_comment,
        'table_status': table_status,  # 'pending', 'approved', or None
        'columns': columns
    }


def get_object_type(catalog: str, schema: str, table: str) -> str:
    """
    Determine if an object is a TABLE, VIEW, or MATERIALIZED VIEW.
    Returns 'TABLE', 'VIEW', or 'MATERIALIZED VIEW'.
    """
    try:
        query = f"""
        SELECT table_type
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        """
        result = execute_query(query)
        if not result.empty and 'table_type' in result.columns:
            table_type = result.iloc[0]['table_type']
            if table_type in ('VIEW', 'MATERIALIZED VIEW'):
                return table_type
        return 'TABLE'
    except:
        return 'TABLE'  # Default to TABLE if we can't determine


def update_table_comment(catalog: str, schema: str, table: str, comment: str):
    """Update table/view description."""
    full_name = f"{catalog}.{schema}.{table}"
    # Escape single quotes in comment
    comment = comment.replace("'", "''")
    
    # Determine object type and use appropriate command
    object_type = get_object_type(catalog, schema, table)
    if object_type == 'VIEW':
        command = f"COMMENT ON VIEW {full_name} IS '{comment}'"
    elif object_type == 'MATERIALIZED VIEW':
        command = f"COMMENT ON MATERIALIZED VIEW {full_name} IS '{comment}'"
    else:
        command = f"COMMENT ON TABLE {full_name} IS '{comment}'"
    
    return execute_command(command)


def update_column_comment(catalog: str, schema: str, table: str, column: str, comment: str):
    """Update column description for table or view."""
    full_name = f"{catalog}.{schema}.{table}"
    # Escape single quotes in comment
    comment = comment.replace("'", "''")
    
    # Determine object type and use appropriate command
    object_type = get_object_type(catalog, schema, table)
    if object_type in ('VIEW', 'MATERIALIZED VIEW'):
        # For views, use COMMENT ON COLUMN syntax
        command = f"COMMENT ON COLUMN {full_name}.`{column}` IS '{comment}'"
    else:
        # For tables, use ALTER TABLE syntax
        command = f"ALTER TABLE {full_name} ALTER COLUMN `{column}` COMMENT '{comment}'"
    
    return execute_command(command)




# Main UI
# ASDA Logo and Header
st.markdown("""
<div class="asda-logo-container">
    <svg class="asda-logo" viewBox="0 0 200 60" xmlns="http://www.w3.org/2000/svg">
        <rect width="200" height="60" fill="white" rx="8"/>
        <text x="100" y="40" font-family="Arial Black, sans-serif" font-size="32" font-weight="900" 
              fill="#78BE20" text-anchor="middle">ASDA</text>
    </svg>
</div>
""", unsafe_allow_html=True)

# Main header
st.markdown('<div class="main-header">🛒 Unity Catalog Metadata Editor</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">View, edit, and manage metadata for Unity Catalog tables and views. Submit changes for approval and track your metadata governance workflow.</div>', unsafe_allow_html=True)


def get_metadata_kpis():
    """Get metadata population KPIs from cache table (fast!)."""
    try:
        warehouse_id = get_warehouse_id()
        if not warehouse_id:
            return None
        
        cache_table = f"{STAGING_CATALOG}.{STAGING_SCHEMA}.metadata_kpis"
        
        connection_params = {
            "server_hostname": cfg.host,
            "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
            "credentials_provider": lambda: cfg.authenticate
        }
        
        with sql.connect(**connection_params) as connection:
            with connection.cursor() as cursor:
                # Read all cached KPIs in one query
                query = f"SELECT * FROM {cache_table}"
                cursor.execute(query)
                cache_data = cursor.fetchall_arrow().to_pandas()
                
                if cache_data.empty:
                    return None
                
                # Extract metrics
                metrics = cache_data[cache_data['metric_name'].isin([
                    'total_tables', 'tables_with_desc', 'tables_all_cols_desc', 'tables_partial_cols_desc'
                ])]
                
                # Extract leaderboards
                top_suggestors = cache_data[cache_data['metric_name'] == 'top_suggestor'][
                    ['user_id', 'user_count', 'user_rank']
                ].rename(columns={'user_id': 'suggestor_id', 'user_count': 'suggestion_count'}).sort_values('user_rank')
                
                top_reviewers = cache_data[cache_data['metric_name'] == 'top_reviewer'][
                    ['user_id', 'user_count', 'user_rank']
                ].rename(columns={'user_id': 'reviewer_id', 'user_count': 'review_count'}).sort_values('user_rank')
                
                top_rejected_suggestors = cache_data[cache_data['metric_name'] == 'top_rejected_suggestor'][
                    ['user_id', 'user_count', 'user_rank']
                ].rename(columns={'user_id': 'suggestor_id', 'user_count': 'rejection_count'}).sort_values('user_rank')
                
                # Build result dictionary
                result = {}
                for _, row in metrics.iterrows():
                    metric_name = row['metric_name']
                    result[metric_name] = int(row['metric_value']) if pd.notna(row['metric_value']) else 0
                
                result['top_suggestors'] = top_suggestors
                result['top_reviewers'] = top_reviewers
                result['top_rejected_suggestors'] = top_rejected_suggestors
                
                # Get last refresh time for display
                last_refresh = cache_data['refresh_timestamp'].max()
                result['last_refresh'] = last_refresh
                
                return result
    except Exception as e:
        # If cache table doesn't exist or has issues, return None
        return None


# Get user information for display purposes (username tracking)
user_info = get_user_info()

# Ensure staging table exists
ensure_staging_table_exists()

# Sidebar for navigation
with st.sidebar:
    # Connection status at the top
    with st.expander("🔐 Connection Info", expanded=False):
        st.success("✅ Using Service Principal authentication")
        st.caption("**User Information (for tracking):**")
        if user_info.get('display_name'):
            st.caption(f"Display Name: {user_info['display_name']}")
        if user_info.get('username'):
            st.caption(f"Username: {user_info['username']}")
        if user_info.get('email'):
            st.caption(f"Email: {user_info['email']}")
        if user_info.get('user'):
            st.caption(f"User ID: {user_info['user']}")
        
        st.divider()
        
        try:
            st.caption(f"**Host:** {cfg.host}")
            warehouse_id = get_warehouse_id()
            if warehouse_id and len(str(warehouse_id)) > 20:
                st.caption(f"**Warehouse ID:** {str(warehouse_id)[:20]}...")
            elif warehouse_id:
                st.caption(f"**Warehouse ID:** {warehouse_id}")
            else:
                st.error("**Warehouse ID:** Not configured!")
        except Exception as e:
            st.caption(f"**Config error:** {str(e)}")
    
    st.divider()
    
    # Mode Selection
    st.header("🎯 Mode Selection")
    
    # Check user permissions from reviewers_config.yaml
    user_is_reviewer = is_reviewer(user_info)
    user_is_approver = is_approver(user_info)
    
    # Determine available modes
    if user_is_reviewer or user_is_approver:
        mode_options = ["Browse & Suggest", "Review & Approve"]
    else:
        mode_options = ["Browse & Suggest"]
    
    mode = st.radio(
        "Choose mode:",
        options=mode_options,
        key="mode_selector"
    )
    st.session_state.current_view = 'browse' if mode == "Browse & Suggest" else 'approve'
    
    # Show permission info
    if user_is_reviewer or user_is_approver:
        role_text = "**Approver**" if user_is_approver else "**Reviewer** (read-only)"
        st.caption(f"🔑 Your role: {role_text}")
    else:
        with st.expander("ℹ️ About Review & Approve", expanded=False):
            st.caption("""
            **Review & Approve** mode is only available to authorized users.
            
            - **Reviewers**: Can view pending changes (read-only)
            - **Approvers**: Can approve/reject and apply changes
            
            Contact your administrator to be added to `reviewers_config.yaml`.
            """)
    
    st.divider()
    
    # Different sidebar based on mode
    if st.session_state.current_view == 'browse':
        st.header("🔍 Browse & Suggest")
        
        # Add sub-mode selector
        browse_mode = st.radio(
            "Select action:",
            options=["Make New Suggestions", "View My Suggestions"],
            key="browse_mode_selector"
        )
        
        st.divider()
        
        if browse_mode == "Make New Suggestions":
            st.subheader("Select Table/View")
        
            # Catalog selection (only show for new suggestions)
            catalogs = get_catalogs()
            if catalogs:
                selected_catalog = st.selectbox(
                    "Catalog",
                    options=catalogs,
                    key="catalog_selector"
                )
            
                if selected_catalog:
                    # Schema selection
                    schemas = get_schemas(selected_catalog)
                    if schemas:
                        selected_schema = st.selectbox(
                            "Schema",
                            options=schemas,
                            key="schema_selector"
                        )
                        
                        if selected_schema:
                            # Table selection
                            tables = get_tables(selected_catalog, selected_schema)
                            if tables:
                                table_names = [t['tableName'] for t in tables]
                                selected_table = st.selectbox(
                                    "Table/View",
                                    options=table_names,
                                    key="table_selector"
                                )
                                
                                # Clear cached metadata if selection changed
                                if (st.session_state.get('selected_catalog') != selected_catalog or
                                    st.session_state.get('selected_schema') != selected_schema or
                                    st.session_state.get('selected_table') != selected_table):
                                    # Selection changed - clear cached metadata
                                    st.session_state.metadata = None
                                    st.session_state.edit_mode = False
                                
                                if st.button("📖 Load Metadata", type="primary", use_container_width=True):
                                    st.session_state.selected_catalog = selected_catalog
                                    st.session_state.selected_schema = selected_schema
                                    st.session_state.selected_table = selected_table
                                    st.session_state.edit_mode = False
                                    
                                    with st.spinner("Loading metadata..."):
                                        metadata = get_table_metadata(
                                            selected_catalog, 
                                            selected_schema, 
                                            selected_table
                                        )
                                        st.session_state.metadata = metadata
                                    st.rerun()
                            else:
                                st.info("No tables or views found in this schema")
                    else:
                        st.info("No schemas found in this catalog")
            else:
                st.warning("No catalogs available or insufficient permissions")
        
        st.divider()
        
        # Show current selection
        if st.session_state.selected_table:
            st.success(f"**Current Selection:**")
            st.text(f"{st.session_state.selected_catalog}."
                    f"{st.session_state.selected_schema}."
                    f"{st.session_state.selected_table}")
            
            if st.button("🏠 Return to Home", use_container_width=True):
                st.session_state.selected_catalog = None
                st.session_state.selected_schema = None
                st.session_state.selected_table = None
                st.session_state.edit_mode = False
                st.session_state.metadata = None
                st.rerun()
        else:  # View My Suggestions mode
            display_name = user_info.get('display_name') or user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_user'
            st.caption(f"Logged in as: **{display_name}**")
    
    else:  # approve mode
        st.header("✅ Review Pending Changes")
        
        with st.spinner("Loading pending changes..."):
            pending_df = get_pending_changes()
        
        if not pending_df.empty:
            st.metric("Pending Tables", len(pending_df))
            st.caption(f"Total changes: {pending_df['change_count'].sum()}")
            
            st.divider()
            
            # List of tables with pending changes
            for _, row in pending_df.iterrows():
                full_name = f"{row['catalog']}.{row['schema']}.{row['asset']}"
                if st.button(
                    f"📋 {row['asset']}\n({row['change_count']} changes)",
                    key=f"select_{row['catalog']}_{row['schema']}_{row['asset']}",
                    use_container_width=True
                ):
                    st.session_state.approval_selected_table = {
                        'catalog': row['catalog'],
                        'schema': row['schema'],
                        'table': row['asset']
                    }
                    st.rerun()
            
            if st.session_state.approval_selected_table:
                st.divider()
                if st.button("🏠 Return to List", use_container_width=True):
                    st.session_state.approval_selected_table = None
                    st.rerun()
        else:
            st.info("No pending changes to review")
            st.caption("All changes have been processed!")

# Main content area - My Suggestions History
if st.session_state.current_view == 'browse' and 'browse_mode_selector' in st.session_state and st.session_state.browse_mode_selector == "View My Suggestions":
    st.subheader("📊 My Suggestion History")
    
    # Get username from user info (use username for database queries, display_name for display)
    username = user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_user'
    
    # Filter options
    col1, col2 = st.columns([1, 3])
    with col1:
        status_filter = st.selectbox(
            "Filter by status:",
            options=['all', 'pending', 'approved', 'rejected'],
            index=0,
            key="status_filter_select"
        )
    
    # Reset detailed view when filter changes
    if 'previous_status_filter' not in st.session_state:
        st.session_state.previous_status_filter = status_filter
    if st.session_state.previous_status_filter != status_filter:
        st.session_state.show_detailed_suggestions = False
        st.session_state.previous_status_filter = status_filter
    
    # Load KPIs only (fast query)
    with st.spinner("Loading summary..."):
        kpis_df = get_user_suggestions_kpis(username, status_filter)
    
    if not kpis_df.empty and kpis_df.iloc[0]['total_suggestions'] > 0:
        # Summary metrics
        kpi_row = kpis_df.iloc[0]
        total_suggestions = int(kpi_row['total_suggestions'])
        pending_count = int(kpi_row['pending_count'])
        approved_count = int(kpi_row['approved_count'])
        rejected_count = int(kpi_row['rejected_count'])
        
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        with metric_col1:
            st.metric("Total Suggestions", total_suggestions)
        with metric_col2:
            st.metric("⏳ Pending", pending_count)
        with metric_col3:
            st.metric("✅ Approved", approved_count)
        with metric_col4:
            st.metric("❌ Rejected", rejected_count)
        
        st.divider()
        
        # Button to load detailed suggestions
        if not st.session_state.show_detailed_suggestions:
            if st.button("📋 Load Detailed Suggestion List", type="primary", use_container_width=True):
                st.session_state.show_detailed_suggestions = True
                st.rerun()
            st.info("💡 Click the button above to view the detailed list of your suggestions.")
        else:
            # Load and display detailed suggestions
            with st.spinner("Loading detailed suggestions..."):
                suggestions_df = get_user_suggestions(username, status_filter)
            
            if not suggestions_df.empty:
                # Add button to hide details
                if st.button("🔼 Hide Detailed List", type="secondary", use_container_width=True):
                    st.session_state.show_detailed_suggestions = False
                    st.rerun()
                
                st.divider()
                
                # Display suggestions
                for idx, suggestion in suggestions_df.iterrows():
                    # Status badge
                    status_icon = {
                        'pending': '⏳',
                        'approved': '✅',
                        'rejected': '❌'
                    }.get(suggestion['status'], '❓')
                    
                    status_color = {
                        'pending': '🟡',
                        'approved': '🟢',
                        'rejected': '🔴'
                    }.get(suggestion['status'], '⚪')
                    
                    # Create expander for each suggestion
                    full_asset_name = f"{suggestion['catalog']}.{suggestion['schema']}.{suggestion['asset']}"
                    expander_title = f"{status_icon} {suggestion['suggestion_format'].title()} - {full_asset_name}"
                    
                    with st.expander(expander_title, expanded=False):
                        # Suggestion details
                        st.markdown("**Suggestion Details:**")
                        detail_cols = st.columns([1, 1])
                        with detail_cols[0]:
                            st.text(f"Date: {suggestion['suggestion_date']}")
                            st.text(f"Asset Type: {suggestion['asset_type']}")
                            st.text(f"Format: {suggestion['suggestion_format']}")
                        with detail_cols[1]:
                            st.text(f"Target: {suggestion['suggestion_on']}")
                            st.text(f"Type: {suggestion['suggestion_type']}")
                            st.text(f"Status: {status_color} {suggestion['status'].upper()}")
                        
                        st.divider()
                        
                        # Fetch current value from UC dynamically
                        current_value = get_current_metadata_value(
                            suggestion['catalog'],
                            suggestion['schema'],
                            suggestion['asset'],
                            suggestion['suggestion_format'],
                            suggestion['suggestion_on']
                        )
                        
                        # Show values based on suggestion type
                        if suggestion['suggestion_type'] == 'modify':
                            # Show side-by-side for modify
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**Current Value in UC:**")
                                if current_value:
                                    st.code(current_value, language=None)
                                else:
                                    st.text("(empty)")
                            with col2:
                                st.markdown("**Your Suggestion:**")
                                if suggestion['suggestion']:
                                    st.code(suggestion['suggestion'], language=None)
                                else:
                                    st.text("(empty)")
                        
                        elif suggestion['suggestion_type'] == 'add new':
                            st.markdown("**Your Suggestion (New):**")
                            if suggestion['suggestion']:
                                st.code(suggestion['suggestion'], language=None)
                            else:
                                st.text("(empty)")
                            
                            # Show if value was later added to UC
                            if current_value:
                                st.info(f"ℹ️ Note: A value now exists in UC: `{current_value[:50]}...`")
                        
                        elif suggestion['suggestion_type'] == 'delete':
                            st.markdown("**Current Value in UC (to be deleted):**")
                            if current_value:
                                st.code(current_value, language=None)
                            else:
                                st.text("(empty)")
                                if suggestion['status'] == 'pending':
                                    st.info("ℹ️ Note: No metadata currently found in UC.")
                        
                        # Show review info if reviewed
                        if suggestion['status'] in ['approved', 'rejected']:
                            st.divider()
                            st.markdown("**Review Information:**")
                            
                            review_col1, review_col2 = st.columns(2)
                            with review_col1:
                                st.text(f"Reviewed by: {suggestion['reviewer_id']}")
                                st.text(f"Review date: {suggestion['review_date']}")
                            
                            with review_col2:
                                if suggestion['reviewer_comments']:
                                    st.markdown("**Reviewer Comments:**")
                                    st.info(suggestion['reviewer_comments'])
                                else:
                                    st.caption("_No comments provided by reviewer_")
                
                # Download option
                st.divider()
                csv = suggestions_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download History as CSV",
                    data=csv,
                    file_name=f"my_suggestions_{username.replace('@', '_')}_{status_filter}.csv",
                    mime="text/csv"
                )
    
    else:
        if status_filter == 'all':
            st.info("📝 You haven't made any suggestions yet. Switch to 'Make New Suggestions' to get started!")
        else:
            st.info(f"📝 No {status_filter} suggestions found.")

# Main content area - Browse Mode (Make New Suggestions)
elif st.session_state.current_view == 'browse' and st.session_state.metadata:
    metadata = st.session_state.metadata
    
    # Display/Edit mode toggle
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.subheader(f"Metadata for {metadata['table_name']}")
    with col3:
        if not st.session_state.edit_mode:
            if st.button("✏️ Edit Metadata", type="primary"):
                st.session_state.edit_mode = True
                st.rerun()
        else:
            if st.button("❌ Cancel", type="secondary"):
                st.session_state.edit_mode = False
                st.rerun()
    
    st.divider()
    
    # View Mode
    if not st.session_state.edit_mode:
        # Table Description
        table_status = metadata.get('table_status')
        if table_status == 'pending':
            st.markdown("### 📝 Table Description ⏳")
            st.warning("⏳ **Pending change** - A suggestion is awaiting review")
        elif table_status == 'approved':
            st.markdown("### 📝 Table Description ✅")
            st.success("✅ **Approved** - This description will be synced to UC")
        else:
            st.markdown("### 📝 Table Description")
        
        if metadata['table_comment']:
            st.info(metadata['table_comment'])
        else:
            st.info("_No table description set_")
        
        st.divider()
        
        # Column Descriptions
        st.markdown("### 📋 Column Descriptions")
        
        # Show summary of column statuses
        pending_cols = [c['name'] for c in metadata['columns'] if c.get('status') == 'pending']
        approved_cols = [c['name'] for c in metadata['columns'] if c.get('status') == 'approved']
        
        if pending_cols or approved_cols:
            status_col1, status_col2 = st.columns(2)
            with status_col1:
                if pending_cols:
                    st.warning(f"⏳ {len(pending_cols)} column(s) with pending changes")
            with status_col2:
                if approved_cols:
                    st.success(f"✅ {len(approved_cols)} column(s) with approved changes")
        
        for col in metadata['columns']:
            col_status = col.get('status')
            
            # Add status indicator to column name
            if col_status == 'pending':
                expander_label = f"⏳ **{col['name']}** ({col['type']})"
            elif col_status == 'approved':
                expander_label = f"✅ **{col['name']}** ({col['type']})"
            else:
                expander_label = f"**{col['name']}** ({col['type']})"
            
            with st.expander(expander_label, expanded=False):
                # Show status message if applicable
                if col_status == 'pending':
                    st.caption("⏳ A suggestion for this column is awaiting review")
                elif col_status == 'approved':
                    st.caption("✅ Approved description pending sync to UC")
                
                st.markdown("**Description:**")
                if col['comment']:
                    st.write(col['comment'])
                else:
                    st.write("_No description set_")
    
    # Edit Mode
    else:
        with st.form("metadata_form"):
            st.markdown("### 📝 Edit Table Description")
            
            # Show warning if table description has pending change
            table_status = metadata.get('table_status')
            if table_status == 'pending':
                st.warning("⏳ **Pending change** - This field already has a suggestion awaiting review. New submissions will be blocked until the pending change is processed.")
            elif table_status == 'approved':
                st.info("✅ **Approved change** - An approved suggestion exists and will be synced to UC. You can still submit a new suggestion to override it.")
            
            new_table_comment = st.text_area(
                "Table Description",
                value=metadata['table_comment'],
                height=100,
                help="Enter a description for this table"
            )
            
            st.divider()
            st.markdown("### 📋 Edit Column Descriptions")
            
            # Show summary of columns with pending changes
            pending_cols = [c['name'] for c in metadata['columns'] if c.get('status') == 'pending']
            if pending_cols:
                st.warning(f"⏳ {len(pending_cols)} column(s) have pending changes: {', '.join(pending_cols[:5])}{'...' if len(pending_cols) > 5 else ''}")
            
            # Create tabs for each column
            column_tabs = st.tabs([col['name'] for col in metadata['columns']])
            
            column_updates = {}
            for idx, col in enumerate(metadata['columns']):
                with column_tabs[idx]:
                    st.caption(f"Type: {col['type']}")
                    
                    # Show status indicator for this column
                    col_status = col.get('status')
                    if col_status == 'pending':
                        st.warning("⏳ This column has a pending change awaiting review")
                    elif col_status == 'approved':
                        st.info("✅ This column has an approved change pending sync to UC")
                    
                    col_comment = st.text_area(
                        "Column Description",
                        value=col['comment'],
                        key=f"col_comment_{col['name']}",
                        height=80
                    )
                    
                    column_updates[col['name']] = {
                        'comment': col_comment
                    }
            
            st.divider()
            
            # Submit button
            submitted = st.form_submit_button("💾 Submit Changes for Approval", type="primary", use_container_width=True)
            
            if submitted:
                with st.spinner("Submitting changes for approval..."):
                    # Get username from user info
                    username = user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_user'
                    
                    # Collect all changes first (no database calls yet)
                    changes_to_submit = []
                    
                    # Collect table comment change
                    if new_table_comment != metadata['table_comment']:
                        changes_to_submit.append({
                            'change_type': 'table_comment',
                            'column_name': None,
                            'old_value': metadata['table_comment'],
                            'new_value': new_table_comment
                        })
                    
                    # Collect column comment changes
                    for col in metadata['columns']:
                        col_name = col['name']
                        updates = column_updates[col_name]
                        
                        if updates['comment'] != col['comment']:
                            changes_to_submit.append({
                                'change_type': 'column_comment',
                                'column_name': col_name,
                                'old_value': col['comment'],
                                'new_value': updates['comment']
                            })
                    
                    # Submit all changes in a single bulk operation
                    result = submit_metadata_changes_bulk(
                        st.session_state.selected_catalog,
                        st.session_state.selected_schema,
                        st.session_state.selected_table,
                        changes_to_submit,
                        username
                    )
                    
                    successful_submissions = result['successful']
                    failed_submissions = result['failed']
                    total_attempted = len(successful_submissions) + len(failed_submissions)
                    
                    # Display results summary
                    if total_attempted == 0:
                        st.info("ℹ️ No changes detected.")
                        st.session_state.edit_mode = False
                    elif len(failed_submissions) == 0:
                        # All succeeded
                        st.success(f"✅ Successfully submitted {len(successful_submissions)} change(s) for approval!")
                        if successful_submissions:
                            with st.expander("📋 Submitted changes", expanded=False):
                                for target in successful_submissions:
                                    st.markdown(f"- ✅ {target}")
                        st.info("💡 Changes will be applied after review and approval.")
                        st.session_state.edit_mode = False
                        st.rerun()
                    elif len(successful_submissions) == 0:
                        # All failed
                        st.error(f"❌ Failed to submit {len(failed_submissions)} change(s)")
                        st.markdown("**Failed submissions:**")
                        for fail in failed_submissions:
                            st.markdown(f"- ❌ **{fail['target']}**: {fail['reason']}")
                        st.warning("⚠️ Please wait for pending changes to be reviewed before resubmitting.")
                    else:
                        # Mixed results
                        st.warning(f"⚠️ Partial success: {len(successful_submissions)} submitted, {len(failed_submissions)} failed")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**✅ Successfully submitted:**")
                            for target in successful_submissions:
                                st.markdown(f"- {target}")
                        with col2:
                            st.markdown("**❌ Failed to submit:**")
                            for fail in failed_submissions:
                                st.markdown(f"- **{fail['target']}**: {fail['reason']}")
                        
                        st.info("💡 Successfully submitted changes will be applied after review.")

# Approval Mode Content
elif st.session_state.current_view == 'approve' and st.session_state.approval_selected_table:
    sel_table = st.session_state.approval_selected_table
    
    st.subheader(f"📋 Review Changes for {sel_table['catalog']}.{sel_table['schema']}.{sel_table['table']}")
    
    # Get pending changes for this table
    changes_df = get_changes_for_table(sel_table['catalog'], sel_table['schema'], sel_table['table'])
    
    if not changes_df.empty:
        st.markdown(f"**Total pending changes:** {len(changes_df)}")
        st.divider()
        
        # Get current metadata for comparison
        current_metadata = get_table_metadata(sel_table['catalog'], sel_table['schema'], sel_table['table'])
        
        # Display each change with approve/reject buttons
        for idx, change in changes_df.iterrows():
            change_type_label = {
                'table description': '📝 Table Description',
                'column description': '📝 Column Description'
            }.get(change['suggestion_format'], change['suggestion_format'])
            
            # For column-level changes, show the column name
            col_info = ""
            if change['suggestion_format'] == 'column description':
                col_info = f" - **{change['suggestion_on']}**"
            
            # Add suggestion type badge
            type_badge = {
                'add new': '🆕',
                'modify': '✏️',
                'delete': '🗑️'
            }.get(change['suggestion_type'], '')
            
            with st.expander(f"{type_badge} {change_type_label}{col_info}", expanded=True):
                st.caption(f"Suggested by: {change['suggestor_id']} on {change['suggestion_date']}")
                st.caption(f"Suggestion type: **{change['suggestion_type']}**")
                
                # Asset information section
                st.markdown("**Asset Information:**")
                st.text(f"Type: {change['asset_type']}")
                st.text(f"Target: {change['suggestion_on']}")
                
                st.divider()
                
                # Fetch current value from UC dynamically
                current_value = get_current_metadata_value(
                    change['catalog'],
                    change['schema'],
                    change['asset'],
                    change['suggestion_format'],
                    change['suggestion_on']
                )
                
                # Show old and new values side-by-side for modify type
                if change['suggestion_type'] == 'modify':
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Current Value in UC:**")
                        if current_value:
                            st.code(current_value, language=None)
                        else:
                            st.text("(empty)")
                    
                    with col2:
                        st.markdown("**Proposed Value:**")
                        if change['suggestion']:
                            st.code(change['suggestion'], language=None)
                        else:
                            st.text("(will be cleared)")
                
                # For add new type, only show proposed value
                elif change['suggestion_type'] == 'add new':
                    st.markdown("**Proposed Metadata (New):**")
                    if change['suggestion']:
                        st.code(change['suggestion'], language=None)
                    else:
                        st.text("(empty)")
                    
                    # Show if there's already a value in UC (edge case)
                    if current_value:
                        st.info(f"ℹ️ Note: A value already exists in UC: `{current_value[:50]}...`")
                
                # For delete type, only show current value
                elif change['suggestion_type'] == 'delete':
                    st.markdown("**Current Metadata in UC (Will be deleted):**")
                    if current_value:
                        st.code(current_value, language=None)
                    else:
                        st.text("(empty)")
                        st.info("ℹ️ Note: No metadata found in UC to delete.")
                
                st.divider()
                
                # Check if user is an approver (can approve/reject)
                can_approve = is_approver(user_info)
                
                # Add comment input field
                reviewer_comment = st.text_area(
                    "Reviewer Comments (optional)",
                    key=f"comment_{change['change_id']}",
                    placeholder="Add comments about this suggestion (e.g., why approved/rejected, additional context, etc.)",
                    height=80,
                    disabled=not can_approve
                )
                
                # Show role-based message
                if not can_approve:
                    st.info("👁️ **Reviewer mode**: You can view changes but cannot approve/reject. Contact an approver to take action.")
                
                btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
                with btn_col1:
                    if st.button("✅ Approve", key=f"approve_{change['change_id']}", type="primary", disabled=not can_approve):
                        username = user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_reviewer'
                        
                        if approve_change(change['change_id'], username, reviewer_comment):
                            # Note: Approved changes stay in staging table - a separate workflow will push to UC
                            st.success("✅ Change approved! It will be pushed to UC by the scheduled sync workflow.")
                            st.rerun()
                        else:
                            st.error("❌ Failed to approve change.")
                
                with btn_col2:
                    if st.button("❌ Reject", key=f"reject_{change['change_id']}", type="secondary", disabled=not can_approve):
                        username = user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_reviewer'
                        
                        if reject_change(change['change_id'], username, reviewer_comment):
                            st.warning("⚠️ Change rejected.")
                            st.rerun()
                        else:
                            st.error("❌ Failed to reject change.")
        
        st.divider()
        
        # Bulk actions (only for approvers)
        can_approve_bulk = is_approver(user_info)
        
        st.markdown("### Bulk Actions")
        if can_approve_bulk:
            st.caption("Apply the same action and comment to all pending changes for this table")
        else:
            st.caption("🔒 Bulk actions are only available to approvers")
        
        bulk_comment = st.text_area(
            "Bulk Action Comment (optional)",
            key="bulk_comment",
            placeholder="This comment will be applied to all approved/rejected changes",
            height=60,
            disabled=not can_approve_bulk
        )
        
        bulk_col1, bulk_col2 = st.columns(2)
        with bulk_col1:
            if st.button("✅ Approve All Changes", type="primary", use_container_width=True, disabled=not can_approve_bulk):
                username = user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_reviewer'
                
                with st.spinner("Approving all changes in bulk..."):
                    # Collect all change_ids and approve in a single bulk UPDATE
                    # Note: Approved changes stay in staging table - a separate workflow will push to UC
                    all_change_ids = changes_df['change_id'].tolist()
                    
                    if approve_changes_bulk(all_change_ids, username, bulk_comment):
                        st.success(f"✅ Approved {len(all_change_ids)} change(s)! They will be pushed to UC by the scheduled sync workflow.")
                    else:
                        st.error("❌ Failed to approve changes.")
                st.rerun()
        
        with bulk_col2:
            if st.button("❌ Reject All Changes", type="secondary", use_container_width=True, disabled=not can_approve_bulk):
                username = user_info.get('username') or user_info.get('email') or user_info.get('user') or 'authenticated_reviewer'
                
                with st.spinner("Rejecting all changes in bulk..."):
                    # Collect all change_ids and reject in a single bulk UPDATE
                    all_change_ids = changes_df['change_id'].tolist()
                    
                    if reject_changes_bulk(all_change_ids, username, bulk_comment):
                        st.warning(f"⚠️ Rejected {len(all_change_ids)} change(s).")
                    else:
                        st.error("❌ Failed to reject changes.")
                st.rerun()
    else:
        st.info("No pending changes for this table.")

else:
    # Welcome screen
    if st.session_state.current_view == 'browse':
        st.info("👈 Select a catalog, schema, and table/view from the sidebar to get started")
        
        # Display Metadata KPIs
        st.markdown("---")
        
        col_title, col_refresh = st.columns([4, 1])
        with col_title:
            st.markdown("### 📊 Workspace Metadata Health")
        with col_refresh:
            # We'll add the refresh time after loading KPIs
            pass
        
        with st.spinner("Loading metadata statistics..."):
            kpis = get_metadata_kpis()
        
        if kpis:
            # Show last refresh time
            with col_refresh:
                if 'last_refresh' in kpis and pd.notna(kpis['last_refresh']):
                    refresh_time = pd.to_datetime(kpis['last_refresh'])
                    st.caption(f"🕐 Updated: {refresh_time.strftime('%H:%M')}")
            
            st.markdown("")
            # Row 1 - Tables with Descriptions
            pct_with_desc = (kpis['tables_with_desc'] / kpis['total_tables'] * 100) if kpis['total_tables'] > 0 else 0
            st.markdown(f"**📝 Tables with Descriptions**")
            st.markdown(f"<h2 style='margin: 0; color: #78BE20;'>{pct_with_desc:.1f}%</h2>", unsafe_allow_html=True)
            st.markdown(f"<p style='margin: 0; color: #666; font-size: 0.875rem;'>{kpis['tables_with_desc']:,}/{kpis['total_tables']:,} tables</p>", unsafe_allow_html=True)
            
            st.divider()
            
            # Row 2 - Column Description Metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                pct_all_cols = (kpis['tables_all_cols_desc'] / kpis['total_tables'] * 100) if kpis['total_tables'] > 0 else 0
                st.markdown(f"**✅ All Columns Described**")
                st.markdown(f"<h2 style='margin: 0; color: #78BE20;'>{pct_all_cols:.1f}%</h2>", unsafe_allow_html=True)
                st.markdown(f"<p style='margin: 0; color: #666; font-size: 0.875rem;'>{kpis['tables_all_cols_desc']:,}/{kpis['total_tables']:,} tables</p>", unsafe_allow_html=True)
            
            with col2:
                pct_partial = (kpis['tables_partial_cols_desc'] / kpis['total_tables'] * 100) if kpis['total_tables'] > 0 else 0
                st.markdown(f"**⚠️ Partially Described**")
                st.markdown(f"<h2 style='margin: 0; color: #78BE20;'>{pct_partial:.1f}%</h2>", unsafe_allow_html=True)
                st.markdown(f"<p style='margin: 0; color: #666; font-size: 0.875rem;'>{kpis['tables_partial_cols_desc']:,}/{kpis['total_tables']:,} tables</p>", unsafe_allow_html=True)
            
            with col3:
                no_cols_desc = kpis['total_tables'] - kpis['tables_all_cols_desc'] - kpis['tables_partial_cols_desc']
                pct_no_desc = (no_cols_desc / kpis['total_tables'] * 100) if kpis['total_tables'] > 0 else 0
                st.markdown(f"**❌ No Column Descriptions**")
                st.markdown(f"<h2 style='margin: 0; color: #78BE20;'>{pct_no_desc:.1f}%</h2>", unsafe_allow_html=True)
                st.markdown(f"<p style='margin: 0; color: #666; font-size: 0.875rem;'>{no_cols_desc:,}/{kpis['total_tables']:,} tables</p>", unsafe_allow_html=True)
            
            st.divider()
            
            # Row 3 - Leaderboards
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("#### 🏆 Top Contributors")
                if not kpis['top_suggestors'].empty:
                    for idx, row in kpis['top_suggestors'].iterrows():
                        medal = ["🥇", "🥈", "🥉"][idx] if idx < 3 else "🏅"
                        # Extract just the username part before @ if it's an email
                        username = row['suggestor_id'].split('@')[0] if '@' in row['suggestor_id'] else row['suggestor_id']
                        st.markdown(f"{medal} **{username}** - {row['suggestion_count']} suggestions")
                else:
                    st.caption("_No suggestions yet_")
            
            with col2:
                st.markdown("#### ✅ Top Reviewers")
                if not kpis['top_reviewers'].empty:
                    for idx, row in kpis['top_reviewers'].iterrows():
                        medal = ["🥇", "🥈", "🥉"][idx] if idx < 3 else "🏅"
                        # Extract just the username part before @ if it's an email
                        username = row['reviewer_id'].split('@')[0] if '@' in row['reviewer_id'] else row['reviewer_id']
                        st.markdown(f"{medal} **{username}** - {row['review_count']} reviews")
                else:
                    st.caption("_No reviews yet_")
            
            with col3:
                st.markdown("#### ❌ Most Rejections")
                if not kpis['top_rejected_suggestors'].empty:
                    for idx, row in kpis['top_rejected_suggestors'].iterrows():
                        medal = ["🥇", "🥈", "🥉"][idx] if idx < 3 else "🏅"
                        # Extract just the username part before @ if it's an email
                        username = row['suggestor_id'].split('@')[0] if '@' in row['suggestor_id'] else row['suggestor_id']
                        st.markdown(f"{medal} **{username}** - {row['rejection_count']} rejections")
                else:
                    st.caption("_No rejections yet_")
        else:
            st.warning("Unable to load metadata statistics. Please check your connection.")
        
        st.markdown("---")
        
        st.markdown("""
        ### 🏠 Browse & Suggest Mode
        
        In this mode, you can:
        - Browse Unity Catalog tables and views
        - View current metadata descriptions
        - Suggest changes to descriptions
        - Submit changes for approval
        
        ### 📝 How it works:
        
        1. Select a catalog, schema, and table from the sidebar
        2. Click "Load Metadata" to view current metadata
        3. Click "Edit Metadata" to propose changes
        4. Submit your changes for review
        5. Changes will be queued for approval by a reviewer
        
        ### Required Permissions:
        
        **For Users:**
        - `USE CATALOG` and `USE SCHEMA` on the respective catalog and schema
        - `SELECT` on tables/views to read metadata
        
        **For the App (to apply approved changes):**
        - `USE CATALOG` and `USE SCHEMA` permissions
        - `MODIFY` on tables to update metadata
        - Access to SQL Warehouse
        - OAuth API scope for user authentication
        """)
    else:
        st.info("👈 Select a table with pending changes from the sidebar")
        
        st.markdown("""
        ### ✅ Review & Approve Mode
        
        In this mode, you can:
        - See all tables with pending metadata changes
        - Review proposed changes side-by-side with current values
        - Approve or deny individual changes
        - Apply bulk actions to approve/deny all changes for a table
        
        ### 📋 Review Process:
        
        1. Select a table with pending changes from the sidebar
        2. Review each proposed change
        3. Compare current vs. proposed values
        4. Approve or deny individual changes
        5. Approved changes are immediately written to Unity Catalog
        6. Denied changes are removed from the queue
        
        ### 🔐 Permissions:
        
        - Reviewers need `MODIFY` permissions on tables to apply approved changes
        - OAuth authentication is used for user identification
        
        ### Metadata Types:
        
        - **Table Descriptions**: Add context about the table's purpose
        - **Column Descriptions**: Document what each column contains
        """)
