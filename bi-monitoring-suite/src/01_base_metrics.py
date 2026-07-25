# Databricks notebook source
# MAGIC %md
# MAGIC # BI Monitoring Suite — Base Metrics Notebook
# MAGIC
# MAGIC Aggregates **Genie Agents + AIBI** usage from Databricks **system tables** into the
# MAGIC lowest-grain, PII-anonymised fact table `bi_monitoring_suite.monitoring_assets.bi_usage_fact`
# MAGIC plus a companion `bi_freshness_snapshot` table. Runs twice daily (00:00 / 12:00) via the
# MAGIC pipeline in `../resources/bi_monitoring_job.yml`.
# MAGIC
# MAGIC **Cloud portability:** This notebook is identical on **AWS and Azure**. All sources are
# MAGIC `system.*` tables whose schema is cloud-independent, and `list_prices` is joined on
# MAGIC `u.cloud = lp.cloud` so pricing self-selects the correct cloud. The only value that ever
# MAGIC differs is the `cloud` string itself (`AWS` / `AZURE` / `GCP`), which flows through the data,
# MAGIC not the code. See README §"AWS vs Azure" for the full audit of divergences (there are none in
# MAGIC syntax or setup).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ REQUIRED PERMISSIONS (read before first run)
# MAGIC
# MAGIC The **run-as identity** of this notebook/job needs the ACLs below. Schema-level grants on
# MAGIC `system.*` require an **account admin** (workspace admin is NOT sufficient — verified on the
# MAGIC target workspace). Ask an account admin to run these once, substituting the run-as principal:
# MAGIC
# MAGIC > **Granting a non-admin direct read access:** a non-admin cannot grant themselves `system.*`
# MAGIC > access. An **account admin runs block (a) with the non-admin user (or a UC group they belong
# MAGIC > to) as the grantee** — substitute the person/group for `<run_as>`. Those six grants are the
# MAGIC > exact, complete read surface this suite needs; nothing broader is required. The admin must
# MAGIC > also have enabled these system schemas on the metastore (account console → Metastore → System
# MAGIC > schemas) or the grants have nothing to point at.
# MAGIC
# MAGIC ```sql
# MAGIC -- (a) Read system tables (account admin only) — grantee = pipeline run-as identity
# MAGIC GRANT USE CATALOG ON CATALOG system                     TO `<run_as>`;   -- may already be held
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA system.billing       TO `<run_as>`;   -- costs, DBUs, genie.surface
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA system.access        TO `<run_as>`;   -- audit: users, interactions, ratings
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA system.query         TO `<run_as>`;   -- query completion times
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow      TO `<run_as>`;   -- table/pipeline freshness
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA system.compute       TO `<run_as>`;   -- warehouse names (asset names)
# MAGIC
# MAGIC -- (b) Write the output (workspace admin / catalog owner is sufficient) — grantee = run_as
# MAGIC GRANT USE CATALOG ON CATALOG bi_monitoring_suite                    TO `<run_as>`;
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
# MAGIC   ON SCHEMA bi_monitoring_suite.monitoring_assets                   TO `<run_as>`;
# MAGIC ```
# MAGIC
# MAGIC **Output location (catalog/schema).** `bi_monitoring_suite.monitoring_assets` is a dedicated
# MAGIC home created by this project — not a pre-existing UC object. This notebook **creates it on the
# MAGIC first run** (`CREATE CATALOG/SCHEMA IF NOT EXISTS` below, idempotent thereafter), so there is
# MAGIC no separate DDL to run — the run-as identity just needs **`CREATE CATALOG` on the metastore**
# MAGIC (or have the catalog pre-created for it). To store the tables in an EXISTING catalog/schema
# MAGIC instead, set the `target_catalog` / `target_schema` job parameters (defaults in
# MAGIC `databricks.yml` → `variables`), update the `CAT`/`SCH` constants in
# MAGIC `dashboard/build_dashboard.py`, and substitute your names into the (b)/(c) grants — the
# MAGIC `IF NOT EXISTS` calls are then harmless no-ops. See the README "Output location" section.
# MAGIC
# MAGIC **Individual tables touched** (all read-only except the two outputs):
# MAGIC | Object | Privilege | Why |
# MAGIC |---|---|---|
# MAGIC | `system.billing.usage` | SELECT | cost + DBU per user/agent/surface |
# MAGIC | `system.billing.list_prices` | SELECT | DBU → USD conversion |
# MAGIC | `system.access.audit` | SELECT | unique users, interactions, thumbs up/down ratings |
# MAGIC | `system.access.workspaces_latest` | SELECT | workspace_id → workspace name (same schema, no extra grant) |
# MAGIC | `system.compute.warehouses` | SELECT | warehouse_id → warehouse name (asset names) |
# MAGIC | `system.query.history` | SELECT | query completion time (proxy for load time) |
# MAGIC | `system.lakeflow.pipelines` / table history | SELECT | freshness (days since last refresh) |
# MAGIC | `bi_monitoring_suite.monitoring_assets.bi_usage_fact` | CREATE/MODIFY | primary output |
# MAGIC | `bi_monitoring_suite.monitoring_assets.bi_freshness_snapshot` | CREATE/MODIFY | freshness output |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config (from job parameters — nothing environment-specific is hard-coded)

# COMMAND ----------

dbutils.widgets.text("target_catalog", "bi_monitoring_suite", "Output catalog")
dbutils.widgets.text("target_schema", "monitoring_assets", "Output schema")
dbutils.widgets.text("lookback_days", "395", "Days of history to (re)aggregate")  # ~13 months for YoY
dbutils.widgets.text("pii_salt_scope", "", "Optional secret scope holding the PII hash salt")
dbutils.widgets.text("pii_salt_key", "", "Optional secret key holding the PII hash salt")
# PII toggle. "true" → deterministic sha2 hash of the user identity (spec default: anonymised but
# still countable). "false" → store the raw email/run_as, so the dashboard shows real user names.
# Reversible: flip this and rerun; the user_name column is repopulated in place.
dbutils.widgets.dropdown("anonymize_pii", "false", ["true", "false"], "Anonymise user identity (hash)?")

CATALOG   = dbutils.widgets.get("target_catalog")
SCHEMA    = dbutils.widgets.get("target_schema")
LOOKBACK  = int(dbutils.widgets.get("lookback_days"))
ANONYMIZE = dbutils.widgets.get("anonymize_pii").lower() == "true"
FQ        = f"`{CATALOG}`.`{SCHEMA}`"

# PII salt: if a secret is configured use it (so the hash isn't reversible by rainbow table);
# otherwise fall back to an unsalted sha2 (still non-reversible for counting, documented in README).
_scope = dbutils.widgets.get("pii_salt_scope")
_key   = dbutils.widgets.get("pii_salt_key")
try:
    PII_SALT = dbutils.secrets.get(_scope, _key) if _scope and _key else ""
except Exception:
    PII_SALT = ""

# Salt is injected directly into the SQL (below) as a string literal — NOT via spark.conf/${...}
# substitution, which is unavailable on serverless/Spark Connect. Escape single quotes defensively.
SALT_LITERAL = PII_SALT.replace("'", "''")
print(f"Output: {FQ}  |  lookback={LOOKBACK}d  |  salted PII hash={'yes' if PII_SALT else 'no (unsalted sha2-256)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## THIN SLICE — run this FIRST when the system-table grants land
# MAGIC Confirms every read path + two schema shapes that vary by workspace
# MAGIC (`usage_metadata.genie.surface` and the freshness source) before the full build writes anything.

# COMMAND ----------

def thin_slice_check():
    """Prints row counts + distinct surfaces/comment_types for the last 7 days. Read-only."""
    checks = {
        "billing.usage (GENIE, 7d)":
            "SELECT count(*) FROM system.billing.usage "
            "WHERE billing_origin_product='GENIE' AND usage_date >= current_date()-INTERVAL 7 DAYS",
        "distinct genie.surface":
            "SELECT DISTINCT usage_metadata.genie.surface FROM system.billing.usage "
            "WHERE billing_origin_product='GENIE' AND usage_date >= current_date()-INTERVAL 7 DAYS",
        "access.audit genie events (7d)":
            "SELECT service_name, action_name, count(*) n FROM system.access.audit "
            "WHERE event_date >= current_date()-INTERVAL 7 DAYS "
            "AND service_name IN ('aibiGenie','genieChat') "
            "GROUP BY 1,2 ORDER BY n DESC LIMIT 20",
        "distinct feedback_rating (ratings)":
            "SELECT coalesce(request_params.feedback_rating, "
            "get_json_object(request_params.feedback_payload,'$.conversation_feedback.feedback_rating')) AS rating, "
            "count(*) n FROM system.access.audit "
            "WHERE action_name IN ('updateConversationMessageFeedback','updateGenieChatConversationFeedback') "
            "AND event_date >= current_date()-INTERVAL 90 DAYS GROUP BY 1",
        "query.history (7d)":
            "SELECT count(*), avg(total_duration_ms) FROM system.query.history "
            "WHERE start_time >= current_date()-INTERVAL 7 DAYS",
    }
    for label, sql in checks.items():
        print(f"\n### {label}")
        try:
            spark.sql(sql).show(20, truncate=False)
        except Exception as e:
            print(f"  !! {type(e).__name__}: {str(e)[:200]}")

# Uncomment to run interactively during recon:
# thin_slice_check()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII anonymisation helper
# MAGIC Deterministic `sha2(lower(email) || salt, 256)` → stable across refreshes (unique-user counts
# MAGIC still work) but not reversible. Salt (if configured) blocks rainbow-table de-anonymisation.

# COMMAND ----------

# Reusable SQL fragment applied identically in every CTE. When ANONYMIZE is on, deterministic
# salted sha2 (stable across refreshes → unique-user counts still work, identity hidden). When off,
# the raw lower-cased email/run_as is stored so the dashboard shows real user names. The output
# column is named `user_hash` either way, so the metric view / dashboard contract is unchanged.
if ANONYMIZE:
    USER_HASH = "sha2(concat(lower(coalesce({col}, 'unknown')), '" + SALT_LITERAL + "'), 256)"
else:
    USER_HASH = "lower(coalesce({col}, 'unknown'))"
print(f"PII: {'ANONYMISED (salted sha2)' if ANONYMIZE else 'RAW user identity stored (not anonymised)'}")

# Human-readable display name derived from the identity. `{col}` is a user_hash-style column.
# - first.last@domain      → "First Last"  (split local-part on '.', title-case each token)
# - other email@domain     → local-part verbatim (e.g. "vgiri")
# - uuid (service principal)→ "Service principal (·<last4>)"
# - 'unknown' / null        → "Unknown"
# When ANONYMIZE is on, the identity is a hash → we can't parse a name, so fall back to the hash
# itself (display = raw value); documented, since anonymised mode intentionally hides identity.
def _display_name_expr(col):
    if ANONYMIZE:
        return f"{col}"
    return f"""
      CASE
        WHEN {col} IS NULL OR {col} = 'unknown' THEN 'Unknown'
        WHEN {col} RLIKE '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-' THEN concat('Service principal (·', right({col}, 4), ')')
        WHEN {col} LIKE '%.%@%' THEN
          concat_ws(' ', transform(split(split({col}, '@')[0], '\\\\.'),
                                   x -> initcap(x)))
        WHEN {col} LIKE '%@%' THEN split({col}, '@')[0]
        ELSE {col}
      END"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Asset-name enrichment (Genie spaces + AIBI dashboards)
# MAGIC
# MAGIC Warehouse names are in `system.compute.warehouses` (resolved in the fact SQL). Genie **space**
# MAGIC and AIBI **dashboard** names are NOT in any system table — only their IDs appear in the audit /
# MAGIC billing / query-history sources. This cell resolves those IDs → human names via the workspace
# MAGIC REST APIs and materialises a small lookup table `bi_asset_names` that the fact SQL LEFT JOINs.
# MAGIC
# MAGIC **Design:** LIST endpoints (paginated) are used, not per-ID GETs — ~14 pages cover all
# MAGIC dashboards and ~16 cover all genie spaces, versus tens of thousands of per-ID calls. IDs that
# MAGIC don't resolve (e.g. a genie `asset_id` that is actually a conversation_id, or a since-deleted
# MAGIC asset) simply have no lookup row and fall back to the id in the fact — strictly better coverage
# MAGIC than before, never worse. The step is wrapped in try/except so an API/permission hiccup degrades
# MAGIC gracefully to id-only names (the prior behaviour) instead of failing the whole run.
# MAGIC
# MAGIC Auth: uses the notebook/job run-as identity via `databricks-sdk` (no tokens in code). The run-as
# MAGIC principal must be able to LIST Lakeview dashboards and Genie spaces in this workspace.

# COMMAND ----------

def build_asset_names():
    """Return a list of {asset_id, asset_name, asset_kind} dicts from the Lakeview + Genie LIST APIs.
    Paginates fully. Any failure raises to the caller (which degrades gracefully)."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    rows = []

    def _paginate(path, items_key, id_key, name_key, kind):
        token = None
        pages = 0
        while True:
            query = {"page_size": 1000}
            if token:
                query["page_token"] = token
            resp = w.api_client.do("GET", path, query=query)
            for it in (resp.get(items_key) or []):
                aid = it.get(id_key)
                nm = it.get(name_key)
                if aid and nm:
                    rows.append({"asset_id": aid, "asset_name": nm, "asset_kind": kind})
            token = resp.get("next_page_token")
            pages += 1
            if not token or pages > 100:   # 100-page safety cap (far above the ~16 pages seen)
                break

    # AIBI dashboards: display_name. Genie spaces: title.
    _paginate("/api/2.0/lakeview/dashboards", "dashboards", "dashboard_id", "display_name", "dashboard")
    _paginate("/api/2.0/genie/spaces",        "spaces",     "space_id",     "title",        "genie_space")
    return rows

# Ensure the output catalog/schema exist before writing any table (this is the first write in the
# run). Idempotent no-ops if they already exist. CREATE CATALOG is only needed on the very first run.
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

# Always (re)create the table so the fact SQL can join it unconditionally. On any API/permission
# failure, fall back to an EMPTY lookup (schema-only) — the LEFT JOIN then yields no names and the
# fact coalesces to the id, exactly the prior behaviour.
from pyspark.sql.types import StructType, StructField, StringType
_names_schema = StructType([
    StructField("asset_id", StringType(), True),
    StructField("asset_name", StringType(), True),
    StructField("asset_kind", StringType(), True),
])
try:
    _name_rows = build_asset_names()
    _names_df = spark.createDataFrame(_name_rows, schema=_names_schema) if _name_rows \
                else spark.createDataFrame([], schema=_names_schema)
    # Dedup defensively (an id should be unique per kind; keep one name per id).
    _names_df.createOrReplaceTempView("_asset_names_new")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {FQ}.bi_asset_names AS
        SELECT asset_id, max(asset_name) AS asset_name, max(asset_kind) AS asset_kind
        FROM _asset_names_new WHERE asset_id IS NOT NULL GROUP BY asset_id
    """)
    print(f"Wrote {FQ}.bi_asset_names — {spark.table(f'{FQ}.bi_asset_names').count()} resolved names "
          f"({len(_name_rows)} raw from APIs)")
except Exception as e:
    # Graceful degradation: create an empty lookup so the fact JOIN still resolves (to id-only names).
    spark.createDataFrame([], schema=_names_schema).write.mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(f"{FQ}.bi_asset_names")
    print(f"!! Name enrichment skipped (id-only fallback): {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the primary fact table
# MAGIC
# MAGIC **Grain:** one row per `(usage_date, account_id, workspace_id, product, surface, asset_id,
# MAGIC asset_type, owner, user_hash)`. Costs, interactions, ratings and query-duration each come from a
# MAGIC different system table at a different natural grain, so each is aggregated to this common grain
# MAGIC in its own CTE and combined by FULL OUTER JOIN on the grain key. Metrics are **additive** (safe
# MAGIC to SUM at any roll-up); averages are derived in the metric view from sum/count pairs.

# COMMAND ----------

base_sql = f"""
WITH
-- ---------- COST + DBUs (system.billing) ----------
-- Genie Agents cost, split by surface. AIBI/SQL warehouse cost attributed via warehouse usage.
genie_cost AS (
  SELECT
    u.usage_date,
    u.account_id,
    CAST(u.workspace_id AS STRING)              AS workspace_id,
    'GENIE_AGENTS'                              AS product,
    u.usage_metadata.genie.surface              AS surface,
    u.usage_metadata.genie.agent_id             AS asset_id,
    'genie_space'                               AS asset_type,
    {USER_HASH.format(col='u.identity_metadata.run_as')} AS user_hash,
    u.custom_tags                               AS governed_tags,
    SUM(u.usage_quantity)                       AS dbus,
    SUM(u.usage_quantity * lp.pricing.default)  AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud                       -- cloud self-selects: identical code on AWS/Azure
   AND u.sku_name = lp.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.billing_origin_product = 'GENIE'
    AND u.usage_metadata.genie.surface = 'GENIE_AGENTS'
    AND u.usage_date >= current_date() - INTERVAL {LOOKBACK} DAYS
  GROUP BY 1,2,3,4,5,6,7,8,9
),
-- AIBI / SQL warehouse cost (dashboards run SQL on warehouses). Attributed to warehouse as asset.
aibi_cost AS (
  SELECT
    u.usage_date,
    u.account_id,
    CAST(u.workspace_id AS STRING)              AS workspace_id,
    'AIBI'                                      AS product,
    'SQL_WAREHOUSE'                             AS surface,
    u.usage_metadata.warehouse_id               AS asset_id,
    'sql_warehouse'                             AS asset_type,
    {USER_HASH.format(col='u.identity_metadata.run_as')} AS user_hash,
    u.custom_tags                               AS governed_tags,
    SUM(u.usage_quantity)                       AS dbus,
    SUM(u.usage_quantity * lp.pricing.default)  AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.billing_origin_product IN ('SQL','DATABRICKS_SQL')
    AND u.usage_metadata.warehouse_id IS NOT NULL
    AND u.usage_date >= current_date() - INTERVAL {LOOKBACK} DAYS
  GROUP BY 1,2,3,4,5,6,7,8,9
),
costs AS (SELECT * FROM genie_cost UNION ALL SELECT * FROM aibi_cost),

-- ---------- ASSET NAME lookup ----------
-- Warehouse names (AIBI SQL_WAREHOUSE assets) ARE in system.compute.warehouses. Genie space and
-- AIBI dashboard names are NOT in any system table (audit/billing carry only IDs) — they are
-- resolved via the REST-API enrichment step above into `bi_asset_names` (id → name). Any id that
-- still doesn't resolve (e.g. a genie conversation_id, or a deleted asset) falls back to the id.
wh_names AS (
  SELECT warehouse_id, warehouse_name
  FROM system.compute.warehouses
  QUALIFY row_number() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1
),
api_names AS (
  SELECT asset_id, asset_name FROM {FQ}.bi_asset_names
),
-- Workspace names from system.access.workspaces_latest (human-readable, one row per workspace).
ws_names AS (
  SELECT CAST(workspace_id AS STRING) AS workspace_id, workspace_name
  FROM system.access.workspaces_latest
),

-- ---------- INTERACTIONS + RATINGS (system.access.audit) ----------
-- Shapes VERIFIED on live data 2026-07-21 (corrected from the initial doc-based guess):
--   * Genie services are 'aibiGenie' and 'genieChat' (NOT 'genie'/'dashboards').
--   * An interaction = a submitted user message: createConversationMessage / genieStartConversationMessage
--     (aibiGenie) or createGenieChatResponse (genieChat).
--   * Ratings live in updateConversationMessageFeedback.request_params.feedback_rating = THUMBS_UP/DOWN
--     (aibiGenie) and updateGenieChatConversationFeedback (genieChat), whose rating is
--     'ONE_CHAT_FEEDBACK_RATING_THUMBS_UP/DOWN' either top-level or nested in feedback_payload JSON.
--   * comment_type (createConversationMessageComment) is always NULL here — do NOT use it.
interactions AS (
  SELECT
    a.event_date                                        AS usage_date,
    a.account_id,
    CAST(a.workspace_id AS STRING)                      AS workspace_id,
    'GENIE_AGENTS'                                      AS product,   -- all audit BI interactions are Genie
    {USER_HASH.format(col='a.user_identity.email')}     AS user_hash,
    coalesce(a.request_params.space_id, a.request_params.conversation_id) AS asset_id,
    SUM(CASE WHEN a.action_name IN
          ('createConversationMessage','genieStartConversationMessage','createGenieChatResponse')
        THEN 1 ELSE 0 END)                              AS interactions,
    SUM(CASE WHEN a.action_name IN
          ('updateConversationMessageFeedback','updateGenieChatConversationFeedback')
        AND coalesce(a.request_params.feedback_rating,
              get_json_object(a.request_params.feedback_payload,'$.conversation_feedback.feedback_rating'))
            ILIKE '%THUMBS_UP%'   THEN 1 ELSE 0 END)    AS thumbs_up,
    SUM(CASE WHEN a.action_name IN
          ('updateConversationMessageFeedback','updateGenieChatConversationFeedback')
        AND coalesce(a.request_params.feedback_rating,
              get_json_object(a.request_params.feedback_payload,'$.conversation_feedback.feedback_rating'))
            ILIKE '%THUMBS_DOWN%' THEN 1 ELSE 0 END)    AS thumbs_down
  FROM system.access.audit a
  WHERE a.event_date >= current_date() - INTERVAL {LOOKBACK} DAYS
    AND a.service_name IN ('aibiGenie','genieChat')
    AND a.action_name IN
      ('createConversationMessage','genieStartConversationMessage','createGenieChatResponse',
       'updateConversationMessageFeedback','updateGenieChatConversationFeedback')
  GROUP BY 1,2,3,4,5,6
),

-- ---------- QUERY COMPLETION TIME (system.query.history) ----------
-- Substitutes dashboard load time (user-approved). Genie SQL carries a genie space id in query_source.
-- SCOPING (corrected 2026-07-22): AIBI = queries that ACTUALLY came from a dashboard
-- (query_source.dashboard_id / legacy_dashboard_id present). Generic warehouse SQL — ad-hoc SQL
-- editor, jobs, notebooks, API — has NO source id and is NOT BI activity; it was previously
-- mislabelled 'AIBI' (~89% of the volume, 43M rows), swamping real dashboards. Those rows are now
-- EXCLUDED. Only genie-space or dashboard-attributed queries are kept.
query_perf AS (
  SELECT
    to_date(q.start_time)                               AS usage_date,
    q.account_id,
    CAST(q.workspace_id AS STRING)                      AS workspace_id,
    CASE WHEN q.query_source.genie_space_id IS NOT NULL THEN 'GENIE_AGENTS' ELSE 'AIBI' END AS product,
    {USER_HASH.format(col='q.executed_by')}             AS user_hash,
    coalesce(q.query_source.genie_space_id, q.query_source.legacy_dashboard_id, q.query_source.dashboard_id) AS asset_id,
    -- Source data quality: system.query.history has ~10k FINISHED rows with NEGATIVE
    -- total_duration_ms (down to -282s), which corrupt the avg (negative completion time is
    -- impossible). Sum ONLY positive durations, and count them separately so the avg =
    -- sum(valid duration)/count(valid duration), not /all-queries. query_count keeps ALL finished
    -- queries (a query still happened even if its recorded duration is junk).
    SUM(CASE WHEN q.total_duration_ms > 0 THEN q.total_duration_ms ELSE 0 END) AS total_duration_ms,
    SUM(CASE WHEN q.total_duration_ms > 0 THEN 1 ELSE 0 END)                   AS duration_query_count,
    COUNT(*)                                            AS query_count
  FROM system.query.history q
  WHERE q.start_time >= current_date() - INTERVAL {LOOKBACK} DAYS
    AND q.execution_status = 'FINISHED'
    -- keep ONLY BI-attributed queries: from a genie space OR from a dashboard. Excludes ad-hoc SQL.
    AND (q.query_source.genie_space_id IS NOT NULL
         OR q.query_source.dashboard_id IS NOT NULL
         OR q.query_source.legacy_dashboard_id IS NOT NULL)
  GROUP BY 1,2,3,4,5,6
)

-- ---------- COMBINE to common grain ----------
SELECT
  coalesce(c.usage_date, i.usage_date, p.usage_date)          AS usage_date,
  coalesce(c.account_id, i.account_id, p.account_id)          AS account_id,
  coalesce(c.workspace_id, i.workspace_id, p.workspace_id)    AS workspace_id,
  coalesce(wn.workspace_name, coalesce(c.workspace_id, i.workspace_id, p.workspace_id)) AS workspace_name,
  coalesce(c.product, i.product, p.product)                   AS product,
  c.surface,
  coalesce(c.asset_id, i.asset_id, p.asset_id)                AS asset_id,
  c.asset_type,
  -- asset_name: warehouse name (system table) → REST-API-resolved genie/dashboard name → id fallback
  coalesce(w.warehouse_name, an.asset_name, c.asset_id, i.asset_id, p.asset_id) AS asset_name,
  coalesce(c.user_hash, i.user_hash, p.user_hash)             AS user_hash,
  -- user_display: human-readable "First Last" (or SP/local-part fallback). Raw user_hash kept above
  -- for filtering/joins; display used as the chart LABEL, with the email/id shown alongside.
  {_display_name_expr('coalesce(c.user_hash, i.user_hash, p.user_hash)')} AS user_display,
  c.governed_tags,
  coalesce(c.dbus, 0)               AS dbus,
  coalesce(c.cost_usd, 0)           AS cost_usd,
  coalesce(i.interactions, 0)       AS interactions,
  coalesce(i.thumbs_up, 0)          AS thumbs_up,
  coalesce(i.thumbs_down, 0)        AS thumbs_down,
  coalesce(p.total_duration_ms, 0)  AS total_duration_ms,
  coalesce(p.duration_query_count, 0) AS duration_query_count,
  coalesce(p.query_count, 0)        AS query_count,
  current_timestamp()               AS _loaded_at
FROM costs c
FULL OUTER JOIN interactions i
  ON  c.usage_date = i.usage_date AND c.workspace_id = i.workspace_id
  AND c.product = i.product AND c.asset_id = i.asset_id AND c.user_hash = i.user_hash
FULL OUTER JOIN query_perf p
  ON  coalesce(c.usage_date, i.usage_date) = p.usage_date
  AND coalesce(c.workspace_id, i.workspace_id) = p.workspace_id
  AND coalesce(c.product, i.product) = p.product
  AND coalesce(c.asset_id, i.asset_id) = p.asset_id
  AND coalesce(c.user_hash, i.user_hash) = p.user_hash
LEFT JOIN wh_names w
  ON coalesce(c.asset_id, i.asset_id, p.asset_id) = w.warehouse_id
LEFT JOIN api_names an
  ON coalesce(c.asset_id, i.asset_id, p.asset_id) = an.asset_id
LEFT JOIN ws_names wn
  ON coalesce(c.workspace_id, i.workspace_id, p.workspace_id) = wn.workspace_id
"""

# COMMAND ----------

# Catalog/schema already ensured in the enrichment cell above (kept idempotent here in case this
# cell is run in isolation).
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
fact = spark.sql(base_sql)
(fact.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{FQ}.bi_usage_fact"))
print(f"Wrote {FQ}.bi_usage_fact — {spark.table(f'{FQ}.bi_usage_fact').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the per-query ESTIMATED cost table (drill-down support)
# MAGIC
# MAGIC **Why estimated, not measured:** `system.billing.usage` bills SQL warehouses per
# MAGIC `(warehouse_id, hour)` — there is NO per-statement cost anywhere, and one warehouse-hour can
# MAGIC cover thousands of queries. So we ALLOCATE each warehouse-hour's USD cost across that hour's
# MAGIC finished queries in proportion to each query's `total_task_duration_ms` (compute-time weight).
# MAGIC
# MAGIC **Caveats (documented, load-bearing):**
# MAGIC - Only ~35% of warehouse cost lands in hours that actually ran queries; the rest is idle /
# MAGIC   provisioned-but-unused uptime and is NOT allocated. So est_cost is a lower bound and best used
# MAGIC   as a RELATIVE ranking ("which queries/users/dashboards drove the most compute"), not an
# MAGIC   absolute bill. Excludes result-fetch time; overlapping parallel queries share the hour.
# MAGIC - Genie SQL is attributed via `query_source.genie_space_id`; dashboards via `dashboard_id`.
# MAGIC Grain: one row per finished statement (that ran on a warehouse) with an estimated USD cost.

# COMMAND ----------

query_cost_sql = f"""
WITH wh_hourly AS (
  SELECT usage_metadata.warehouse_id AS warehouse_id,
         date_trunc('HOUR', u.usage_start_time) AS hr,
         SUM(u.usage_quantity * lp.pricing.default) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.billing_origin_product IN ('SQL','DATABRICKS_SQL')
    AND u.usage_metadata.warehouse_id IS NOT NULL
    AND u.usage_start_time >= current_date() - INTERVAL {LOOKBACK} DAYS
  GROUP BY 1,2
),
q AS (
  SELECT
    q.statement_id,
    to_date(q.start_time)                                   AS usage_date,
    q.account_id,
    CAST(q.workspace_id AS STRING)                          AS workspace_id,
    CASE WHEN q.query_source.genie_space_id IS NOT NULL THEN 'GENIE_AGENTS' ELSE 'AIBI' END AS product,
    coalesce(q.query_source.genie_space_id, q.query_source.dashboard_id, q.query_source.legacy_dashboard_id) AS asset_id,
    {USER_HASH.format(col='q.executed_by')}                 AS user_hash,
    q.compute.warehouse_id                                  AS warehouse_id,
    date_trunc('HOUR', q.start_time)                        AS hr,
    q.total_duration_ms,
    greatest(q.total_task_duration_ms, 1)                   AS task_wt,
    substr(q.statement_text, 1, 300)                        AS statement_preview
  FROM system.query.history q
  WHERE q.start_time >= current_date() - INTERVAL {LOOKBACK} DAYS
    AND q.execution_status = 'FINISHED'
    AND q.compute.warehouse_id IS NOT NULL
    -- BI-attributed only (dashboard or genie), matching the fact's scoping
    AND (q.query_source.genie_space_id IS NOT NULL
         OR q.query_source.dashboard_id IS NOT NULL
         OR q.query_source.legacy_dashboard_id IS NOT NULL)
)
SELECT
  q.statement_id, q.usage_date, q.account_id, q.workspace_id, q.product,
  q.asset_id,
  -- asset_name: REST-API-resolved genie/dashboard name (from bi_asset_names), else the id. Lets the
  -- Cost-tab Top-N + drill charts (which read this table, not the fact) show human names too.
  coalesce(an.asset_name, q.asset_id) AS asset_name,
  q.user_hash,
  {_display_name_expr('q.user_hash')} AS user_display,
  q.warehouse_id, q.total_duration_ms, q.statement_preview,
  wh_hourly.cost_usd * q.task_wt
    / SUM(q.task_wt) OVER (PARTITION BY q.warehouse_id, q.hr) AS est_cost_usd,
  current_timestamp() AS _loaded_at
FROM q
JOIN wh_hourly ON q.warehouse_id = wh_hourly.warehouse_id AND q.hr = wh_hourly.hr
LEFT JOIN {FQ}.bi_asset_names an ON q.asset_id = an.asset_id
"""
qc = spark.sql(query_cost_sql)
(qc.write.mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(f"{FQ}.bi_query_cost"))
print(f"Wrote {FQ}.bi_query_cost — {spark.table(f'{FQ}.bi_query_cost').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the freshness companion table
# MAGIC Freshness is **non-additive** and per-asset-per-day, so it lives in its own table to avoid
# MAGIC corrupting the additive roll-ups in the fact. One row per asset per snapshot day.

# COMMAND ----------

# NOTE (verified 2026-07-21): system.lakeflow.pipelines exposes `change_time` (last *modified*),
# not a true last-*refresh*/last-*update* timestamp — so freshness here is a proxy: "days since the
# pipeline definition last changed". Excludes deleted pipelines. If a truer freshness source is
# needed, system.lakeflow pipeline *update/run* history or table history (DESCRIBE HISTORY) is the
# follow-up — flagged in README.
freshness_sql = f"""
SELECT
  current_date()                                  AS snapshot_date,
  p.account_id,
  CAST(p.workspace_id AS STRING)                  AS workspace_id,
  'AIBI'                                          AS product,
  p.pipeline_id                                   AS asset_id,
  'pipeline'                                      AS asset_type,
  p.name                                          AS asset_name,
  max(p.change_time)                              AS last_refresh_time,
  date_diff(current_date(), to_date(max(p.change_time))) AS days_since_last_refresh
FROM system.lakeflow.pipelines p
WHERE p.delete_time IS NULL
GROUP BY 1,2,3,4,5,6,7
"""
try:
    fresh = spark.sql(freshness_sql)
    # Idempotent upsert keyed on (snapshot_date, asset_id): preserves genuine day-over-day history
    # but a rerun on the SAME day REPLACES that day's rows instead of appending duplicates
    # (blind append + twice-daily runs would double the table every day).
    if spark.catalog.tableExists(f"{FQ}.bi_freshness_snapshot"):
        fresh.createOrReplaceTempView("_fresh_new")
        spark.sql(f"""
            MERGE INTO {FQ}.bi_freshness_snapshot t
            USING _fresh_new s
              ON t.snapshot_date = s.snapshot_date AND t.asset_id = s.asset_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        fresh.write.saveAsTable(f"{FQ}.bi_freshness_snapshot")
    print(f"Upserted freshness snapshot — {fresh.count()} assets for today")
except Exception as e:
    # Freshness source shape can vary by workspace; don't fail the whole run on it.
    print(f"!! Freshness step skipped (confirm system.lakeflow shape): {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the per-asset cost rollup (Cost tab: avg cost / agent / dashboard, all + active)
# MAGIC Total cost per asset = agentic DBU cost (fact, Genie only) + estimated allocated SQL cost
# MAGIC (bi_query_cost, both products). `activity` lets the dashboard compute avg cost per ACTIVE
# MAGIC asset (denominator = assets with >0 interactions/queries) as well as per all assets.

# COMMAND ----------

try:
    asset_cost = spark.sql(f"""
        WITH agentic AS (
          SELECT asset_id, product, workspace_id, sum(cost_usd) agentic_cost, sum(interactions)+sum(query_count) activity
          FROM {FQ}.bi_usage_fact WHERE product='GENIE_AGENTS' AND asset_id IS NOT NULL GROUP BY 1,2,3),
        sqlc AS (
          SELECT asset_id, product, workspace_id, sum(est_cost_usd) sql_cost, count(*) activity
          FROM {FQ}.bi_query_cost WHERE asset_id IS NOT NULL GROUP BY 1,2,3)
        SELECT coalesce(a.asset_id,s.asset_id) AS asset_id,
               coalesce(a.product,s.product) AS product,
               coalesce(a.workspace_id,s.workspace_id) AS workspace_id,
               coalesce(a.agentic_cost,0) AS agentic_cost,
               coalesce(s.sql_cost,0) AS sql_cost,
               coalesce(a.agentic_cost,0)+coalesce(s.sql_cost,0) AS total_cost,
               coalesce(a.activity,0)+coalesce(s.activity,0) AS activity,
               current_timestamp() AS _loaded_at
        FROM agentic a FULL OUTER JOIN sqlc s
          ON a.asset_id=s.asset_id AND a.product=s.product AND a.workspace_id=s.workspace_id
    """)
    (asset_cost.write.mode("overwrite").option("overwriteSchema","true")
        .saveAsTable(f"{FQ}.bi_asset_cost"))
    print(f"Wrote {FQ}.bi_asset_cost — {asset_cost.count()} assets")
except Exception as e:
    print(f"!! Asset-cost rollup skipped: {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the source-composition table (Quality tab: "source % as metric views")
# MAGIC Per-dashboard % of source objects that are metric views, from `system.access.table_lineage`
# MAGIC (`entity_type = 'DASHBOARD_V3'` is a consumer; its `source_type` values include METRIC_VIEW).
# MAGIC AIBI-only — genie agents have no dashboard-lineage. Signals adoption of governed metric views
# MAGIC as dashboard sources.

# COMMAND ----------

try:
    src = spark.sql(f"""
        SELECT entity_id AS asset_id, 'AIBI' AS product,
               count(*) AS total_sources,
               sum(CASE WHEN source_type='METRIC_VIEW' THEN 1 ELSE 0 END) AS metric_view_sources,
               round(100.0*sum(CASE WHEN source_type='METRIC_VIEW' THEN 1 ELSE 0 END)/count(*),1) AS pct_metric_view,
               current_timestamp() AS _loaded_at
        FROM system.access.table_lineage
        WHERE entity_type='DASHBOARD_V3' AND source_type IS NOT NULL
          AND event_date >= current_date() - INTERVAL {LOOKBACK} DAYS
        GROUP BY entity_id
    """)
    (src.write.mode("overwrite").option("overwriteSchema","true")
        .saveAsTable(f"{FQ}.bi_source_composition"))
    print(f"Wrote {FQ}.bi_source_composition — {src.count()} dashboards")
except Exception as e:
    print(f"!! Source-composition step skipped: {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build per-DASHBOARD freshness (Quality tab: stalest DASHBOARDS, not their source tables)
# MAGIC The pipeline freshness above is keyed by pipeline. But the Quality tab wants **dashboards ranked
# MAGIC by staleness** with the driving source shown on hover. A dashboard has no refresh of its own —
# MAGIC its freshness = the staleness of its **stalest source table**. We get that by joining
# MAGIC `system.access.table_lineage` (DASHBOARD_V3 → source_table_full_name) to
# MAGIC `system.information_schema.tables.last_altered` (true last-altered per table), then taking the
# MAGIC MAX days-stale per dashboard and `max_by` to name the driving source. Dashboard names come from
# MAGIC `bi_asset_names`. AIBI-only (Genie agents have no dashboard lineage).

# COMMAND ----------

try:
    dash_fresh = spark.sql(f"""
        WITH lin AS (
          SELECT DISTINCT entity_id AS dashboard_id, lower(source_table_full_name) AS tbl
          FROM system.access.table_lineage
          WHERE entity_type = 'DASHBOARD_V3' AND source_table_full_name IS NOT NULL
            AND event_date >= current_date() - INTERVAL {LOOKBACK} DAYS
        ),
        tbl_fresh AS (
          SELECT lower(concat_ws('.', table_catalog, table_schema, table_name)) AS tbl,
                 date_diff(current_date(), to_date(last_altered)) AS days_stale
          FROM system.information_schema.tables WHERE last_altered IS NOT NULL
        ),
        joined AS (
          SELECT l.dashboard_id, l.tbl, f.days_stale
          FROM lin l JOIN tbl_fresh f ON l.tbl = f.tbl
        )
        SELECT
          current_date()                              AS snapshot_date,
          'AIBI'                                       AS product,
          j.dashboard_id                               AS asset_id,
          coalesce(n.asset_name, j.dashboard_id)       AS asset_name,   -- dashboard NAME (label)
          MAX(j.days_stale)                            AS days_since_last_refresh,
          MAX_BY(j.tbl, j.days_stale)                  AS stalest_source, -- driving source (tooltip)
          COUNT(*)                                     AS n_sources,
          current_timestamp()                          AS _loaded_at
        FROM joined j
        LEFT JOIN {FQ}.bi_asset_names n ON j.dashboard_id = n.asset_id
        GROUP BY 1,2,3,4
    """)
    (dash_fresh.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{FQ}.bi_dashboard_freshness"))
    print(f"Wrote {FQ}.bi_dashboard_freshness — {dash_fresh.count()} dashboards")
except Exception as e:
    print(f"!! Dashboard-freshness step skipped: {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build per-source-table staleness (Quality tab — "Data Sources by Staleness")
# MAGIC The left "Data Sources by Staleness" chart is scoped to the SAME population as the right
# MAGIC "Top N Stalest Dashboards" chart: the source tables reached through **AIBI dashboard lineage**
# MAGIC (`system.access.table_lineage`, DASHBOARD_V3). For each such table we take its **`last_altered`**
# MAGIC from `system.information_schema.tables` — when the table's data/metadata was last modified by ANY
# MAGIC writer (pipeline run, job, manual MERGE/INSERT, ALTER). This differs from the pipeline-run time
# MAGIC in `bi_freshness_snapshot` (which is `max(change_time)` per Lakeflow pipeline, pipelines only).
# MAGIC Using `last_altered` over the lineage sources makes the two Quality-tab charts consistent: the
# MAGIC dashboard-freshness driver on the right is, by construction, one of the bars on the left.
# MAGIC Grain: ONE row per source table (GROUP BY the fully-qualified name) so the bar chart never sums
# MAGIC duplicate-named rows (pass-3 bug). `bi_source_freshness` supersedes `bi_freshness_snapshot` as
# MAGIC the left chart's source; the pipeline snapshot table is still written (freshness KPIs use it).

# COMMAND ----------

try:
    src_fresh = spark.sql(f"""
        WITH lin AS (
          SELECT DISTINCT lower(source_table_full_name) AS tbl
          FROM system.access.table_lineage
          WHERE entity_type = 'DASHBOARD_V3' AND source_table_full_name IS NOT NULL
            AND event_date >= current_date() - INTERVAL {LOOKBACK} DAYS
        ),
        tbl_fresh AS (
          SELECT lower(concat_ws('.', table_catalog, table_schema, table_name)) AS tbl,
                 max(last_altered) AS last_altered
          FROM system.information_schema.tables
          WHERE last_altered IS NOT NULL
          GROUP BY 1                                   -- one row per FQ table name (dedupe)
        )
        SELECT
          current_date()                                          AS snapshot_date,
          l.tbl                                                   AS asset_name,   -- FQ source table (bar label)
          date_diff(current_date(), to_date(f.last_altered))      AS days_since_last_refresh,
          f.last_altered                                          AS last_refresh_time,
          current_timestamp()                                     AS _loaded_at
        FROM lin l JOIN tbl_fresh f ON l.tbl = f.tbl               -- INNER JOIN: only sources we can date
    """)
    (src_fresh.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{FQ}.bi_source_freshness"))
    print(f"Wrote {FQ}.bi_source_freshness — {src_fresh.count()} lineage source tables")
except Exception as e:
    print(f"!! Source-freshness step skipped: {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build 30-day engagement status per asset (User Engagement tab)
# MAGIC Every KNOWN asset (from `bi_asset_names` — all dashboards + genie spaces) labelled with whether
# MAGIC it saw ANY engagement (interactions or queries) in the last 30 days. Powers:
# MAGIC   * "no engagement in past month" lists (agents + dashboards that exist but went cold), and
# MAGIC   * cross-referenced with `bi_dashboard_freshness`, "cold dashboards still being refreshed"
# MAGIC     (wasted-refresh signal) — a dashboard with 0 engagement whose sources refreshed recently.
# MAGIC Grain: one row per asset. `last_activity_date` is the most recent active day (NULL if never).

# COMMAND ----------

try:
    engagement_30d = spark.sql(f"""
        WITH act AS (
          SELECT asset_id,
                 MAX(usage_date)                                  AS last_activity_date,
                 SUM(CASE WHEN usage_date >= current_date() - 30
                          THEN interactions + query_count ELSE 0 END) AS activity_30d
          FROM {FQ}.bi_usage_fact
          WHERE asset_id IS NOT NULL
          GROUP BY asset_id
        )
        SELECT
          n.asset_id,
          n.asset_name,
          n.asset_kind,                                          -- 'dashboard' | 'genie_space'
          coalesce(a.activity_30d, 0)                            AS activity_30d,
          a.last_activity_date,
          date_diff(current_date(), a.last_activity_date)        AS days_since_last_activity,
          CASE WHEN coalesce(a.activity_30d, 0) = 0 THEN true ELSE false END AS no_engagement_30d,
          current_timestamp()                                    AS _loaded_at
        FROM {FQ}.bi_asset_names n
        LEFT JOIN act a ON n.asset_id = a.asset_id
    """)
    (engagement_30d.write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{FQ}.bi_asset_engagement_30d"))
    print(f"Wrote {FQ}.bi_asset_engagement_30d — {engagement_30d.count()} assets")
except Exception as e:
    print(f"!! Engagement-30d step skipped: {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governed-tag exploded view (LEGACY — build disabled)
# MAGIC `custom_tags` is a map; this once materialised an exploded helper. The dashboard no longer uses
# MAGIC `bi_usage_fact_tags` (the metric view exposes tags as scalar map-access dimensions instead), so
# MAGIC building it every run was pure overhead — disabled in pass 3 (data-source optimisation). Kept
# MAGIC here, commented, in case a future tag-explode consumer needs it.

# COMMAND ----------

# DISABLED (pass 3): unused by the dashboard; skip to save a full fact re-scan + write each run.
# spark.sql(f"""
# CREATE OR REPLACE TABLE {FQ}.bi_usage_fact_tags AS
# SELECT f.*, t.key AS tag_key, t.value AS tag_value
# FROM {FQ}.bi_usage_fact f
# LATERAL VIEW OUTER explode(f.governed_tags) t AS key, value
# """)
# print("Wrote bi_usage_fact_tags")
print("Skipped bi_usage_fact_tags (legacy, unused by dashboard)")
