# BI Monitoring Suite — Genie Agents + AIBI

A self-serve monitoring suite for **BI-related usage** across **Genie Agents** and **AIBI**
(dashboards). It tracks adoption, cost, quality and freshness of your Genie/AIBI assets, sliced by
time (day/week/month/quarter/year) and by account / workspace / governed tag / owner / individual
asset.

> **Deployment target:** set your workspace `host` in `databricks.yml` (e.g.
> `https://<your-azure-workspace>.azuredatabricks.net`).
> Runs identically on **AWS and Azure** — see [AWS vs Azure](#aws-vs-azure).

---

## The six components

| # | Component | Path | What it does |
|---|---|---|---|
| 1 | **README** | `README.md` | This file — deployment, usage, permissions. |
| 2 | **Base-query notebook** | `src/01_base_metrics.py` | Aggregates system tables → lowest-grain, PII-anonymised metrics. |
| 3 | **Pipeline (job)** | `databricks.yml` | Serverless job running the notebook **twice daily (00:00 & 12:00 UTC)**. |
| 4 | **Output tables** | `bi_monitoring_suite.monitoring_assets.*` | `bi_usage_fact` (+ `_tags`), `bi_freshness_snapshot`. |
| 5 | **Metric view** | `metric_view/bi_usage_metric_view.yaml` | UC metric view — measures/dimensions for correct roll-ups. |
| 6 | **AIBI dashboard** | `dashboard/build_dashboard.py` → `.lvdash.json` | Overview + Quality + Engagement + Costs tabs. |

### Why two tables instead of one
The spec asked for a single lowest-grain table. Metrics like **freshness** are *non-additive* and
live at a different grain (per-asset-per-day, not per-interaction), so folding them into the usage
fact would corrupt roll-up math. The suite therefore has **one additive usage fact**
(`bi_usage_fact`, plus a tag-exploded `bi_usage_fact_tags` for filtering) and **one freshness
snapshot** (`bi_freshness_snapshot`). The metric view sits over the additive fact. This is a
deliberate, correctness-driven split.

---

## Metrics

| Metric | Where | Notes |
|---|---|---|
| # unique users | fact | `COUNT(DISTINCT user_hash)` — hashed, still countable |
| # total interactions / queries | fact | interactions (audit) + query_count (query history) |
| Cost total / per user / per query | fact | USD via `list_prices`; DBUs too |
| Cost by surface | fact | `GENIE_AGENTS` vs `SQL_WAREHOUSE` (Genie Code/One appear if present) |
| Genie ratings % (+/−) | fact | from audit `THUMBS_UP/DOWN_COMMENT` |
| Avg freshness (days since refresh) | freshness table | non-additive; separate table |
| Avg query completion time | fact | `total_duration_ms` — **substitutes** dashboard load time |

**Dropped (agreed):** Ask-Genie follow-up counts (nice-to-have, not reliably in system tables);
dashboard render time (UI-only) — substituted by query completion time.

---

## Required permissions (ACLs)

The **run-as identity** of the pipeline needs the grants below. The notebook repeats them in its
first cell. **Schema grants on `system.*` require an _account admin_** — a *workspace* admin is
**not** sufficient (verified on the target workspace: `is_account_group_member('admins')` returns
`false` for a workspace admin, and the grant fails with `User is not an account admin`).

> **Granting a non-admin access to the system tables:** a non-admin cannot grant themselves
> `system.*` access — an **account admin must run block (a) with the non-admin user (or a UC group
> they belong to) as the grantee**. That is the *only* privilege an admin needs to hand over for
> someone to query the right system tables directly; substitute the person/group for `<run_as>` in
> block (a). The same five schema grants (`billing`, `access`, `query`, `lakeflow`, `compute`) plus
> `USE CATALOG ON CATALOG system` are exactly the read surface this suite touches — nothing broader
> is required. The account admin must also have **enabled those system schemas on the metastore**
> (account console → Metastore → System schemas) or the grants have nothing to point at.

```sql
-- (a) READ system tables — ACCOUNT ADMIN ONLY. Grantee = pipeline run-as identity.
GRANT USE CATALOG ON CATALOG system                TO `<run_as>`;   -- may already be held
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing  TO `<run_as>`;   -- costs, DBUs, genie.surface
GRANT USE SCHEMA, SELECT ON SCHEMA system.access   TO `<run_as>`;   -- users, interactions, ratings
GRANT USE SCHEMA, SELECT ON SCHEMA system.query    TO `<run_as>`;   -- query completion times
GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow TO `<run_as>`;   -- freshness
GRANT USE SCHEMA, SELECT ON SCHEMA system.compute  TO `<run_as>`;   -- warehouse names (asset names)

-- (b) WRITE the output — workspace admin / catalog owner is sufficient.
GRANT USE CATALOG ON CATALOG bi_monitoring_suite                 TO `<run_as>`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
  ON SCHEMA bi_monitoring_suite.monitoring_assets                TO `<run_as>`;
-- NB: the `bi_monitoring_suite.monitoring_assets` catalog/schema is a **dedicated home created by
-- this project** to hold its monitoring tables + metric view — it is not a pre-existing UC object.
-- If you'd rather store these in an EXISTING catalog/schema in your metastore, you can reuse it:
-- see the "Output location — reuse vs. create" note just below this block for what to change.

-- (c) DASHBOARD readers — REQUIRED for anyone to open the dashboard. Without these, the tables and
--     the SQL warehouse are locked and the dashboard fails to load for that user. Grantee = the
--     person/group who should see the dashboard (`<readers>`). By default only the deployer (owner)
--     has access; substitute a real group (e.g. `account users`, or a named UC group) to share.
GRANT USE CATALOG ON CATALOG bi_monitoring_suite                                    TO `<readers>`;
GRANT USE SCHEMA  ON SCHEMA  bi_monitoring_suite.monitoring_assets                  TO `<readers>`;
-- Governed source the dashboard queries (metric view) + the tables datasets read directly:
GRANT SELECT ON VIEW  bi_monitoring_suite.monitoring_assets.bi_usage_metrics        TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_usage_fact           TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_query_cost           TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_asset_cost           TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_asset_names          TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_freshness_snapshot   TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_source_composition   TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_dashboard_freshness  TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_asset_engagement_30d TO `<readers>`;
GRANT SELECT ON TABLE bi_monitoring_suite.monitoring_assets.bi_usage_fact_tags      TO `<readers>`;  -- legacy; dashboard no longer uses it
-- The SQL warehouse the published dashboard runs on — grant via UI (Warehouse → Permissions) or:
--   databricks warehouses set-permissions <warehouse_id> \
--     --json '{"access_control_list":[{"group_name":"<readers>","permission_level":"CAN_USE"}]}'
-- Shortcut for the whole schema instead of table-by-table (covers future tables too):
--   GRANT USE CATALOG ON CATALOG bi_monitoring_suite TO `<readers>`;
--   GRANT USE SCHEMA, SELECT ON SCHEMA bi_monitoring_suite.monitoring_assets TO `<readers>`;
```

> **Access default:** as deployed, only the run-as/owner identity can read these assets. Reader
> grants above are a deliberate, separate step — decide the audience, then run them (or the schema
> shortcut) and grant `CAN USE` on the warehouse. Nothing is shared until you do.

### Output location — reuse an existing catalog/schema, or create the default

The `bi_monitoring_suite` catalog and `monitoring_assets` schema were **created specifically to house
this suite's tables and metric view** — they are not assumed to pre-exist in your metastore.

- **Reuse your own:** if you already have a catalog/schema where you'd like these monitoring tables
  to live, just point the suite at it. Change the output location in **two places**:
  1. `databricks.yml` → `variables.target_catalog.default` and `variables.target_schema.default`
     (drives the notebook/job — these are passed to the notebook as `target_catalog` / `target_schema`
     widgets, so no notebook edit is needed).
  2. `dashboard/build_dashboard.py` → the `CAT` and `SCH` constants at the top of the file (drives
     every dashboard dataset query + the metric-view reference).
  Also substitute your catalog/schema name into the block (b)/(c) grants above and into the
  `CREATE OR REPLACE VIEW <catalog>.<schema>.bi_usage_metrics …` metric-view statement (Step 4). The
  run-as identity needs `USE CATALOG` + `USE SCHEMA, CREATE TABLE, MODIFY, SELECT` on that existing
  schema (block **b**) instead of on the default.
- **Use the default but haven't created it yet — is there a DDL?** You don't need to write one. The
  notebook **creates the catalog and schema itself on the first run** —
  `src/01_base_metrics.py` runs `CREATE CATALOG IF NOT EXISTS` / `CREATE SCHEMA IF NOT EXISTS` before
  its first write (idempotent no-ops thereafter). The only prerequisite is that the run-as identity
  holds **`CREATE CATALOG` on the metastore** (a metastore admin grants this, or pre-creates the
  catalog for you). If you reuse an existing catalog/schema, the `IF NOT EXISTS` calls are harmless
  no-ops. Standalone equivalent if you'd rather create it by hand first:
  ```sql
  CREATE CATALOG IF NOT EXISTS bi_monitoring_suite;
  CREATE SCHEMA  IF NOT EXISTS bi_monitoring_suite.monitoring_assets;
  ```

**Exact objects read (all read-only except the outputs):** `system.billing.usage`,
`system.billing.list_prices`, `system.access.audit`, `system.query.history`,
`system.lakeflow.pipelines`.

---

## Deploying in YOUR environment (start here)

This repo is currently configured for one specific Azure workspace. To stand it up in a **different
workspace / account / cloud**, work through the four stages below in order. Nothing here is
cloud-specific in syntax — only the values you edit in step 1 differ between AWS/Azure/GCP.

### Step 1 — Update configuration (what you MUST change)

| Where | Setting | Change to |
|---|---|---|
| `databricks.yml` → `targets.<cloud>.workspace.host` | workspace URL | your workspace host (e.g. `https://adb-….azuredatabricks.net` or `https://….cloud.databricks.com`) |
| `databricks.yml` → `targets.<cloud>.run_as.user_name` | run-as identity | the user or **service principal** the job runs as (a service principal is recommended for prod) |
| `databricks.yml` → `variables.warehouse_id.default` | SQL warehouse | a warehouse id in your workspace (dashboard + metric-view queries run here) |
| `databricks.yml` → `variables.target_catalog` / `target_schema` | output location | the UC catalog/schema you want the tables + metric view written to (default `bi_monitoring_suite.monitoring_assets`) |
| `databricks.yml` → job `base_parameters.anonymize_pii` | PII toggle | `"true"` to store salted-hash user identities instead of real emails (default `"false"`) |
| CLI profile | auth | `databricks auth login --host <your-host> --profile <p>` — use that `--profile <p>` in every command below |

Everything else (the notebook SQL, the metric-view YAML, the dashboard script) is portable as-is.
The `aws` target in `databricks.yml` is a ready example — uncomment it and set its `host`.

### Step 2 — Prerequisites a human/admin must do first
1. **Account admin grants the run-as identity SELECT on the `system.*` schemas** (see "Required
   permissions", block **a**). Schema grants on `system.*` require an *account* admin — a workspace
   admin is not enough. Without these the job fails on the first read.
2. **Enable the `system` schemas** (`billing`, `access`, `query`, `lakeflow`, `compute`) on the
   metastore (account console → Metastore → System schemas). Account-admin/UI only.
3. **A running SQL warehouse** whose id you put in `databricks.yml` (step 1).
4. The run-as identity must be able to **list Lakeview dashboards and Genie spaces** (drives
   name enrichment; degrades to id-only names if not).

### Step 3 — Deploy the DAB (bundle) — CLI or UI

**CLI (recommended):**
```bash
databricks bundle validate -t <cloud> --profile <p>     # <cloud> = azure | aws
databricks bundle deploy   -t <cloud> --profile <p>     # uploads notebook + creates the job
databricks bundle run bi_monitoring_refresh -t <cloud> --profile <p>   # first run → populates all tables
```
> On some machines `bundle deploy` needs a local terraform to avoid an `openpgp: key expired` error:
> `DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform DATABRICKS_TF_VERSION=1.13.1 databricks bundle deploy …`

**Via the Databricks UI** (if you prefer not to use the CLI): open your workspace →
**Workspace → Users → your dir**, use **Git folders** to clone this repo, then
**Deploy** the bundle from the DABs UI. Databricks docs:
<https://docs.databricks.com/aws/en/dev-tools/bundles/> (Asset Bundles) and
<https://docs.databricks.com/aws/en/dev-tools/bundles/work-tasks> (deploy/run a bundle).
The UI path creates the same job resource as the CLI.

### Step 4 — Metric view + dashboard (after the first job run)
The metric view depends on columns the job creates, so run it AFTER step 3's first run.
```bash
# Metric view — recreate from the YAML spec (metric_view/bi_usage_metric_view.yaml):
databricks api post /api/2.0/sql/statements --profile <p> --json '{
  "warehouse_id":"<wh>",
  "statement":"CREATE OR REPLACE VIEW <catalog>.<schema>.bi_usage_metrics WITH METRICS LANGUAGE YAML AS $$ <paste YAML body from version: onward> $$"}'

# Dashboard — generate the JSON and create (first time) or PATCH+publish (updates, keeps the URL):
python3 dashboard/build_dashboard.py                    # regenerates dashboard/bi_monitoring.lvdash.json
#   NB: edit the CAT/SCH constants at the top of build_dashboard.py if you changed catalog/schema.
databricks api post /api/2.0/lakeview/dashboards --profile <p> --json "{
  \"display_name\":\"BI Monitoring — Genie + AIBI\",
  \"warehouse_id\":\"<wh>\",
  \"parent_path\":\"/Users/<you>\",
  \"serialized_dashboard\":\"$(python3 -c 'import json;print(json.dumps(open("dashboard/bi_monitoring.lvdash.json").read()))')\"}"
```

### Step 5 — Validate the deployment
- **Job ran clean:** `databricks jobs list-runs --job-id <id> --profile <p>` → latest `SUCCESS`.
- **Tables populated** (in `<catalog>.<schema>`): `bi_usage_fact`, `bi_query_cost`, `bi_asset_cost`,
  `bi_asset_names`, `bi_freshness_snapshot`, `bi_dashboard_freshness`, `bi_asset_engagement_30d`,
  `bi_source_composition`, and the `bi_usage_metrics` view. Quick check:
  `SELECT count(*) FROM <catalog>.<schema>.bi_usage_fact;` (expect > 0, `max(usage_date)` = today/yesterday).
- **Data quality:** 0 negative durations, 0 null-asset AIBI rows (the notebook enforces this).
- **Dashboard loads** and its 4 tabs render; the global filters recompute; Top-N dropdown changes N.

### Where to find everything the deployment creates
| Asset | Location in the workspace |
|---|---|
| The job | **Workflows → Jobs → "[BI Monitoring] Genie + AIBI metrics refresh"** (twice-daily 00:00/12:00 UTC) |
| The notebook | `/Workspace/Users/<run-as>/.bundle/bi-monitoring-suite/<target>/files/src/01_base_metrics.py` |
| Tables + metric view | **Catalog → `<catalog>` → `<schema>`** (8 tables + `bi_usage_metrics` view) |
| The dashboard | **Dashboards** (AI/BI), named "BI Monitoring — Genie + AIBI" |

### Grants — who needs what
Reader grants are a **deliberate, separate step**; as deployed only the run-as/owner can see anything.
Run the exact SQL in "Required permissions → (c)" for whichever audience applies:

- **(a) Maintainers / developers** of these assets (can edit the notebook/job, rebuild tables,
  edit the dashboard): `USE CATALOG` + `USE SCHEMA` + `SELECT, MODIFY` on the schema (or table-level
  `MODIFY`), **CAN MANAGE** on the job (Workflows → job → Permissions), **CAN EDIT** on the dashboard,
  and `CAN USE` on the warehouse. They also need the `system.*` read grants (block **a**) if they'll
  re-run the pipeline.
- **(b) Consumers** (view the dashboard only): `USE CATALOG` + `USE SCHEMA` + **`SELECT`** on the
  output tables + the metric view (block **c**), **CAN VIEW** on the published dashboard (Dashboard →
  Share), and **`CAN USE`** on the SQL warehouse. No `system.*` grants needed — consumers read only
  the curated outputs, never the system tables.

Grant to a **UC group** (e.g. `bi-monitoring-readers`) rather than individuals so membership is
managed in one place.

---

## Known data gaps

- **Asset names:** warehouse names (AIBI cost assets) resolve from `system.compute.warehouses`.
  Genie space and AIBI dashboard names are NOT in any system table, so the pipeline resolves them
  via the Lakeview + Genie **list APIs** into the `bi_asset_names` lookup, joined into
  `bi_usage_fact` and `bi_query_cost` (`asset_name`). Coverage is partial by nature — ~65–75% of
  activity resolves; the rest are conversation IDs, deleted/cross-workspace assets, and fall back to
  the raw id. (~60% of *stale* dashboards resolve — the unresolved remainder appear in lineage but
  aren't returned by the list API, typically deleted or outside the run-as's visibility; per-id GET
  fallback was judged not worth ~2k calls/run.) Enrichment is best-effort: any API/permission failure
  degrades gracefully to id-only names (never fails the run).
- **User display names:** owners/users are shown as **"First Last"** parsed from the email
  local-part (`first.last@` → "First Last"); non-dot emails show the local-part, service-principal
  UUIDs show "Service principal (·<last4>)", and the raw email/id is available on hover. The raw
  identity (`user_hash`) is retained for filtering/joins. When `anonymize_pii=true`, the identity is
  a hash and no name can be parsed (display falls back to the hash) — intended for anonymised mode.
- **Dashboard freshness (`bi_dashboard_freshness`):** a dashboard has no refresh of its own, so its
  staleness = the staleness of its **stalest source table**, derived by joining
  `system.access.table_lineage` (DASHBOARD_V3 → source tables) to
  `system.information_schema.tables.last_altered`. AIBI-only.
- **No-engagement / cold-but-refreshing:** the "still refreshing" signal is source-table-based
  (via the dashboard-freshness join above); AIBI dashboard *schedule* metadata isn't cleanly in
  system tables, so "refreshing" means the dashboard's source tables were altered ≤30 days ago.
- **Freshness = last-modified proxy:** `system.lakeflow.pipelines.change_time`, not a true
  last-refresh. Written via idempotent MERGE on `(snapshot_date, asset_id)` so twice-daily reruns
  don't duplicate rows.
- **AIBI interactions = dashboard queries** (`query_count`), by definition; Genie interactions =
  audit messages. The metric view's `Total Interactions` measure switches on product.
- **AIBI query scope (corrected 2026-07-22):** AIBI counts ONLY queries actually sourced from a
  dashboard (`query_source.dashboard_id`/`legacy_dashboard_id`). Generic warehouse SQL — ad-hoc SQL
  editor, jobs, notebooks, API (~89% of non-Genie query volume) — is EXCLUDED, not mislabelled AIBI.
- **Cost attribution boundary:** `system.billing.usage` bills per **warehouse** (`warehouse_id`), with
  NO query/dashboard attribution. So "Total Cost (SQL)" is whole-warehouse cost — it covers dashboard
  SQL *and* any ad-hoc/jobs SQL on the same warehouse. It's an upper bound for AIBI cost and cannot
  be split further from system tables. (Interactions/queries CAN be scoped to dashboards; cost cannot.)
- **Font / branding:** the dashboard uses the default AI/BI theme font, not a corporate brand font —
  Lakeview dashboards don't support custom font embedding, so there's no brand-font option to set.

## Dashboard layout

- **Overview** — account/workspace KPIs (users, interactions, queries, cost, DBUs, positive-rating
  %) + weekly cost/users trends + top workspaces/assets.
- **Quality Monitoring** — avg query completion time, avg freshness, source-%-metric-views;
  **Data Sources by Staleness** (per-source distribution) + **Top-N Stalest Dashboards** (dashboard
  names ranked by their stalest source; the driving source table is on hover, not the axis).
- **User Engagement** — users, interactions, thumbs up/down, positive/negative rating %, top users
  (shown as **First Last**, email/SP-id on hover), power users; plus **Disengagement** — counts +
  lists of agents/dashboards with **no engagement in the past 30 days**, and **cold-but-refreshing**
  dashboards (0 engagement yet sources still refreshing — a wasted-refresh signal).
- **Costs Breakdown** — total / per-user / per-query cost, cost by surface (agentic vs SQL
  warehouse), DBUs by product, top users/assets by cost. Every average tile states its **aggregation
  window** (e.g. per-day vs whole-range); each tab has a **Metric definitions** panel with formulas.

Global filters (all pages): **date range, Workspace ID, Owner, Top N (charts).** Workspace/Owner are
injected as parameters into every dataset's WHERE (empty = All); the **Owner filter lists display
names** (e.g. "Aaron Chong"), matching on the parsed name, not raw emails/ids. Top N caps every
Top-N chart at once (auto-bound to all `:top_n` datasets). Time grains (day/week/month/quarter/year)
are dimensions in the metric view.

**Ask Genie (NL follow-ups):** the published dashboard exposes Databricks' built-in **Ask Genie**
assistant (top-right), scoped to the dashboard's own datasets — no separate Genie space to configure.
It answers natural-language follow-ups ("who are the power users?", "how has weekly cost trended?")
with tables/charts. Enabled by default on the published dashboard.

**Governed tags, not domains:** mapping assets → domains isn't currently collectable, so filtering
is by **governed tag** (`custom_tags`) instead — as agreed.

---

## AWS vs Azure

**There are no syntax or setup differences.** The suite is a single codebase for both clouds:

- All data comes from `system.*` tables, whose schema is **cloud-independent**.
- `list_prices` is joined on `u.cloud = lp.cloud`, so pricing self-selects the right cloud — the
  `cloud` value flows through the *data*, never the *code*.
- The job is **serverless** (no cloud-specific cluster/instance config).
- The **only** per-environment value is the workspace `host` (and optionally `warehouse_id`),
  set in the `targets:` block of `databricks.yml` — not in any query or notebook logic.

If you ever find a divergence, document it here. As built and verified, there is none.

---

## Verification / testing

**Done by build (against live workspace):**
- ✅ Recon: cloud, identity, target catalog writable, system-table access boundary confirmed.
- ✅ Dashboard JSON generates and is valid; bundle validates.

**To run after the account-admin grants land (repeatable checks):**
1. In the notebook, uncomment `thin_slice_check()` and run it — confirms all 4 read paths + the
   `genie.surface` and `comment_type` shapes + freshness source. "Good" = non-zero counts and the
   expected distinct values (`GENIE_AGENTS`, `THUMBS_UP_COMMENT`/`THUMBS_DOWN_COMMENT`).
2. `databricks bundle run bi_monitoring_refresh` → then
   `SELECT count(*) FROM bi_monitoring_suite.monitoring_assets.bi_usage_fact` returns > 0.
3. Open the dashboard; each widget renders (no "Invalid widget definition"). If a widget is empty,
   check the corresponding column exists in `bi_usage_fact_tags`.

**Manual-only (cannot be scripted):**
- The twice-daily schedule actually firing at 00:00 / 12:00 — verify in the job run history next day.

---

## Implementation notes (validated against a live workspace)

The suite has been built and run end-to-end (pipeline → metric view → published dashboard, with
every widget validated against real data). The following are non-obvious facts confirmed during
that build — useful if you extend or debug the queries:
- Genie audit **services** are `aibiGenie` / `genieChat` (not `genie` / `dashboards`).
- **Interactions** = `createConversationMessage` / `genieStartConversationMessage` /
  `createGenieChatResponse`.
- **Ratings** = `updateConversationMessageFeedback.feedback_rating` (+ genieChat nested variant);
  `comment_type` is always NULL and must not be used.
- **Freshness** proxy = `system.lakeflow.pipelines.change_time` (last *modified*; no true
  last-refresh column exists).
- **Cost fan-out** — headline widgets use unduplicated `bi_usage_fact`; the tag-exploded
  `bi_usage_fact_tags` is used only for governed-tag filtering/breakdown.
- **Serverless** — the PII salt is baked into SQL as a literal (spark-conf `${...}` substitution
  is unavailable on serverless).
