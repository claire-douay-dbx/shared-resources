#!/usr/bin/env python3
"""
Generates the AIBI dashboard serialized JSON for the BI Monitoring Suite.

SOURCE OF TRUTH: every dataset queries the governed metric view
  bi_monitoring_suite.monitoring_assets.bi_usage_metrics
via MEASURE(...) in the DATASET SQL (grouped by the dimension a widget needs). No metric logic
lives in the dashboard — it lives once, in the metric view. This is the architecture the spec
asked for and matches Databricks' guidance (business logic in the governed layer, dashboard just
references it; validate dynamic charts against metric-view outputs).

Consumption rules for a metric view (verified live 2026-07-21):
  * Measures MUST be wrapped in MEASURE(...) — done in the dataset SQL, aliased to a plain column.
  * Widgets then reference the plain alias with NO further aggregation (`col`, disaggregated:true) —
    you cannot SUM a distinct-count or a percentage on the widget side.
  * Counters use an UNGROUPED dataset (one grand-total row); charts GROUP BY the needed dimension.
  * Filters recompute measures correctly (non-additive measures stay correct under filtering).

Pages: Overview · Cost Monitoring · Quality Monitoring · User Engagement (+ global Filters page).
Freshness comes from bi_freshness_snapshot (not a metric-view measure — it's a point-in-time
per-asset snapshot, not an aggregatable event metric).

Cloud portability: AIBI dashboards are identical on AWS and Azure; only warehouse_id/host differ.
"""
import json
import os

CAT, SCH = "bi_monitoring_suite", "monitoring_assets"
MV = f"{CAT}.{SCH}.bi_usage_metrics"          # governed metric view — the single source
FRESH = f"{CAT}.{SCH}.bi_freshness_snapshot"  # per-PIPELINE last-run snapshot (freshness KPIs)
SRCFRESH = f"{CAT}.{SCH}.bi_source_freshness"  # per-SOURCE-TABLE last_altered, AIBI-lineage scoped (Quality left chart)
QCOST = f"{CAT}.{SCH}.bi_query_cost"          # per-statement ESTIMATED cost (drill-down)
SRCCOMP = f"{CAT}.{SCH}.bi_source_composition"  # per-dashboard % metric-view sources (Quality tab)
ASSETCOST = f"{CAT}.{SCH}.bi_asset_cost"      # per-asset total cost (agentic+SQL) + activity flag
DASHFRESH = f"{CAT}.{SCH}.bi_dashboard_freshness"  # per-DASHBOARD staleness via lineage (Quality tab)
ENGAGE30 = f"{CAT}.{SCH}.bi_asset_engagement_30d"  # per-asset 30d engagement status (Engagement tab)

# ---------------------------------------------------------------------------
# esure BRANDING (from the style guide). esure blue tiles + palette. Lakeview can't set arbitrary
# fonts (renders in the workspace default) — colours + counter styling are the themeable surface.
# ---------------------------------------------------------------------------
ESURE_BLUE   = "#3B5FE3"   # primary tile / accent (from the esure logo + square mark)
ESURE_DARK   = "#1F2D6E"   # deep blue for secondary series
ESURE_ACCENT = "#FF7A00"   # orange accent seen in the app imagery (secondary series/highlight)
ESURE_PALETTE = [ESURE_BLUE, ESURE_ACCENT, ESURE_DARK, "#8BCAE7", "#00A972", "#AB4057"]
KPI_BG = ESURE_BLUE        # KPI counter tile background per the wireframe (solid esure blue)

# ---------------------------------------------------------------------------
# DATASETS — all metric datasets read the metric view with MEASURE() in SQL.
# Each "grouped" dataset is grouped by exactly the dimension(s) its widgets need, so widgets do
# NO aggregation. Counters use the ungrouped grand-total dataset (single row).
# ---------------------------------------------------------------------------
MEASURE_SELECT = (
    "  MEASURE(`Unique Users`) AS unique_users, "
    "  MEASURE(`Total Interactions`) AS total_interactions, "
    "  MEASURE(`Total Queries`) AS total_queries, "
    "  MEASURE(`Total Cost (USD)`) AS total_cost, "
    "  MEASURE(`Total DBUs`) AS total_dbus, "
    "  MEASURE(`Cost per User (USD)`) AS cost_per_user, "
    "  MEASURE(`Cost per Query (USD)`) AS cost_per_query, "
    "  MEASURE(`Avg Cost per User per Day (USD)`) AS avg_cost_user_day, "
    "  MEASURE(`Avg Cost per Asset (USD)`) AS avg_cost_asset, "
    "  MEASURE(`Thumbs Up`) AS thumbs_up, "
    "  MEASURE(`Thumbs Down`) AS thumbs_down, "
    "  MEASURE(`Positive Rating %`) AS positive_rating_pct, "
    "  MEASURE(`Negative Rating %`) AS negative_rating_pct, "
    "  MEASURE(`Avg Query Completion (ms)`) AS avg_query_ms "
)

# Global Workspace / Owner / Date filters are implemented as PARAMETERS injected into every dataset
# (native column-filters can't reach columns a pre-aggregated dataset doesn't SELECT — e.g. a KPI
# grand-total row has no date column, so a native date-range picker CANNOT filter it; and native
# filters would force wrong widget-level SUMs on distinct-counts/percentages). Sentinels: empty string
# = "All" for ws/owner; wide date defaults (now-3650d..now) = "all history" for the date range. The
# metric view recomputes correctly for a dimension not in the SELECT (verified live).
FLT_PARAMS = [
    {"displayName": "flt_ws", "keyword": "flt_ws", "dataType": "STRING",
     "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": ""}]}}},
    {"displayName": "flt_owner", "keyword": "flt_owner", "dataType": "STRING",
     "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": ""}]}}},
    # Date range: ONE range parameter driven by the date-range picker. A range picker emits a single
    # STRUCT<min,max> value, referenced in SQL as :flt_dates.min / :flt_dates.max (NOT two scalars —
    # binding two scalar params to one range picker passes the whole struct to each, a type mismatch).
    # Defaults span ~10y so "unset" = all data; the picker overrides when the user selects a range.
    {"displayName": "flt_dates", "keyword": "flt_dates", "dataType": "DATE",
     "defaultSelection": {"range": {"dataType": "DATE",
                                    "min": {"dataType": "DATE", "value": "now-3650d/d"},
                                    "max": {"dataType": "DATE", "value": "now/d"}}}},
]
def flt_where(extra=None):
    """WHERE clause applying the global Workspace/Owner/Date param filters (empty/wide = All), plus
    optional extra. For METRIC-VIEW datasets (dims `Workspace` / `Owner Name` / `Date`)."""
    parts = ["(:flt_ws = '' OR `Workspace` = :flt_ws)", "(:flt_owner = '' OR `Owner Name` = :flt_owner)",
             "`Date` >= :flt_dates.min", "`Date` <= :flt_dates.max"]
    if extra:
        parts.insert(0, extra)
    return "WHERE " + " AND ".join(parts) + " "

def flt_where_raw(extra=None, has_owner=True, alias="", has_date=True, date_col="usage_date"):
    """WHERE clause for RAW-TABLE datasets (columns `workspace_id` / `user_hash` / a date column).
    has_owner=False for tables without a user column (e.g. freshness). has_date=False for tables with
    no per-event date to range-filter (freshness snapshot, per-asset cost rollup) or where a fixed
    window is definitional (30-day active users). `alias` prefixes the columns (e.g. 'q')."""
    p = f"{alias}." if alias else ""
    parts = [f"(:flt_ws = '' OR {p}workspace_id = :flt_ws)"]
    if has_owner:
        # Owner filter matches the display NAME (not raw user_hash) so the dropdown shows names.
        parts.append(f"(:flt_owner = '' OR {p}user_display = :flt_owner)")
    if has_date:
        parts.append(f"{p}{date_col} >= :flt_dates.min")
        parts.append(f"{p}{date_col} <= :flt_dates.max")
    if extra:
        parts.insert(0, extra)
    return "WHERE " + " AND ".join(parts) + " "

# pre-built raw-table WHERE clauses used inside f-strings (avoids escaped quotes in f-string exprs)
_GENIE_RAW_WHERE = flt_where_raw("product='GENIE_AGENTS'")  # honors ws/owner/date (usage_date)
# freshness snapshot: workspace only, no user col, and no per-event date to range-filter (each row is
# a point-in-time staleness snapshot) → has_date=False.
_WS_ONLY_WHERE = flt_where_raw(has_owner=False, has_date=False)
# bi_asset_cost: workspace-only filter (per-asset rollup has workspace_id but no user/date col) + product
_ASSETCOST_GENIE_WHERE = flt_where_raw("product='GENIE_AGENTS'", has_owner=False, has_date=False)
_ASSETCOST_AIBI_WHERE = flt_where_raw("product='AIBI'", has_owner=False, has_date=False)

def grouped_ds(name, display, dims):
    """dims: list of (metric-view dim name, output alias). Groups the view by those dims.
    Carries the global Workspace/Owner filter params."""
    sel_dims = "".join(f"  `{d}` AS {a}, " for d, a in dims)
    grp = ", ".join(f"`{d}`" for d, a in dims)
    return {"name": name, "displayName": display, "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT ", sel_dims, MEASURE_SELECT, f"FROM {MV} ", flt_where(), f"GROUP BY {grp}"]}

def xf_topn_ds(name, product):
    """Cross-filter dataset restricted to the TOP-N assets by total est cost (:top_n), at
    (asset, user, query) grain. Feeds a clickable top-N asset bar + users bar + queries table that
    all share it — clicking an asset bar cross-filters the other two."""
    inner_extra = "product = '" + product + "' AND asset_id IS NOT NULL"
    outer_extra = "q.product = '" + product + "' AND q.asset_id IS NOT NULL"
    return {"name": name, "displayName": name,
        "parameters": [{"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
            "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}]
            + list(FLT_PARAMS),
        "queryLines": [
            "WITH ranked AS ( ",
            "  SELECT asset_id, sum(est_cost_usd) tot ",
            f"  FROM {QCOST} q {flt_where_raw(inner_extra, alias='q')}GROUP BY asset_id ",
            "  QUALIFY row_number() OVER (ORDER BY tot DESC) <= :top_n ) ",
            # asset_name (REST-API-resolved, falls back to id) is the display label; asset_id stays in
            # the grain so cross-filter clicks still key on the unambiguous id.
            "SELECT q.asset_id, q.asset_name, q.user_hash, q.user_display, q.statement_preview, sum(q.est_cost_usd) AS est_cost_usd ",
            f"FROM {QCOST} q JOIN ranked r ON q.asset_id = r.asset_id ",
            f"{flt_where_raw(outer_extra, alias='q')}",
            "GROUP BY q.asset_id, q.asset_name, q.user_hash, q.user_display, q.statement_preview ",
            # ORDER BY cost DESC at the DATASET level: the query-table widget paginates over the dataset
            # result and its client-side column sort only orders the CURRENT page, so without a
            # server-side order the first page showed arbitrary rows. Ordering here puts the highest-cost
            # rows on page 1 (the widget's DESC sortColumns then keeps them ordered within the page).
            "ORDER BY est_cost_usd DESC"]}

def power_users_ds(name, product):
    """Power users: interactions above mean + 1 std dev, for the given product (pure statistical cut).
    Carries global Workspace/Owner filter params. Capped at :top_n (bound to the global Top-N filter)
    so a busy workspace can't render an unbounded bar list — the statistical cut can still leave many
    users above the threshold, so we keep only the top :top_n of them by interactions."""
    prod_clause = "`Product` = '" + product + "'"
    return {"name": name, "displayName": name,
        "parameters": [{"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
            "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}]
            + list(FLT_PARAMS),
        "queryLines": [
            # Group by display name (label) + raw Owner email (tooltip/secondary). Two users could
            # share a display name → group by both so counts stay per-identity.
            "WITH u AS (SELECT `Owner Name` AS owner_name, `Owner` AS owner_email, "
            "                  MEASURE(`Total Interactions`) AS ints ",
            f"           FROM {MV} {flt_where(prod_clause)}GROUP BY `Owner Name`, `Owner`) ",
            "SELECT owner_name, owner_email, ints AS metric_value FROM u ",
            "WHERE ints >= (SELECT avg(ints) + stddev(ints) FROM u) ",
            "QUALIFY row_number() OVER (ORDER BY ints DESC) <= :top_n"]}

def topn_qcost_ds(name, product):
    """Top-N assets (dashboard/agent id) by ESTIMATED cost per user per day, from bi_query_cost.
    Capped at :top_n. Uses the per-asset grain so it aligns with the drill-down asset filter."""
    return {"name": name, "displayName": name,
        "parameters": [{"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
            "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}],
        "queryLines": [
            "SELECT asset_name, metric_value FROM ( ",
            "  SELECT asset_id AS asset_name, ",
            "    sum(est_cost_usd) / NULLIF(count(DISTINCT user_hash),0) / NULLIF(count(DISTINCT usage_date),0) AS metric_value ",
            f"  FROM {QCOST} WHERE product = '{product}' AND asset_id IS NOT NULL GROUP BY asset_id ",
            ") QUALIFY row_number() OVER (ORDER BY metric_value DESC) <= :top_n"]}

def drill_users_ds(name, product, sel_param):
    """Drill-down: top users by ESTIMATED spend for the selected asset (:sel_param), capped at :top_n.
    Reads per-statement bi_query_cost directly (not the metric view)."""
    return {"name": name, "displayName": name,
        "parameters": [
            {"displayName": sel_param, "keyword": sel_param, "dataType": "STRING",
             "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": ""}]}}},
            {"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
             "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}],
        "queryLines": [
            "SELECT user_hash, est_cost FROM ( ",
            "  SELECT user_hash, sum(est_cost_usd) AS est_cost ",
            f"  FROM {QCOST} WHERE product = '{product}' AND asset_id = :{sel_param} ",
            "  GROUP BY user_hash ",
            ") QUALIFY row_number() OVER (ORDER BY est_cost DESC) <= :top_n"]}

def drill_queries_ds(name, product, sel_param):
    """Drill-down: top queries by ESTIMATED cost for the selected asset (:sel_param), capped at :top_n."""
    return {"name": name, "displayName": name,
        "parameters": [
            {"displayName": sel_param, "keyword": sel_param, "dataType": "STRING",
             "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": ""}]}}},
            {"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
             "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}],
        "queryLines": [
            "SELECT statement_preview, est_cost FROM ( ",
            "  SELECT statement_preview, sum(est_cost_usd) AS est_cost ",
            f"  FROM {QCOST} WHERE product = '{product}' AND asset_id = :{sel_param} ",
            "  GROUP BY statement_preview ",
            ") QUALIFY row_number() OVER (ORDER BY est_cost DESC) <= :top_n"]}

def topn_ds(name, dim, dim_alias, measure, val_alias, where=None):
    """Top-N dataset: rank `dim` by `measure`, keep top :top_n rows. :top_n param defaults to 10.
    NB: metric views don't allow MEASURE() inside QUALIFY, so aggregate in an inner query first,
    then rank the plain result in an outer query. Carries global Workspace/Owner filter params."""
    return {"name": name, "displayName": name,
        "parameters": [{"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
            "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}]
            + list(FLT_PARAMS),
        "queryLines": [
            f"SELECT {dim_alias}, {val_alias} FROM ( ",
            f"  SELECT `{dim}` AS {dim_alias}, MEASURE(`{measure}`) AS {val_alias} ",
            f"  FROM {MV} {flt_where(where)}GROUP BY `{dim}` ",
            ") ",
            f"QUALIFY row_number() OVER (ORDER BY {val_alias} DESC) <= :top_n"]}

datasets = [
    # ungrouped grand total → counters
    {"name": "totals", "displayName": "Grand totals (metric view)", "parameters": list(FLT_PARAMS),
     "queryLines": ["SELECT ", MEASURE_SELECT.rstrip(", ") + " ", f"FROM {MV} ", flt_where()]},
    # by_week: `Week` = week-start calendar DATE (date_trunc WEEK). Plotted on a temporal axis so the
    # trend line is continuous across the calendar year (no yy-week# label — a per-point string in the
    # tooltip fragmented the line into disconnected dots; see the trend widgets below).
    {"name": "by_week", "displayName": "By week", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT `Week` AS week, `Product` AS product, ",
        MEASURE_SELECT, f"FROM {MV} ", flt_where(), "GROUP BY `Week`, `Product`"]},
    grouped_ds("by_month",   "By month",     [("Month", "month"), ("Product", "product")]),
    grouped_ds("by_product", "By product",   [("Product", "product")]),
    grouped_ds("by_surface", "By surface",   [("Surface", "surface")]),
    grouped_ds("by_workspace","By workspace",[("Workspace Name", "workspace")]),
    grouped_ds("by_asset",   "By asset",     [("Asset Name", "asset_name")]),
    grouped_ds("by_owner",   "By owner",     [("Owner Name", "owner")]),  # dropdown lists display NAMES
    grouped_ds("by_day",     "By day",       [("Day", "day"), ("Product", "product")]),
    # Product-scoped cost-by-workspace datasets so SQL-warehouse (~$2.1M) and Genie (~$300) each get
    # their OWN axis — a shared chart flattens Genie to the axis floor. WHERE filters in the view.
    {"name": "aibi_cost_by_ws", "displayName": "AIBI cost by workspace", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT `Workspace Name` AS workspace, MEASURE(`Total Cost (USD)`) AS total_cost ",
        f"FROM {MV} ", flt_where("`Product` = 'AIBI'"), "GROUP BY `Workspace Name`"]},
    {"name": "genie_cost_by_ws", "displayName": "Genie Agents cost by workspace", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT `Workspace Name` AS workspace, MEASURE(`Total Cost (USD)`) AS total_cost ",
        f"FROM {MV} ", flt_where("`Product` = 'GENIE_AGENTS'"), "GROUP BY `Workspace Name`"]},
    # Genie weekly cost = agentic DBU cost (fact) + allocated SQL cost (bi_query_cost). Two SEPARATE
    # cost sources (agent compute + the warehouse SQL the agent triggers), summed into one additive
    # `total_cost` line. agentic_cost + sql_cost carried alongside for the hover tooltip breakdown.
    {"name": "genie_cost_by_wk", "displayName": "Genie weekly cost (agentic + SQL)", "parameters": list(FLT_PARAMS), "queryLines": [
        "WITH agentic AS ( ",
        "  SELECT date_trunc('WEEK', usage_date) AS week, sum(cost_usd) AS agentic_cost ",
        f"  FROM {CAT}.{SCH}.bi_usage_fact {_GENIE_RAW_WHERE}GROUP BY 1 ), ",
        "sql_c AS ( ",
        "  SELECT date_trunc('WEEK', usage_date) AS week, sum(est_cost_usd) AS sql_cost ",
        f"  FROM {QCOST} {_GENIE_RAW_WHERE}GROUP BY 1 ) ",
        "SELECT coalesce(a.week, s.week) AS week, ",
        "  coalesce(agentic_cost,0) AS agentic_cost, coalesce(sql_cost,0) AS sql_cost, ",
        "  coalesce(agentic_cost,0) + coalesce(sql_cost,0) AS total_cost ",
        "FROM agentic a FULL OUTER JOIN sql_c s ON a.week = s.week"]},
    {"name": "aibi_cost_by_wk", "displayName": "AIBI weekly cost", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT `Week` AS week, ",
        "  MEASURE(`Total Cost (USD)`) AS total_cost ",
        f"FROM {MV} ", flt_where("`Product` = 'AIBI'"), "GROUP BY `Week`"]},
    # --- product-scoped grand totals for KPI tiles (genie-only / aibi-only) ---
    {"name": "totals_genie", "displayName": "Genie totals", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT ", MEASURE_SELECT.rstrip(", ") + " ", f"FROM {MV} ", flt_where("`Product` = 'GENIE_AGENTS'")]},
    {"name": "totals_aibi", "displayName": "AIBI totals", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT ", MEASURE_SELECT.rstrip(", ") + " ", f"FROM {MV} ", flt_where("`Product` = 'AIBI'")]},
    # --- Top-N datasets (n driven by the :top_n dashboard parameter, default 10) ---
    # Each ranks a dimension by a measure and keeps the top :top_n via QUALIFY row_number.
    topn_ds("topn_ws_users", "Workspace Name", "workspace", "Unique Users", "metric_value"),
    # `Asset IS NOT NULL` excludes the ~1.4k Genie rows with a NULL asset_id (they otherwise surface as
    # a spurious `null` bar, sometimes the largest) — see pass-3 bug #5.
    topn_ds("topn_genie_views", "Asset Name", "asset_name", "Total Interactions", "metric_value",
            where="`Product` = 'GENIE_AGENTS' AND `Asset` IS NOT NULL"),
    topn_ds("topn_aibi_views", "Asset Name", "asset_name", "Total Queries", "metric_value",
            where="`Product` = 'AIBI' AND `Asset` IS NOT NULL"),
    # freshness (own table, not the metric view)
    # "Data Sources by Staleness" — per-SOURCE-TABLE staleness by `last_altered`, scoped to the tables
    # reached through AIBI dashboard lineage (SAME population as the Top-N Stalest Dashboards chart, so
    # that chart's stalest-source driver is one of these bars). bi_source_freshness is already one row
    # per FQ table at its latest snapshot; take MAX days per name defensively (dedupe if a rerun leaves
    # >1 snapshot_date). No workspace_id on this table (lineage is account-scoped), so no ws filter.
    {"name": "freshness", "displayName": "Source-table staleness (AIBI lineage)", "queryLines": [
        "SELECT asset_name, max(days_since_last_refresh) AS days_since_last_refresh ",
        f"FROM {SRCFRESH} ",
        "GROUP BY asset_name"]},
    # --- power users: pure statistical cut — interactions above mean + 1 std dev. Skewed
    # distribution → ~1-2% of users, the intended mathematically-clean definition. ---
    power_users_ds("power_users_genie", "GENIE_AGENTS"),
    # choices dataset for the Top-N (:top_n) parameter filter — gives the dropdown its values.
    {"name": "topn_choices", "displayName": "Top-N choices", "queryLines": [
        "SELECT * FROM (VALUES (5), (10), (20), (50)) AS t(n)"]},
    # Choices for the Workspace filter — workspace IDs (the flt_ws param compares to `Workspace` id).
    {"name": "ws_choices", "displayName": "Workspace choices", "queryLines": [
        "SELECT DISTINCT `Workspace` AS workspace_id FROM ", f"{MV} ORDER BY workspace_id"]},
    # ---- Cross-filter shared datasets: one per product, LIMITED to the top-N assets by est cost
    # (:top_n), at (asset, user, query) grain. The top-N asset bar, users bar and queries table all
    # read the SAME dataset, so clicking an asset bar cross-filters the other two. This merges the
    # old "Top-N chart" + "click-to-drill chart" into one clean clickable chart per column.
    xf_topn_ds("xf_genie", "GENIE_AGENTS"),
    xf_topn_ds("xf_dash", "AIBI"),
    # ---- Per-asset avg cost KPIs (agentic + SQL): all assets AND active-only, per product ----
    # bi_asset_cost now carries workspace_id, so these KPIs honour the global Workspace filter
    # (no user column → has_owner=False; Owner filter would over-restrict a per-asset rollup anyway).
    {"name": "assetcost_genie", "displayName": "Genie avg cost/agent", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT round(avg(total_cost),2) AS avg_all, ",
        "  round(sum(total_cost)/NULLIF(count(CASE WHEN activity>0 THEN 1 END),0),2) AS avg_active ",
        f"FROM {ASSETCOST} {_ASSETCOST_GENIE_WHERE}"]},
    {"name": "assetcost_aibi", "displayName": "AIBI avg cost/dashboard", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT round(avg(total_cost),2) AS avg_all, ",
        "  round(sum(total_cost)/NULLIF(count(CASE WHEN activity>0 THEN 1 END),0),2) AS avg_active ",
        f"FROM {ASSETCOST} {_ASSETCOST_AIBI_WHERE}"]},
    # ---- QUALITY tab datasets ----
    # Freshness KPIs (AIBI only — pipelines; genie agents have no refresh concept).
    {"name": "freshness_kpi", "displayName": "Freshness KPI", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT round(avg(days_since_last_refresh),1) AS avg_days, max(days_since_last_refresh) AS max_days FROM ( ",
        f"  SELECT asset_id, days_since_last_refresh FROM {FRESH} {_WS_ONLY_WHERE}",
        "  QUALIFY row_number() OVER (PARTITION BY asset_id ORDER BY snapshot_date DESC)=1 )"]},
    # Source-% metric views KPI (AIBI dashboards).
    {"name": "srccomp_kpi", "displayName": "Source composition KPI", "queryLines": [
        "SELECT round(avg(pct_metric_view),1) AS avg_pct_metric_view, count(*) AS dashboards ",
        f"FROM {SRCCOMP}"]},
    # Avg query run time KPIs per product (ms) — from the metric view, product-scoped.
    # (reuses totals_genie / totals_aibi avg_query_ms below)
    # Top-N stalest assets (AIBI) capped at :top_n.
    # Top-N STALEST DASHBOARDS (pass-3 #23): reads bi_dashboard_freshness — a dashboard's staleness =
    # its stalest source table (via lineage). Label = dashboard NAME; the driving source is carried as
    # `stalest_source` for the tooltip (NOT the axis label). No workspace col on this table (lineage is
    # account-scoped), so no ws filter param here.
    {"name": "topn_stale", "displayName": "Top-N stalest dashboards", "parameters": [
        {"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
         "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}],
     "queryLines": [
        "SELECT asset_name, days_since_last_refresh AS days, stalest_source, n_sources ",
        f"FROM {DASHFRESH} ",
        "QUALIFY row_number() OVER (ORDER BY days_since_last_refresh DESC) <= :top_n"]},
    # ---- NO-ENGAGEMENT (past 30d) datasets (pass-3 feature) ----
    # KPI counts: how many known agents / dashboards saw ZERO engagement in the last 30 days.
    {"name": "noeng_counts", "displayName": "No-engagement counts (30d)", "queryLines": [
        "SELECT ",
        "  sum(CASE WHEN asset_kind='genie_space' AND no_engagement_30d THEN 1 ELSE 0 END) AS cold_agents, ",
        "  sum(CASE WHEN asset_kind='dashboard'   AND no_engagement_30d THEN 1 ELSE 0 END) AS cold_dashboards ",
        f"FROM {ENGAGE30}"]},
    # Lists: cold assets by how long since last activity (never-active sort last). Capped by :top_n.
    {"name": "noeng_agents", "displayName": "Cold genie agents (30d)", "parameters": [
        {"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
         "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}],
     "queryLines": [
        "SELECT asset_name, coalesce(days_since_last_activity, 9999) AS days_since_last_activity ",
        f"FROM {ENGAGE30} WHERE asset_kind='genie_space' AND no_engagement_30d ",
        "QUALIFY row_number() OVER (ORDER BY days_since_last_activity DESC) <= :top_n"]},
    {"name": "noeng_dashboards", "displayName": "Cold dashboards (30d)", "parameters": [
        {"displayName": "top_n", "keyword": "top_n", "dataType": "INTEGER",
         "defaultSelection": {"values": {"dataType": "INTEGER", "values": [{"value": "10"}]}}}],
     "queryLines": [
        "SELECT asset_name, coalesce(days_since_last_activity, 9999) AS days_since_last_activity ",
        f"FROM {ENGAGE30} WHERE asset_kind='dashboard' AND no_engagement_30d ",
        "QUALIFY row_number() OVER (ORDER BY days_since_last_activity DESC) <= :top_n"]},
    # ---- ENGAGEMENT tab datasets ----
    # Active users (>1 active day in last 30d) per product. The 30-day window is DEFINITIONAL (this is
    # the "active users" metric), so it ignores the global Date-range filter (has_date=False) — only
    # Workspace/Owner apply. Documented in the Engagement metric-definitions box.
    {"name": "active_users", "displayName": "Active users (>1 day/30d)", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT product, count(*) AS active_users FROM ( ",
        "  SELECT product, user_hash FROM ", f"{CAT}.{SCH}.bi_usage_fact ",
        f"  {flt_where_raw('usage_date >= current_date() - INTERVAL 30 DAYS AND (interactions>0 OR query_count>0)', has_date=False)}",
        "  GROUP BY product, user_hash HAVING count(DISTINCT usage_date) > 1 ) GROUP BY product"]},
    # Total active users (>1 active day/30d) across both products — distinct users, single row.
    # Same definitional 30-day window → ignores the global Date-range filter (has_date=False).
    {"name": "active_users_total", "displayName": "Total active users", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT count(*) AS active_users FROM ( ",
        "  SELECT user_hash FROM ", f"{CAT}.{SCH}.bi_usage_fact ",
        f"  {flt_where_raw('usage_date >= current_date() - INTERVAL 30 DAYS AND (interactions>0 OR query_count>0)', has_date=False)}",
        "  GROUP BY user_hash HAVING count(DISTINCT usage_date) > 1 )"]},
    # % of genie users who add ratings.
    {"name": "pct_rating", "displayName": "% genie users rating", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT round(100.0*count(DISTINCT CASE WHEN thumbs_up+thumbs_down>0 THEN user_hash END)"
        "/NULLIF(count(DISTINCT user_hash),0),1) AS pct_rating_users ",
        f"FROM {CAT}.{SCH}.bi_usage_fact {_GENIE_RAW_WHERE}"]},
    # Weekly unique-users trend, product-scoped (engagement "view trend as unique users").
    {"name": "genie_users_by_wk", "displayName": "Genie weekly users", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT `Week` AS week, ",
        "  MEASURE(`Unique Users`) AS unique_users ",
        f"FROM {MV} ", flt_where("`Product`='GENIE_AGENTS'"), "GROUP BY `Week`"]},
    {"name": "aibi_users_by_wk", "displayName": "AIBI weekly users", "parameters": list(FLT_PARAMS), "queryLines": [
        "SELECT `Week` AS week, ",
        "  MEASURE(`Unique Users`) AS unique_users ",
        f"FROM {MV} ", flt_where("`Product`='AIBI'"), "GROUP BY `Week`"]},
    # Top-N assets by unique users, per product (:top_n).
    topn_ds("topn_genie_users_asset", "Asset Name", "asset_name", "Unique Users", "metric_value",
            where="`Product` = 'GENIE_AGENTS' AND `Asset` IS NOT NULL"),
    topn_ds("topn_aibi_users_asset", "Asset Name", "asset_name", "Unique Users", "metric_value",
            where="`Product` = 'AIBI' AND `Asset` IS NOT NULL"),
    # Power users per product (mean+1sd of interactions).
    power_users_ds("power_users_aibi", "AIBI"),
]
_removed_toggle_datasets = [
    # Metric-toggle datasets removed per user feedback (toggle no longer needed). Kept out of the
    # datasets list; definitions parked here as a no-op reference in case the toggle returns.
    {"name": "metric_choices", "displayName": "Metric selector values", "queryLines": [
        "SELECT * FROM (VALUES ('Total Cost'), ('Unique Users'), ('Total Interactions'), "
        "('Total Queries')) AS t(metric_selection)"]},
    {"name": "toggle_by_ws", "displayName": "Toggle metric by workspace", "parameters": [
        {"displayName": "user_metric", "keyword": "user_metric",
         "dataType": "STRING", "defaultSelection": {"values": {"dataType": "STRING",
            "values": [{"value": "Total Cost"}]}}}],
     "queryLines": [
        "SELECT `Workspace Name` AS workspace, ",
        "  CASE ",
        "    WHEN :user_metric = 'Total Cost'        THEN MEASURE(`Total Cost (USD)`) ",
        "    WHEN :user_metric = 'Unique Users'      THEN MEASURE(`Unique Users`) ",
        "    WHEN :user_metric = 'Total Interactions' THEN MEASURE(`Total Interactions`) ",
        "    WHEN :user_metric = 'Total Queries'     THEN MEASURE(`Total Queries`) ",
        "  END AS metric_value ",
        f"FROM {MV} ", "GROUP BY `Workspace Name`"]},
]

# ---------------------------------------------------------------------------
# WIDGET HELPERS
# ---------------------------------------------------------------------------
def text(name, md, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

# Number-format specs for units (item 2). "currency-dollar" → $, plain number, or percent.
FMT_USD = {"type": "number-currency", "currencyCode": "USD", "decimalPlaces": {"type": "max", "places": 2}}
FMT_NUM = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 0}}
FMT_PCT = {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}}

def counter(name, field, title, x, y, w=2, h=3, ds="totals", fmt=None, desc=None):
    """Counter over a pre-aggregated single-row dataset: reference the plain column, no agg.
    esure-branded: value in esure blue. Two-line tile style (matches the user's UI edits):
    `title` = short bold header, `desc` = parenthetical qualifier shown as the frame description."""
    value_enc = {"fieldName": field, "displayName": title,
                 "style": {"rules": [], "color": ESURE_BLUE}}
    if fmt:
        value_enc["format"] = fmt
    frame = {"showTitle": True, "title": title}
    if desc:
        frame["showDescription"] = True
        frame["description"] = desc
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds,
            "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter",
            "encodings": {"value": value_enc},
            "frame": frame}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def topn_bar(name, ds, cat_field, val_field, title, x, y, w=3, h=6, fmt=None, cat_label=None,
             tooltip_field=None, tooltip_label=None):
    """Horizontal Top-N bar: measure on x (sorted desc), category on y. Reads a :top_n dataset.
    tooltip_field: extra column shown on hover (e.g. the email behind a display name)."""
    y_enc = {"fieldName": val_field, "scale": {"type": "quantitative"}, "displayName": title}
    if fmt:
        y_enc["format"] = fmt
    fields = [{"name": cat_field, "expression": f"`{cat_field}`"},
              {"name": val_field, "expression": f"`{val_field}`"}]
    enc = {"x": {"fieldName": val_field, "scale": {"type": "quantitative"}, "displayName": title},
           "y": {"fieldName": cat_field, "scale": {"type": "categorical", "sort": {"by": "x-reversed"}},
                 "displayName": cat_label or cat_field}}
    if tooltip_field:
        fields.append({"name": tooltip_field, "expression": f"`{tooltip_field}`"})
        enc["extra"] = [{"fieldName": tooltip_field, "displayName": tooltip_label or tooltip_field}]
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields,
            "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "bar",
            "encodings": enc,
            "frame": {"showTitle": True, "title": title},
            "mark": {"colors": ESURE_PALETTE}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def xf_bar(name, ds, cat_field, sum_field, title, x, y, w=3, h=6, fmt=None, cat_label=None):
    """Aggregating horizontal bar for CROSS-FILTER: groups by cat_field, SUMs sum_field at the widget
    level (disaggregated:false). Clicking a bar emits a cross-filter selection on cat_field that
    filters other widgets sharing the same dataset."""
    agg_name = f"sum({sum_field})"
    y_enc = {"fieldName": agg_name, "scale": {"type": "quantitative"}, "displayName": title}
    if fmt:
        y_enc["format"] = fmt
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [
            {"name": cat_field, "expression": f"`{cat_field}`"},
            {"name": agg_name, "expression": f"SUM(`{sum_field}`)"}], "disaggregated": False}}],
        "spec": {"version": 3, "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": agg_name, "scale": {"type": "quantitative"}, "displayName": title},
                "y": {"fieldName": cat_field, "scale": {"type": "categorical", "sort": {"by": "x-reversed"}},
                      "displayName": cat_label or cat_field}},
            "frame": {"showTitle": True, "title": title},
            "mark": {"colors": ESURE_PALETTE}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def xf_table(name, ds, cat_field, cat_label, sum_field, sum_label, title, x, y, w=3, h=6):
    """Aggregating table for CROSS-FILTER: groups by cat_field, SUMs sum_field. Cost column first,
    sorted by cost descending (most expensive queries at the top)."""
    agg_name = f"sum({sum_field})"
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [
            {"name": agg_name, "expression": f"SUM(`{sum_field}`)"},
            {"name": cat_field, "expression": f"`{cat_field}`"}], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "table",
            "sortColumns": [{"fieldName": agg_name, "sortDirection": "DESC"}],
            "encodings": {"columns": [
                {"fieldName": agg_name, "displayName": sum_label},
                {"fieldName": cat_field, "displayName": cat_label}]},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def table_widget(name, ds, cols, title, x, y, w=3, h=6):
    """Table: cols = list of (field, label). Reads raw rows (disaggregated). For long text like SQL."""
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds,
            "fields": [{"name": c[0], "expression": f"`{c[0]}`"} for c in cols], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table",
            "encodings": {"columns": [{"fieldName": c[0], "displayName": c[1]} for c in cols]},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def line(name, ds, x_field, y_field, color_field, title, x, y, w=3, h=6, y_label=None, tooltip_fields=None):
    """Line over pre-aggregated data: temporal x, plain measure y, optional color. No agg.
    y_label: SHORT y-axis label (defaults to title).
    tooltip_fields: list of (field, label) added to the hover tooltip via the `extra` encoding.
    CAUTION: a per-point STRING extra (e.g. a yy-week# label) fragments the line into disconnected
    dots — that's why the trend charts dropped their week labels. Numeric measure breakdowns (e.g.
    agentic vs SQL cost) are safe and keep the line continuous (verified in-browser). Use sparingly."""
    fields = [{"name": x_field, "expression": f"`{x_field}`"},
              {"name": y_field, "expression": f"`{y_field}`"}]
    enc = {"x": {"fieldName": x_field, "scale": {"type": "temporal"}, "displayName": "Date"},
           "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label or title}}
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        enc["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}, "displayName": "Product"}
    if tooltip_fields:
        for f, lbl in tooltip_fields:
            fields.append({"name": f, "expression": f"`{f}`"})
        enc["extra"] = [{"fieldName": f, "displayName": lbl} for f, lbl in tooltip_fields]
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "line", "encodings": enc,
            "frame": {"showTitle": True, "title": title},
            "mark": {"colors": ESURE_PALETTE}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def bar(name, ds, cat_field, val_field, title, x, y, w=3, h=6, topn=True, fmt=None, cat_label=None):
    """Bar over pre-aggregated data: categorical x, plain measure y. No agg."""
    y_enc = {"fieldName": val_field, "scale": {"type": "quantitative"}, "displayName": title}
    if fmt:
        y_enc["format"] = fmt
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [
            {"name": cat_field, "expression": f"`{cat_field}`"},
            {"name": val_field, "expression": f"`{val_field}`"}], "disaggregated": True}}],
        "spec": {"version": 3, "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": cat_field, "scale": {"type": "categorical",
                      **({"sort": {"by": "y-reversed"}} if topn else {})}, "displayName": cat_label or cat_field},
                "y": y_enc},
            "frame": {"showTitle": True, "title": title},
            "mark": {"colors": ["#FFAB00", "#00A972", "#FF3621", "#8BCAE7", "#AB4057"]}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

# ---------------------------------------------------------------------------
# GLOBAL FILTERS PAGE
# ---------------------------------------------------------------------------
def filter_widget(name, field, title, qname, ds, x, y, kind="filter-multi-select"):
    return {"widget": {"name": name,
        "queries": [{"name": qname, "query": {"datasetName": ds,
            "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": kind,
            "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qname}]},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": 2, "height": 2}}

# Param-driving global filter: a searchable single-select whose chosen value is pushed into the
# named parameter (flt_ws / flt_owner) on EVERY dataset that declares it. `choices_ds`/`choice_field`
# supply the dropdown values. This is what makes Workspace/Owner filter the whole dashboard even
# though most datasets don't SELECT those columns (the param injects a WHERE into each).
def _datasets_with_param(param_kw):
    return [d["name"] for d in datasets if any(p["keyword"] == param_kw for p in d.get("parameters", []))]

def param_filter_widget(name, param_kw, choice_field, choices_ds, title, x, y):
    bound = _datasets_with_param(param_kw)
    queries = [{"name": f"{name}_choices", "query": {"datasetName": choices_ds,
        "fields": [{"name": choice_field, "expression": f"`{choice_field}`"}], "disaggregated": False}}]
    fields = [{"fieldName": choice_field, "displayName": title, "queryName": f"{name}_choices"}]
    for i, dsn in enumerate(bound):
        qn = f"{name}_p{i}"
        queries.append({"name": qn, "query": {"datasetName": dsn,
            "parameters": [{"name": param_kw, "keyword": param_kw}], "disaggregated": False}})
        fields.append({"parameterName": param_kw, "queryName": qn})
    return {"widget": {"name": name, "queries": queries,
        "spec": {"version": 2, "widgetType": "filter-single-select",
            "encodings": {"fields": fields},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": 2, "height": 2}}

def date_range_param_widget(name, title, x, y, w=2, h=2):
    """Global Date-range picker bound to the single flt_dates RANGE PARAMETER (not a dataset field),
    so it filters EVERY dataset that declares it — including pre-aggregated KPI grand-totals and the
    weekly-trend datasets that a native field-filter can't reach. One query per bound dataset declares
    the param; one encoding field binds the picker's STRUCT<min,max> value into all of them (the SQL
    references :flt_dates.min / :flt_dates.max). Mirrors param_filter_widget (single parameterName)."""
    bound = _datasets_with_param("flt_dates")
    queries, fields = [], []
    for i, dsn in enumerate(bound):
        qn = f"{name}_p{i}"
        queries.append({"name": qn, "query": {"datasetName": dsn,
            "parameters": [{"name": "flt_dates", "keyword": "flt_dates"}], "disaggregated": False}})
        fields.append({"parameterName": "flt_dates", "queryName": qn})
    return {"widget": {"name": name, "queries": queries,
        "spec": {"version": 2, "widgetType": "filter-date-range-picker",
            "encodings": {"fields": fields},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

def topn_filter_widget(name, choices_ds, choice_field, title, x, y):
    """Single-select driving the :top_n INTEGER parameter on EVERY dataset that declares it
    (auto-discovered — same pattern as param_filter_widget), so one dropdown caps all Top-N charts."""
    bound = _datasets_with_param("top_n")
    queries = [{"name": f"{name}_choices", "query": {"datasetName": choices_ds,
        "fields": [{"name": choice_field, "expression": f"`{choice_field}`"}], "disaggregated": False}}]
    fields = [{"fieldName": choice_field, "displayName": title, "queryName": f"{name}_choices"}]
    for i, dsn in enumerate(bound):
        qn = f"{name}_p{i}"
        queries.append({"name": qn, "query": {"datasetName": dsn,
            "parameters": [{"name": "top_n", "keyword": "top_n"}], "disaggregated": False}})
        fields.append({"parameterName": "top_n", "queryName": qn})
    return {"widget": {"name": name, "queries": queries,
        "spec": {"version": 2, "widgetType": "filter-single-select",
            "encodings": {"fields": fields},
            "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": 2, "height": 2}}

# The metric-toggle global filter: single-select driving BOTH the metric_choices dropdown AND the
# :user_metric parameter on toggle_by_ws (per the shared guide).
metric_toggle_filter = {"widget": {"name": "f-metric-toggle",
    "queries": [
        {"name": "q_metric_choices", "query": {"datasetName": "metric_choices",
            "fields": [{"name": "metric_selection", "expression": "`metric_selection`"}], "disaggregated": False}},
        {"name": "q_metric_param", "query": {"datasetName": "toggle_by_ws",
            "parameters": [{"name": "user_metric", "keyword": "user_metric"}], "disaggregated": False}}],
    "spec": {"version": 2, "widgetType": "filter-single-select",
        "encodings": {"fields": [
            {"fieldName": "metric_selection", "displayName": "Metric", "queryName": "q_metric_choices"},
            {"parameterName": "user_metric", "queryName": "q_metric_param"}]},
        "frame": {"showTitle": True, "title": "Metric toggle (Overview chart)"}}},
    "position": {"x": 4, "y": 2, "width": 2, "height": 2}}

global_filters = {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
    "layout": [
        # Date range: PARAMETER-bound (flt_start/flt_end), not a dataset field — so it reaches the KPI
        # grand-totals and weekly trends too (a native field-filter only touched by_day). See
        # date_range_param_widget. Defaults span ~10y (all history) until the user picks a range.
        date_range_param_widget("f-date", "Date range", 0, 0),
        # Filters (per user feedback): removed Product (AIBI/Genie), Surface, and the Metric toggle.
        # Workspace filter labelled "Workspace ID" — the value falls back to the id when a workspace
        # name isn't resolvable in system.access.workspaces_latest, so it's a mix, labelled honestly.
        # Owner filter lists display NAMES (by_owner selects `Owner Name`); the param matches on the
        # display name across datasets so the dropdown is human-readable, not raw ids/emails.
        param_filter_widget("f-workspace", "flt_ws", "workspace_id", "ws_choices", "Workspace ID", 2, 0),
        param_filter_widget("f-owner", "flt_owner", "owner", "by_owner", "Owner", 4, 0),
        # Top-N (:top_n) parameter filter — single-select dropdown (5/10/20/50) bound to the top_n
        # parameter on EVERY dataset that declares it (auto-discovered), so users toggle N for every
        # Top-N chart at once — including the power-users and per-asset-users charts.
        topn_filter_widget("f-topn", "topn_choices", "n", "Top N (charts)", 0, 2),
        # (Drill-down asset selectors removed — replaced by native click-to-cross-filter on the Cost
        # tab's drill-down charts, which share a dataset with their users/queries widgets.)
    ]}

# ---------------------------------------------------------------------------
# METRIC DEFINITIONS panels (pass-3 #9/#10) — one per tab, listing each metric's formula + the
# aggregation window, plus the weekly-bucketing note. Markdown newlines (\n) render as separate lines
# in a text widget (unlike the `lines` array, which concatenates). "Over time range" = whatever the
# global Date-range filter is set to (all history if unset).
# ---------------------------------------------------------------------------
_WEEK_NOTE = ("**Weekly = calendar week:** each date is bucketed into its week of the year "
                  "(Monday-start) and plotted at the week-start calendar date, so the trend line is "
                  "continuous across the year.")

_DEFS_OVERVIEW = ("### Metric definitions\n"
    "- **Unique Users** — distinct users (Genie + AIBI), over time range.\n"
    "- **Total Interactions (Genie)** — count of submitted Genie messages.\n"
    "- **Total Queries (AIBI)** — count of finished dashboard queries.\n"
    "- **Total Cost (Agentic)** — Σ Genie DBU cost (USD).\n"
    "- **Total Cost (SQL)** — Σ warehouse SQL cost (USD), whole-warehouse (upper bound for AIBI).\n"
    "- **Avg Cost / User / Day** — total cost ÷ distinct users ÷ distinct active days.\n"
    "- Weekly trends bucket by calendar week. " + _WEEK_NOTE)

_DEFS_COST = ("### Metric definitions\n"
    "- **Total Cost (Agentic / SQL)** — Σ Genie DBU cost / Σ warehouse SQL cost (USD), over time range.\n"
    "- **Avg Cost / User / Day** — total cost ÷ distinct users ÷ distinct active days.\n"
    "- **Avg Cost / Active Genie Agent** — whole-range total cost (agentic + est. SQL) ÷ agents with "
    "activity. NOT per day.\n"
    "- **Avg Cost / Dashboard** — whole-range total est. SQL cost ÷ dashboards. NOT per day.\n"
    "- **Avg Cost / Query** — total cost ÷ total queries (per query).\n"
    "- **Estimated allocated cost** — no per-query cost exists in system tables; each warehouse-hour's "
    "DBU cost is split across that hour's queries by task-duration. Only ~35% of warehouse cost lands "
    "in query hours (rest = idle uptime), so treat est. cost as a RELATIVE ranking, not an exact bill.\n"
    "- Weekly trends bucket by calendar week. " + _WEEK_NOTE)

_DEFS_QUALITY = ("### Metric definitions\n"
    "- **Avg Query Run Time** — mean query completion time (ms), over time range. Excludes rows with "
    "invalid negative durations.\n"
    "- **Avg Freshness (AIBI)** — mean days since last pipeline refresh (latest snapshot). Pipelines "
    "only — AIBI dashboard refresh schedules aren't in system tables.\n"
    "- **Source % as Metric Views** — mean % of a dashboard's source objects that are governed metric "
    "views.\n"
    "- **Data Sources by Staleness** — each source table feeding an AIBI dashboard (via lineage), by "
    "days since its **last-altered** time. One bar per source table.\n"
    "- **Top N Stalest AIBI Dashboards** — dashboards ranked by staleness, where a dashboard's "
    "staleness = its stalest source table (from lineage). Hover shows the driving source table. "
    "Capped by the Top N filter.\n"
    "- **Last altered vs. last pipeline run** — the two staleness charts use **last-altered** "
    "(`information_schema.tables.last_altered`): when a table's data/metadata was last modified by ANY "
    "writer (a pipeline run, a job, a manual MERGE/INSERT, an ALTER). This is broader than a pipeline's "
    "*last-run* time (`system.lakeflow.pipelines`, pipelines only) — the **Avg Freshness KPI above still "
    "uses last pipeline run**. Both staleness charts share the SAME population (source tables reached "
    "through AIBI dashboard lineage) and the SAME metric (last-altered), so the stalest dashboard's "
    "driver on the right is always one of the bars on the left.\n"
    "- Genie agents have no refresh or dashboard lineage, so freshness/source metrics are AIBI-only.")

_DEFS_ENGAGEMENT = ("### Metric definitions\n"
    "- **Total Active Users** — distinct users with >1 visit in the past 30 days. This is a fixed "
    "30-day window by definition, so it ignores the global **Date range** filter (Workspace/Owner "
    "still apply).\n"
    "- **Unique Users (Genie / AIBI)** — distinct users per product, over time range.\n"
    "- **% Users Adding Ratings** — users who rated ÷ genie users.\n"
    "- **% Positive / Negative Ratings** — thumbs up|down ÷ total ratings.\n"
    "- **Power Users** — users with interactions above (mean + 1 std dev), capped by Top N.\n"
    "- **No-engagement (30d)** — known assets (from the name catalog) with 0 interactions/queries in "
    "the last 30 days; the lists rank them by days since last activity.\n"
    "- Users show as **First Last** (parsed from email); service principals as 'Service principal "
    "(·id)'. The full email/id is on hover.\n"
    "- Weekly trends bucket by calendar week. " + _WEEK_NOTE)

# ---------------------------------------------------------------------------
# PAGE 1: OVERVIEW
# ---------------------------------------------------------------------------
overview = {"name": "overview", "displayName": "Overview", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("ov-title", "## BI Monitoring Suite: AIBI dashboards and Genie Agents", 0, 0, 6, 1),
    text("ov-sub", "**Overview tab** — top-level KPIs across Genie Agents + AIBI. All figures from the governed `bi_usage_metrics` metric view; filter globally on the Filters tab. Titles state scope + how each metric is aggregated.", 0, 1, 6, 1),
    # --- KPI row 1 (signposted titles per wireframe callouts) ---
    counter("ov-users", "unique_users", "Unique Users", 0, 2, fmt=FMT_NUM, ds="totals", desc="(distinct users, both Genie & AIBI)"),
    counter("ov-genie-int", "total_interactions", "Total Interactions on Genie Agents", 2, 2, fmt=FMT_NUM, ds="totals_genie", desc="(submitted messages)"),
    # NB: the two diffs vs live are intentional normalisations — ov-users gets a real description
    # (live had empty "()" ) and ov-genie-int drops the redundant title suffix. Both are consistent
    # with the two-line style; confirmed with the user rather than silently reverting.
    counter("ov-aibi-q", "total_queries", "Total Queries Run in AIBI", 4, 2, fmt=FMT_NUM, ds="totals_aibi", desc="(finished dashboard queries)"),
    # --- KPI row 2 ---
    counter("ov-cost-genie", "total_cost", "Total Cost (Agentic)", 0, 5, fmt=FMT_USD, ds="totals_genie", desc="(Genie DBU cost, USD)"),
    counter("ov-cost-sql", "total_cost", "Total Cost (SQL)", 2, 5, fmt=FMT_USD, ds="totals_aibi", desc="(both products, USD)"),
    counter("ov-cost-user-day", "avg_cost_user_day", "Avg Cost / User / Day", 4, 5, fmt=FMT_USD, ds="totals", desc="(agentic + SQL)"),
    # --- weekly trends ---
    text("ov-trends", "### Weekly trends by product (tip: single-select Product to isolate Genie vs AIBI)", 0, 8, 6, 1),
    line("ov-cost-trend", "by_week", "week", "total_cost", "product", "Weekly Cost by Product ($) — sum of DBU cost per week", 0, 9, 3, 6, y_label="Cost (USD)"),
    line("ov-users-trend", "by_week", "week", "unique_users", "product", "Weekly Unique Users by Product — distinct users per week", 3, 9, 3, 6, y_label="Unique users"),
    # --- Top-N charts (n = :top_n parameter, default 10) ---
    text("ov-topn", "### Top-N breakdowns (N set by the `top_n` parameter, default 10)", 0, 15, 6, 1),
    topn_bar("ov-topn-ws", "topn_ws_users", "workspace", "metric_value", "Top N Workspaces by Unique BI Users", 0, 16, 3, 6, fmt=FMT_NUM, cat_label="Workspace"),
    topn_bar("ov-power-users", "power_users_genie", "owner_name", "metric_value", "Genie Agent Power Users — interactions above mean + 1 std dev", 3, 16, 3, 6, fmt=FMT_NUM, cat_label="User", tooltip_field="owner_email", tooltip_label="Email / id"),
    topn_bar("ov-topn-genie", "topn_genie_views", "asset_name", "metric_value", "Top N Genie Agents by Daily Views (interactions)", 0, 22, 3, 6, fmt=FMT_NUM, cat_label="Genie agent"),
    topn_bar("ov-topn-aibi", "topn_aibi_views", "asset_name", "metric_value", "Top N AIBI Dashboards by Daily Views (queries)", 3, 22, 3, 6, fmt=FMT_NUM, cat_label="Dashboard / warehouse"),
    text("ov-defs", _DEFS_OVERVIEW, 0, 28, 6, 4),
]}

# ---------------------------------------------------------------------------
# PAGE 2: QUALITY
# ---------------------------------------------------------------------------
quality = {"name": "quality", "displayName": "Quality Monitoring", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("q-title", "## BI Monitoring Suite: AIBI dashboards and Genie Agents", 0, 0, 6, 1),
    text("q-sub", "**Quality Monitoring tab** — query run time, data freshness and source composition. Freshness and source-% are AIBI-only (Genie agents have no refresh/dashboard-lineage).", 0, 1, 6, 2),
    # --- KPIs: 4 real metrics as a balanced 2x2 (width 3). Dropped the two N/A Genie freshness/source
    # tiles (not applicable to Genie) per pass-3 feedback. Row 1 = query run time (both products);
    # row 2 = AIBI-only freshness + source-composition.
    counter("q-genie-dur", "avg_query_ms", "Avg Query Run Time — Genie", 0, 3, w=3, ds="totals_genie", desc="(ms, per query)"),
    counter("q-aibi-dur", "avg_query_ms", "Avg Query Run Time — AIBI", 3, 3, w=3, ds="totals_aibi", desc="(ms, per query)"),
    counter("q-aibi-fresh", "avg_days", "Avg Freshness — AIBI", 0, 6, w=3, ds="freshness_kpi", desc="(mean days since refresh)"),
    counter("q-aibi-src", "avg_pct_metric_view", "Source % as Metric Views — AIBI", 3, 6, w=3, ds="srccomp_kpi", fmt=FMT_PCT, desc="(% of dashboard sources)"),
    # --- staleness ranking + top-N stale ---
    text("q-stale-h", "### Staleness — AIBI dashboard source tables (by last-altered) + stalest dashboards. Top-N by the `Top N (charts)` filter.", 0, 9, 6, 1),
    bar("q-stale-all", "freshness", "asset_name", "days_since_last_refresh", "Data Sources by Staleness (days since last altered)", 0, 10, 3, 6, fmt=FMT_NUM, cat_label="Source table"),
    # Dashboard NAMES ranked by staleness; the driving source table is on hover, not the axis (pass-3 #23).
    topn_bar("q-topn-stale", "topn_stale", "asset_name", "days", "Top N Stalest AIBI Dashboards (days since refresh)", 3, 10, 3, 6, fmt=FMT_NUM, cat_label="Dashboard", tooltip_field="stalest_source", tooltip_label="Stalest source table"),
    # --- completion-time trend (non-negative) ---
    text("q-dur-h", "### Query completion time trend", 0, 16, 6, 1),
    line("q-dur-trend", "by_day", "day", "avg_query_ms", "product", "Avg Daily Query Completion (ms) by Product", 0, 17, 6, 6),
    text("q-defs", _DEFS_QUALITY, 0, 23, 6, 5),
]}

# ---------------------------------------------------------------------------
# PAGE 3: ENGAGEMENT
# ---------------------------------------------------------------------------
engagement = {"name": "engagement", "displayName": "User Engagement", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("e-title", "## BI Monitoring Suite: AIBI dashboards and Genie Agents", 0, 0, 6, 1),
    text("e-sub", "**User engagement tab** — user interactions and engagement across Genie Agents + AIBI. Active users = seen on >1 day in the last 30 days. Titles state scope + how each metric is aggregated.", 0, 1, 6, 2),
    # --- KPI row 1 ---
    counter("e-active", "active_users", "Total Active Users", 0, 3, fmt=FMT_NUM, ds="active_users_total", desc="(>1 visit, past 30 days)"),
    counter("e-genie-users", "unique_users", "Unique Users — Genie Agents", 2, 3, fmt=FMT_NUM, ds="totals_genie", desc="(distinct genie users)"),
    counter("e-aibi-users", "unique_users", "Unique Users — AIBI", 4, 3, fmt=FMT_NUM, ds="totals_aibi", desc="(distinct dashboard users)"),
    # --- KPI row 2 ---
    counter("e-pct-rating", "pct_rating_users", "% Users Adding Ratings — Genie", 0, 6, fmt=FMT_PCT, ds="pct_rating", desc="(rated ÷ genie users)"),
    counter("e-posrate", "positive_rating_pct", "% Positive Ratings", 2, 6, fmt=FMT_PCT, ds="totals_genie", desc="(thumbs up ÷ ratings)"),
    counter("e-negrate", "negative_rating_pct", "% Negative Ratings", 4, 6, fmt=FMT_PCT, ds="totals_genie", desc="(thumbs down ÷ ratings)"),
    # --- weekly view trends (as unique users) ---
    text("e-trend-h", "### Weekly view trend, as unique users", 0, 9, 6, 1),
    line("e-trend-genie", "genie_users_by_wk", "week", "unique_users", None, "Weekly Unique Users — Genie Agents", 0, 10, 3, 6),
    line("e-trend-aibi", "aibi_users_by_wk", "week", "unique_users", None, "Weekly Unique Users — AIBI", 3, 10, 3, 6),
    # --- top-N by unique users ---
    text("e-topn-h", "### Top-N assets by unique users (N by the `Top N (charts)` filter)", 0, 16, 6, 1),
    topn_bar("e-topn-genie", "topn_genie_users_asset", "asset_name", "metric_value", "Top N Genie Agents by Unique Users", 0, 17, 3, 6, fmt=FMT_NUM, cat_label="Genie agent"),
    topn_bar("e-topn-aibi", "topn_aibi_users_asset", "asset_name", "metric_value", "Top N AIBI Dashboards by Unique Users", 3, 17, 3, 6, fmt=FMT_NUM, cat_label="Dashboard"),
    # --- power users per product (mean + 1 std dev) ---
    text("e-power-h", "### Power users — interactions above mean + 1 std dev", 0, 23, 6, 1),
    topn_bar("e-power-genie", "power_users_genie", "owner_name", "metric_value", "Genie Agent Power Users", 0, 24, 3, 6, fmt=FMT_NUM, cat_label="User", tooltip_field="owner_email", tooltip_label="Email / id"),
    topn_bar("e-power-aibi", "power_users_aibi", "owner_name", "metric_value", "AIBI Power Users", 3, 24, 3, 6, fmt=FMT_NUM, cat_label="User", tooltip_field="owner_email", tooltip_label="Email / id"),
    # --- disengagement: assets with NO engagement in the past 30 days (pass-3 feature) ---
    text("e-cold-h", "### Disengagement — assets with no user engagement in the past 30 days", 0, 30, 6, 1),
    counter("e-cold-agents", "cold_agents", "Genie Agents — No Engagement", 0, 31, 3, 3, fmt=FMT_NUM, ds="noeng_counts", desc="(0 interactions, past 30 days)"),
    counter("e-cold-dash", "cold_dashboards", "Dashboards — No Engagement", 3, 31, 3, 3, fmt=FMT_NUM, ds="noeng_counts", desc="(0 queries, past 30 days)"),
    topn_bar("e-cold-agents-list", "noeng_agents", "asset_name", "days_since_last_activity", "Cold Genie Agents (days since last activity)", 0, 34, 3, 6, fmt=FMT_NUM, cat_label="Genie agent"),
    topn_bar("e-cold-dash-list", "noeng_dashboards", "asset_name", "days_since_last_activity", "Cold Dashboards (days since last activity)", 3, 34, 3, 6, fmt=FMT_NUM, cat_label="Dashboard"),
    text("e-defs", _DEFS_ENGAGEMENT, 0, 40, 6, 6),
]}

# ---------------------------------------------------------------------------
# PAGE 4: COSTS
# ---------------------------------------------------------------------------
costs = {"name": "costs", "displayName": "Cost Monitoring", "pageType": "PAGE_TYPE_CANVAS", "layout": [
    text("c-title", "## BI Monitoring Suite: AIBI dashboards and Genie Agents", 0, 0, 6, 1),
    text("c-sub", "**Cost Monitoring tab** — detail on the cost metrics. Agentic = Genie DBUs; SQL = warehouse cost (whole-warehouse, an upper bound for AIBI). Titles state scope + aggregation.\n\n**On estimated cost:** system tables bill SQL warehouses per (warehouse, hour) with no per-query cost, so anywhere you see *estimated allocated cost* (per-query, per-user, per-asset, and the Genie weekly total's SQL portion), each warehouse-hour's DBU cost has been allocated across that hour's queries in proportion to their task-duration. Only ~35% of warehouse cost falls in hours that ran queries (the rest is idle/provisioned uptime), so estimated cost is best used as a **relative ranking**, not an exact bill.", 0, 1, 6, 3),
    # --- KPI row 1 ---
    counter("c-cost-genie", "total_cost", "Total Cost (Agentic)", 0, 4, fmt=FMT_USD, ds="totals_genie", desc="(Genie DBU cost, USD)"),
    counter("c-cost-sql", "total_cost", "Total Cost (SQL)", 2, 4, fmt=FMT_USD, ds="totals_aibi", desc="(both products' warehouse SQL, USD)"),
    counter("c-cost-user-day", "avg_cost_user_day", "Avg Cost / User / Day", 4, 4, fmt=FMT_USD, ds="totals", desc="(per user per active day)"),
    # --- KPI row 2: avg cost per active agent, per dashboard, per query (agentic + est. SQL) ---
    counter("c-cost-agent-act", "avg_active", "Avg Cost / Active Genie Agent", 0, 7, fmt=FMT_USD, ds="assetcost_genie", desc="(per agent, whole range — not per day)"),
    counter("c-cost-dash", "avg_all", "Avg Cost / Dashboard", 2, 7, fmt=FMT_USD, ds="assetcost_aibi", desc="(per dashboard, whole range — not per day)"),
    counter("c-cost-query", "cost_per_query", "Avg Cost / Query", 4, 7, fmt=FMT_USD, ds="totals", desc="(per query)"),
    # --- Weekly cost trends ---
    text("c-trend-h", "### Weekly cost trends", 0, 10, 6, 1),
    line("c-trend-genie", "genie_cost_by_wk", "week", "total_cost", None, "Weekly Cost Trend — Genie Agents, total agentic + SQL (USD)", 0, 11, 3, 6, y_label="Total cost (USD)",
         tooltip_fields=[("agentic_cost", "Agentic DBU cost (USD)"), ("sql_cost", "Estimated allocated SQL cost (USD)")]),
    line("c-trend-aibi", "aibi_cost_by_wk", "week", "total_cost", None, "Weekly Cost Trend — AIBI (USD)", 3, 11, 3, 6, y_label="Total cost (USD)"),
    # --- Top-N + DRILL-DOWN merged, CROSS-FILTER style. The top-N asset bar, users bar and queries
    # table in each column share ONE dataset (xf_genie / xf_dash, limited to top-:top_n assets by est
    # cost), so CLICKING a top-N asset bar filters the users + queries below it. Cost is ESTIMATED
    # (duration-weighted allocation of warehouse cost; relative ranking, not an exact bill).
    # Header shrunk to h=2 (was 3 — too much empty space per pass-3 #5-layout); drill block pulled up
    # one row accordingly (asset bars y=19, users y=25, tables y=31) so no gap is introduced.
    text("c-dd-h", "### Top-N assets by estimated cost (click a bar in the \"Top N\" visualisation of an asset to drill into for top users + query costs)\nN is set by the `Top N (charts)` filter (default 10).", 0, 17, 6, 2),
    xf_bar("c-dd-genie-assets", "xf_genie", "asset_name", "est_cost_usd", "Top N Genie Agents by Est. Cost — click to drill (USD)", 0, 19, 3, 6, fmt=FMT_USD, cat_label="Genie agent"),
    xf_bar("c-dd-dash-assets", "xf_dash", "asset_name", "est_cost_usd", "Top N AIBI Dashboards by Est. Cost — click to drill (USD)", 3, 19, 3, 6, fmt=FMT_USD, cat_label="Dashboard"),
    xf_bar("c-dd-genie-users", "xf_genie", "user_display", "est_cost_usd", "Top Users by Est. Spend — filtered by selected agent (USD)", 0, 25, 3, 6, fmt=FMT_USD, cat_label="User"),
    xf_bar("c-dd-dash-users", "xf_dash", "user_display", "est_cost_usd", "Top Users by Est. Spend — filtered by selected dashboard (USD)", 3, 25, 3, 6, fmt=FMT_USD, cat_label="User"),
    xf_table("c-dd-genie-q", "xf_genie", "statement_preview", "Query (first 300 chars)", "est_cost_usd", "Est. cost (USD)", "Top Queries by Est. Cost — filtered by selected agent", 0, 31, 3, 7),
    xf_table("c-dd-dash-q", "xf_dash", "statement_preview", "Query (first 300 chars)", "est_cost_usd", "Est. cost (USD)", "Top Queries by Est. Cost — filtered by selected dashboard", 3, 31, 3, 7),
    text("c-defs", _DEFS_COST, 0, 38, 6, 5),
]}

dashboard = {"datasets": datasets,
             "pages": [overview, costs, quality, engagement, global_filters],
             "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False}}

if __name__ == "__main__":
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bi_monitoring.lvdash.json")
    with open(out, "w") as f:
        json.dump(dashboard, f, indent=2)
    print(f"Wrote {out}")
    for p in dashboard["pages"]:
        print(f"  page '{p['displayName']}': {len(p['layout'])} widgets")
    print("  datasets:", [d["name"] for d in dashboard["datasets"]])
