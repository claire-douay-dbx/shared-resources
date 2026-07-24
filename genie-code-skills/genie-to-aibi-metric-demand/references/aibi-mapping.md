# AI/BI mapping — from ranked metric demand to a Lakeview build plan

How to turn a ranked metric group (with its grains) into concrete, step-by-step AI/BI (Lakeview)
build guidance. **Guidance only — this skill never creates or deploys assets.**

## The mapping, in one line

```
semantic group  -> a measure (and often a reusable metric view)
aggregation grain -> the dashboard dataset's GROUP BY + the widget's dimensions/time axis
recurring filters -> dashboard filter controls / parameters
```

## Step-by-step, per top metric group

### 1. Choose the AI/BI asset shape

- **Metric view vs dashboard dataset:**
  - If the metric is asked at **several grains** (e.g. Revenue by region×month *and* by
    category×day), recommend a **metric view** — a reusable, governed metric definition — so the
    measure is defined once and reused across dashboards/grains. Then the dashboard datasets
    reference it.
  - If it's a **single grain, one dashboard**, a plain **dashboard dataset** (a saved query) is
    enough — don't over-engineer.
- **New vs existing dashboard:** group related metrics onto one dashboard (e.g. all Revenue grains
  on a "Revenue Overview" dashboard) rather than one dashboard per grain.

### 2. Define the measure + source

- **Measure:** the aggregate from the semantic group — `SUM(order_value)`, `COUNT(DISTINCT user_id)`,
  a ratio, etc.
- **Source table(s):** infer from the SQL Genie generated for the group's questions (the `FROM` /
  `JOIN` clauses). Cite the conversation the SQL came from. If observed SQL disagrees across
  questions in the same group, prefer the most complete one and note the discrepancy.

### 3. Set dimensions + time grain from the ranked grains

- The **primary grain** (highest recurrence) becomes the dashboard's default view: its dimensions
  are the chart's group-by/series, its time grain is the time axis.
- **Secondary grains** in the same group become **filter controls / parameters** on the same
  dashboard (e.g. a product-category filter, a day/month/quarter time-grain toggle) so the one
  dashboard answers all the recurring cuts — not a separate dashboard per grain.
- **Recurring filters** from the grain definition become default filter values or parameters.

### 4. Candidate SQL (draft — verify against schema)

- Provide a starting dataset query built from the observed generated SQL, generalized to the
  primary grain. Always label it **draft — verify against the real schema**; Genie's generated SQL
  can be truncated (long queries) or reflect a one-off phrasing.
- Keep it a `GROUP BY` at the primary grain; leave secondary dimensions available for filtering.

Example (Revenue, region × month primary; category as filter):
```sql
-- DRAFT — verify table/column names against the schema. Source: conv #4471.
SELECT r.region,
       date_trunc('month', o.order_date) AS month,
       p.category,                         -- kept for the category filter control
       SUM(o.order_value)                  AS revenue
FROM sales.orders o
JOIN sales.regions r  ON o.region_id = r.id
JOIN sales.products p ON o.product_id = p.id
GROUP BY 1, 2, 3
ORDER BY 2, 1;
```

### 5. Widget suggestions

Map grain → visualization:
- **Time grain present** → time-series (line/area), series = primary dimension.
- **Categorical, no time** → bar chart ranked by the measure.
- **Single total** → counter / big-number widget.
- **Two dimensions** → the second dimension becomes series or a small-multiple, or a filter.
- Add filter widgets for every secondary grain and recurring filter identified in Level 2.

### 6. Promotion note (the payoff)

- State the expected load absorbed: tie to the group's ask count and distinct users
  ("~9 recurring asks/week from 5 users").
- **Feed it back to Genie:** recommend adding the dashboard link into the Genie agent's
  instructions/context so future users asking this metric are routed to the governed dashboard
  instead of the agent regenerating SQL each time. This is what actually shifts load off Genie.

## Prioritization for the report

- Default to the **top 5** metric groups by recurrence score; state the cutoff explicitly.
- Order the build guidance by recurrence, so the developer builds the highest-load dashboards first.
- If two grains within one group both rank high, present them as **one dashboard, two views**
  (primary grain + filter), not two separate builds.

## Going beyond 7 days (programmatic path — optional, not this skill's default)

Analyze Space Usage and the Weekly digest are fixed to the **last 7 days**. For a longer window or
a full question census, the demand data can be reconstructed programmatically — this is a
*different mechanism* and out of scope for the pass-one skill, but note it for the user:

- `system.access.audit` (`service_name = 'aibiGenie'`) gives **which** messages happened, at scale,
  with `space_id` / `conversation_id` / `message_id`, user email, and timestamps — but **no**
  question text or SQL.
- The **Genie Conversation API** supplies the **content** for those IDs: `message.content`
  (question), `attachments[].query.sql` (generated SQL), `query_description`, `status`.
- Join the two, then run the same bucketing rubric (`bucketing-guide.md`) over the full window.

The sibling `genie-weekly-monitor` skill already implements this audit + Conversation API pattern
(for problem-interaction RCA); reuse its query/endpoint reference if a longer window is needed.
Keep the same guardrails: rate-limit backoff, an enrichment cap, and no silent truncation.
