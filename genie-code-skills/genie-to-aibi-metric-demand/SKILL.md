---
name: genie-to-aibi-metric-demand
description: Mine a Genie agent's past-week queries to find recurring metric demand and turn it into an AI/BI (Lakeview) build plan. Rides on the Monitor tab's "Analyze Space Usage" (which launches Genie Code over the last 7 days), then buckets the week's questions into semantic groups, breaks each group out by the aggregation grain repeatedly asked, ranks by recurrence, and emits step-by-step AI/BI implementation guidance so recurring BI load moves off ad-hoc Genie asks onto pre-built AI/BI dashboards. Use when a developer asks to find repeat/recurring Genie questions, spot metrics to promote into AI/BI, review a Genie agent's weekly query patterns, or decide what dashboards to build from Genie usage.
---

# Genie → AI/BI metric demand

**Goal.** Find the metrics that Genie users ask for **repeatedly** over the past week, and hand
the developer a ranked, grain-aware list plus concrete steps to build those metrics in **AI/BI
(Lakeview)** — so recurring, expensive ad-hoc Genie load shifts onto governed, reusable
dashboards.

**How you run.** You are Genie Code, operating inside the Databricks workspace with the target
Genie agent open. The Monitor tab is UI-only — there is no API for the aggregated monitor view —
so this workflow drives the UI's built-in **"Analyze Space Usage"** entry point and then does
the metric-demand analysis on top of what it surfaces.

**What you produce.** A single report (markdown) with three sections:
1. **Recurring metric groups**, ranked, each broken out by aggregation grain.
2. **Top metrics to promote to AI/BI**, with the reasoning (frequency × distinct users × grain stability).
3. **Step-by-step AI/BI build guidance** per top metric — guidance only, you do **not** create assets.

---

## Workflow (5 steps)

### 1. Open the Monitor tab and run "Analyze Space Usage"

- Confirm which Genie agent (space) you're analyzing. This workflow is **per-agent** — Analyze
  Space Usage is scoped to one space. If the user names several, do them one at a time.
- In that agent, go to the **Monitor** tab → **Weekly digest** section → click **Analyze Space
  Usage**. This launches you (Genie Code) to review **the last seven days** of user messages,
  feedback, and issues, and reports common topics, recurring issues, and suggested context
  improvements — with **citations back to the conversations**.
- Also read the **Weekly digest** counters (weekly message volume, active users, 👍/👎) for
  context on how much of the load these recurring asks represent.
- **Anchor everything to the 7-day window.** If the user asks for a different window, say plainly
  that Analyze Space Usage / the digest are fixed to the last 7 days, and that a longer window
  needs the programmatic path (see `references/aibi-mapping.md` → "Going beyond 7 days").

### 2. Inventory the week's questions

- From the Analyze Space Usage output and the conversation citations, assemble the list of
  **user questions** asked in the window. Capture, per question: the question text, who asked
  (to count distinct users), and — where visible via "Show code" — the **generated SQL** or the
  entities/columns/filters involved. The SQL is the strongest signal for grain; use it when present.
- Note total questions found and roughly what fraction of weekly volume they represent, so the
  report can state coverage honestly. **Never silently drop questions** — if Analyze Space Usage
  summarizes rather than enumerates and you can't recover every question, say how many you
  actually inspected.

### 3. Bucket into semantic groups (the DS/analysis pass)

Cluster the questions by **what is being measured** — the metric intent — not by wording. Follow
the rubric in `references/bucketing-guide.md`. In short:

- A **semantic group** = questions that ask for the *same underlying metric* regardless of
  phrasing ("total sales", "how much revenue", "sum of order value" → one group: **Revenue**).
- Name each group by its metric (e.g. *Revenue*, *Active users*, *Order count*, *Conversion
  rate*, *Inventory on hand*).
- Keep groups at the metric level; the **aggregation grain** is handled in the next step, not by
  splitting groups here.

### 4. Break out aggregation grains and rank

This is the core of the deliverable. **Within each semantic group, separate the distinct
aggregation grains that were repeatedly asked** — because the grain is what tells you which AI/BI
cut to build.

- A **grain** = the combination of (dimensions / group-bys) × (time grain) × (recurring filters).
  Example, within **Revenue**:
  - *Revenue by region by month* — asked 9×, 5 users
  - *Revenue by product category by day* — asked 4×, 3 users
  - *Total revenue this week* (no breakdown) — asked 2×, 2 users
- **Rank** semantic groups, and grains within them, by a **recurrence score**:
  `frequency (question count) × distinct users`, with grain stability as a tiebreak (a grain asked
  the same way by many users is a stronger dashboard candidate than one-off phrasings). Show the
  raw numbers, not just the score — the developer needs to see why something ranked.
- Drop truly one-off asks (frequency 1, single user) from the "recurring" list, but keep a short
  **"long tail / one-offs"** note so nothing looks silently omitted.

Present this as a table:

| Rank | Metric (semantic group) | Aggregation grain (dims × time × filters) | Asks | Distinct users | Recurrence score | Citations |
|---|---|---|---|---|---|---|
| 1 | Revenue | region × month | 9 | 5 | 45 | [conv links] |
| 2 | Revenue | product category × day | 4 | 3 | 12 | [conv links] |
| 3 | Active users | segment × week | 6 | 4 | 24 | [conv links] |

### 5. Emit step-by-step AI/BI implementation guidance

For each **top** metric group (default: top 5 by recurrence — state the cutoff), produce a
concrete AI/BI build plan **as guidance only — do not create dashboards, datasets, or metric
views.** Follow `references/aibi-mapping.md`. Each plan includes:

1. **Target AI/BI asset** — a new or existing Lakeview dashboard, and whether this metric warrants
   a **metric view** (reusable definition) vs a dashboard dataset.
2. **Dataset / metric definition** — the measure (e.g. `SUM(order_value)`), the source table(s)
   inferred from the observed SQL, and the **dimensions + time grain** taken from the ranked grains
   (so the dashboard answers the recurring asks directly, including any secondary grains as
   filters/parameters).
3. **Candidate SQL** — a starting query for the dataset, based on the SQL Genie generated for
   these questions (cite it). Mark it **draft — verify against the schema** since observed SQL can
   be truncated.
4. **Widget suggestions** — how to lay out the recurring grains (e.g. a time-series for
   region × month, a bar for product category, filter controls for the secondary dimensions).
5. **Promotion note** — one line on the expected payoff: how much recurring Genie load this
   dashboard is expected to absorb (tie back to the ask counts / distinct users), and a note to
   add the dashboard link into the Genie agent's instructions so future users are routed to it.

Close the report with a short **"Feed it back to Genie"** note: once the AI/BI dashboards exist,
add references to them in the Genie agent's context/instructions so the agent points users at the
dashboard for these recurring metrics instead of regenerating SQL each time.

---

## Worked mini-example (shows the target shape)

> **Agent:** Sales Analytics. **Window:** last 7 days. **Weekly digest:** 210 messages, 24 users.
>
> **Recurring metric groups (ranked):**
>
> | Rank | Metric | Grain | Asks | Users | Score |
> |---|---|---|---|---|---|
> | 1 | Revenue | region × month | 9 | 5 | 45 |
> | 2 | Active users | segment × week | 6 | 4 | 24 |
> | 3 | Revenue | product category × day | 4 | 3 | 12 |
>
> One-offs (not promoted): "refund rate for SKU-2231 yesterday" (1 ask), a few ad-hoc lookups.
>
> **#1 Revenue by region by month → AI/BI build:**
> - *Asset:* new "Revenue Overview" Lakeview dashboard; create a **metric view** `revenue` so the
>   measure is reusable across dashboards.
> - *Definition:* `SUM(o.order_value)` from `sales.orders o` joined to `sales.regions r`;
>   dimensions `r.region`, time grain `month(o.order_date)`; product category available as a filter.
> - *Candidate SQL (draft — verify schema; from Genie's generated query, conv #4471):*
>   `SELECT r.region, date_trunc('month', o.order_date) AS month, SUM(o.order_value) AS revenue
>    FROM sales.orders o JOIN sales.regions r ON o.region_id = r.id GROUP BY 1,2 ORDER BY 2,1;`
> - *Widgets:* line chart revenue over month, series = region; region + category filter controls.
> - *Promotion note:* absorbs ~9 recurring asks/week from 5 users; add the dashboard link to the
>   Sales Analytics agent instructions so future "revenue by region" asks are routed there.

Use this as the format target. Real reports scale the number of groups to what the week shows.

---

## Guardrails (do not violate)

- **Guidance only — no asset creation.** You produce a build plan. You must **not** create,
  update, or deploy Lakeview dashboards, datasets, metric views, or modify the Genie agent's
  config. If the user wants you to build the dashboard, that's a separate, explicitly-confirmed
  request handled by a Lakeview/AI-BI skill — not this one.
- **Honesty about coverage.** State the window (7 days), how many questions you inspected vs the
  weekly volume, and flag when Analyze Space Usage summarized rather than enumerated. Never let a
  summary read as a full census. No silent truncation of the question list.
- **Grain is first-class.** Do not collapse different aggregation grains into one row — the whole
  point is telling the developer *which cut* to build. "Revenue by region by month" and "revenue
  by product by day" are separate ranked asks.
- **Draft SQL is draft.** Observed/generated SQL can be truncated or wrong. Always label candidate
  SQL as draft-to-verify against the real schema.
- **Privacy.** Questions and results can contain sensitive data. Keep the report to metric
  patterns and citations; do not copy sensitive row-level results into it.
- **One agent per run.** Analyze Space Usage is per-space; analyze one agent at a time and say so.

## Reference files

- `references/bucketing-guide.md` — the clustering rubric: how to form semantic groups by metric
  intent and how to enumerate aggregation grains within a group.
- `references/aibi-mapping.md` — how a ranked metric-group maps to an AI/BI build (metric view vs
  dashboard dataset, grain → widgets), and the programmatic path for windows beyond 7 days.

Read these when you need the precise rubric or mapping detail rather than reproducing it from memory.
