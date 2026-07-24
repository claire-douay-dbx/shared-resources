# Bucketing guide — semantic groups + aggregation grains

How the DS/analysis pass should cluster a week of Genie questions. Two levels: **semantic
group** (the metric) and, within it, **aggregation grain** (the cut). Get this split right and
the AI/BI mapping falls out almost mechanically.

## Level 1 — semantic groups (by metric intent)

Cluster questions by **what is being measured**, ignoring wording, tense, and politeness.

- Signals to read: the **measure/aggregate** in the generated SQL (`SUM`, `COUNT`, `AVG`,
  `COUNT(DISTINCT ...)`, a ratio), the noun in the question ("revenue", "users", "orders",
  "conversion"), and the fact table hit.
- Same metric, different phrasing → **one group**:
  - "total sales", "how much did we make", "sum of order value", "revenue last week" → **Revenue**
  - "how many people used it", "active users", "distinct logins" → **Active users**
- Different measure → **different group**, even on the same table: *Revenue* (`SUM(order_value)`)
  vs *Order count* (`COUNT(*)`) vs *Average order value* (`AVG(order_value)`) are three groups.
- Ratios/derived metrics are their own group: *Conversion rate*, *Refund rate*, *Churn %*.
- Name each group by the metric in plain business language. Keep it stable across the report.

**Do not** split a group by its breakdown at this level — breakdowns are Level 2. "Revenue by
region" and "revenue by product" are the *same* semantic group (Revenue) with two grains.

## Level 2 — aggregation grains (within a group)

A **grain** is the specific cut repeatedly asked, defined by three parts:

```
grain = (dimensions / group-bys)  ×  (time grain)  ×  (recurring filters)
```

- **Dimensions:** the `GROUP BY` columns that aren't time — region, product category, segment,
  channel, store, etc.
- **Time grain:** day / week / month / quarter / year, or "no time breakdown" (a single total).
- **Recurring filters:** `WHERE` predicates that show up repeatedly and change the meaning
  (e.g. "for enterprise customers", "excluding refunds", "in EMEA"). One-off filters are noise;
  a filter that recurs across users is part of the grain.

Enumerate the **distinct grains actually asked** within each group. Example, group **Revenue**:

| Grain | Dimensions | Time | Filters | Asks | Users |
|---|---|---|---|---|---|
| region × month | region | month | — | 9 | 5 |
| product category × day | product_category | day | — | 4 | 3 |
| total this week | — | none (single value) | current week | 2 | 2 |

Guidance:
- **Merge near-identical grains.** "revenue by region this month" and "monthly revenue per
  region" are the *same* grain — count them together.
- **Keep genuinely different grains apart.** Different dimension or different time grain = different
  grain = different row. This is the signal the developer needs.
- **Prefer SQL over question text** for grain when both exist — the `GROUP BY` is ground truth.
  When only the question text is available, infer the grain but mark it inferred.

## Ranking

Score each grain by **recurrence**:

```
recurrence_score = ask_count × distinct_users
```

- Rank grains across all groups by score for the "top metrics to promote" list.
- **Tiebreak on grain stability:** a grain asked identically by many users beats one where the
  same score comes from one power user asking repeatedly — the former generalizes to a dashboard,
  the latter may be a personal workflow. Note this when it affects ranking.
- **Distinct users matters as much as raw count.** A metric asked 3× by 3 different people is a
  better AI/BI candidate than one asked 6× by one person. Show both columns so the reasoning is visible.
- **Long tail:** anything at frequency 1 / single user goes into a short "one-offs" note, not the
  ranked list — but it is *listed as omitted*, never silently dropped.

## Sanity checks before finalizing

- Did every inspected question land in a group (or the one-offs note)? No orphans.
- Are any two "groups" actually the same metric with different names? Merge them.
- Are any two "grains" in a group actually identical after normalizing wording? Merge them.
- Does the top-ranked grain actually correspond to real repeated demand, or to one loud user?
  Re-read the distinct-user count.
