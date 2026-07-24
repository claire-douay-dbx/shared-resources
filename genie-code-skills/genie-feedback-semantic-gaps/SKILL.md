---
name: genie-feedback-semantic-gaps
description: Mine a Genie agent's past-week user feedback (thumbs up, thumbs down, review requests, and comments) to find where the room's semantics are weak, and hand the developer a prioritised, fix-oriented action list to improve answer quality and accuracy. Rides on the Monitor tab's "Analyze Space Usage" (which reviews the last 7 days of messages, feedback, and issues), then buckets the feedback (positive and negative) into logical groups, reviews the generated SQL behind each group, drafts concrete semantic fixes (instruction text, example SQL, synonyms / value-dictionary entries, join specs, column comments), prioritises by frequency + severity, and emits ranked developer next-steps. Use when a developer asks to review Genie feedback, find quality/accuracy gaps in a Genie room, act on thumbs-down or review requests, improve Genie semantics, or decide what to fix in an agent based on the monitoring tab.
---

# Genie feedback → semantic gap fixes

**Goal.** Turn a week of Genie **feedback** (👍 / 👎 / review requests / comments) into a
prioritised list of **semantic gaps** in the agent, each with a concrete drafted fix — so the
developer knows exactly what to improve to raise answer quality and accuracy.

**How you run.** You are Genie Code, operating inside the Databricks workspace with the target
Genie agent open. The Monitor tab is UI-only — there is no API for the aggregated feedback view,
and **comment text is not retrievable programmatically at all** — so this workflow drives the
UI's built-in **"Analyze Space Usage"** entry point (which reviews the last 7 days of messages,
**feedback, and issues**) and does the gap analysis on top of what it surfaces.

**What you produce.** A single report (markdown) with four sections:
1. **Feedback overview** — the week's 👍 / 👎 / review-request / comment counts (from the digest).
2. **Logical groupings**, positive and negative, each with the semantics behind it and — for gaps —
   a review of the generated SQL.
3. **Prioritised gaps** ranked by **frequency + severity**, each with a **drafted semantic fix**.
4. **Ranked developer next-steps** — what to change first, and the "what's working" to protect.

**Guardrail up front:** you draft fixes. You do **not** modify the agent's configuration, apply
instructions, or edit anything live. See Guardrails.

---

## Workflow (6 steps)

### 1. Open the Monitor tab and run "Analyze Space Usage"

- Confirm which Genie agent (space) you're analyzing. This workflow is **per-agent** — analyze one
  at a time.
- In that agent, go to the **Monitor** tab → **Weekly digest** → click **Analyze Space Usage**.
  This launches you (Genie Code) to review **the last seven days** of user messages, **feedback,
  and issues**, reporting common topics, recurring issues, and suggested context improvements,
  with **citations back to the conversations**.
- Read the **Weekly digest** counters (message volume, active users, 👍/👎 totals) for the
  feedback overview and to gauge how much of the week each gap represents.
- **Anchor to the 7-day window.** If asked for a longer window, say plainly that Analyze Space
  Usage / the digest are fixed to 7 days, and that comment *text* is UI-only even via API
  (see `references/semantic-fix-playbook.md` → "Going beyond 7 days").

### 2. Collect all four feedback types

For the window, assemble every piece of feedback, not just the negative:

- **👎 thumbs-down** — the strongest gap signal.
- **Review requests** — a user explicitly flagged the answer for a human.
- **Comments** — the *typed text* on ratings/review requests. This is the richest signal for
  *why* something was wrong, and it is only available here in the UI. Capture it verbatim where visible.
- **👍 thumbs-up** — what's working (see step 3's positive handling).

Per feedback item, capture: the question, the feedback type + any comment text, who gave it (to
count distinct users), and — via **"Show code"** / the conversation citation — the **generated
SQL**. **Never silently drop feedback**: if Analyze Space Usage summarizes rather than enumerates,
state how many items you actually inspected vs the digest totals.

### 3. Bucket into logical groupings (the DS/analysis pass)

Cluster the feedback into **logical groups** by the underlying semantic issue (for gaps) or theme
(for wins) — not by wording. Follow `references/feedback-bucketing.md`. In short:

- **Negative groups = gap types.** Cluster 👎 / review requests / negative comments by the *kind of
  semantic failure*: wrong table, wrong/missing filter, missing or wrong join, misunderstood
  business terminology, ambiguous question handling, missing metric/column, wrong grain,
  hallucinated object, calculation error, missing context/instruction.
- **Positive groups = wins.** Cluster 👍 (and positive comments) by topic/metric that's working well.
- Name each group by its issue or win in plain language. Keep names stable through the report.
- Assign each group a **severity** (see the rubric in `references/feedback-bucketing.md`):
  *critical* (flat-wrong answer / wrong numbers), *moderate* (right idea, wrong cut or missing
  filter), *minor* (phrasing, formatting, cosmetic). Severity drives prioritisation in step 6.

### 4. Review the generated SQL per group

For each **negative** group, inspect the **generated SQL** behind its flagged messages to pinpoint
the *actual* semantic defect — this is what makes the fix concrete rather than a guess:

- Read the `FROM`/`JOIN`/`WHERE`/`GROUP BY` against what the question asked. Identify the specific
  defect: wrong source table, missing join, incorrect filter value, wrong column, wrong grain, a
  business term mapped to the wrong field, etc.
- Note when SQL is **truncated or missing** — say so; don't infer a defect you can't see.
- For each **positive** group, note *what the SQL did right* (the winning table/join/filter/example)
  so it can be replicated on weak areas (step 5).

### 5. Draft a semantic fix per group

For each negative group, draft a **concrete** fix using `references/semantic-fix-playbook.md`,
which maps each gap type to a specific Genie semantic remedy. A fix is one or more of:

- **Instruction text** — general guidance to add to the agent (e.g. "'active user' means a user
  with ≥1 session in the period; use `fact_sessions`, not `dim_users`.").
- **Example SQL (SQL example / query)** — a curated correct query for the recurring question,
  labeled **draft — verify against schema**.
- **Synonyms / value-dictionary entries** — map the business terms users actually used to the
  right columns/values (e.g. "revenue" → `orders.order_value`; "EMEA" → region codes).
- **Join specs** — the correct join path when the model kept missing or mis-joining it.
- **Column / table comments (metadata)** — descriptions that remove the ambiguity that caused the miss.

**Protect + replicate the positives.** For each win group, add a **"protect"** note (the semantics
behind it must not regress when other fixes land) and, where relevant, **reuse its winning pattern**
(good example SQL, clear instruction) as the template for fixing a related weak area.

### 6. Prioritise and emit ranked next-steps

Rank the **negative** groups by **frequency + severity**:

- **Primary sort: frequency** — `count of feedback items in the group` (weight 👎 and review
  requests heavier than a mild comment if you like, but state how you counted).
- **Severity breaks ties and can lift a rare-but-bad gap** above a frequent-but-minor one — a
  *critical* group (wrong numbers) with 2 hits outranks a *minor* phrasing group with 5.
- **Show both numbers** (frequency and severity) in the table so the developer sees *why* something
  ranked where it did — never just a black-box score.

Present the ranked gaps as a table, then the next-steps:

| Rank | Gap group | Gap type | Freq | Distinct users | Severity | Drafted fix (summary) | Citations |
|---|---|---|---|---|---|---|---|
| 1 | "Revenue excludes refunds" | wrong/missing filter | 6 | 4 | critical | add instruction + example SQL excluding refunds | [conv links] |
| 2 | "Region rollup wrong" | missing join | 4 | 3 | moderate | add join spec dim_region → fact_orders | [conv links] |

Then a **"Do this first"** ordered list of developer actions tied to the drafted fixes, and a
**"Protect (working well)"** short list from the positive groups. Close with a one-line note to
**re-run this weekly** to confirm 👎 on fixed groups drops.

---

## Worked mini-example (shows the target shape)

> **Agent:** Sales Analytics. **Window:** last 7 days.
> **Feedback overview:** 210 messages · 👍 38 · 👎 11 · review requests 4 · comments 9.
>
> **Prioritised gaps:**
>
> | Rank | Gap group | Type | Freq | Users | Severity | Fix |
> |---|---|---|---|---|---|---|
> | 1 | Revenue includes refunds | wrong/missing filter | 6 | 4 | critical | instruction + example SQL |
> | 2 | Region totals double-count | missing join | 4 | 3 | moderate | join spec |
> | 3 | "MTD" misread as calendar month | terminology | 2 | 2 | moderate | synonym + instruction |
>
> **#1 Revenue includes refunds (critical, 6 asks, 4 users):**
> - *SQL review (conv #5120):* `SUM(order_value)` over `sales.orders` with **no** refund filter →
>   refunded orders inflate revenue. Users commented "this is too high vs finance".
> - *Drafted fix:*
>   - **Instruction:** "Revenue excludes refunded/cancelled orders. Always filter
>     `order_status NOT IN ('refunded','cancelled')` unless the user explicitly asks for gross."
>   - **Example SQL (draft — verify schema):**
>     `SELECT SUM(order_value) AS revenue FROM sales.orders WHERE order_status NOT IN ('refunded','cancelled');`
>   - **Synonym:** map "revenue"/"net revenue" → this filtered measure.
>
> **Do this first:** 1) add the revenue-refund instruction + example SQL; 2) add the region join
> spec; 3) add the MTD synonym.
> **Protect (working well):** "orders by day" answers (7 👍) — the `date_trunc('day', order_date)`
> pattern is correct; reuse it as the template for other time-series asks. Don't regress it.

Use this as the format target. Scale the number of groups to what the week shows.

---

## Guardrails (do not violate)

- **Guidance only — no config changes.** You draft instructions, example SQL, synonyms, join
  specs, and comments. You must **not** call the management API or otherwise modify the agent's
  configuration, instructions, or metadata. Applying the fixes is the developer's reviewed step.
- **Comment text is sensitive and UI-only.** Quote comments only as needed to justify a gap; do
  not copy sensitive row-level results into the report. There is no API for comment text — don't
  claim one.
- **Honesty about coverage.** State the 7-day window, how many feedback items you inspected vs the
  digest totals, and flag when Analyze Space Usage summarized rather than enumerated. No silent
  truncation; no summary passed off as a full census.
- **Frequency first, severity as tiebreak/lifter — and always show both.** Never present a ranking
  as an opaque score. The developer must see the frequency and severity that produced each rank.
- **Draft SQL is draft.** Generated/observed SQL can be truncated or one-off; every example SQL is
  labeled draft-to-verify against the real schema. Don't diagnose a defect from SQL you couldn't see.
- **Positives are handled, not ignored.** Every run must include the protect/replicate treatment of
  👍 groups, not only the gaps.
- **One agent per run.** Analyze Space Usage is per-space.

## Reference files

- `references/feedback-bucketing.md` — how to cluster all four feedback types into logical groups
  (gap types + win themes) and the severity rubric.
- `references/semantic-fix-playbook.md` — gap-type → concrete Genie semantic remedy (instruction,
  example SQL, synonym/value-dictionary, join spec, column comment), plus the >7-day programmatic path.

Read these when you need the precise rubric or remedy detail rather than reproducing it from memory.
