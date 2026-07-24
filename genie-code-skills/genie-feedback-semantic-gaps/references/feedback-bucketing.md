# Feedback bucketing guide — logical groups + severity

How the DS/analysis pass should cluster a week of Genie **feedback** (👍, 👎, review requests,
comments) into logical groups, and how to assign severity. Two tracks: **negative → gap types**
and **positive → win themes**. Get the grouping right and the fix playbook applies almost
mechanically.

## Inputs — all four feedback types

- **👎 thumbs-down** — strongest gap signal; the answer was wrong or unhelpful.
- **Review requests** — user explicitly flagged for human review; treat as a strong gap signal.
- **Comments** — the typed text on a rating/review request. Richest "why" signal; **UI-only**,
  no API. Read it closely — it usually names the actual defect ("this double counts", "wrong
  region", "should exclude refunds").
- **👍 thumbs-up** — positive; feeds the win track.

Count **distinct users** per group as well as raw frequency — a gap hit by many users generalizes;
one hit repeatedly by a single user may be a personal edge case.

## Track A — negative feedback → gap types (by semantic failure)

Cluster 👎 / review requests / negative comments by the **kind of semantic failure**, read from the
comment text and confirmed against the generated SQL (see step 4 of the skill). Standard gap types:

| Gap type | What it looks like | Typical root in the room's semantics |
|---|---|---|
| **Wrong table** | Answer pulled from the wrong source | Missing/weak instruction on which table is authoritative |
| **Wrong / missing filter** | Numbers too high/low; includes things it shouldn't | Undocumented business rule (e.g. exclude refunds) |
| **Missing / wrong join** | Double-counting, missing rows, wrong rollup | No join spec, or ambiguous relationship |
| **Terminology misread** | "MTD", "active", "churn" interpreted wrong | No synonym / value-dictionary entry for the term |
| **Ambiguous question handling** | Plausible but not what the user meant | No instruction on default assumptions |
| **Missing metric / column** | "I can't answer that" or a poor proxy | Metric/column not described or not present |
| **Wrong grain** | Right metric, wrong aggregation level | No guidance on default grain |
| **Calculation error** | Formula/derivation wrong | Metric definition not pinned in instructions |
| **Hallucinated object** | References a table/column that doesn't exist | Schema not well described |

Rules:
- **Cluster by root cause, not symptom.** Three different questions that all fail because refunds
  aren't excluded are **one** group ("Revenue includes refunds"), not three.
- **Use the comment text to disambiguate** which gap type applies — it usually says why.
- **Merge near-duplicates**, keep genuinely different failures apart. One group = one fix.

## Track B — positive feedback → win themes (protect + replicate)

Cluster 👍 (and positive comments) by the **topic/metric that's working well**. For each win group,
capture the **winning pattern** from its SQL (the correct table, join, filter, or example) so it can
be:
- **Protected** — flagged as semantics not to regress when other fixes land.
- **Replicated** — reused as the template for fixing a related weak area (e.g. a correct time-series
  pattern applied to a metric that's currently answered at the wrong grain).

Do not derive "fixes" for win groups — they're guardrails and templates, not problems.

## Severity rubric

Assign each **negative** group a severity. Severity is the tiebreak/lifter in prioritisation
(frequency is primary — see the skill's step 6).

- **Critical** — the answer is *flat wrong*: wrong numbers, wrong entity, hallucinated object,
  calculation error. A user acting on it would be misled. Wrong-filter and wrong-join gaps that
  change the numbers are usually critical.
- **Moderate** — right idea, wrong cut: wrong grain, missing secondary filter, terminology misread
  that's recoverable, ambiguous-handling. Useful but needs correction.
- **Minor** — cosmetic or phrasing: formatting, wording, a mild comment with no accuracy impact.

When a group spans severities, take the **highest** severity present (one critical wrong-number
report in a group makes the group critical).

## Sanity checks before finalizing

- Did every inspected feedback item land in a group (gap or win)? No orphans; list any omitted
  long-tail explicitly.
- Are two "gap groups" actually the same root cause under different names? Merge them.
- Is a group's severity driven by the *worst* real instance, not the average impression?
- Does a high frequency come from many distinct users or one loud user? Show both columns.
- For every win group, is there a protect note (and a replicate note where it applies)?
