# genie-to-aibi-metric-demand

A **Genie Code** skill that mines a Genie agent's **past-week** queries to find **recurring
metric demand** and turns it into an **AI/BI (Lakeview) build plan** — so recurring, ad-hoc Genie
load moves onto governed, reusable dashboards.

It rides on the Monitor tab's built-in **"Analyze Space Usage"** (which launches Genie Code over
the last 7 days), then adds three things on top:
1. buckets the week's questions into **semantic metric groups**,
2. breaks each group out by the **aggregation grain** repeatedly asked, ranked by recurrence,
3. emits **step-by-step AI/BI implementation guidance** (guidance only — it does not build assets).

## What's in here

```
genie-metric-demand/
  SKILL.md                     # the workflow Genie Code follows (5 steps + worked example + guardrails)
  README.md                    # this file
  references/
    bucketing-guide.md         # clustering rubric: semantic groups + aggregation grains + ranking
    aibi-mapping.md            # ranked demand -> Lakeview build plan; plus the >7-day programmatic path
```

## Install (into Genie Code)

Genie Code loads skills from `~/.assistant/skills/<skill-name>/SKILL.md` in your Databricks
workspace. This folder is already named for the skill (`genie-to-aibi-metric-demand`), so it
imports as-is.

1. Clone this repo and copy the skill into your workspace files. Two ways:

   **a. Databricks CLI** (recommended) — from the repo root:
   ```bash
   databricks workspace import-dir \
     ./genie-code-skills/genie-to-aibi-metric-demand \
     /Workspace/Users/<you@example.com>/.assistant/skills/genie-to-aibi-metric-demand \
     --profile <your-cli-profile>
   ```
   (Needs the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) authenticated to your
   workspace: `databricks auth login --host <workspace-url>`.)

   **b. Workspace UI** — in your workspace, browse to `Users/<you>/.assistant/skills/`, create a
   folder named `genie-to-aibi-metric-demand`, and upload `SKILL.md` + the `references/` files
   into it.

   Either way the end state in the workspace is:
   ```
   ~/.assistant/skills/genie-to-aibi-metric-demand/
     SKILL.md
     references/bucketing-guide.md
     references/aibi-mapping.md
   ```
2. Add a custom instruction so Genie Code loads it for relevant asks (prompt-to-genie pattern),
   e.g.: *"When asked to find recurring Genie questions or which metrics to promote into AI/BI,
   always load first: `~/.assistant/skills/genie-to-aibi-metric-demand/SKILL.md`."*
3. Invoke by asking Genie Code, with the target agent open, something like:
   *"Find the recurring metrics my users asked this week and tell me what to build in AI/BI."*

## Prerequisites (human)

- **`CAN MANAGE` on the target Genie agent** — required to see the **Monitor** tab and use
  **Analyze Space Usage**. Without it the workflow can't start.
- The agent must have had **real usage in the last 7 days** — the analysis is only as good as the
  week's traffic. A quiet week yields a thin report; that's expected, not a bug.

## Verification / testing (run these to confirm it works)

This skill's runtime is **UI-only** — it executes inside Genie Code in the workspace, so it can't
be tested from an external CLI. Verify it manually:

1. **Launch check.** With the target agent open, run the skill. Confirm it opens the **Monitor
   tab → Weekly digest → Analyze Space Usage** and reports over the **last 7 days**. *Good:* it
   cites the 7-day window and reads the digest counters.
2. **Inventory honesty.** Confirm the report states how many questions it inspected vs the weekly
   message volume, and flags if Analyze Space Usage summarized rather than enumerated. *Good:* no
   claim of a "full census" without the coverage caveat.
3. **Bucketing sanity.** Check that same-metric/different-wording questions landed in one group,
   and that distinct aggregation grains within a group are shown as **separate rows** (not
   collapsed). *Good:* "revenue by region by month" and "revenue by product by day" appear
   separately. See `references/bucketing-guide.md`.
4. **Ranking transparency.** Confirm the ranked table shows raw **asks** and **distinct users**,
   not just a score, so you can see why something ranked. *Good:* a metric asked by many users
   outranks one asked repeatedly by a single power user.
5. **AI/BI guidance actionability.** For a top metric, confirm the plan names a target dashboard,
   a measure/metric-view, dimensions + time grain from the ranked grain, **draft** candidate SQL
   (labeled draft-to-verify), widget suggestions, and a promotion note. *Good:* you could hand the
   plan to a developer and they'd know what to build.
6. **Guardrails held.** Confirm it produced **guidance only** — it did not create/modify any
   Lakeview dashboard, dataset, metric view, or the Genie agent's config.

## Scope & limits

- **Per-agent, per-week.** Analyze Space Usage is scoped to one space and a fixed 7-day window.
  Analyze multiple agents one at a time. For longer windows or a full question census, use the
  programmatic audit + Conversation API path described in `references/aibi-mapping.md`
  ("Going beyond 7 days") — a different mechanism, out of scope for this skill's default flow.
- **Guidance, not automation.** Output is a build plan. Creating the AI/BI assets is a separate,
  explicitly-confirmed step handled by a Lakeview/AI-BI skill.
- **Observed SQL can be truncated or one-off** — candidate SQL is always draft-to-verify.

## Related

- Docs: Genie agent monitoring — Monitor tab, Weekly digest, Analyze Space Usage
  (`https://docs.databricks.com/aws/en/genie-agents/monitor`).
- Sibling skill `genie-weekly-monitor` — the programmatic audit + Conversation API RCA pipeline
  this skill borrows from for the >7-day path.
- Pattern reference: `prompt-to-genie` (`github.com/sean-zhang-dbx/prompt-to-genie`) — the
  Genie-Code-native skill format this follows.
```
