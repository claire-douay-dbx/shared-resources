# genie-feedback-semantic-gaps

A **Genie Code** skill that mines a Genie agent's **past-week feedback** — 👍, 👎, review
requests, and comments — to find where the room's **semantics are weak**, and hands the developer
a **prioritised, fix-oriented action list** to improve answer quality and accuracy.

Sibling to `genie-to-aibi-metric-demand`. Where that skill mines *what users ask* (to feed AI/BI),
this one mines *how users react* (to fix the room). It rides on the Monitor tab's built-in
**"Analyze Space Usage"** (which reviews the last 7 days of messages, feedback, and issues), then:
1. collects all four feedback types,
2. buckets them (positive + negative) into **logical groups**,
3. reviews the **generated SQL** behind each group,
4. drafts a **concrete semantic fix** per gap group,
5. prioritises by **frequency + severity**,
6. emits **ranked developer next-steps** (plus a "protect what's working" list).

Output is **guidance only** — it drafts fixes; it does not modify the agent.

## What's in here

```
genie-feedback-gaps/
  SKILL.md                        # the 6-step workflow (+ worked example + guardrails)
  README.md                       # this file
  references/
    feedback-bucketing.md         # cluster all 4 feedback types into gap types + win themes; severity rubric
    semantic-fix-playbook.md      # gap type -> concrete Genie remedy; plus the >7-day programmatic path
```

## Install (into Genie Code)

Genie Code loads skills from `~/.assistant/skills/<skill-name>/SKILL.md` in your Databricks
workspace. This folder is already named for the skill (`genie-feedback-semantic-gaps`), so it
imports as-is.

1. Clone this repo and copy the skill into your workspace files. Two ways:

   **a. Databricks CLI** (recommended) — from the repo root:
   ```bash
   databricks workspace import-dir \
     ./genie-code-skills/genie-feedback-semantic-gaps \
     /Workspace/Users/<you@example.com>/.assistant/skills/genie-feedback-semantic-gaps \
     --profile <your-cli-profile>
   ```
   (Needs the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) authenticated to your
   workspace: `databricks auth login --host <workspace-url>`.)

   **b. Workspace UI** — in your workspace, browse to `Users/<you>/.assistant/skills/`, create a
   folder named `genie-feedback-semantic-gaps`, and upload `SKILL.md` + the `references/` files
   into it.

   Either way the end state in the workspace is:
   ```
   ~/.assistant/skills/genie-feedback-semantic-gaps/
     SKILL.md
     references/feedback-bucketing.md
     references/semantic-fix-playbook.md
   ```
2. Add a custom instruction so Genie Code loads it for relevant asks, e.g.: *"When asked to review
   Genie feedback or find quality/accuracy gaps in a Genie room, always load first:
   `~/.assistant/skills/genie-feedback-semantic-gaps/SKILL.md`."*
3. Invoke by asking Genie Code, with the target agent open, e.g.: *"Review this week's feedback and
   tell me the biggest semantic gaps to fix, prioritised."*

## Prerequisites (human)

- **`CAN MANAGE` on the target Genie agent** — required to see the **Monitor** tab, comment text,
  and use **Analyze Space Usage**. Without it the workflow can't start.
- The agent must have had **feedback in the last 7 days** — the analysis is only as good as the
  week's ratings/comments. A week with no 👎/comments yields a thin gap list; that's expected.

## Verification / testing (run these to confirm it works)

This skill's runtime is **UI-only** — it runs inside Genie Code in the workspace and can't be
tested from an external CLI. Verify it manually:

1. **Launch check.** With the target agent open, run the skill. Confirm it opens **Monitor →
   Weekly digest → Analyze Space Usage** over the **last 7 days** and reports the feedback overview
   (👍 / 👎 / review requests / comment counts).
2. **All four types collected.** Confirm it captured 👍, 👎, review requests, **and comment text** —
   not just thumbs-down. *Good:* comment text is quoted where it explains a gap.
3. **Bucketing sanity.** Check that feedback is grouped by **root cause** (same underlying defect =
   one group), with distinct gap types, and that positives are grouped as wins. See
   `references/feedback-bucketing.md`.
4. **SQL reviewed per group.** Confirm each negative group cites and diagnoses the generated SQL
   (or states the SQL was truncated/unavailable). *Good:* the fix names the actual defect in the SQL.
5. **Fixes are concrete + drafted.** For a top gap, confirm the fix is paste-ready (instruction /
   example SQL labeled draft-to-verify / synonym / join spec / comment), tied to the citing
   conversation. See `references/semantic-fix-playbook.md`.
6. **Prioritisation transparent.** Confirm the ranked table shows **frequency AND severity** (not
   an opaque score), frequency is the primary sort, and a rare-but-critical gap can outrank a
   frequent-but-minor one.
7. **Positives handled.** Confirm the report includes a "protect (working well)" list and, where
   relevant, replicates a winning pattern onto a weak area.
8. **Guardrails held.** Confirm it produced **guidance only** — it did not modify the agent's
   instructions, config, or metadata.

## Scope & limits

- **Per-agent, per-week.** Analyze Space Usage is scoped to one space and a fixed 7-day window.
  Analyze multiple agents one at a time.
- **Comment text is UI-only.** There is no API for the comment text that explains *why* users
  flagged answers — this skill's UI path is the only way to read it. For windows > 7 days, a
  programmatic path can recover *which* messages got 👎/review requests and their SQL, but **not**
  the comment text (see `references/semantic-fix-playbook.md` → "Going beyond 7 days").
- **Guidance, not automation.** Output is drafted fixes. Applying them to the agent is a separate,
  developer-reviewed step.
- **Observed SQL can be truncated or one-off** — candidate/example SQL is always draft-to-verify.

## Related

- Docs: Genie agent monitoring — Monitor tab, Weekly digest, Analyze Space Usage
  (`https://docs.databricks.com/aws/en/genie-agents/monitor`).
- Sibling skill `genie-to-aibi-metric-demand` — mines *what users ask* to feed AI/BI dashboards.
- Sibling skill `genie-weekly-monitor` — the programmatic audit + Conversation API RCA pipeline
  used for the >7-day path (and the source of the comment-text-is-UI-only boundary).
- Pattern reference: `prompt-to-genie` (`github.com/sean-zhang-dbx/prompt-to-genie`) — the
  Genie-Code-native skill format this follows.
```
