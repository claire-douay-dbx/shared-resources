# Genie Code skills

Two **Genie Code** skills for monitoring a Databricks Genie agent. Both run *inside* Genie Code
in the Databricks workspace (not Claude Code — different runtime), following the
[`prompt-to-genie`](https://github.com/sean-zhang-dbx/prompt-to-genie) skill format: a `SKILL.md`
workflow plus `references/`, loaded from `~/.assistant/skills/<skill-name>/`.

Both ride on the Monitor tab's built-in **"Analyze Space Usage"** (which reviews the last 7 days
of the agent's activity) and layer analysis on top. They are **guidance only** — they draft
findings and plans; they never modify the agent, dashboards, or any config.

| Skill | Mines | Produces |
|---|---|---|
| [`genie-feedback-semantic-gaps`](./genie-feedback-semantic-gaps) | the week's **feedback** (👍 / 👎 / review requests / comments) | a prioritised list of semantic gaps, each with a concrete drafted fix |
| [`genie-to-aibi-metric-demand`](./genie-to-aibi-metric-demand) | the week's **queries** | recurring metric demand ranked into an AI/BI (Lakeview) build plan |

They're siblings: one mines *how users react* (to fix the room), the other *what users ask* (to
promote recurring load onto governed dashboards).

## Install

Each skill folder is named for its skill, so it imports as-is. From the repo root, using the
[Databricks CLI](https://docs.databricks.com/dev-tools/cli/) authenticated to your workspace:

```bash
databricks workspace import-dir \
  ./genie-code-skills/genie-feedback-semantic-gaps \
  /Workspace/Users/<you@example.com>/.assistant/skills/genie-feedback-semantic-gaps \
  --profile <your-cli-profile>
```

(Repeat for `genie-to-aibi-metric-demand`.) See each skill's own `README.md` for the full install
options, prerequisites, invocation examples, and a manual verification checklist.

## Prerequisites

- **`CAN MANAGE`** on the target Genie agent (required to see the Monitor tab and run Analyze
  Space Usage).
- The agent must have had real usage/feedback in the **last 7 days** — the analysis is only as
  good as the week's traffic.
