# shared-resources

A collection of standalone Databricks resources — demos, deployable assets, and utilities. Each
top-level folder is an independent project with its own docs.

## Contents

| Project | What it is |
|---|---|
| [`arch-day`](./arch-day) | Scripts + sample CSVs to set up a retail data model for a Genie Agent workshop. See `SETUP_GUIDE.md`. |
| [`ash-mcp`](./ash-mcp) | Data-standards and sensitive-data-policy references plus an SDP SQL template. |
| [`bi-monitoring-suite`](./bi-monitoring-suite) | Deployable Databricks Asset Bundle that monitors BI usage across Genie Agents and AIBI dashboards — adoption, cost, quality, freshness. Base-metrics notebook + twice-daily serverless job + metric view + Lakeview dashboard. Portable across AWS and Azure. |
| [`genie-code-skills`](./genie-code-skills) | Two Genie Code skills for monitoring a Genie agent: `genie-feedback-semantic-gaps` (feedback → drafted semantic fixes) and `genie-to-aibi-metric-demand` (queries → AI/BI build plan). |
| [`metadata-app`](./metadata-app) | Unity Catalog Metadata Editor — a Databricks App for editing UC metadata, with an approval workflow and permissions model. See its `README.md` / `QUICKSTART.md`. |
| [`ontos-deployment`](./ontos-deployment) | Deployment reference — endpoints to whitelist. |

See each project's own folder for setup and usage instructions.
