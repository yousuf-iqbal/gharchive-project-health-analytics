# Phase 3: Gold layer, automation and Power BI

Back to the [main README](../README.md). Full report: [GHArchive_Phase3_Final_Report.pdf](GHArchive_Phase3_Final_Report.pdf).

## Gold star schema

Built in `workspace.gharchive` by job task 3 (`silver_to_gold`, depends on `bronze_to_silver`) at the end of every pipeline run. Full refresh with `INSERT OVERWRITE`, so a rerun always gives the same result.

| Table | Type | Grain (one row per) | Key |
|---|---|---|---|
| dim_date | dimension | calendar day, with hours loaded and a complete-day flag | date_key |
| dim_hour | dimension | hour of day (UTC), with Pakistan hour | hour_key |
| dim_event_type | dimension | GitHub event type, with business category | event_type |
| dim_repo | dimension | tracked repository (3+ contributors or 3+ community events) | repo_id |
| fact_activity_hourly | fact | day x hour x event type, all repositories | date_key, hour_key, event_type |
| fact_daily_summary | fact | day, all repositories | date_key |
| fact_repo_daily | fact | tracked repo x day: counts by type, PRs opened/merged/closed, issues opened/closed | date_key, repo_id |
| fact_repo_trend | fact | tracked repo x day: 7-day baseline, z-score, momentum, surge type (Spike/Breakout) | date_key, repo_id |

ER diagram and every column: [GOLD_DATA_DICTIONARY.md](GOLD_DATA_DICTIONARY.md). Optimization: Liquid Clustering on `dim_repo(repo_id)`, `fact_activity_hourly(date_key)`, `fact_repo_daily` and `fact_repo_trend(date_key, repo_id)`.

## Notebooks

| Notebook | Purpose |
|---|---|
| `notebooks/common_gold.ipynb` | Gold table names, business rules (tracked repos, complete days, surge thresholds), helper functions |
| `notebooks/00b_setup_gold.ipynb` | Run once: creates the Gold tables with explicit types and clustering |
| `notebooks/04_silver_to_gold.ipynb` | Job task 3: builds all Gold tables; logs to `pipeline_execution_logs` (layer `Silver-to-Gold`) and `gold_aggregation_metrics` |
| `notebooks/05_gold_checks.ipynb` | Evidence: 0 duplicate keys, 0 orphan foreign keys, reconciliation with Silver, OPTIMIZE, query plan, surge and momentum checks, run times |

Run order the first time: `00b_setup_gold`, then `04_silver_to_gold` (or the whole job), then `05_gold_checks`.

## Power BI dashboard

`powerbi/gharchive_dashboard.pbix` (template: `powerbi/gharchive_dashboard.pbit`), connected to the Databricks Serverless SQL Warehouse in Import mode. The model reflects the star schema (dimension 1 to fact many).

| Page | Question |
|---|---|
| 1 Overview | How does activity change over time? KPIs, push vs other events per day, categories, weekday x hour heatmap |
| 2 Repositories | Which repositories gain attention? Fastest growing (7 vs 7 days), most starred, most active |
| 3 Collaboration | Pull requests opened vs merged, issues opened vs closed, contributors per day |
| 4 Surges | Spikes and breakouts against each repo's own 7-day baseline; contributor slider separates bots from communities |
| 5 Hype vs Health | Stars (attention) vs work events (maintenance); watch list of popular but fragile repos |
| 6 Pipeline health | Audit and error logs: runs, rows per layer, quarantine reasons, schema drift, compute minutes |

The .pbix contains a data snapshot (to 18 Nov 2026) and opens offline. To refresh: start the SQL warehouse, then Refresh and sign in with a Databricks personal access token.

## Phase 1 plan vs final Gold model

Phase 1 listed candidate Gold tables (`gold_repo_activity`, `gold_repo_momentum`, `gold_event_mix`, `gold_activity_anomalies`, and others). After profiling the data they were consolidated into a star schema: repo activity is `fact_repo_daily`, momentum and anomalies are `fact_repo_trend`, event mix is `fact_activity_hourly` with `dim_event_type`, and pull request and issue activity are columns of `fact_repo_daily`. Separate review and contributor-level tables were not built (reviews are counted by event type; contributor identities are hashed for privacy).

## Data findings

* On several days in October and November the GH Archive source files contain almost no PushEvents (confirmed in Bronze, which stores the files unchanged). Surge detection therefore uses community events only, and the Overview shows pushes separately.
* GH Archive reports a merge as its own pull-request action (`merged`), so Gold counts `prs_merged` separately from `prs_closed`.
* Stars are not contributors: stargazers appear as actors, so repository health is measured with work events (pushes, pull requests, issues).
