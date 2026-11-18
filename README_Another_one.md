# Open-Source Project Health & Momentum Analytics (GH Archive)

DS-3001 Data Analysis and Visualization, 
Muhammad Yousaf (24L-2539), Shahryar Ehtisham (22L-8372).

An end-to-end data pipeline on **GH Archive** (every public GitHub event, one file per hour) built with the **Medallion architecture** on **Databricks Free Edition**, automated with **GitHub Actions**, and visualised in **Power BI**. It answers: which repositories are gaining attention, how activity changes over time, how contributors collaborate, which repositories show unusual surges, and whether popular projects are backed by real maintenance.

```
GH Archive (hourly .json.gz)
   |  GitHub Actions (every 6 hours): download, upload to the Volume, start the job
   v
Databricks job gharchive_pipeline (serverless)
   task 1 raw_to_bronze  ->  bronze_events (+ bronze_quarantine, schema_drift_log)
   task 2 bronze_to_silver ->  silver_event, silver_push, silver_issue, silver_pull_request (+ silver_quarantine)
   task 3 silver_to_gold  ->  Gold star schema: 4 dimensions, 4 facts
   every step logs to pipeline_execution_logs (+ gold_aggregation_metrics)
   v
SQL Warehouse  ->  Power BI dashboard (6 pages)
```

## Documentation by phase

| Phase | What it covers | Document |
|---|---|---|
| Phase 1 | Proposal, data discovery, sample profiling | [docs/PHASE1_PROPOSAL.md](docs/PHASE1_PROPOSAL.md) |
| Phase 2 | Bronze and Silver: setup, execution guide, data models, idempotency, drift, quarantine, PII | [docs/README_PHASE2.md](docs/README_PHASE2.md) |
| Phase 3 | Gold star schema, job task 3, Power BI dashboard, final report | [docs/PHASE3_GOLD_POWERBI.md](docs/PHASE3_GOLD_POWERBI.md) |
| Gold data dictionary | ER diagram, grain, keys, every column | [docs/GOLD_DATA_DICTIONARY.md](docs/GOLD_DATA_DICTIONARY.md) |
| Final report (PDF) | Architecture, decision log, DAG analysis, FinOps, dashboard findings | [docs/GHArchive_Phase3_Final_Report.pdf](docs/GHArchive_Phase3_Final_Report.pdf) |

## Repository layout

```
.github/workflows/gharchive_pipeline.yml   schedule + manual trigger
scripts/fetch_gharchive.py                 download from GH Archive, upload to the Volume
scripts/trigger_job.py                     start the Databricks job and wait for the result
src/profile_gharchive.py                   Phase 1 sample profiling script
notebooks/common.py                        shared names, explicit schemas, helpers
notebooks/00_setup.py                      create schema, volume, Bronze/Silver tables (run once)
notebooks/01_raw_to_bronze.py              job task 1
notebooks/02_bronze_to_silver.py           job task 2
notebooks/03_prove_it.py                   Phase 2 evidence: idempotency, backfill, drift, quarantine
notebooks/common_gold.ipynb                Gold names, business rules, helpers
notebooks/00b_setup_gold.ipynb             create Gold tables (run once)
notebooks/04_silver_to_gold.ipynb          job task 3
notebooks/05_gold_checks.ipynb             Phase 3 evidence: keys, reconciliation, OPTIMIZE, query plan
tests/test_local_transforms.py             local test of the Phase 2 data logic (plain PySpark)
powerbi/GHArchive_Dashboard.pbix           dashboard (contains a data snapshot)
powerbi/GHArchive_Dashboard.pbit           template (no data)
docs/                                      phase documents, data dictionary, final report, local test results
docs/Screenshots_evidence/                 screenshots of job runs, checks and dashboard
data_discovery_evidence.md                 Phase 1 data discovery evidence
data/samples/                              Phase 1 sample hourly files
```

## Quick start

1. Databricks: create a Git folder from this repository; check the catalog name (`workspace`).
2. Run `notebooks/00_setup`, then `notebooks/00b_setup_gold` (once).
3. Create the job `gharchive_pipeline` with three dependent notebook tasks: `01_raw_to_bronze`, `02_bronze_to_silver`, `04_silver_to_gold`.
4. Add GitHub secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`; run the workflow from the Actions tab.
5. Open `powerbi/GHArchive_Dashboard.pbix` in Power BI Desktop (works offline from the saved snapshot; refreshing needs the SQL warehouse and a token).

Full steps are in the Phase 2 and Phase 3 documents.