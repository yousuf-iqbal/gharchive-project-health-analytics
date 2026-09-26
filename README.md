# Open-Source Project Health & Momentum Analytics Using GH Archive

## Project Overview

This project is a data engineering and analytics pipeline for studying public open-source software activity recorded by **GH Archive**.

GH Archive provides GitHub public activity as hourly compressed JSON archives. The project will use these archives to build an **Apache Spark** pipeline following a Bronze, Silver, and Gold Medallion architecture. The final analytical datasets will support an interactive **Power BI** dashboard focused on repository activity, collaboration, momentum, event composition, and unusual activity patterns.

## Project Objectives

The project aims to:

* Ingest real-world GitHub activity data from GH Archive.
* Process a bounded historical baseline as a full load.
* Process newly available hourly archives as incremental batches.
* Build Bronze, Silver, and Gold data layers using Apache Spark.
* Produce analytical datasets for repository, contributor, pull request, issue, review, and event-level analysis.
* Develop a Power BI dashboard based on the resulting Gold tables.

## Data Source

**GH Archive:** https://www.gharchive.org/

GH Archive provides public GitHub timeline activity in hourly JSON archives.

Archive format:

```text
https://data.gharchive.org/YYYY-MM-DD-H.json.gz
```

Each hourly archive is treated as an append-oriented ingestion unit. The project does not model GH Archive as a conventional transactional source with updates and deletes.

## Phase 1 Data Evidence

Three consecutive hourly archives were collected and profiled during Phase 1:

| Sample               |      Events | Compressed Size | Uncompressed Size |
| -------------------- | ----------: | --------------: | ----------------: |
| 2026-09-25 10:00 UTC |      84,754 |        14.11 MB |          66.74 MB |
| 2026-09-25 11:00 UTC |      89,852 |        14.20 MB |          67.65 MB |
| 2026-09-25 12:00 UTC |      93,655 |        14.73 MB |          70.54 MB |
| **Total**            | **268,261** |    **43.04 MB** |     **204.93 MB** |

The observed average was approximately **89,420 events/hour** and **14.35 MB compressed/hour**. Based on these samples, the planning estimate is approximately **344 MB compressed per day**, **2.41 GB per week**, and **10.3 GB over 30 days**.

These figures are estimates from the sampled hours and will be reassessed before a larger historical load.

## Pipeline Architecture

```text
GH Archive hourly JSON
        |
        v
Bronze
Raw source archives
        |
        v
Silver
Cleaned common event table
+ event-specific tables
        |
        v
Gold
Analytical aggregate tables
        |
        v
Power BI
```

### Bronze

Raw GH Archive files will be preserved for traceability and reproducibility. Ingestion metadata will be maintained to prevent the same hourly source from being processed more than once.

### Silver

A common event table will provide consistent analytical fields such as event ID, event type, timestamp, repository, actor, organization, and ingestion hour.

Event-specific Silver tables will be used for areas such as:

* Pull requests
* Issues
* Issue comments
* Reviews
* Releases

The Silver layer will also handle type casting, normalization, duplicate protection, and data minimization.

### Gold

Planned analytical tables include:

* `gold_repo_activity`
* `gold_repo_momentum`
* `gold_contributor_activity`
* `gold_event_mix`
* `gold_pull_request_activity`
* `gold_issue_activity`
* `gold_review_activity`
* `gold_activity_anomalies`

The exact fields and aggregations will be finalized during implementation after profiling the available data.

## Power BI Dashboard & Business Questions

The final Power BI dashboard will present a high-level view of open-source project activity and momentum using the Gold-layer datasets.

The dashboard will focus on questions such as:

* Which repositories show the highest levels of activity during the observation period?
* How does repository activity change over time?
* What types of GitHub events make up the majority of project activity?
* Which repositories show notable changes or unusual activity patterns?
* How do pull request, issue, review, and contributor activities vary across repositories?
* Which contributors are most active within the observed dataset?

Planned dashboard elements include repository activity trends, event-type distribution, contributor activity, pull request and issue metrics, and indicators of unusual activity. Interactive filters will allow users to explore the results by repository, time period, and event type where appropriate.

## Technology Stack

* Apache Spark
* Databricks Free Edition
* Python
* Git / GitHub
* Power BI
* GH Archive
