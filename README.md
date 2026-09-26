# Open-Source Project Health & Momentum Analytics

DS-3001 Data Analysis and Visualization - Phase 1

## Contents
- `docs/phase1_proposal.docx`
- `data/samples/` - three real GH Archive hourly `.json.gz` samples
- `src/profile_gharchive.py` - reproducible streaming profiler
- `data_discovery_evidence.md` - measured evidence and design decisions

## Source
GH Archive: https://www.gharchive.org/
Archive pattern: `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`

## Architecture
GH Archive → Bronze → Silver → Gold → Power BI

## Full vs Incremental sample
The 10:00 and 11:00 files are representative raw sample payloads for the initial/full-load baseline. The 12:00 file is the subsequent incremental payload. The actual Phase 2 full-load baseline will use a larger bounded historical window selected for Free Edition constraints.

## Run the profiler
```bash
python src/profile_gharchive.py data/samples/2026-09-25-10.json.gz
```
Multiple files may be supplied.

## Privacy
The raw source contains public actor/user information and may contain user-generated text. Bronze preserves source data for traceability; Silver removes unnecessary profile fields/free text; Gold exposes aggregates.
