# Phase 1 Data Discovery Evidence  GH Archive

Three consecutive real hourly archives were profiled:

- `2026-09-25-10.json.gz`
- `2026-09-25-11.json.gz`
- `2026-09-25-12.json.gz`

**Rubric mapping:** the 10:00 and 11:00 files are representative raw sample payloads for the initial/full-load baseline; the 12:00 file is the subsequent incremental payload. The actual Phase 2 full-load baseline will use a larger bounded historical window. These are sample payloads, not the entire historical archive.

## Measured volume

| File | Events | Compressed MB | Unique repos | Unique actors |
|---|---:|---:|---:|---:|
| 2026-09-25-10.json.gz | 84,754 | 14.11 | 46,299 | 34,256 |
| 2026-09-25-11.json.gz | 89,852 | 14.20 | 48,450 | 34,265 |
| 2026-09-25-12.json.gz | 93,655 | 14.73 | 51,032 | 37,395 |

**3-hour total:** 268,261 events and approximately 43.04 MB compressed.

**Observed average:** approximately 89,420 events/hour and 14.35 MB compressed/hour.

**Planning extrapolation:** approximately 344 MB/day and 2.41 GB/week compressed. These are estimates from three hours and will vary.

## Incremental evidence

- Event-ID overlap between 10:00 and 11:00: **0**
- Event-ID overlap between 11:00 and 12:00: **0**

This supports an append-oriented hourly design; the pipeline will still implement idempotency/deduplication.

## Dominant event types observed

- PushEvent
- CreateEvent
- DeleteEvent
- PullRequestEvent
- IssueCommentEvent
- IssuesEvent
- PullRequestReviewEvent
- PullRequestReviewCommentEvent
- WatchEvent
- ReleaseEvent
- ForkEvent

## Resulting design decisions

1. Hourly archives are the natural incremental unit.
2. Raw archives are preserved in Bronze.
3. Silver uses a common event backbone plus event-specific tables.
4. Unnecessary profile fields and user-generated free text are removed before Silver/Gold.
5. Event IDs plus processed-source metadata are used for idempotency.
6. Commit-author and programming-language analytics are not core claims because the sampled payloads do not reliably provide those fields.
