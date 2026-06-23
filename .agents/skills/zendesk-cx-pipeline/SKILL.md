---
name: zendesk-cx-pipeline
description: >-
  Full context for migrating the Zendesk → Snowflake medallion pipeline (Snowpark
  stored procedures and Snowflake tasks) into a well-organized dbt project. Use when
  building, migrating, or extending Zendesk CX models — ingestion, bronze, silver, or
  gold — or when creating sources, staging, intermediate, mart models, seeds, or tests
  for Zendesk data in this project.
---

# Zendesk CX pipeline → dbt

## What this skill is for

This project replicates an existing **Zendesk customer-experience (CX) pipeline** —
originally built as Snowflake **Snowpark Python stored procedures** + **tasks** — inside
dbt. The goal is to gain the abstraction, lineage, testing, and visibility that dbt
provides over hand-written procedures.

Use this skill whenever you migrate one of the five source stages into dbt, add Zendesk
models, or wire up seeds/tests. It carries the context of what each source script does so
you don't have to re-derive intent from raw SQL. Pair it with the built-in
`using-dbt-for-analytics-engineering`, `running-dbt-commands`, and `adding-dbt-unit-test`
skills, and with the migration skills when relevant.

## Source pipeline at a glance

The legacy pipeline runs in five numbered stages across two domains — a generic
**WILD_CARD** landing domain and the final **CX** consumer domain:

| # | Stage | Domain | What it does | dbt equivalent |
|---|-------|--------|--------------|----------------|
| 1 | Ingestion | WILD_CARD | Snowpark proc extracts Zendesk API → Parquet in S3 stage | **Out of dbt** (EL); becomes a `source` |
| 2 | Bronze tables | WILD_CARD | Raw landing tables loaded from the S3 stage | dbt **`source`** (raw, untyped) |
| 3 | Bronze views | CX | Cross-domain views exposing bronze to CX | dbt **`stg_` staging** models |
| 4 | Silver tables | CX | Cleaned, conformed, deduplicated entities | dbt **`int_` intermediate** models |
| 5 | Gold tables | CX | Analytics-ready facts/dimensions/aggregates | dbt **`fct_`/`dim_`/`agg_` marts** |

This is a classic medallion architecture and maps cleanly onto dbt's
source → staging → intermediate → marts layering. The WILD_CARD→CX domain hop is a good
candidate for **dbt Mesh** (separate projects/groups) if you want governed cross-domain
boundaries; otherwise model it as two schemas in one project.

## Source scripts (all five present)

All five SQL scripts live in `legacy_sql/` and are documented in full in
`references/source-pipeline.md`. **Keep the scripts in the repo — the skill is a digest, not a
lossless copy; the agent should open the SQL for exact literals (the 17 custom-field ids, MERGE
column lists, masking roles).**

- `01_Zendesk.Ingestion.DBT (Wild Card).sql` — Snowpark API → S3 (EL).
- `02_Zendesk.Bronze.DBT (Wild Card).sql` — raw `RAW_DATA VARIANT` tables + Snowpipes.
- `03_Zendesk.Bronze.DBT (CX).sql` — CX secure views (brand filter).
- `04_Zendesk.Silver.DBT (CX).sql` — typed tables, custom-field pivot, events/comments, PII masking.
- `05_Zendesk.Gold.DBT (CX).sql` — analytics tables with PII dropped/masked.

**One real gap:** the gold task calls `CX.GOLD.KPI_TICKETS()` but its definition is **not in
these scripts**. Do **not** invent the KPI logic — ask the user for that proc before building
a `kpi_tickets`/metrics mart.

## Zendesk entities in scope

Eight bronze tables (each a `RAW_DATA VARIANT` + `_SNOWFLAKE_LOADED_AT`):
`groups`, `organizations`, `ticket_fields`, `ticket_metrics` (legacy `ticket_metric_events`),
`ticket_metrics_full` (the `metric_sets` sideload — reliable reply/resolution times after the
standalone metric_events endpoint stopped returning them on **2026-05-04**), `tickets`,
`tickets_events`, `users`. The CX layer filters tickets to **brand_ids
`34126647548180`, `43696510017940`** and propagates that filter to events.

## How to build the dbt project

Target warehouse is **Snowflake**. Follow the layout, the VARIANT-reading pattern, and the
model-by-model mapping in `references/target-dbt-project.md`. In short:

1. **Sources** — one `zendesk` source over the 8 `WILD_CARD.BRONZE.ZENDESK_*` VARIANT tables;
   freshness matched to the 6-hour ingestion cadence.
2. **Staging** (`stg_zendesk__*`) — parse `RAW_DATA` with `PARSE_JSON`, extract fields via
   `raw_data:field::type`, apply the **CX brand filter** to tickets/events (script 03),
   `qualify row_number() ... = 1` per id. Views.
3. **Intermediate** (`int_zendesk__*`) — the silver logic: **custom-field pivot**, the
   **metrics union/dedup** (legacy reply_time-not-null ∪ metrics_full), **child_events
   unpivot**, and **comments** extraction. See the "tricky transforms" section in the target doc.
4. **Marts** (`fct_tickets`, `dim_users`, `dim_agents`, `dim_groups`, `fct_ticket_events`,
   `fct_ticket_comments`, …) — the gold layer, PII dropped/masked. Tables; incremental for
   high-volume facts.
5. **Tests & docs** — `unique`/`not_null` on PKs, `relationships` on FKs, `accepted_values`
   on status/role (note silver UPPERCASEs them). Document every model and column **in Portuguese**.
6. **Seeds** — see below.

Don't reinvent transforms — the source SQL is the spec. Reproduce the brand filter, the
hard-coded custom-field-id pivot, the PII masking strategy, and the metrics union exactly;
make the magic ids data-driven via the `cx` reference seeds.

## Seeds with dummy data

Referentially-consistent dummy seeds are installed at `seeds/zendesk/` (eight CSVs +
`_zendesk__seeds.yml`). They mirror the raw bronze tables' **`RAW_DATA` VARIANT shape**
(`raw_data` JSON string + `_snowflake_loaded_at`) so the **entire DAG can be built and tested
locally without warehouse or API access** — including the brand filter (a non-CX ticket `1099`
is included to prove it), custom-field pivot, and `child_events` unpivot. Dummy content is in
**Portuguese**. Schema, referential map, and the seed→source swap pattern are in
`references/seeds.md`. Run `dbt seed` then `dbt build`.

## Conventions

- `dbt` is always lowercase. The managed product is the **dbt platform**.
- **Localization: the client is Brazilian — write all model, column, and seed
  `description:` text in Portuguese (pt-BR).** Keep code identifiers (model/column names)
  in English snake_case for portability; dummy seed data is in Portuguese.
- Staging: `stg_zendesk__<entity>`. Intermediate: `int_zendesk__<verb_phrase>`.
  Marts: `fct_`/`dim_`/`agg_`. Seeds keep the `zendesk_` prefix + original field names.
- Read VARIANT via `parse_json(raw_data):field::type`; isolate the seed-vs-source difference
  in the import CTE (or a macro) so models stay swap-ready.
- Reference `source()`/`ref()` everywhere — never hard-code `WILD_CARD.BRONZE...`.
- Keep changes additive and one concern per PR. Build + test before declaring done.

## References

- `references/source-pipeline.md` — detailed breakdown of every source stage and script 01.
- `references/target-dbt-project.md` — target folder layout, model-by-model mapping, configs.
- `references/seeds.md` — seed schema, dummy data description, and dev-target wiring.
