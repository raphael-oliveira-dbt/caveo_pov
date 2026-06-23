# Source pipeline — detailed reference

What the legacy **Snowflake** Snowpark/SQL pipeline does, stage by stage, so dbt models can
reproduce it faithfully. All five scripts live in `legacy_sql/` (`01..05_*.sql`) — read them
for exact literals (custom-field ids, MERGE column lists, masking roles).
Platform is **Snowflake** (VARIANT, PARSE_JSON, LATERAL FLATTEN, MERGE, masking policies,
tasks, pipes).

## Domains & objects

- **WILD_CARD** — generic landing domain. `WILD_CARD.BRONZE` holds raw tables, logs, the
  incremental cursor, S3 stages, ingestion procs/tasks, and Snowpipe pipes.
- **CX** — final consumer domain. `CX.BRONZE` (secure views), `CX.SILVER` (cleaned +
  PII masking), `CX.GOLD` (analytics-ready, PII removed/masked).
- Roles: `ROLE_DATA_WILD_CARD`, `ROLE_DATA_CX`, `ROLE_DOMAIN_CX`, `ROLE_DOMAIN_CX_PII`.
  Warehouse: `WH_DATA_SERVICES`. Task cadence: every 6h, staggered
  (ingestion `0`, silver `30`, gold `40`).
- **CX brand_ids**: `34126647548180`, `43696510017940` (the CX-domain filter).

## Stage 1 — Ingestion (`01_*.sql`, WILD_CARD) — stays EL, outside dbt

Snowpark Python procs pull the Zendesk REST API → Parquet/Snappy in S3 stage
`WILD_CARD.BRONZE.ZENDESK_S3_DATAMESH` (partitioned `year/month/day_updated_at=`).

- **Auth**: OAuth (or email/api_token) via secret `ZENDESK_OAUTH_SECRET`; network rule +
  external access integration.
- **`ZENDESK_TO_S3()`** iterates entities: `groups` (full), `organizations`, `ticketMetrics`
  (`/incremental/ticket_metric_events.json`), `tickets`, `tickets_events`
  (`/incremental/ticket_events.json?include=comment_events`), `users` (incremental);
  `ticketFields` (full). Incremental cursor = last `SUCCESS` ts in `EXTRACTION_LOG_ZENDESK`
  (fallback 30 days). Nested dict/list fields are `json.dumps`'d to strings; every row gets
  `datalake_updated_at` (UTC-3).
- **`ZENDESK_TICKET_METRICS_FULL_TO_S3()`** uses `/incremental/tickets.json?include=metric_sets`
  to recover `reply_time_in_minutes` (calendar+business) + resolution times that
  `ticket_metric_events` stopped returning on **2026-05-04**. Cursor in
  `ZENDESK_INCREMENTAL_CURSOR`; robust 429/5xx/401/network retries.
- **DQ checks** (logged to `DATA_QUALITY_LOG_ZENDESK`, block upload on FAIL): row_count,
  duplicate_ids, required_field null %. → reproduce as dbt source freshness + `unique` +
  `not_null` tests.

## Stage 2 — Bronze tables (`02_*.sql`, WILD_CARD) → dbt **sources**

- File format `ZENDESK_PARQUET`; external stages (datalake history + datamesh incremental).
- 8 raw tables, each **`RAW_DATA VARIANT` + `_SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ`**:
  `ZENDESK_GROUPS, ZENDESK_ORGANIZATIONS, ZENDESK_TICKET_FIELDS, ZENDESK_TICKET_METRICS,
  ZENDESK_TICKET_METRICS_FULL, ZENDESK_TICKETS, ZENDESK_TICKETS_EVENTS, ZENDESK_USERS`.
- Snowpipes (`AUTO_INGEST=TRUE`, `ON_ERROR=CONTINUE`) COPY each stage subfolder → table.
- **dbt:** declare these 8 as the `zendesk` source. The seeds in this skill reproduce their
  exact `RAW_DATA`+`_SNOWFLAKE_LOADED_AT` shape (RAW_DATA as a JSON string).

## Stage 3 — Bronze views (`03_*.sql`, CX) → dbt **staging (brand filter)**

Secure views `CX.BRONZE.ZENDESK_*` over the WILD_CARD tables, still carrying
`RAW_DATA, _SNOWFLAKE_LOADED_AT` (no field extraction yet):
- **TICKETS**: filtered to CX brands `RAW_DATA:brand_id::NUMBER IN (34126647548180, 43696510017940)`.
- **TICKETS_EVENTS**: kept only where the parent ticket is a CX-brand ticket (EXISTS join on
  `ticket_id`).
- GROUPS, ORGANIZATIONS, USERS, TICKET_FIELDS, TICKET_METRICS, TICKET_METRICS_FULL: replicated
  in full (no brand on these).
- **dbt:** this brand filter is the first job of the staging layer (or a dedicated
  `models/staging/zendesk/` filter). Apply the CX-brand filter to tickets, and filter events to
  CX tickets, before/while extracting fields.

## Stage 4 — Silver (`04_*.sql`, CX) → dbt **intermediate (+ field extraction)**

SQL stored procedures `MERGE` into typed `CX.SILVER.*` tables, incremental by a
`_SNOWFLAKE_LOADED_AT` watermark, deduped by `ROW_NUMBER() OVER (PARTITION BY id ORDER BY
updated_at DESC)`. This is where VARIANT → columns happens. Models to build:

- **`ZENDESK_TICKETS`** (the big one): extracts ~18 top-level fields from `RAW_DATA`
  (UPPER/TRIM on subject/status/priority), **pivots 17 custom fields** by hard-coded field
  id (`OBJECT_AGG` over a `LATERAL FLATTEN` of `custom_fields`, deduped) into `CF_*` columns,
  keeps `CUSTOM_FIELDS_JSON` raw, and **LEFT JOINs metrics**: a UNION of `ZENDESK_TICKET_METRICS`
  (where `reply_time_in_minutes IS NOT NULL`, i.e. pre-cutoff) and `ZENDESK_TICKET_METRICS_FULL`,
  taking the latest per `ticket_id`. Metric time fields are stored as **STRING** (the value can be
  a calendar/business JSON object).
- **`ZENDESK_USERS`**: extracts user fields (UPPER name/role, LOWER email) + later adds
  `EXTERNAL_ID`, `PHONE`.
- **`ZENDESK_GROUPS`**: id/name/description/`default`→IS_DEFAULT/deleted.
- **`ZENDESK_TICKETS_EVENTS`**: **unpivots** `child_events` (a JSON string) into one row per
  (audit id, field_name) — emits rows for status/assignee_id/group_id/priority/brand_id/
  requester_id/organization_id/ticket_form_id/is_public/subject/type/custom_status_id/
  sla_policy/comment_present/comment_public/tags + two hard-coded custom fields
  (`36210587567380`, `36627293345556`); UNIONs legacy root-level `field_name` rows. MERGE key
  = (ID, FIELD_NAME).
- **`ZENDESK_TICKET_COMMENTS`**: from `child_events` where `event_type='Comment'` → id, audit_id,
  ticket_id, author_id, is_public, via, type, body/html_body/plain_body, created_at.
- **PII masking policies** (`MASK_*`): full-text mask on user NAME, ticket DESCRIPTION,
  comment BODY/HTML/PLAIN; SHA2 on user EMAIL and PHONE; partial mask (first 4 chars) on
  ticket SUBJECT. Unmasked only for `ROLE_DATA_CX`, `ROLE_DOMAIN_CX_PII`, `ACCOUNTADMIN`.
  → in dbt, reproduce via Snowflake masking policies applied through post-hooks / a
  governance macro, or model PII columns separately and control access (see target doc).
- Task `TASK_ZENDESK_SILVER` calls the 5 procs in order.

## Stage 5 — Gold (`05_*.sql`, CX) → dbt **marts**

`MERGE` from SILVER → `CX.GOLD.*`, dropping/masking PII:
- **`ZENDESK_TICKETS`**: same columns as silver **minus DESCRIPTION**; SUBJECT masked
  (partial). This is the ticket fact (wide, with CF_* + metrics).
- **`ZENDESK_USERS`**: **no NAME/EMAIL**; analytics attributes only (+ `EXTERNAL_ID`).
- **`ZENDESK_GROUPS`**, **`ZENDESK_TICKETS_EVENTS`** (passthrough), **`ZENDESK_TICKET_COMMENTS`**
  (metadata only — **no body**).
- **`ZENDESK_AGENTS`** (view) / **`ZENDESK_AGENTS_NAMES`** (table): users where `ROLE='AGENT'`,
  exposing NAME (agent name is staff, not customer PII), granted to `ROLE_DOMAIN_CX`.
- **`ZENDESK_TICKET_FIELD_OPTIONS`** (view): flattens `custom_field_options` from bronze
  `ZENDESK_TICKET_FIELDS` → field_id, field_title, option_value, option_label.
- **`ZENDESK_BRANDS`**: a tiny static map `brand_id → name` (the 2 CX brands). → dbt **seed**.
- ⚠️ **`CX.GOLD.KPI_TICKETS()` is CALLED by the gold task but its definition is NOT in these
  scripts.** Ask the user for it before modeling a `kpi_tickets` / metrics mart; do not invent
  the KPI logic. Good candidate for the dbt **Semantic Layer** once defined.
- Task `TASK_ZENDESK_GOLD` cascades after silver.

## Cross-cutting notes for the migration

- **Grain & dedup**: every silver entity is 1-row-per-id, latest by `updated_at`. In dbt use
  `qualify row_number() over (partition by id order by updated_at desc) = 1`.
- **Incremental**: sprocs are watermark-incremental on `_SNOWFLAKE_LOADED_AT`. High-volume
  models (`tickets`, `tickets_events`, `ticket_comments`) → dbt `incremental` materialization;
  small dims (`groups`, `brands`) → table/view.
- **Metrics union quirk**: keep the `reply_time_in_minutes IS NOT NULL` filter on the legacy
  metrics source and prefer `*_FULL`.
- **Hard-coded custom-field ids** are CX-specific; keep them in a seed or a documented macro so
  the pivot is maintainable rather than buried in SQL.
