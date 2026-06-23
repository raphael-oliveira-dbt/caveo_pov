# Target dbt project layout (Snowflake)

The repo is a fresh `dbt init` starter (`dbt_project.yml` name `my_new_project`, profile
`default`). Target warehouse is **Snowflake**. Build the Zendesk pipeline into the layout
below. Rename the project (e.g. `caveo_pov`) when convenient and update the `models:` /
`seeds:` keys accordingly.

## Layer mapping (source → dbt)

| Source | dbt layer | Materialization |
|---|---|---|
| `02` WILD_CARD.BRONZE raw VARIANT tables | `sources` (+ these seeds in dev) | n/a |
| `03` CX.BRONZE secure views (brand filter) | `staging/zendesk/stg_zendesk__*` | view |
| `04` CX.SILVER typed + custom-field pivot + events unpivot | `intermediate/zendesk/int_zendesk__*` | incremental (tickets/events/comments), view (dims) |
| `05` CX.GOLD PII-managed analytics tables | `marts/cx/*` | table / incremental |

## Folder layout

```text
models/
  staging/zendesk/
    _zendesk__sources.yml        # source 'zendesk' → 8 WILD_CARD.BRONZE.ZENDESK_* tables
    _zendesk__models.yml
    stg_zendesk__tickets.sql         # CX brand filter + extract RAW_DATA fields
    stg_zendesk__tickets_events.sql  # CX-ticket filter; keep audit + child_events
    stg_zendesk__users.sql
    stg_zendesk__groups.sql
    stg_zendesk__organizations.sql
    stg_zendesk__ticket_fields.sql
    stg_zendesk__ticket_metrics.sql        # legacy; reply_time not null only
    stg_zendesk__ticket_metrics_full.sql   # metric_sets (authoritative)
  intermediate/zendesk/
    _int_zendesk__models.yml
    int_zendesk__tickets_metrics_joined.sql   # union+dedup metrics, latest per ticket
    int_zendesk__tickets_custom_fields.sql    # pivot custom_fields → cf_* (uses seed map)
    int_zendesk__ticket_events_unpivoted.sql  # child_events → 1 row per (audit, field)
    int_zendesk__ticket_comments.sql          # child_events event_type='Comment'
  marts/cx/
    _cx__models.yml
    fct_tickets.sql              # gold ticket fact (no description; subject masked)
    dim_users.sql                # no name/email
    dim_agents.sql               # role = AGENT (name allowed — staff, not customer PII)
    dim_groups.sql
    fct_ticket_events.sql
    fct_ticket_comments.sql      # metadata only, no body
    zendesk_ticket_field_options.sql   # flatten custom_field_options
    # kpi_tickets.sql            # ONLY after the user supplies CX.GOLD.KPI_TICKETS()
seeds/
  zendesk/                       # raw bronze fixtures (this skill) — see seeds.md
  cx/
    zendesk_brands.csv           # brand_id → name (the 2 CX brands)
    zendesk_custom_field_map.csv # cf id → cf_* column name (drives the pivot)
```

## dbt_project.yml additions

```yaml
models:
  caveo_pov:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +schema: cx
      fct_tickets:        { +materialized: incremental }
      fct_ticket_events:  { +materialized: incremental }
      fct_ticket_comments:{ +materialized: incremental }

seeds:
  caveo_pov:
    +quote_columns: false
    zendesk:
      +schema: bronze
```

## Reading the raw VARIANT (key pattern)

Source tables expose a VARIANT `RAW_DATA`. The seeds expose `raw_data` as a JSON **string**.
Isolate the difference in one import CTE so a model is identical otherwise:

```sql
-- stg_zendesk__tickets.sql
with source as (
    -- dev: seed (string) → parse_json; prod: source (already VARIANT) → drop parse_json
    select parse_json(raw_data) as raw_data, _snowflake_loaded_at
    from {{ ref('zendesk_tickets') }}
),
cx_filtered as (              -- script 03 brand filter
    select * from source
    where raw_data:brand_id::number in (34126647548180, 43696510017940)
),
renamed as (
    select
        raw_data:id::number                         as ticket_id,
        upper(trim(raw_data:subject::string))       as subject,
        raw_data:description::string                as description,
        upper(trim(raw_data:status::string))        as status,
        upper(trim(raw_data:priority::string))      as priority,
        raw_data:brand_id::number                   as brand_id,
        raw_data:assignee_id::number                as assignee_id,
        raw_data:requester_id::number               as requester_id,
        raw_data:organization_id::number            as organization_id,
        raw_data:group_id::number                   as group_id,
        raw_data:tags::string                       as tags,
        raw_data:custom_fields::string              as custom_fields_json,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        _snowflake_loaded_at                        as loaded_at
    from cx_filtered
)
select * from renamed
qualify row_number() over (partition by ticket_id order by updated_at desc nulls last) = 1
```

> Recommended: wrap the import in a small macro, e.g. `{{ zendesk_source('zendesk_tickets') }}`,
> that emits `parse_json(raw_data)` for seeds and bare `raw_data` for the real source based on
> `target.name`. Keeps every staging model swap-ready.

## Tricky transforms (don't lose these from `04`/`05`)

- **Custom-field pivot** (`int_zendesk__tickets_custom_fields`): `lateral flatten` the
  `custom_fields` array, `object_agg(id, value)`, then read `cf_obj['<id>']`. Drive the
  id→column mapping from the `zendesk_custom_field_map` seed instead of hard-coding the 17 ids in SQL.
- **Metrics join**: `union all` legacy `stg_zendesk__ticket_metrics` (reply_time not null) +
  `stg_zendesk__ticket_metrics_full`, `qualify row_number() ... = 1` per ticket; left join to
  tickets. Time fields stay as strings (calendar/business JSON).
- **Events unpivot** (`int_zendesk__ticket_events_unpivoted`): `lateral flatten` parsed
  `child_events`, emit one row per known field via `union all`, 1 row per (id, field_name).
- **Comments** (`int_zendesk__ticket_comments`): `child_events` where `event_type='Comment'`.

## PII handling — reproduce the source's masking policies (chosen approach)

Match the source exactly: keep the PII **columns present** in silver and apply Snowflake
**masking policies** gated by role — do **not** drop columns. Implement the policies in dbt
and attach them via a **`post-hook`** so dbt owns the governance and it stays in parity with
scripts `04`/`05`.

Policies to recreate (unmasked only for `ROLE_DATA_CX`, `ROLE_DOMAIN_CX_PII`, `ACCOUNTADMIN`):

| Policy | Columns | Masked behavior |
|---|---|---|
| `mask_zendesk_user_name_pii` | silver users `name` | `'**********'` |
| `mask_zendesk_user_email_pii` | silver users `email` | `sha2(val)` |
| `mask_zendesk_user_phone_pii` | silver users `phone` | `sha2(val)` |
| `mask_zendesk_ticket_description_pii` | silver tickets `description` | `'**********'` |
| `mask_zendesk_ticket_subject_pii` | silver + gold tickets `subject` | first 4 chars + `*`×(len-4) |
| `mask_zendesk_comment_body_pii` | silver comments `body`/`html_body`/`plain_body` | `'**********'` |

Recommended structure:
- `macros/create_masking_policies.sql` — `create masking policy ...` for each, run once via an
  `on-run-start` hook or a dedicated op (idempotent `create or replace`).
- On each PII-bearing model, `+post-hook: "{{ apply_masking_policy(this, 'column', 'policy') }}"`
  (`alter table ... modify column ... set masking policy ...`).
- **Gold still mirrors `05`'s column choices**: gold tickets has no `description`; gold users
  drops `name`/`email` (keeps `external_id`); `fct_ticket_comments` keeps metadata only (no
  body). The `dim_agents`/`zendesk_agents_names` view exposes agent `name` (staff, not customer
  PII), granted to `ROLE_DOMAIN_CX`.

> Masking policies require Snowflake Enterprise. If unavailable in the POV account, fall back to
> column-drop in gold and note the divergence — but the target is policy parity with source.

## Tests, docs, seeds-driven references
- PK `unique`+`not_null` on every model; FK `relationships` (tickets→users/orgs/groups, etc.).
- `accepted_values` on status (NEW/OPEN/PENDING/HOLD/SOLVED/CLOSED) and role (AGENT/ADMIN/END-USER)
  — note silver UPPERCASEs these.
- Source freshness matched to the 6h ingestion cadence.
- Seeds `zendesk_brands` and `zendesk_custom_field_map` make brand names and the CF pivot
  data-driven and testable.
- Add `adding-dbt-unit-test` unit tests for the custom-field pivot, the metrics union/dedup,
  and the child_events unpivot — the three highest-risk transforms.

## Migration order
1. `dbt seed` (bronze fixtures + cx reference seeds).
2. Sources + `stg_zendesk__*` → `dbt build -s staging`.
3. `int_zendesk__*` → `dbt build -s intermediate`.
4. `marts/cx` → `dbt build -s marts`.
5. Full `dbt build`; review lineage.

> **`kpi_tickets` — documented placeholder.** The gold task calls `CX.GOLD.KPI_TICKETS()` but
> the proc is **not defined** in the supplied scripts. Do not implement it. Instead leave a
> visible marker so it isn't forgotten — e.g. a `models/marts/cx/kpi_tickets.sql` stub
> `{{ config(enabled=false) }}` with a comment, or an entry in `_cx__models.yml` noting the
> dependency. Build it only when the source proc is provided.
