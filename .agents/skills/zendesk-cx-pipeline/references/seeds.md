# Seeds — dummy data reference

Eight CSV seeds in `seeds/zendesk/` reproduce the raw `WILD_CARD.BRONZE.ZENDESK_*` tables so
the whole DAG (staging → intermediate → marts) can be built and tested locally without
warehouse or API access. Properties + tests are in `seeds/zendesk/_zendesk__seeds.yml`.

## Shape: matches the real bronze (VARIANT)

Each bronze table is a single **`RAW_DATA VARIANT` + `_SNOWFLAKE_LOADED_AT`**. The seeds carry
exactly two columns:

| column | meaning |
|---|---|
| `raw_data` | the Zendesk record as a **JSON string** (stands in for the VARIANT) |
| `_snowflake_loaded_at` | load timestamp (matches the source column) |

Staging parses it: `parse_json(raw_data):field::type` — identical to the source procedures'
`RAW_DATA:field::type`. **Dummy content is in Portuguese** (subjects, descriptions, comment
bodies, group/org names) since the client is Brazilian.

## Files & grain

| Seed | Mirrors source table | Rows |
|---|---|---|
| `zendesk_groups.csv` | ZENDESK_GROUPS | 4 |
| `zendesk_organizations.csv` | ZENDESK_ORGANIZATIONS | 4 |
| `zendesk_users.csv` | ZENDESK_USERS | 7 (3 internos, 4 clientes) |
| `zendesk_ticket_fields.csv` | ZENDESK_TICKET_FIELDS | 5 (inc. 2 taggers c/ options) |
| `zendesk_tickets.csv` | ZENDESK_TICKETS | 8 (7 CX + 1 não-CX) |
| `zendesk_tickets_events.csv` | ZENDESK_TICKETS_EVENTS | 5 audits (child_events) |
| `zendesk_ticket_metrics.csv` | ZENDESK_TICKET_METRICS | 2 (reply_time nulo) |
| `zendesk_ticket_metrics_full.csv` | ZENDESK_TICKET_METRICS_FULL | 7 |

## Referential map (FKs resolve; tests pass)

- Groups `10–13`; Orgs `100–103` (each → a group).
- Users: agents/admin `200,201,202`; clientes `300–303` (each → an org).
- Tickets `1001–1007`: requester (cliente), assignee (agent), group, organization all valid.
  **`1099` is a non-CX brand (`99999999999999`)** — included on purpose so you can verify the
  CX bronze brand filter drops it (CX brands are `34126647548180`, `43696510017940`).
- `tickets_events`, `ticket_metrics`, `ticket_metrics_full` reference valid ticket ids.

## Faithful to the pipeline quirks

- **`custom_fields`** on tickets is an array `[{id, value}]` using the real CX field ids
  (`36210587567380` Motivo de Contato, `36211214371476` Tag de Atendimento) → exercises the
  silver custom-field pivot.
- **`ticket_metrics_full`** stores `reply_time_in_minutes` / resolution times as
  `{calendar, business}` objects (silver reads them as strings).
- **`ticket_metrics`** (legacy) has `reply_time_in_minutes = null` → filtered out by the
  union, so `metrics_full` wins (the post-2026-05-04 reality).
- **`tickets_events.child_events`** is a **JSON string** inside `raw_data` (matches the source);
  it contains both a `Comment` event (→ ticket_comments) and `Change` events like
  `status`/`assignee_id` (→ events unpivot), plus one `custom_ticket_fields` change.
- **`custom_field_options`** present on the tagger ticket_fields → feeds the gold
  `ticket_field_options` view.
- Every record has `datalake_updated_at` (UTC-3).

## How to use

1. `dbt seed` — loads the eight seeds into the `bronze` schema.
2. Staging reads them. Isolate the seed-vs-prod difference in one import CTE:
   - **Seed (dev):** `select parse_json(raw_data) as raw_data, _snowflake_loaded_at from {{ ref('zendesk_tickets') }}`
   - **Real source (prod):** `select raw_data, _snowflake_loaded_at from {{ source('zendesk','zendesk_tickets') }}` (already VARIANT)
   A small macro keyed on `target.name` keeps every staging model swap-ready.
3. `dbt build` — seeds + models + tests end to end.

## Reference seeds (provided at `seeds/cx/`)

Two `cx` reference seeds make the pipeline rules data-driven instead of hard-coded:
- `zendesk_brands.csv` — brand_id → name for the 2 CX brands (names are placeholders; equals
  `CX.GOLD.ZENDESK_BRANDS`). Replace with the real brand names.
- `zendesk_custom_field_map.csv` — the full custom-field id → `CF_*` column map transcribed
  from script 04 (17 ticket fields + the `STATUS_TRANSBORDO` field used in the events unpivot),
  with pt-BR labels and a `used_in` flag. Drive the custom-field pivot from this seed.

## Editing

- Keep ids consistent across files or `relationships` tests fail.
- Regenerate, don't hand-edit, if you change many rows — large JSON-in-CSV is error-prone.
  CSV quoting (doubled `""`) is handled correctly when written by a JSON+csv writer.
