-- ============================================================
-- Snowpark Python Stored Procedure
-- Extração Zendesk → S3 (Parquet/Snappy)
-- ============================================================
-- Objetos extraídos : groups, organizations, ticket_fields, ticket_metrics, tickets, tickets_events, users
-- Destino           : WILD_CARD.BRONZE.ZENDESK_<ENTIDADE>
-- Auth              : OAuth via ZENDESK_OAUTH_SECRET
-- Domínio Zendesk   : <YOUR_ZENDESK_SUBDOMAIN>.zendesk.com
-- ============================================================

-- ------------------------------------------------------------
-- 0. SETUP INICIAL
-- ------------------------------------------------------------

-- ──────────────────────────────────────────────
-- Secret — OAuth Zendesk
-- ──────────────────────────────────────────────
-- ⚠️  NÃO versionar o client_secret em texto puro. O objeto
--     WILD_CARD.BRONZE.ZENDESK_OAUTH_SECRET já existe no Snowflake.
--     Para (re)criar, rode o comando manualmente (fora do arquivo
--     versionado) substituindo os placeholders pelos valores reais.

-- ──────────────────────────────────────────────
-- Network Rule + External Access Integration
-- ──────────────────────────────────────────────
CREATE OR REPLACE NETWORK RULE WILD_CARD.BRONZE.ZENDESK_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    '<YOUR_ZENDESK_SUBDOMAIN>.zendesk.com',
    'oauth.zendesk.com',
    'pypi.org',
    'files.pythonhosted.org'
  );

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ZENDESK_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (WILD_CARD.BRONZE.ZENDESK_NETWORK_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (WILD_CARD.BRONZE.ZENDESK_OAUTH_SECRET)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION ZENDESK_ACCESS_INTEGRATION TO ROLE ROLE_DATA_WILD_CARD;

USE ROLE ROLE_DATA_WILD_CARD;

-- ------------------------------------------------------------
-- 1. TABELAS DE LOG
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WILD_CARD.BRONZE.EXTRACTION_LOG_ZENDESK (
    LOG_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    OBJECT_NAME  STRING,
    ROW_COUNT    NUMBER,
    COLUMNS      STRING,
    STATUS       STRING,
    MESSAGE      STRING,
    EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS WILD_CARD.BRONZE.DATA_QUALITY_LOG_ZENDESK (
    DQ_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    OBJECT_NAME  STRING,
    CHECK_NAME   STRING,
    STATUS       STRING,   -- PASS | WARN | FAIL
    DETAIL       STRING,
    CHECKED_AT   TIMESTAMP_NTZ
);

-- ------------------------------------------------------------
-- 2. STORED PROCEDURE
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE WILD_CARD.BRONZE.ZENDESK_TO_S3()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'requests',
    'pandas',
    'pyarrow'
)
EXTERNAL_ACCESS_INTEGRATIONS = (ZENDESK_ACCESS_INTEGRATION)
SECRETS = (
    'zd_oauth' = WILD_CARD.BRONZE.ZENDESK_OAUTH_SECRET
)
HANDLER = 'run_extraction'
EXECUTE AS CALLER
AS
$$
import io
import uuid
import json
import time as time_mod
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from snowflake.snowpark import Session
import _snowflake

MAX_EXECUTION_SECONDS = 480
BATCH_PAGE_LIMIT = 30


ZENDESK_SUBDOMAIN = "<YOUR_ZENDESK_SUBDOMAIN>"
ZENDESK_API_BASE = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2"

ENTITIES = {
    "groups": {
        "endpoint": "/groups.json",
        "list_key": "groups",
        "incremental": False,
        "id_field": "id",
    },
    "organizations": {
        "endpoint": "/incremental/organizations.json",
        "list_key": "organizations",
        "incremental": True,
        "id_field": "id",
    },
    "ticketFields": {
        "endpoint": "/ticket_fields.json",
        "list_key": "ticket_fields",
        "incremental": False,
        "id_field": "id",
    },
    "ticketMetrics": {
        "endpoint": "/incremental/ticket_metric_events.json",
        "list_key": "ticket_metric_events",
        "incremental": True,
        "id_field": "id",
    },
    "tickets": {
        "endpoint": "/incremental/tickets.json",
        "list_key": "tickets",
        "incremental": True,
        "id_field": "id",
    },
    "tickets_events": {
        "endpoint": "/incremental/ticket_events.json?include=comment_events",
        "list_key": "ticket_events",
        "incremental": True,
        "id_field": "id",
    },
    "users": {
        "endpoint": "/incremental/users.json",
        "list_key": "users",
        "incremental": True,
        "id_field": "id",
    },
}

REQUIRED_FIELDS = {
    "groups":        ["id", "name"],
    "organizations": ["id", "name"],
    "ticketFields":  ["id", "title"],
    "ticketMetrics": ["id"],
    "tickets":       ["id", "subject", "status"],
    "tickets_events": ["id", "ticket_id"],
    "users":         ["id", "name"],
}


def get_oauth_token() -> str:
    secret = json.loads(_snowflake.get_generic_secret_string('zd_oauth'))
    if "access_token" in secret:
        return secret["access_token"]
    if "api_token" in secret and "email" in secret:
        return None
    token_url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/oauth/tokens"
    resp = requests.post(token_url, data={
        "client_id": secret["client_id"],
        "client_secret": secret["client_secret"],
        "grant_type": "client_credentials",
        "scope": "read"
    })
    if resp.status_code == 200:
        data = resp.json()
        return data.get("access_token", data.get("token", {}).get("full_token", ""))
    raise Exception(f"OAuth falhou: {resp.status_code} - {resp.text}")


def get_auth_headers() -> dict:
    secret = json.loads(_snowflake.get_generic_secret_string('zd_oauth'))
    if "access_token" in secret:
        return {"Authorization": f"Bearer {secret['access_token']}"}
    if "api_token" in secret and "email" in secret:
        import base64
        credentials = f"{secret['email']}/token:{secret['api_token']}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}
    token = get_oauth_token()
    return {"Authorization": f"Bearer {token}"}


def fetch_paginated(endpoint: str, list_key: str, headers: dict, execution_start: float, max_pages: int = None) -> list:
    max_pages = max_pages or BATCH_PAGE_LIMIT
    all_records = []
    url = f"{ZENDESK_API_BASE}{endpoint}"
    page = 0

    while url and page < max_pages:
        elapsed = time_mod.time() - execution_start
        if elapsed > MAX_EXECUTION_SECONDS:
            break
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            if elapsed + retry_after > MAX_EXECUTION_SECONDS:
                break
            time_mod.sleep(retry_after)
            continue
        resp.raise_for_status()
        data = resp.json()
        records = data.get(list_key, [])
        all_records.extend(records)

        url = data.get("next_page")
        if not url and "end_time" in data and not data.get("end_of_stream", True):
            url = f"{ZENDESK_API_BASE}{endpoint.split('?')[0]}?start_time={data['end_time']}"
        page += 1

    return all_records


def fetch_incremental(endpoint: str, list_key: str, headers: dict, start_time: int, execution_start: float, max_pages: int = None) -> tuple:
    max_pages = max_pages or BATCH_PAGE_LIMIT
    all_records = []
    separator = "&" if "?" in endpoint else "?"
    url = f"{ZENDESK_API_BASE}{endpoint}{separator}start_time={start_time}"
    page = 0
    last_end_time = start_time
    timed_out = False

    while url and page < max_pages:
        elapsed = time_mod.time() - execution_start
        if elapsed > MAX_EXECUTION_SECONDS:
            timed_out = True
            break
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            if elapsed + retry_after > MAX_EXECUTION_SECONDS:
                timed_out = True
                break
            time_mod.sleep(retry_after)
            continue
        resp.raise_for_status()
        data = resp.json()
        records = data.get(list_key, [])
        all_records.extend(records)

        if data.get("end_of_stream", False):
            break
        end_time = data.get("end_time")
        if end_time:
            last_end_time = end_time
            base_endpoint = endpoint.split("?")[0]
            url = f"{ZENDESK_API_BASE}{base_endpoint}?start_time={end_time}"
        else:
            url = data.get("next_page")
        page += 1

    return all_records, last_end_time, timed_out


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    df = df.convert_dtypes()
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def upload_to_stage(session: Session, parquet_bytes: bytes, stage_path: str, entity_name: str, run_ts: datetime) -> None:
    file_name = f"{entity_name.lower()}_{run_ts.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
    tmp = f"/tmp/{file_name}"
    with open(tmp, "wb") as f:
        f.write(parquet_bytes)
    session.file.put(
        local_file_name=tmp,
        stage_location=stage_path,
        auto_compress=False,
        overwrite=False,
    )


def build_stage_path(base_stage: str, entity_name: str, run_ts: datetime) -> str:
    return (
        f"@{base_stage}/{entity_name}/"
        f"year_updated_at={run_ts.year}/"
        f"month_updated_at={run_ts.month}/"
        f"day_updated_at={run_ts.day}/"
    )


def dq_row_count(df: pd.DataFrame, entity_name: str) -> dict:
    count = len(df)
    return {
        "check": "row_count",
        "status": "FAIL" if count == 0 else "PASS",
        "detail": f"{count} registros extraídos",
    }


def dq_duplicates(df: pd.DataFrame, entity_name: str, id_field: str) -> dict:
    if id_field not in df.columns:
        return {"check": "duplicate_ids", "status": "WARN", "detail": f"Coluna {id_field} não encontrada"}
    dupes = df[id_field].duplicated().sum()
    return {
        "check": "duplicate_ids",
        "status": "WARN" if dupes > 0 else "PASS",
        "detail": f"{dupes} id(s) duplicado(s)",
    }


def dq_required_fields(df: pd.DataFrame, entity_name: str) -> list:
    results = []
    for field in REQUIRED_FIELDS.get(entity_name, []):
        if field not in df.columns:
            results.append({
                "check": f"required_field_{field}",
                "status": "WARN",
                "detail": f"Campo '{field}' ausente no DataFrame",
            })
            continue
        null_count = df[field].isna().sum()
        total = len(df)
        pct = (null_count / total * 100) if total > 0 else 0
        results.append({
            "check": f"required_field_{field}",
            "status": "FAIL" if pct > 50 else ("WARN" if pct > 10 else "PASS"),
            "detail": f"{null_count} nulos ({pct:.1f}%) no campo '{field}'",
        })
    return results


def run_dq_checks(df: pd.DataFrame, entity_name: str, id_field: str) -> tuple:
    checks = []
    checks.append(dq_row_count(df, entity_name))
    checks.append(dq_duplicates(df, entity_name, id_field))
    checks.extend(dq_required_fields(df, entity_name))
    has_failures = any(c["status"] == "FAIL" for c in checks)
    return checks, not has_failures


def log_extraction(session, obj_name, row_count, columns, status, message, run_ts):
    session.sql(
        """
        INSERT INTO WILD_CARD.BRONZE.EXTRACTION_LOG_ZENDESK
            (OBJECT_NAME, ROW_COUNT, COLUMNS, STATUS, MESSAGE, EXTRACTED_AT)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        params=[obj_name, row_count, json.dumps(columns), status, message[:2000], run_ts.isoformat()]
    ).collect()


def log_dq(session, obj_name, checks, run_ts):
    for c in checks:
        session.sql(
            """
            INSERT INTO WILD_CARD.BRONZE.DATA_QUALITY_LOG_ZENDESK
                (OBJECT_NAME, CHECK_NAME, STATUS, DETAIL, CHECKED_AT)
            VALUES (?, ?, ?, ?, ?)
            """,
            params=[obj_name, c["check"], c["status"], c["detail"], run_ts.isoformat()]
        ).collect()


def get_last_successful_extraction(session, entity_name):
    try:
        rows = session.sql(
            """
            SELECT EXTRACTED_AT
            FROM WILD_CARD.BRONZE.EXTRACTION_LOG_ZENDESK
            WHERE OBJECT_NAME = ? AND STATUS = 'SUCCESS'
            ORDER BY EXTRACTED_AT DESC
            LIMIT 1
            """,
            params=[entity_name]
        ).collect()
        if rows:
            ts = rows[0][0]
            if hasattr(ts, 'timestamp'):
                return int(ts.timestamp())
            return int(datetime.fromisoformat(str(ts)).timestamp())
        return None
    except Exception:
        return None


def run_extraction(session: Session) -> str:
    BASE_STAGE = "WILD_CARD.BRONZE.ZENDESK_S3_DATAMESH"
    run_ts = datetime.now(timezone(timedelta(hours=-3)))
    execution_start = time_mod.time()
    results = []

    headers = get_auth_headers()

    for entity_name, config in ENTITIES.items():
        elapsed = time_mod.time() - execution_start
        if elapsed > MAX_EXECUTION_SECONDS:
            results.append({"entity": entity_name, "rows": 0, "status": "TIMEOUT_SKIPPED"})
            continue
        try:
            endpoint = config["endpoint"]
            list_key = config["list_key"]
            id_field = config["id_field"]

            if config["incremental"]:
                last_ts = get_last_successful_extraction(session, entity_name)
                start_time = last_ts if last_ts else int((run_ts - timedelta(days=30)).timestamp())
                records, last_end_time, timed_out = fetch_incremental(endpoint, list_key, headers, start_time, execution_start)
            else:
                records = fetch_paginated(endpoint, list_key, headers, execution_start)

            if not records:
                log_extraction(session, entity_name, 0, [], "SKIPPED",
                               "Nenhum registro retornado pela API.", run_ts)
                results.append({"entity": entity_name, "rows": 0, "status": "SKIPPED"})
                continue

            df = pd.DataFrame(records)
            for col in df.columns:
                if df[col].apply(lambda v: isinstance(v, (dict, list))).any():
                    df[col] = df[col].apply(
                        lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                    )

            df["datalake_updated_at"] = run_ts.isoformat()
            row_count = len(df)
            columns = df.columns.tolist()

            checks, can_upload = run_dq_checks(df, entity_name, id_field)
            log_dq(session, entity_name, checks, run_ts)

            if not can_upload:
                fails = [c for c in checks if c["status"] == "FAIL"]
                msg = f"Upload bloqueado por {len(fails)} check(s) FAIL: " + \
                      " | ".join(c['detail'] for c in fails)
                log_extraction(session, entity_name, row_count, columns, "DQ_FAIL", msg, run_ts)
                results.append({"entity": entity_name, "rows": row_count, "status": "DQ_FAIL"})
                continue

            parquet_bytes = df_to_parquet_bytes(df)
            stage_path = build_stage_path(BASE_STAGE, entity_name, run_ts)
            upload_to_stage(session, parquet_bytes, stage_path, entity_name, run_ts)

            warns = [c for c in checks if c["status"] == "WARN"]
            status_msg = f"Enviado para {stage_path}" + (
                f" | {len(warns)} WARN(s)" if warns else ""
            )
            log_extraction(session, entity_name, row_count, columns, "SUCCESS", status_msg, run_ts)
            results.append({
                "entity": entity_name,
                "rows": row_count,
                "columns": len(columns),
                "status": "SUCCESS",
                "warnings": len(warns),
            })

        except Exception as exc:
            err = str(exc)[:2000]
            log_extraction(session, entity_name, 0, [], "ERROR", err, run_ts)
            results.append({"entity": entity_name, "rows": 0, "status": "ERROR", "error": err})

    return json.dumps({"run_at": run_ts.isoformat(), "results": results}, ensure_ascii=False)
$$;

-- ──────────────────────────────────────────────
-- 3. TASK — Extração a cada 6h
-- ──────────────────────────────────────────────
CREATE OR REPLACE TASK WILD_CARD.BRONZE.TASK_ZENDESK_INGESTION
  WAREHOUSE = WH_DATA_SERVICES
  SCHEDULE  = 'USING CRON 0 */6 * * * America/Sao_Paulo'
AS
  CALL WILD_CARD.BRONZE.ZENDESK_TO_S3();

ALTER TASK WILD_CARD.BRONZE.TASK_ZENDESK_INGESTION RESUME;

-- ============================================================
-- 5. EXTRAÇÃO COMPLEMENTAR — ticket metrics via incremental tickets sideload
-- ------------------------------------------------------------
-- Motivo: o endpoint /incremental/ticket_metric_events.json (usado em
-- ticketMetrics) não retorna mais reply_time_in_minutes / business
-- hours desde 2026-05-04.
--
-- Endpoint /api/v2/ticket_metrics.json (full snapshot) tentado antes
-- mas inviável (>60min para 461k tickets, estoura warehouse timeout).
--
-- Solução adotada: /api/v2/incremental/tickets.json?include=metric_sets
-- — incremental por start_time, paginado, com sideload de metric_sets.
-- Cada metric_set traz reply_time_in_minutes (calendar+business),
-- first_resolution_time, etc. Cursor persistido em
-- WILD_CARD.BRONZE.ZENDESK_INCREMENTAL_CURSOR. Primeira execução
-- pode levar várias rodadas (cada uma <55min); rodadas posteriores
-- ficam em poucos minutos (apenas delta).
-- ============================================================

CREATE TABLE IF NOT EXISTS WILD_CARD.BRONZE.ZENDESK_INCREMENTAL_CURSOR (
    ENTITY     STRING       NOT NULL,
    END_TIME   NUMBER       NOT NULL,
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ZENDESK_INCREMENTAL_CURSOR PRIMARY KEY (ENTITY)
);

CREATE OR REPLACE PROCEDURE WILD_CARD.BRONZE.ZENDESK_TICKET_METRICS_FULL_TO_S3()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'requests',
    'pandas',
    'pyarrow'
)
EXTERNAL_ACCESS_INTEGRATIONS = (ZENDESK_ACCESS_INTEGRATION)
SECRETS = (
    'zd_oauth' = WILD_CARD.BRONZE.ZENDESK_OAUTH_SECRET
)
HANDLER = 'run_extraction'
EXECUTE AS CALLER
AS
$$
import io
import uuid
import json
import time as time_mod
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from snowflake.snowpark import Session
import _snowflake

MAX_EXECUTION_SECONDS = 3300
BATCH_PAGE_LIMIT      = 50000
PAGE_SIZE             = 1000

ZENDESK_SUBDOMAIN = "<YOUR_ZENDESK_SUBDOMAIN>"
ZENDESK_API_BASE = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2"

ENTITY_NAME = "ticketMetricsFull"
ENDPOINT    = "/incremental/tickets.json"
LIST_KEY    = "metric_sets"
ID_FIELD    = "id"
REQUIRED_FIELDS = ["id", "ticket_id"]


def get_oauth_token() -> str:
    secret = json.loads(_snowflake.get_generic_secret_string('zd_oauth'))
    if "access_token" in secret:
        return secret["access_token"]
    if "api_token" in secret and "email" in secret:
        return None
    token_url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/oauth/tokens"
    resp = requests.post(token_url, data={
        "client_id": secret["client_id"],
        "client_secret": secret["client_secret"],
        "grant_type": "client_credentials",
        "scope": "read"
    })
    if resp.status_code == 200:
        data = resp.json()
        return data.get("access_token", data.get("token", {}).get("full_token", ""))
    raise Exception(f"OAuth falhou: {resp.status_code} - {resp.text}")


def get_auth_headers() -> dict:
    secret = json.loads(_snowflake.get_generic_secret_string('zd_oauth'))
    if "access_token" in secret:
        return {"Authorization": f"Bearer {secret['access_token']}"}
    if "api_token" in secret and "email" in secret:
        import base64
        credentials = f"{secret['email']}/token:{secret['api_token']}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}
    token = get_oauth_token()
    return {"Authorization": f"Bearer {token}"}


def get_cursor(session: Session) -> int:
    rows = session.sql(
        "SELECT END_TIME FROM WILD_CARD.BRONZE.ZENDESK_INCREMENTAL_CURSOR WHERE ENTITY = ?",
        params=[ENTITY_NAME]
    ).collect()
    if rows:
        return int(rows[0][0])
    return 0


def save_cursor(session: Session, end_time: int) -> None:
    session.sql(
        """
        MERGE INTO WILD_CARD.BRONZE.ZENDESK_INCREMENTAL_CURSOR tgt
        USING (SELECT ? AS ENTITY, ?::NUMBER AS END_TIME) src
            ON tgt.ENTITY = src.ENTITY
        WHEN MATCHED THEN UPDATE SET END_TIME = src.END_TIME, UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (ENTITY, END_TIME) VALUES (src.ENTITY, src.END_TIME)
        """,
        params=[ENTITY_NAME, int(end_time)]
    ).collect()


def fetch_incremental_with_sideload(headers: dict, start_time: int, execution_start: float) -> tuple:
    all_records = []
    last_end_time = start_time
    end_of_stream = False
    timed_out = False

    sep = "&" if "?" in ENDPOINT else "?"
    url = f"{ZENDESK_API_BASE}{ENDPOINT}{sep}include=metric_sets&per_page={PAGE_SIZE}&start_time={start_time}"

    MAX_5XX_ATTEMPTS = 6
    MAX_NETWORK_ATTEMPTS = 4
    MAX_401_ATTEMPTS = 2
    page = 0

    while url and page < BATCH_PAGE_LIMIT:
        elapsed = time_mod.time() - execution_start
        if elapsed > MAX_EXECUTION_SECONDS:
            timed_out = True
            break

        attempt_5xx = 0
        attempt_net = 0
        attempt_401 = 0
        resp = None
        while True:
            try:
                resp = requests.get(url, headers=headers, timeout=60)
            except (requests.ConnectionError, requests.Timeout) as e:
                attempt_net += 1
                if attempt_net > MAX_NETWORK_ATTEMPTS:
                    raise Exception(f"Falha de rede após {attempt_net} tentativas: {e}")
                wait = 5 * (2 ** (attempt_net - 1))
                if (time_mod.time() - execution_start) + wait > MAX_EXECUTION_SECONDS:
                    timed_out = True
                    break
                time_mod.sleep(wait)
                continue

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                if (time_mod.time() - execution_start) + retry_after > MAX_EXECUTION_SECONDS:
                    timed_out = True
                    break
                time_mod.sleep(retry_after)
                continue

            if resp.status_code == 401:
                attempt_401 += 1
                if attempt_401 > MAX_401_ATTEMPTS:
                    resp.raise_for_status()
                try:
                    new_headers = get_auth_headers()
                    headers.clear()
                    headers.update(new_headers)
                except Exception as auth_err:
                    raise Exception(f"401 e falha ao refrescar token: {auth_err}")
                time_mod.sleep(2)
                continue

            if 500 <= resp.status_code < 600:
                attempt_5xx += 1
                if attempt_5xx > MAX_5XX_ATTEMPTS:
                    resp.raise_for_status()
                wait = 5 * (2 ** (attempt_5xx - 1))
                if (time_mod.time() - execution_start) + wait > MAX_EXECUTION_SECONDS:
                    timed_out = True
                    break
                time_mod.sleep(wait)
                continue

            break

        if timed_out:
            break

        resp.raise_for_status()
        data = resp.json()
        records = data.get(LIST_KEY, []) or []
        all_records.extend(records)

        if data.get("end_time"):
            last_end_time = int(data["end_time"])

        if data.get("end_of_stream") is True:
            end_of_stream = True
            url = None
            break

        url = data.get("next_page")
        page += 1

    return all_records, last_end_time, end_of_stream, timed_out


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    df = df.convert_dtypes()
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def upload_to_stage(session: Session, parquet_bytes: bytes, stage_path: str, run_ts: datetime) -> None:
    file_name = f"{ENTITY_NAME.lower()}_{run_ts.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
    tmp = f"/tmp/{file_name}"
    with open(tmp, "wb") as f:
        f.write(parquet_bytes)
    session.file.put(
        local_file_name=tmp,
        stage_location=stage_path,
        auto_compress=False,
        overwrite=False,
    )


def build_stage_path(base_stage: str, run_ts: datetime) -> str:
    return (
        f"@{base_stage}/{ENTITY_NAME}/"
        f"year_updated_at={run_ts.year}/"
        f"month_updated_at={run_ts.month}/"
        f"day_updated_at={run_ts.day}/"
    )


def run_dq_checks(df: pd.DataFrame) -> tuple:
    checks = []
    count = len(df)
    checks.append({"check": "row_count", "status": "PASS" if count > 0 else "WARN", "detail": f"{count} registros extraídos"})
    if ID_FIELD in df.columns:
        dupes = int(df[ID_FIELD].duplicated().sum())
        checks.append({"check": "duplicate_ids", "status": "WARN" if dupes > 0 else "PASS", "detail": f"{dupes} id(s) duplicado(s)"})
    else:
        checks.append({"check": "duplicate_ids", "status": "WARN", "detail": f"Coluna {ID_FIELD} não encontrada"})
    for field in REQUIRED_FIELDS:
        if field not in df.columns:
            checks.append({"check": f"required_field_{field}", "status": "WARN", "detail": f"Campo '{field}' ausente"})
            continue
        null_count = int(df[field].isna().sum())
        pct = (null_count / count * 100) if count > 0 else 0
        status = "FAIL" if pct > 50 else ("WARN" if pct > 10 else "PASS")
        checks.append({"check": f"required_field_{field}", "status": status, "detail": f"{null_count} nulos ({pct:.1f}%) em '{field}'"})
    has_failures = any(c["status"] == "FAIL" for c in checks)
    return checks, not has_failures


def log_extraction(session, row_count, columns, status, message, run_ts):
    session.sql(
        """
        INSERT INTO WILD_CARD.BRONZE.EXTRACTION_LOG_ZENDESK
            (OBJECT_NAME, ROW_COUNT, COLUMNS, STATUS, MESSAGE, EXTRACTED_AT)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        params=[ENTITY_NAME, row_count, json.dumps(columns), status, message[:2000], run_ts.isoformat()]
    ).collect()


def log_dq(session, checks, run_ts):
    for c in checks:
        session.sql(
            """
            INSERT INTO WILD_CARD.BRONZE.DATA_QUALITY_LOG_ZENDESK
                (OBJECT_NAME, CHECK_NAME, STATUS, DETAIL, CHECKED_AT)
            VALUES (?, ?, ?, ?, ?)
            """,
            params=[ENTITY_NAME, c["check"], c["status"], c["detail"], run_ts.isoformat()]
        ).collect()


def run_extraction(session: Session) -> str:
    BASE_STAGE = "WILD_CARD.BRONZE.ZENDESK_S3_DATAMESH"
    run_ts = datetime.now(timezone(timedelta(hours=-3)))
    execution_start = time_mod.time()

    try:
        start_time = get_cursor(session)
        headers = get_auth_headers()

        records, last_end_time, end_of_stream, timed_out = fetch_incremental_with_sideload(
            headers, start_time, execution_start
        )

        elapsed = int(time_mod.time() - execution_start)
        progress = f"start={start_time} -> end={last_end_time} | end_of_stream={end_of_stream} | timed_out={timed_out} | {elapsed}s"

        if not records:
            if last_end_time > start_time:
                save_cursor(session, last_end_time)
            log_extraction(session, 0, [], "SKIPPED", f"Sem metric_sets retornados | {progress}", run_ts)
            return json.dumps({"run_at": run_ts.isoformat(), "entity": ENTITY_NAME, "rows": 0, "status": "SKIPPED", "progress": progress}, ensure_ascii=False)

        df = pd.DataFrame(records)
        for col in df.columns:
            if df[col].apply(lambda v: isinstance(v, (dict, list))).any():
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                )
        df["datalake_updated_at"] = run_ts.isoformat()
        row_count = len(df)
        columns = df.columns.tolist()

        checks, can_upload = run_dq_checks(df)
        log_dq(session, checks, run_ts)

        if not can_upload:
            fails = [c for c in checks if c["status"] == "FAIL"]
            msg = f"Upload bloqueado por {len(fails)} check(s) FAIL: " + " | ".join(c['detail'] for c in fails)
            log_extraction(session, row_count, columns, "DQ_FAIL", f"{msg} | {progress}", run_ts)
            return json.dumps({"run_at": run_ts.isoformat(), "entity": ENTITY_NAME, "rows": row_count, "status": "DQ_FAIL", "progress": progress}, ensure_ascii=False)

        parquet_bytes = df_to_parquet_bytes(df)
        stage_path = build_stage_path(BASE_STAGE, run_ts)
        upload_to_stage(session, parquet_bytes, stage_path, run_ts)

        save_cursor(session, last_end_time)

        warns = [c for c in checks if c["status"] == "WARN"]
        suffix = " | TIMED_OUT (rerun para continuar)" if timed_out else (" | END_OF_STREAM" if end_of_stream else "")
        status_msg = f"Enviado para {stage_path} | {progress}" + (f" | {len(warns)} WARN(s)" if warns else "") + suffix
        log_extraction(session, row_count, columns, "SUCCESS", status_msg, run_ts)
        return json.dumps({
            "run_at": run_ts.isoformat(), "entity": ENTITY_NAME, "rows": row_count,
            "elapsed_seconds": elapsed, "timed_out": timed_out, "end_of_stream": end_of_stream,
            "cursor_start": start_time, "cursor_end": last_end_time, "status": "SUCCESS"
        }, ensure_ascii=False)

    except Exception as exc:
        err = str(exc)[:2000]
        log_extraction(session, 0, [], "ERROR", err, run_ts)
        return json.dumps({"run_at": run_ts.isoformat(), "entity": ENTITY_NAME, "status": "ERROR", "error": err}, ensure_ascii=False)
$$;

-- ──────────────────────────────────────────────
-- TASK — a cada 6h BRT (0,6,12,18), sincronizada com as demais tasks Zendesk.
-- Sem USER_TASK_TIMEOUT_MS: o limite efetivo é o STATEMENT_TIMEOUT_IN_SECONDS
-- do warehouse (600s). A proc é reentrant via cursor, então delta grande
-- apenas exige execuções extras (sem perda de dados).
-- ──────────────────────────────────────────────
CREATE OR REPLACE TASK WILD_CARD.BRONZE.TASK_ZENDESK_TICKET_METRICS_FULL
  WAREHOUSE = WH_DATA_SERVICES
  SCHEDULE  = 'USING CRON 0 */6 * * * America/Sao_Paulo'
AS
  CALL WILD_CARD.BRONZE.ZENDESK_TICKET_METRICS_FULL_TO_S3();

ALTER TASK WILD_CARD.BRONZE.TASK_ZENDESK_TICKET_METRICS_FULL RESUME;
