-- ============================================================
-- TRANSFORMAÇÃO: CX.BRONZE → CX.SILVER (Zendesk)
-- Apenas ZENDESK_TICKETS (com métricas embutidas) e ZENDESK_USERS
-- ============================================================

-- ============================================================
-- 1. TABELAS SILVER
-- ============================================================
-- ------------------------------------------------------------
-- 1.1 Tickets (com métricas + custom fields expandidos)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE CX.SILVER.ZENDESK_TICKETS (
  ID                                NUMBER,
  SUBJECT                           STRING,
  DESCRIPTION                       STRING,
  STATUS                            STRING,
  PRIORITY                          STRING,
  BRAND_ID                          NUMBER,
  ASSIGNEE_ID                       NUMBER,
  REQUESTER_ID                      NUMBER,
  SUBMITTER_ID                      NUMBER,
  ORGANIZATION_ID                   NUMBER,
  GROUP_ID                          NUMBER,
  TICKET_FORM_ID                    NUMBER,
  VIA                               STRING,
  TAGS                              STRING,
  SATISFACTION_RATING               STRING,
  IS_PUBLIC                         BOOLEAN,
  HAS_INCIDENTS                     BOOLEAN,
  FROM_MESSAGING_CHANNEL            BOOLEAN,
  CF_ACIONAR_CONTABILIDADE          STRING,
  CF_MOTIVO_DE_CONTATO              STRING,
  CF_TAG_DE_ATENDIMENTO             STRING,
  CF_TAG_DE_ATENDIMENTO_2           STRING,
  CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS STRING,
  CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS STRING,
  CF_CONVERSATION_ID                STRING,
  CF_HSM_DATA                       STRING,
  CF_HSM_DESCRICAO_TEMPLATE         STRING,
  CF_HSM_STATUS                     STRING,
  CF_HSM_TEMPLATE                   STRING,
  CF_WHATSAPP_ID                    STRING,
  CF_MIGRACAO_DE_AREA               STRING,
  CF_TAG_DE_ATENDIMENTO_3           STRING,
  CF_RENOVOU_CERTIFICADO_ECNPJ      STRING,
  CF_ATRIBUICAO_AUTOMATICA          STRING,
  CF_PIECE_OF_CAKE_DISPARO          STRING,
  CUSTOM_FIELDS_JSON                STRING,
  REOPENS                           NUMBER,
  REPLIES                           NUMBER,
  ASSIGNEE_STATIONS                 NUMBER,
  GROUP_STATIONS                    NUMBER,
  AGENT_WAIT_TIME_IN_MINUTES        STRING,
  REQUESTER_WAIT_TIME_IN_MINUTES    STRING,
  FIRST_RESOLUTION_TIME_IN_MINUTES  STRING,
  FULL_RESOLUTION_TIME_IN_MINUTES   STRING,
  ON_HOLD_TIME_IN_MINUTES           STRING,
  REPLY_TIME_IN_MINUTES             STRING,
  ASSIGNED_AT                       TIMESTAMP_NTZ,
  INITIALLY_ASSIGNED_AT             TIMESTAMP_NTZ,
  SOLVED_AT                         TIMESTAMP_NTZ,
  LATEST_COMMENT_ADDED_AT           TIMESTAMP_NTZ,
  CREATED_AT                        TIMESTAMP_NTZ,
  UPDATED_AT                        TIMESTAMP_NTZ,
  DATALAKE_UPDATED_AT               TIMESTAMP_NTZ,
  _SNOWFLAKE_LOADED_AT              TIMESTAMP_NTZ
);

-- ------------------------------------------------------------
-- 1.2 Users
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE CX.SILVER.ZENDESK_USERS (
  ID                   NUMBER,
  NAME                 STRING,
  EMAIL                STRING,
  ROLE                 STRING,
  LOCALE               STRING,
  TIME_ZONE            STRING,
  TAGS                 STRING,
  USER_FIELDS          STRING,
  ACTIVE               BOOLEAN,
  VERIFIED             BOOLEAN,
  SUSPENDED            BOOLEAN,
  MODERATOR            BOOLEAN,
  RESTRICTED_AGENT     BOOLEAN,
  SHARED               BOOLEAN,
  SHARED_AGENT         BOOLEAN,
  TICKET_RESTRICTION   STRING,
  CREATED_AT           TIMESTAMP_NTZ,
  UPDATED_AT           TIMESTAMP_NTZ,
  DATALAKE_UPDATED_AT  TIMESTAMP_NTZ,
  _SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ
);

-- ============================================================
-- 2. MASKING POLICIES (PII)
-- ============================================================
CREATE OR REPLACE MASKING POLICY CX.SILVER.MASK_ZENDESK_USER_NAME_PII
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN')
      THEN val
    ELSE '**********'
  END;

CREATE OR REPLACE MASKING POLICY CX.SILVER.MASK_ZENDESK_USER_EMAIL_PII
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN')
      THEN val
    ELSE SHA2(val)
  END;

CREATE OR REPLACE MASKING POLICY CX.SILVER.MASK_ZENDESK_TICKET_DESCRIPTION_PII
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN')
      THEN val
    ELSE '**********'
  END;

CREATE OR REPLACE MASKING POLICY CX.SILVER.MASK_ZENDESK_TICKET_SUBJECT_PII
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN')
      THEN val
    ELSE LEFT(val, 4) || REPEAT('*', GREATEST(LENGTH(val) - 4, 0))
  END;

ALTER TABLE CX.SILVER.ZENDESK_USERS MODIFY COLUMN NAME
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_USER_NAME_PII;

ALTER TABLE CX.SILVER.ZENDESK_USERS MODIFY COLUMN EMAIL
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_USER_EMAIL_PII;

ALTER TABLE CX.SILVER.ZENDESK_TICKETS MODIFY COLUMN DESCRIPTION
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_TICKET_DESCRIPTION_PII;

ALTER TABLE CX.SILVER.ZENDESK_TICKETS MODIFY COLUMN SUBJECT
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_TICKET_SUBJECT_PII;

-- ============================================================
-- 3. PROCEDURES DE TRANSFORMAÇÃO
-- ============================================================
-- ------------------------------------------------------------
-- 3.1 Tickets (JOIN com ticket_metrics + custom fields pivotados)
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE CX.SILVER.ZENDESK_TICKETS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  LET v_watermark TIMESTAMP_NTZ := (SELECT COALESCE(MAX(_SNOWFLAKE_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ) FROM CX.SILVER.ZENDESK_TICKETS);

  MERGE INTO CX.SILVER.ZENDESK_TICKETS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        T.RAW_DATA:id::NUMBER                        AS ID,
        UPPER(TRIM(T.RAW_DATA:subject::STRING))      AS SUBJECT,
        T.RAW_DATA:description::STRING               AS DESCRIPTION,
        UPPER(TRIM(T.RAW_DATA:status::STRING))       AS STATUS,
        UPPER(TRIM(T.RAW_DATA:priority::STRING))     AS PRIORITY,
        T.RAW_DATA:brand_id::NUMBER                  AS BRAND_ID,
        T.RAW_DATA:assignee_id::NUMBER               AS ASSIGNEE_ID,
        T.RAW_DATA:requester_id::NUMBER              AS REQUESTER_ID,
        T.RAW_DATA:submitter_id::NUMBER              AS SUBMITTER_ID,
        T.RAW_DATA:organization_id::NUMBER           AS ORGANIZATION_ID,
        T.RAW_DATA:group_id::NUMBER                  AS GROUP_ID,
        T.RAW_DATA:ticket_form_id::NUMBER            AS TICKET_FORM_ID,
        T.RAW_DATA:via::STRING                       AS VIA,
        T.RAW_DATA:tags::STRING                      AS TAGS,
        T.RAW_DATA:satisfaction_rating::STRING        AS SATISFACTION_RATING,
        T.RAW_DATA:is_public::BOOLEAN                AS IS_PUBLIC,
        T.RAW_DATA:has_incidents::BOOLEAN            AS HAS_INCIDENTS,
        T.RAW_DATA:from_messaging_channel::BOOLEAN   AS FROM_MESSAGING_CHANNEL,
        cf_obj['35984459197844']::STRING              AS CF_ACIONAR_CONTABILIDADE,
        cf_obj['36210587567380']::STRING              AS CF_MOTIVO_DE_CONTATO,
        cf_obj['36211214371476']::STRING              AS CF_TAG_DE_ATENDIMENTO,
        cf_obj['36211567005076']::STRING              AS CF_TAG_DE_ATENDIMENTO_2,
        cf_obj['36382370672532']::STRING              AS CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS,
        cf_obj['36382757815188']::STRING              AS CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS,
        cf_obj['37023940473876']::STRING              AS CF_CONVERSATION_ID,
        cf_obj['37023985232916']::STRING              AS CF_HSM_DATA,
        cf_obj['37024045863700']::STRING              AS CF_HSM_DESCRICAO_TEMPLATE,
        cf_obj['37024057307540']::STRING              AS CF_HSM_STATUS,
        cf_obj['37024131393428']::STRING              AS CF_HSM_TEMPLATE,
        cf_obj['39367844980244']::STRING              AS CF_WHATSAPP_ID,
        cf_obj['40432754854548']::STRING              AS CF_MIGRACAO_DE_AREA,
        cf_obj['42153988066068']::STRING              AS CF_TAG_DE_ATENDIMENTO_3,
        cf_obj['47105931784596']::STRING              AS CF_RENOVOU_CERTIFICADO_ECNPJ,
        cf_obj['47821519109140']::STRING              AS CF_ATRIBUICAO_AUTOMATICA,
        cf_obj['48706380227860']::STRING              AS CF_PIECE_OF_CAKE_DISPARO,
        T.RAW_DATA:custom_fields::STRING             AS CUSTOM_FIELDS_JSON,
        M.RAW_DATA:reopens::NUMBER                   AS REOPENS,
        M.RAW_DATA:replies::NUMBER                   AS REPLIES,
        M.RAW_DATA:assignee_stations::NUMBER         AS ASSIGNEE_STATIONS,
        M.RAW_DATA:group_stations::NUMBER            AS GROUP_STATIONS,
        M.RAW_DATA:agent_wait_time_in_minutes::STRING AS AGENT_WAIT_TIME_IN_MINUTES,
        M.RAW_DATA:requester_wait_time_in_minutes::STRING AS REQUESTER_WAIT_TIME_IN_MINUTES,
        M.RAW_DATA:first_resolution_time_in_minutes::STRING AS FIRST_RESOLUTION_TIME_IN_MINUTES,
        M.RAW_DATA:full_resolution_time_in_minutes::STRING AS FULL_RESOLUTION_TIME_IN_MINUTES,
        M.RAW_DATA:on_hold_time_in_minutes::STRING   AS ON_HOLD_TIME_IN_MINUTES,
        M.RAW_DATA:reply_time_in_minutes::STRING     AS REPLY_TIME_IN_MINUTES,
        TRY_TO_TIMESTAMP_NTZ(M.RAW_DATA:assigned_at::STRING) AS ASSIGNED_AT,
        TRY_TO_TIMESTAMP_NTZ(M.RAW_DATA:initially_assigned_at::STRING) AS INITIALLY_ASSIGNED_AT,
        TRY_TO_TIMESTAMP_NTZ(M.RAW_DATA:solved_at::STRING) AS SOLVED_AT,
        TRY_TO_TIMESTAMP_NTZ(M.RAW_DATA:latest_comment_added_at::STRING) AS LATEST_COMMENT_ADDED_AT,
        TRY_TO_TIMESTAMP_NTZ(T.RAW_DATA:created_at::STRING) AS CREATED_AT,
        TRY_TO_TIMESTAMP_NTZ(T.RAW_DATA:updated_at::STRING) AS UPDATED_AT,
        TRY_TO_TIMESTAMP_NTZ(T.RAW_DATA:datalake_updated_at::STRING) AS DATALAKE_UPDATED_AT,
        T._SNOWFLAKE_LOADED_AT,
        ROW_NUMBER() OVER (PARTITION BY T.RAW_DATA:id::NUMBER ORDER BY TRY_TO_TIMESTAMP_NTZ(T.RAW_DATA:updated_at::STRING) DESC NULLS LAST) AS rn
      FROM (
        SELECT TKT.RAW_DATA, TKT._SNOWFLAKE_LOADED_AT,
          OBJECT_AGG(cf_id, cf_val) AS cf_obj
        FROM (
          SELECT TKT2.RAW_DATA, TKT2._SNOWFLAKE_LOADED_AT,
                 f.value:id::STRING AS cf_id,
                 f.value:value AS cf_val,
                 ROW_NUMBER() OVER (PARTITION BY TKT2.RAW_DATA:id::NUMBER, f.value:id::STRING ORDER BY f.index) AS dedup_rn
          FROM CX.BRONZE.ZENDESK_TICKETS TKT2,
          LATERAL FLATTEN(input => TRY_PARSE_JSON(TKT2.RAW_DATA:custom_fields::STRING), OUTER => TRUE) f
          WHERE TKT2.RAW_DATA:id IS NOT NULL
            AND TKT2._SNOWFLAKE_LOADED_AT > :v_watermark
        ) TKT
        WHERE TKT.dedup_rn = 1
        GROUP BY TKT.RAW_DATA, TKT._SNOWFLAKE_LOADED_AT
      ) T
      LEFT JOIN (
        -- UNION das duas bronzes:
        --  (a) ZENDESK_TICKET_METRICS: extração antiga (até 2026-05-04) e
        --      eventos pós-corte (descartados via filtro reply_time NOT NULL).
        --  (b) ZENDESK_TICKET_METRICS_FULL: nova fonte primária via
        --      /incremental/tickets?include=metric_sets, traz todos os
        --      campos agregados (calendar+business).
        -- Sem filtro de watermark — sempre pega o snapshot mais recente
        -- por ticket para garantir backfill quando novos metrics chegam.
        SELECT *
        FROM (
          SELECT RAW_DATA, _SNOWFLAKE_LOADED_AT,
            ROW_NUMBER() OVER (
              PARTITION BY RAW_DATA:ticket_id::NUMBER
              ORDER BY TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING) DESC NULLS LAST,
                       _SNOWFLAKE_LOADED_AT DESC NULLS LAST
            ) AS mrn
          FROM (
            SELECT RAW_DATA, _SNOWFLAKE_LOADED_AT
            FROM CX.BRONZE.ZENDESK_TICKET_METRICS
            WHERE RAW_DATA:reply_time_in_minutes IS NOT NULL
            UNION ALL
            SELECT RAW_DATA, _SNOWFLAKE_LOADED_AT
            FROM CX.BRONZE.ZENDESK_TICKET_METRICS_FULL
          )
        ) WHERE mrn = 1
      ) M ON T.RAW_DATA:id::NUMBER = M.RAW_DATA:ticket_id::NUMBER
    )
    WHERE rn = 1
  ) src ON tgt.ID = src.ID

  WHEN MATCHED AND src.UPDATED_AT > tgt.UPDATED_AT THEN UPDATE SET
    SUBJECT = src.SUBJECT, DESCRIPTION = src.DESCRIPTION, STATUS = src.STATUS,
    PRIORITY = src.PRIORITY, BRAND_ID = src.BRAND_ID, ASSIGNEE_ID = src.ASSIGNEE_ID,
    REQUESTER_ID = src.REQUESTER_ID, SUBMITTER_ID = src.SUBMITTER_ID,
    ORGANIZATION_ID = src.ORGANIZATION_ID, GROUP_ID = src.GROUP_ID,
    TICKET_FORM_ID = src.TICKET_FORM_ID, VIA = src.VIA, TAGS = src.TAGS,
    SATISFACTION_RATING = src.SATISFACTION_RATING, IS_PUBLIC = src.IS_PUBLIC,
    HAS_INCIDENTS = src.HAS_INCIDENTS, FROM_MESSAGING_CHANNEL = src.FROM_MESSAGING_CHANNEL,
    CF_ACIONAR_CONTABILIDADE = src.CF_ACIONAR_CONTABILIDADE,
    CF_MOTIVO_DE_CONTATO = src.CF_MOTIVO_DE_CONTATO,
    CF_TAG_DE_ATENDIMENTO = src.CF_TAG_DE_ATENDIMENTO,
    CF_TAG_DE_ATENDIMENTO_2 = src.CF_TAG_DE_ATENDIMENTO_2,
    CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS = src.CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS,
    CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS = src.CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS,
    CF_CONVERSATION_ID = src.CF_CONVERSATION_ID, CF_HSM_DATA = src.CF_HSM_DATA,
    CF_HSM_DESCRICAO_TEMPLATE = src.CF_HSM_DESCRICAO_TEMPLATE,
    CF_HSM_STATUS = src.CF_HSM_STATUS, CF_HSM_TEMPLATE = src.CF_HSM_TEMPLATE,
    CF_WHATSAPP_ID = src.CF_WHATSAPP_ID, CF_MIGRACAO_DE_AREA = src.CF_MIGRACAO_DE_AREA,
    CF_TAG_DE_ATENDIMENTO_3 = src.CF_TAG_DE_ATENDIMENTO_3,
    CF_RENOVOU_CERTIFICADO_ECNPJ = src.CF_RENOVOU_CERTIFICADO_ECNPJ,
    CF_ATRIBUICAO_AUTOMATICA = src.CF_ATRIBUICAO_AUTOMATICA,
    CF_PIECE_OF_CAKE_DISPARO = src.CF_PIECE_OF_CAKE_DISPARO,
    CUSTOM_FIELDS_JSON = src.CUSTOM_FIELDS_JSON,
    REOPENS = src.REOPENS, REPLIES = src.REPLIES,
    ASSIGNEE_STATIONS = src.ASSIGNEE_STATIONS, GROUP_STATIONS = src.GROUP_STATIONS,
    AGENT_WAIT_TIME_IN_MINUTES = src.AGENT_WAIT_TIME_IN_MINUTES,
    REQUESTER_WAIT_TIME_IN_MINUTES = src.REQUESTER_WAIT_TIME_IN_MINUTES,
    FIRST_RESOLUTION_TIME_IN_MINUTES = src.FIRST_RESOLUTION_TIME_IN_MINUTES,
    FULL_RESOLUTION_TIME_IN_MINUTES = src.FULL_RESOLUTION_TIME_IN_MINUTES,
    ON_HOLD_TIME_IN_MINUTES = src.ON_HOLD_TIME_IN_MINUTES,
    REPLY_TIME_IN_MINUTES = src.REPLY_TIME_IN_MINUTES,
    ASSIGNED_AT = src.ASSIGNED_AT, INITIALLY_ASSIGNED_AT = src.INITIALLY_ASSIGNED_AT,
    SOLVED_AT = src.SOLVED_AT, LATEST_COMMENT_ADDED_AT = src.LATEST_COMMENT_ADDED_AT,
    CREATED_AT = src.CREATED_AT, UPDATED_AT = src.UPDATED_AT,
    DATALAKE_UPDATED_AT = src.DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT = src._SNOWFLAKE_LOADED_AT

  WHEN NOT MATCHED THEN INSERT (
    ID, SUBJECT, DESCRIPTION, STATUS, PRIORITY, BRAND_ID, ASSIGNEE_ID,
    REQUESTER_ID, SUBMITTER_ID, ORGANIZATION_ID, GROUP_ID, TICKET_FORM_ID,
    VIA, TAGS, SATISFACTION_RATING, IS_PUBLIC, HAS_INCIDENTS, FROM_MESSAGING_CHANNEL,
    CF_ACIONAR_CONTABILIDADE, CF_MOTIVO_DE_CONTATO, CF_TAG_DE_ATENDIMENTO, CF_TAG_DE_ATENDIMENTO_2,
    CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS, CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS,
    CF_CONVERSATION_ID, CF_HSM_DATA, CF_HSM_DESCRICAO_TEMPLATE, CF_HSM_STATUS, CF_HSM_TEMPLATE,
    CF_WHATSAPP_ID, CF_MIGRACAO_DE_AREA, CF_TAG_DE_ATENDIMENTO_3, CF_RENOVOU_CERTIFICADO_ECNPJ,
    CF_ATRIBUICAO_AUTOMATICA, CF_PIECE_OF_CAKE_DISPARO, CUSTOM_FIELDS_JSON,
    REOPENS, REPLIES, ASSIGNEE_STATIONS, GROUP_STATIONS,
    AGENT_WAIT_TIME_IN_MINUTES, REQUESTER_WAIT_TIME_IN_MINUTES,
    FIRST_RESOLUTION_TIME_IN_MINUTES, FULL_RESOLUTION_TIME_IN_MINUTES,
    ON_HOLD_TIME_IN_MINUTES, REPLY_TIME_IN_MINUTES,
    ASSIGNED_AT, INITIALLY_ASSIGNED_AT, SOLVED_AT, LATEST_COMMENT_ADDED_AT,
    CREATED_AT, UPDATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT
  ) VALUES (
    src.ID, src.SUBJECT, src.DESCRIPTION, src.STATUS, src.PRIORITY, src.BRAND_ID, src.ASSIGNEE_ID,
    src.REQUESTER_ID, src.SUBMITTER_ID, src.ORGANIZATION_ID, src.GROUP_ID, src.TICKET_FORM_ID,
    src.VIA, src.TAGS, src.SATISFACTION_RATING, src.IS_PUBLIC, src.HAS_INCIDENTS, src.FROM_MESSAGING_CHANNEL,
    src.CF_ACIONAR_CONTABILIDADE, src.CF_MOTIVO_DE_CONTATO, src.CF_TAG_DE_ATENDIMENTO, src.CF_TAG_DE_ATENDIMENTO_2,
    src.CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS, src.CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS,
    src.CF_CONVERSATION_ID, src.CF_HSM_DATA, src.CF_HSM_DESCRICAO_TEMPLATE, src.CF_HSM_STATUS, src.CF_HSM_TEMPLATE,
    src.CF_WHATSAPP_ID, src.CF_MIGRACAO_DE_AREA, src.CF_TAG_DE_ATENDIMENTO_3, src.CF_RENOVOU_CERTIFICADO_ECNPJ,
    src.CF_ATRIBUICAO_AUTOMATICA, src.CF_PIECE_OF_CAKE_DISPARO, src.CUSTOM_FIELDS_JSON,
    src.REOPENS, src.REPLIES, src.ASSIGNEE_STATIONS, src.GROUP_STATIONS,
    src.AGENT_WAIT_TIME_IN_MINUTES, src.REQUESTER_WAIT_TIME_IN_MINUTES,
    src.FIRST_RESOLUTION_TIME_IN_MINUTES, src.FULL_RESOLUTION_TIME_IN_MINUTES,
    src.ON_HOLD_TIME_IN_MINUTES, src.REPLY_TIME_IN_MINUTES,
    src.ASSIGNED_AT, src.INITIALLY_ASSIGNED_AT, src.SOLVED_AT, src.LATEST_COMMENT_ADDED_AT,
    src.CREATED_AT, src.UPDATED_AT, src.DATALAKE_UPDATED_AT, src._SNOWFLAKE_LOADED_AT
  );

  RETURN 'CX.SILVER.ZENDESK_TICKETS carregado com sucesso em ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- ------------------------------------------------------------
-- 3.2 Users
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE CX.SILVER.ZENDESK_USERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  LET v_watermark TIMESTAMP_NTZ := (SELECT COALESCE(MAX(_SNOWFLAKE_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ) FROM CX.SILVER.ZENDESK_USERS);

  MERGE INTO CX.SILVER.ZENDESK_USERS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        RAW_DATA:id::NUMBER AS ID, UPPER(TRIM(RAW_DATA:name::STRING)) AS NAME,
        LOWER(TRIM(RAW_DATA:email::STRING)) AS EMAIL, UPPER(TRIM(RAW_DATA:role::STRING)) AS ROLE,
        RAW_DATA:locale::STRING AS LOCALE, RAW_DATA:time_zone::STRING AS TIME_ZONE,
        RAW_DATA:tags::STRING AS TAGS, RAW_DATA:user_fields::STRING AS USER_FIELDS,
        RAW_DATA:active::BOOLEAN AS ACTIVE, RAW_DATA:verified::BOOLEAN AS VERIFIED,
        RAW_DATA:suspended::BOOLEAN AS SUSPENDED, RAW_DATA:moderator::BOOLEAN AS MODERATOR,
        RAW_DATA:restricted_agent::BOOLEAN AS RESTRICTED_AGENT,
        RAW_DATA:shared::BOOLEAN AS SHARED, RAW_DATA:shared_agent::BOOLEAN AS SHARED_AGENT,
        RAW_DATA:ticket_restriction::STRING AS TICKET_RESTRICTION,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:created_at::STRING) AS CREATED_AT,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING) AS UPDATED_AT,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:datalake_updated_at::STRING) AS DATALAKE_UPDATED_AT,
        _SNOWFLAKE_LOADED_AT,
        ROW_NUMBER() OVER (PARTITION BY RAW_DATA:id::NUMBER ORDER BY TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING) DESC NULLS LAST) AS rn
      FROM CX.BRONZE.ZENDESK_USERS
      WHERE RAW_DATA:id IS NOT NULL
        AND _SNOWFLAKE_LOADED_AT > :v_watermark
    ) WHERE rn = 1
  ) src ON tgt.ID = src.ID

  WHEN MATCHED AND src.UPDATED_AT > tgt.UPDATED_AT THEN UPDATE SET
    NAME = src.NAME, EMAIL = src.EMAIL, ROLE = src.ROLE, LOCALE = src.LOCALE,
    TIME_ZONE = src.TIME_ZONE, TAGS = src.TAGS, USER_FIELDS = src.USER_FIELDS,
    ACTIVE = src.ACTIVE, VERIFIED = src.VERIFIED, SUSPENDED = src.SUSPENDED,
    MODERATOR = src.MODERATOR, RESTRICTED_AGENT = src.RESTRICTED_AGENT,
    SHARED = src.SHARED, SHARED_AGENT = src.SHARED_AGENT, TICKET_RESTRICTION = src.TICKET_RESTRICTION,
    CREATED_AT = src.CREATED_AT, UPDATED_AT = src.UPDATED_AT,
    DATALAKE_UPDATED_AT = src.DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT = src._SNOWFLAKE_LOADED_AT

  WHEN NOT MATCHED THEN INSERT (ID, NAME, EMAIL, ROLE, LOCALE, TIME_ZONE, TAGS, USER_FIELDS,
    ACTIVE, VERIFIED, SUSPENDED, MODERATOR, RESTRICTED_AGENT, SHARED, SHARED_AGENT,
    TICKET_RESTRICTION, CREATED_AT, UPDATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT)
  VALUES (src.ID, src.NAME, src.EMAIL, src.ROLE, src.LOCALE, src.TIME_ZONE, src.TAGS, src.USER_FIELDS,
    src.ACTIVE, src.VERIFIED, src.SUSPENDED, src.MODERATOR, src.RESTRICTED_AGENT, src.SHARED, src.SHARED_AGENT,
    src.TICKET_RESTRICTION, src.CREATED_AT, src.UPDATED_AT, src.DATALAKE_UPDATED_AT, src._SNOWFLAKE_LOADED_AT);

  RETURN 'CX.SILVER.ZENDESK_USERS carregado com sucesso em ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- ============================================================
-- 4. TASKS
-- ============================================================
CREATE OR REPLACE TASK CX.SILVER.TASK_ZENDESK_SILVER
  WAREHOUSE = WH_DATA_SERVICES
  SCHEDULE  = 'USING CRON 30 */6 * * * America/Sao_Paulo'
AS
  BEGIN
    CALL CX.SILVER.ZENDESK_TICKETS();
    CALL CX.SILVER.ZENDESK_USERS();
  END;

ALTER TASK CX.SILVER.TASK_ZENDESK_SILVER RESUME;

-- ============================================================
-- 7. EXTENSÕES: TICKETS_EVENTS, GROUPS, USERS (PHONE + EXTERNAL_ID)
-- ============================================================

-- ------------------------------------------------------------
-- 7.1 ZENDESK_TICKETS_EVENTS
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE CX.SILVER.ZENDESK_TICKETS_EVENTS (
  ID                   NUMBER,
  TICKET_ID            NUMBER,
  FIELD_NAME           STRING,
  VALUE                STRING,
  AUTHOR_ID            NUMBER,
  CREATED_AT           TIMESTAMP_NTZ,
  DATALAKE_UPDATED_AT  TIMESTAMP_NTZ,
  _SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ
);

-- ------------------------------------------------------------
-- 7.1.1 ZENDESK_TICKETS_EVENTS — rewrite p/ child_events
-- ------------------------------------------------------------
-- Bronze pré 2026-05-04 trazia field_name/value no ROOT do RAW_DATA
-- (extração antiga via /api/v2/ticket_events). Bronze pós migração
-- traz objeto Audit completo com child_events aninhado (string JSON).
-- A procedure abaixo:
--  (a) extrai campos top-level conhecidos do child_events;
--  (b) desempacota custom_ticket_fields hardcoded
--      (36210587567380=motivo, 36627293345556=status transbordo);
--  (c) preserva o histórico legado (root field_name) via UNION ALL.
-- Chave única do MERGE = (ID, FIELD_NAME) — um child_event pode
-- carregar mais de um campo no mesmo Change.
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE CX.SILVER.ZENDESK_TICKETS_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  LET v_watermark TIMESTAMP_NTZ := (SELECT COALESCE(MAX(_SNOWFLAKE_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ) FROM CX.SILVER.ZENDESK_TICKETS_EVENTS);

  MERGE INTO CX.SILVER.ZENDESK_TICKETS_EVENTS tgt
  USING (
    WITH audits AS (
      SELECT
        RAW_DATA:id::NUMBER                                        AS audit_id,
        RAW_DATA:ticket_id::NUMBER                                 AS ticket_id,
        RAW_DATA:updater_id::NUMBER                                AS author_id,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:created_at::STRING)          AS created_at,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:datalake_updated_at::STRING) AS datalake_updated_at,
        _SNOWFLAKE_LOADED_AT,
        TRY_PARSE_JSON(RAW_DATA:child_events::STRING)              AS child_events,
        RAW_DATA:field_name::STRING                                AS legacy_field_name,
        RAW_DATA:value                                             AS legacy_value,
        RAW_DATA:author_id::NUMBER                                 AS legacy_author_id
      FROM CX.BRONZE.ZENDESK_TICKETS_EVENTS
      WHERE RAW_DATA:id IS NOT NULL
        AND _SNOWFLAKE_LOADED_AT > :v_watermark
    ),
    flat AS (
      SELECT
        a.audit_id, a.ticket_id, a.author_id, a.created_at,
        a.datalake_updated_at, a._SNOWFLAKE_LOADED_AT, ce.value AS evt
      FROM audits a, LATERAL FLATTEN(input => a.child_events, OUTER => TRUE) ce
      WHERE ce.value IS NOT NULL
    ),
    legacy AS (
      SELECT
        audit_id           AS ID,
        ticket_id          AS TICKET_ID,
        legacy_field_name  AS FIELD_NAME,
        legacy_value::STRING AS VALUE,
        legacy_author_id   AS AUTHOR_ID,
        created_at         AS CREATED_AT,
        datalake_updated_at AS DATALAKE_UPDATED_AT,
        _SNOWFLAKE_LOADED_AT
      FROM audits
      WHERE legacy_field_name IS NOT NULL
    ),
    extracted AS (
      SELECT evt:id::NUMBER AS ID, ticket_id AS TICKET_ID, 'status' AS FIELD_NAME,
             evt:status::STRING AS VALUE, author_id AS AUTHOR_ID, created_at AS CREATED_AT,
             datalake_updated_at AS DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:status IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'assignee_id', evt:assignee_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:assignee_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'group_id', evt:group_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:group_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'priority', evt:priority::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:priority IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'brand_id', evt:brand_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:brand_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'requester_id', evt:requester_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:requester_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'organization_id', evt:organization_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:organization_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'ticket_form_id', evt:ticket_form_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:ticket_form_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'is_public', evt:is_public::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:is_public IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'subject', evt:subject::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:subject IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'type', evt:type::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:type IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'custom_status_id', evt:custom_status_id::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:custom_status_id IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'sla_policy', evt:sla_policy::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:sla_policy IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'comment_present', evt:comment_present::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:comment_present IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'comment_public', evt:comment_public::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:comment_public IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, 'tags', evt:tags::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:tags IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, '36210587567380',
             evt:custom_ticket_fields:"36210587567380"::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:custom_ticket_fields:"36210587567380" IS NOT NULL
      UNION ALL
      SELECT evt:id::NUMBER, ticket_id, '36627293345556',
             evt:custom_ticket_fields:"36627293345556"::STRING,
             author_id, created_at, datalake_updated_at, _SNOWFLAKE_LOADED_AT
      FROM flat WHERE evt:custom_ticket_fields:"36627293345556" IS NOT NULL
      UNION ALL
      SELECT ID, TICKET_ID, FIELD_NAME, VALUE, AUTHOR_ID, CREATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT
      FROM legacy
    )
    SELECT ID, TICKET_ID, FIELD_NAME, VALUE, AUTHOR_ID, CREATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ID, FIELD_NAME ORDER BY _SNOWFLAKE_LOADED_AT DESC NULLS LAST) AS rn
      FROM extracted
      WHERE ID IS NOT NULL
    ) WHERE rn = 1
  ) src
    ON tgt.ID = src.ID AND COALESCE(tgt.FIELD_NAME, '') = COALESCE(src.FIELD_NAME, '')
  WHEN MATCHED THEN UPDATE SET
    TICKET_ID = src.TICKET_ID, VALUE = src.VALUE,
    AUTHOR_ID = src.AUTHOR_ID, CREATED_AT = src.CREATED_AT,
    DATALAKE_UPDATED_AT = src.DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT = src._SNOWFLAKE_LOADED_AT
  WHEN NOT MATCHED THEN INSERT (ID, TICKET_ID, FIELD_NAME, VALUE, AUTHOR_ID, CREATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT)
  VALUES (src.ID, src.TICKET_ID, src.FIELD_NAME, src.VALUE, src.AUTHOR_ID, src.CREATED_AT, src.DATALAKE_UPDATED_AT, src._SNOWFLAKE_LOADED_AT);

  RETURN 'CX.SILVER.ZENDESK_TICKETS_EVENTS carregado com sucesso em ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- ------------------------------------------------------------
-- 7.2 ZENDESK_GROUPS
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE CX.SILVER.ZENDESK_GROUPS (
  ID                   NUMBER,
  NAME                 STRING,
  DESCRIPTION          STRING,
  IS_DEFAULT           BOOLEAN,
  DELETED              BOOLEAN,
  CREATED_AT           TIMESTAMP_NTZ,
  UPDATED_AT           TIMESTAMP_NTZ,
  DATALAKE_UPDATED_AT  TIMESTAMP_NTZ,
  _SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ
);

CREATE OR REPLACE PROCEDURE CX.SILVER.ZENDESK_GROUPS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  LET v_watermark TIMESTAMP_NTZ := (SELECT COALESCE(MAX(_SNOWFLAKE_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ) FROM CX.SILVER.ZENDESK_GROUPS);

  MERGE INTO CX.SILVER.ZENDESK_GROUPS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        RAW_DATA:id::NUMBER                                        AS ID,
        RAW_DATA:name::STRING                                      AS NAME,
        RAW_DATA:description::STRING                               AS DESCRIPTION,
        RAW_DATA:default::BOOLEAN                                  AS IS_DEFAULT,
        RAW_DATA:deleted::BOOLEAN                                  AS DELETED,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:created_at::STRING)          AS CREATED_AT,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING)          AS UPDATED_AT,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:datalake_updated_at::STRING) AS DATALAKE_UPDATED_AT,
        _SNOWFLAKE_LOADED_AT,
        ROW_NUMBER() OVER (PARTITION BY RAW_DATA:id::NUMBER ORDER BY TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING) DESC NULLS LAST) AS rn
      FROM CX.BRONZE.ZENDESK_GROUPS
      WHERE RAW_DATA:id IS NOT NULL
        AND _SNOWFLAKE_LOADED_AT > :v_watermark
    ) WHERE rn = 1
  ) src ON tgt.ID = src.ID
  WHEN MATCHED AND src.UPDATED_AT > tgt.UPDATED_AT THEN UPDATE SET
    NAME = src.NAME, DESCRIPTION = src.DESCRIPTION, IS_DEFAULT = src.IS_DEFAULT, DELETED = src.DELETED,
    CREATED_AT = src.CREATED_AT, UPDATED_AT = src.UPDATED_AT,
    DATALAKE_UPDATED_AT = src.DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT = src._SNOWFLAKE_LOADED_AT
  WHEN NOT MATCHED THEN INSERT (ID, NAME, DESCRIPTION, IS_DEFAULT, DELETED, CREATED_AT, UPDATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT)
  VALUES (src.ID, src.NAME, src.DESCRIPTION, src.IS_DEFAULT, src.DELETED, src.CREATED_AT, src.UPDATED_AT, src.DATALAKE_UPDATED_AT, src._SNOWFLAKE_LOADED_AT);

  RETURN 'CX.SILVER.ZENDESK_GROUPS carregado com sucesso em ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- ------------------------------------------------------------
-- 7.3 USERS: adicionar EXTERNAL_ID e PHONE (com masking PII)
-- ------------------------------------------------------------
ALTER TABLE CX.SILVER.ZENDESK_USERS
  ADD COLUMN IF NOT EXISTS
    EXTERNAL_ID STRING,
    PHONE       STRING;

CREATE OR REPLACE MASKING POLICY CX.SILVER.MASK_ZENDESK_USER_PHONE_PII
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN')
      THEN val
    ELSE SHA2(val)
  END;

ALTER TABLE CX.SILVER.ZENDESK_USERS MODIFY COLUMN PHONE
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_USER_PHONE_PII;

CREATE OR REPLACE PROCEDURE CX.SILVER.ZENDESK_USERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  LET v_watermark TIMESTAMP_NTZ := (SELECT COALESCE(MAX(_SNOWFLAKE_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ) FROM CX.SILVER.ZENDESK_USERS);

  MERGE INTO CX.SILVER.ZENDESK_USERS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        RAW_DATA:id::NUMBER AS ID, UPPER(TRIM(RAW_DATA:name::STRING)) AS NAME,
        LOWER(TRIM(RAW_DATA:email::STRING)) AS EMAIL, UPPER(TRIM(RAW_DATA:role::STRING)) AS ROLE,
        RAW_DATA:locale::STRING AS LOCALE, RAW_DATA:time_zone::STRING AS TIME_ZONE,
        RAW_DATA:tags::STRING AS TAGS, RAW_DATA:user_fields::STRING AS USER_FIELDS,
        RAW_DATA:active::BOOLEAN AS ACTIVE, RAW_DATA:verified::BOOLEAN AS VERIFIED,
        RAW_DATA:suspended::BOOLEAN AS SUSPENDED, RAW_DATA:moderator::BOOLEAN AS MODERATOR,
        RAW_DATA:restricted_agent::BOOLEAN AS RESTRICTED_AGENT,
        RAW_DATA:shared::BOOLEAN AS SHARED, RAW_DATA:shared_agent::BOOLEAN AS SHARED_AGENT,
        RAW_DATA:ticket_restriction::STRING AS TICKET_RESTRICTION,
        RAW_DATA:external_id::STRING AS EXTERNAL_ID,
        RAW_DATA:phone::STRING AS PHONE,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:created_at::STRING) AS CREATED_AT,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING) AS UPDATED_AT,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:datalake_updated_at::STRING) AS DATALAKE_UPDATED_AT,
        _SNOWFLAKE_LOADED_AT,
        ROW_NUMBER() OVER (PARTITION BY RAW_DATA:id::NUMBER ORDER BY TRY_TO_TIMESTAMP_NTZ(RAW_DATA:updated_at::STRING) DESC NULLS LAST) AS rn
      FROM CX.BRONZE.ZENDESK_USERS
      WHERE RAW_DATA:id IS NOT NULL
        AND _SNOWFLAKE_LOADED_AT > :v_watermark
    ) WHERE rn = 1
  ) src ON tgt.ID = src.ID

  WHEN MATCHED AND src.UPDATED_AT > tgt.UPDATED_AT THEN UPDATE SET
    NAME = src.NAME, EMAIL = src.EMAIL, ROLE = src.ROLE, LOCALE = src.LOCALE,
    TIME_ZONE = src.TIME_ZONE, TAGS = src.TAGS, USER_FIELDS = src.USER_FIELDS,
    ACTIVE = src.ACTIVE, VERIFIED = src.VERIFIED, SUSPENDED = src.SUSPENDED,
    MODERATOR = src.MODERATOR, RESTRICTED_AGENT = src.RESTRICTED_AGENT,
    SHARED = src.SHARED, SHARED_AGENT = src.SHARED_AGENT, TICKET_RESTRICTION = src.TICKET_RESTRICTION,
    EXTERNAL_ID = src.EXTERNAL_ID, PHONE = src.PHONE,
    CREATED_AT = src.CREATED_AT, UPDATED_AT = src.UPDATED_AT,
    DATALAKE_UPDATED_AT = src.DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT = src._SNOWFLAKE_LOADED_AT

  WHEN NOT MATCHED THEN INSERT (ID, NAME, EMAIL, ROLE, LOCALE, TIME_ZONE, TAGS, USER_FIELDS,
    ACTIVE, VERIFIED, SUSPENDED, MODERATOR, RESTRICTED_AGENT, SHARED, SHARED_AGENT,
    TICKET_RESTRICTION, EXTERNAL_ID, PHONE, CREATED_AT, UPDATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT)
  VALUES (src.ID, src.NAME, src.EMAIL, src.ROLE, src.LOCALE, src.TIME_ZONE, src.TAGS, src.USER_FIELDS,
    src.ACTIVE, src.VERIFIED, src.SUSPENDED, src.MODERATOR, src.RESTRICTED_AGENT, src.SHARED, src.SHARED_AGENT,
    src.TICKET_RESTRICTION, src.EXTERNAL_ID, src.PHONE, src.CREATED_AT, src.UPDATED_AT, src.DATALAKE_UPDATED_AT, src._SNOWFLAKE_LOADED_AT);

  RETURN 'CX.SILVER.ZENDESK_USERS carregado com sucesso em ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- ------------------------------------------------------------
-- 7.4 TASK Silver atualizada (incluir novas procedures)
-- ------------------------------------------------------------
CREATE OR REPLACE TASK CX.SILVER.TASK_ZENDESK_SILVER
  WAREHOUSE = WH_DATA_SERVICES
  SCHEDULE  = 'USING CRON 30 */6 * * * America/Sao_Paulo'
AS
  BEGIN
    CALL CX.SILVER.ZENDESK_TICKETS();
    CALL CX.SILVER.ZENDESK_USERS();
    CALL CX.SILVER.ZENDESK_GROUPS();
    CALL CX.SILVER.ZENDESK_TICKETS_EVENTS();
  END;

ALTER TASK CX.SILVER.TASK_ZENDESK_SILVER RESUME;

-- ============================================================
-- 8. ZENDESK_TICKET_COMMENTS (corpo dos comentários)
--    Fonte: CX.BRONZE.ZENDESK_TICKETS_EVENTS -> child_events[]
--    onde event_type = 'Comment' (disponível após o sideload
--    ?include=comment_events na ingestão). BODY/HTML_BODY/PLAIN_BODY
--    são conteúdo livre do cliente => PII => masking policy.
-- ============================================================

-- ------------------------------------------------------------
-- 8.1 Tabela
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CX.SILVER.ZENDESK_TICKET_COMMENTS (
  ID                   NUMBER,
  AUDIT_ID             NUMBER,
  TICKET_ID            NUMBER,
  AUTHOR_ID            NUMBER,
  IS_PUBLIC            BOOLEAN,
  VIA                  STRING,
  TYPE                 STRING,
  BODY                 STRING,
  HTML_BODY            STRING,
  PLAIN_BODY           STRING,
  CREATED_AT           TIMESTAMP_NTZ,
  DATALAKE_UPDATED_AT  TIMESTAMP_NTZ,
  _SNOWFLAKE_LOADED_AT TIMESTAMP_NTZ
);

-- ------------------------------------------------------------
-- 8.2 Masking policy (mesmo padrão das demais PII de texto livre)
-- ------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY CX.SILVER.MASK_ZENDESK_COMMENT_BODY_PII
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN')
      THEN val
    ELSE '**********'
  END;

ALTER TABLE CX.SILVER.ZENDESK_TICKET_COMMENTS MODIFY COLUMN BODY
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_COMMENT_BODY_PII;
ALTER TABLE CX.SILVER.ZENDESK_TICKET_COMMENTS MODIFY COLUMN HTML_BODY
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_COMMENT_BODY_PII;
ALTER TABLE CX.SILVER.ZENDESK_TICKET_COMMENTS MODIFY COLUMN PLAIN_BODY
  SET MASKING POLICY CX.SILVER.MASK_ZENDESK_COMMENT_BODY_PII;

-- ------------------------------------------------------------
-- 8.3 Procedure (incremental por _SNOWFLAKE_LOADED_AT; dedup por comment id)
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE CX.SILVER.ZENDESK_TICKET_COMMENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  LET v_watermark TIMESTAMP_NTZ := (SELECT COALESCE(MAX(_SNOWFLAKE_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ) FROM CX.SILVER.ZENDESK_TICKET_COMMENTS);

  MERGE INTO CX.SILVER.ZENDESK_TICKET_COMMENTS tgt
  USING (
    WITH audits AS (
      SELECT
        RAW_DATA:id::NUMBER                                        AS audit_id,
        RAW_DATA:ticket_id::NUMBER                                 AS ticket_id,
        TRY_TO_TIMESTAMP_NTZ(RAW_DATA:datalake_updated_at::STRING) AS datalake_updated_at,
        _SNOWFLAKE_LOADED_AT,
        TRY_PARSE_JSON(RAW_DATA:child_events::STRING)             AS child_events
      FROM CX.BRONZE.ZENDESK_TICKETS_EVENTS
      WHERE RAW_DATA:id IS NOT NULL
        AND _SNOWFLAKE_LOADED_AT > :v_watermark
    ),
    flat AS (
      SELECT
        a.audit_id, a.ticket_id, a.datalake_updated_at, a._SNOWFLAKE_LOADED_AT, ce.value AS evt
      FROM audits a, LATERAL FLATTEN(input => a.child_events, OUTER => TRUE) ce
      WHERE ce.value:event_type::STRING = 'Comment'
        AND ce.value:body IS NOT NULL
    )
    SELECT ID, AUDIT_ID, TICKET_ID, AUTHOR_ID, IS_PUBLIC, VIA, TYPE,
           BODY, HTML_BODY, PLAIN_BODY, CREATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT
    FROM (
      SELECT
        evt:id::NUMBER          AS ID,
        audit_id                AS AUDIT_ID,
        ticket_id               AS TICKET_ID,
        evt:author_id::NUMBER   AS AUTHOR_ID,
        evt:public::BOOLEAN     AS IS_PUBLIC,
        evt:via::STRING         AS VIA,
        evt:type::STRING        AS TYPE,
        evt:body::STRING        AS BODY,
        evt:html_body::STRING   AS HTML_BODY,
        evt:plain_body::STRING  AS PLAIN_BODY,
        TRY_TO_TIMESTAMP_NTZ(evt:created_at::STRING) AS CREATED_AT,
        datalake_updated_at     AS DATALAKE_UPDATED_AT,
        _SNOWFLAKE_LOADED_AT,
        ROW_NUMBER() OVER (PARTITION BY evt:id::NUMBER ORDER BY _SNOWFLAKE_LOADED_AT DESC NULLS LAST) AS rn
      FROM flat
      WHERE evt:id IS NOT NULL
    ) WHERE rn = 1
  ) src ON tgt.ID = src.ID
  WHEN MATCHED THEN UPDATE SET
    AUDIT_ID = src.AUDIT_ID, TICKET_ID = src.TICKET_ID, AUTHOR_ID = src.AUTHOR_ID,
    IS_PUBLIC = src.IS_PUBLIC, VIA = src.VIA, TYPE = src.TYPE,
    BODY = src.BODY, HTML_BODY = src.HTML_BODY, PLAIN_BODY = src.PLAIN_BODY,
    CREATED_AT = src.CREATED_AT, DATALAKE_UPDATED_AT = src.DATALAKE_UPDATED_AT,
    _SNOWFLAKE_LOADED_AT = src._SNOWFLAKE_LOADED_AT
  WHEN NOT MATCHED THEN INSERT (ID, AUDIT_ID, TICKET_ID, AUTHOR_ID, IS_PUBLIC, VIA, TYPE,
    BODY, HTML_BODY, PLAIN_BODY, CREATED_AT, DATALAKE_UPDATED_AT, _SNOWFLAKE_LOADED_AT)
  VALUES (src.ID, src.AUDIT_ID, src.TICKET_ID, src.AUTHOR_ID, src.IS_PUBLIC, src.VIA, src.TYPE,
    src.BODY, src.HTML_BODY, src.PLAIN_BODY, src.CREATED_AT, src.DATALAKE_UPDATED_AT, src._SNOWFLAKE_LOADED_AT);

  RETURN 'CX.SILVER.ZENDESK_TICKET_COMMENTS carregado com sucesso em ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- ------------------------------------------------------------
-- 8.4 TASK Silver atualizada (inclui comments)
-- ------------------------------------------------------------
CREATE OR REPLACE TASK CX.SILVER.TASK_ZENDESK_SILVER
  WAREHOUSE = WH_DATA_SERVICES
  SCHEDULE  = 'USING CRON 30 */6 * * * America/Sao_Paulo'
AS
  BEGIN
    CALL CX.SILVER.ZENDESK_TICKETS();
    CALL CX.SILVER.ZENDESK_USERS();
    CALL CX.SILVER.ZENDESK_GROUPS();
    CALL CX.SILVER.ZENDESK_TICKETS_EVENTS();
    CALL CX.SILVER.ZENDESK_TICKET_COMMENTS();
  END;

ALTER TASK CX.SILVER.TASK_ZENDESK_SILVER RESUME;
