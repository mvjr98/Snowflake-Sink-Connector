-- =============================================================================
-- DDL: Tabela de staging _INGEST
-- =============================================================================
-- Substitua os placeholders antes de executar:
--   <DATABASE>   → nome do banco de dados (ex: LZ_SQL_IH_PRD)
--   <SCHEMA>     → nome do schema         (ex: GCOB001D)
--   <TABLE>      → nome da tabela base    (ex: NTFE001D)
--
-- A tabela de staging é sempre <TABLE>_INGEST.
-- Adicione as colunas de negócio conforme a estrutura da tabela origem.
-- =============================================================================

USE DATABASE <DATABASE>;
USE SCHEMA <SCHEMA>;

CREATE TABLE IF NOT EXISTS <TABLE>_INGEST (

    -- =========================================================================
    -- Colunas de negócio — ajuste conforme a tabela origem
    -- =========================================================================
    -- Exemplos:
    IDT_UNC_TSC         NUMBER(38, 0),
    IDT_SEQ_UNC         NUMBER(38, 0),
    NUM_UNC_PTB         VARCHAR(255),
    -- ... adicione as demais colunas aqui ...

    -- =========================================================================
    -- Metadados de controle (prefixo IH_)
    -- =========================================================================
    IH_TOPIC            VARCHAR(255)    NOT NULL,
    IH_PARTITION        INT             NOT NULL,
    IH_OFFSET           NUMBER(38, 0)   NOT NULL,
    IH_OP               VARCHAR(1)      NOT NULL,   -- c, r, u, d
    IH_DATETIME         TIMESTAMP_NTZ   NOT NULL,
    IH_BLOCKID          VARCHAR(40)     NOT NULL,

    -- =========================================================================
    -- PK da tabela de staging (evita duplicatas por offset)
    -- =========================================================================
    CONSTRAINT PK_<TABLE>_INGEST PRIMARY KEY (IH_TOPIC, IH_PARTITION, IH_OFFSET)

);

-- Índice auxiliar para o MERGE (acelera a busca por IH_OP)
-- (Snowflake não tem índices convencionais, mas clustering key pode ajudar
--  em tabelas grandes)
-- ALTER TABLE <TABLE>_INGEST CLUSTER BY (IH_OP, IH_BLOCKID);

-- =============================================================================
-- DDL: Tabela target (final) — apenas colunas de negócio
-- =============================================================================
CREATE TABLE IF NOT EXISTS <TABLE> (

    IDT_UNC_TSC         NUMBER(38, 0)   NOT NULL,
    IDT_SEQ_UNC         NUMBER(38, 0)   NOT NULL,
    NUM_UNC_PTB         VARCHAR(255),
    -- ... adicione as demais colunas aqui ...

    CONSTRAINT PK_<TABLE> PRIMARY KEY (IDT_UNC_TSC, IDT_SEQ_UNC)

);
