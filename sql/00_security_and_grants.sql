-- =============================================================================
-- Bootstrap de seguranca/permissoes para o Snowflake Sink Connector
-- =============================================================================
-- Ajuste os nomes conforme seu ambiente antes de executar:
--   ROLE:       SNFLK_ROLE_KAFKA
--   USER:       SNFLK_USER_KAFKA
--   WAREHOUSE:  COMPUTE_WH
--   DATABASE:   RAW_KAFKA
--   SCHEMA:     NORTHWIND
-- =============================================================================

USE ROLE SECURITYADMIN;

-- 1) Role dedicada do conector
CREATE ROLE IF NOT EXISTS SNFLK_ROLE_KAFKA;

-- 2) Usuario de servico (a chave publica e definida separadamente)
CREATE USER IF NOT EXISTS SNFLK_USER_KAFKA
    DEFAULT_ROLE = SNFLK_ROLE_KAFKA
    DEFAULT_WAREHOUSE = COMPUTE_WH
    COMMENT = 'Usuario de servico para Kafka Connect custom sink';

-- 3) Vinculos de role
GRANT ROLE SNFLK_ROLE_KAFKA TO USER SNFLK_USER_KAFKA;
GRANT ROLE SNFLK_ROLE_KAFKA TO ROLE SYSADMIN;

-- 4) Warehouse
GRANT USAGE   ON WAREHOUSE COMPUTE_WH TO ROLE SNFLK_ROLE_KAFKA;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE SNFLK_ROLE_KAFKA;

USE ROLE SYSADMIN;

-- 5) Estrutura base
CREATE DATABASE IF NOT EXISTS RAW_KAFKA;
CREATE SCHEMA IF NOT EXISTS RAW_KAFKA.NORTHWIND;

-- 6) Permissoes de uso
GRANT USAGE ON DATABASE RAW_KAFKA TO ROLE SNFLK_ROLE_KAFKA;
GRANT USAGE ON SCHEMA RAW_KAFKA.NORTHWIND TO ROLE SNFLK_ROLE_KAFKA;

-- Necessario para modo STAGE (CREATE STAGE IF NOT EXISTS)
GRANT CREATE STAGE ON SCHEMA RAW_KAFKA.NORTHWIND TO ROLE SNFLK_ROLE_KAFKA;

-- 7) DML para tabelas existentes
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA RAW_KAFKA.NORTHWIND
  TO ROLE SNFLK_ROLE_KAFKA;

-- 8) DML para tabelas futuras (evita grant tabela a tabela)
GRANT SELECT, INSERT, UPDATE, DELETE
  ON FUTURE TABLES IN SCHEMA RAW_KAFKA.NORTHWIND
  TO ROLE SNFLK_ROLE_KAFKA;

-- =============================================================================
-- Chave publica (executar apos gerar key pair):
-- USE ROLE SECURITYADMIN;
-- ALTER USER SNFLK_USER_KAFKA
--   SET RSA_PUBLIC_KEY='<CHAVE_PUBLICA_SEM_BEGIN_END>';
-- =============================================================================
