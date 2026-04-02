# Snowflake Custom Sink Connector — Contexto do Projeto

## Objetivo
Construir um Kafka Connect Sink Connector customizado em Java/Maven que:
- Lê eventos de tópicos Kafka
- Suporta 4 formatos de payload (ver abaixo)
- Grava na tabela `<TABELA>_INGEST` no Snowflake
- Executa MERGE da `_INGEST` para a tabela final via Stored Procedure
- Faz cleanup da `_INGEST` após o MERGE

---

## Stack
- **Java 17**
- **Maven** com `maven-shade-plugin` (fat JAR)
- **Kafka Connect API 4.1.0**
- **snowflake-jdbc 3.21.0**
- **Snowflake Ingest SDK** (Snowpipe Streaming)
- **Quartz Scheduler** (job de MERGE agendado por cron)
- **Confluent Schema Registry Client** (para AVRO)
- **Jackson** (para JSON parsing)

---

## Formatos de payload suportados

### 1. FLAT_JSON
JSON simples sem envelope Debezium. PK configurada manualmente via `pk.fields`.
```json
{ "IDT_UNC_TSC": 123, "NOME": "foo" }
```

### 2. DEBEZIUM_JSON (sem schema)
Envelope Debezium sem bloco de schema. PK vem da Kafka message key.
```json
{
  "before": null,
  "after": { "IDT_UNC_TSC": 123, "NOME": "foo" },
  "op": "c",
  "ts_ms": 1773324092574
}
```

### 3. DEBEZIUM_JSON (com schema)
Envelope Debezium com bloco `schema` + `payload`. PK vem da Kafka message key.
```json
{
  "schema": { "type": "struct", "fields": [...] },
  "payload": {
    "before": null,
    "after": { "IDT_UNC_TSC": 123, "NOME": "foo" },
    "op": "c",
    "ts_ms": 1773324092574
  }
}
```

### 4. AVRO (Debezium via Schema Registry)
Deserializado via Confluent Schema Registry. Estrutura interna igual ao Debezium JSON após deserialização.
PK vem da Kafka message key (também em AVRO).

**Exemplo de key:**
```json
{ "IDT_UNC_TSC": 32571316, "IDT_SEQ_UNC": 1 }
```

**Exemplo de value (após deserialização):**
```json
{
  "before": null,
  "after": {
    "IDT_UNC_TSC": 32571316,
    "IDT_SEQ_UNC": 1,
    "NUM_UNC_PTB": "202603020000406987343"
  },
  "source": {
    "version": "3.3.1.Final",
    "connector": "sqlserver",
    "db": "GCOB001D",
    "schema": "dbo",
    "table": "GCOE007D"
  },
  "op": "c",
  "ts_ms": 1773324092574
}
```

---

## Mapeamento de operações

| op (Debezium) | Ação no MERGE |
|---|---|
| `c` (create) | INSERT |
| `r` (read/snapshot) | INSERT (igual ao create) |
| `u` (update) | UPDATE |
| `d` (delete) | DELETE |

---

## Estrutura das tabelas no Snowflake

### Tabela _INGEST (staging)
Campos de negócio + metadados de controle:

```sql
-- Metadados obrigatórios (prefixo KFK_)
KFK_TOPIC     VARCHAR(255) NOT NULL,
KFK_PARTITION INT NOT NULL,
KFK_OFFSET    INT NOT NULL,
KFK_OP        VARCHAR(1) NOT NULL,      -- c, u, d, r
KFK_DATETIME  TIMESTAMP NOT NULL,
KFK_BLOCKID   VARCHAR(40) NOT NULL,     -- UUID do batch

-- PK da INGEST (evita duplicata)
CONSTRAINT pkey PRIMARY KEY (KFK_TOPIC, KFK_PARTITION, KFK_OFFSET)
```

### Tabela target (final)
Apenas campos de negócio, sem metadados KFK_.

---

## Dois modos de ingestão (configurável)

### SNOWPIPE_STREAMING (padrão novo)
- Usa Snowflake Ingest SDK
- Grava direto na `_INGEST` sem passar por Stage
- Latência menor

### STAGE_COPY (padrão legado)
- Grava arquivo CSV/JSON na Snowflake Stage
- Executa `COPY INTO _INGEST`
- Compatível com fluxos existentes

Configurado via `ingestion.mode` no JSON do conector.

---

## Extração de PK por formato

| Formato | Fonte da PK |
|---|---|
| FLAT_JSON | Campo configurado manualmente via `pk.fields` |
| DEBEZIUM_JSON | Kafka message key (deserializada como JSON) |
| AVRO | Kafka message key (deserializada via Schema Registry) |

---

## Configuração do conector (exemplo)

```json
{
  "name": "snowflake-custom-sink-ntfe001d",
  "config": {
    "connector.class": "br.com.suaempresa.connector.SnowflakeSinkConnector",
    "tasks.max": "1",
    "topics": "source-debezium-ntfe001d",

    "payload.format": "AVRO",
    "ingestion.mode": "SNOWPIPE_STREAMING",

    "snowflake.url": "https://account.snowflakecomputing.com",
    "snowflake.user": "USER",
    "snowflake.private.key": "...",
    "snowflake.database": "LZ_SQL_IH_PRD",
    "snowflake.schema": "GCOB001D",
    "snowflake.table": "NTFE001D",

    "schema.registry.url": "http://schema-registry:8081",

    "pk.fields": "IDT_UNC_TSC,IDT_SEQ_UNC",

    "merge.schedule.cron": "0/30 * * * * ?",
    "merge.batch.size": "10000"
  }
}
```

---

## Estrutura de arquivos do projeto

```
snowflake-custom-sink/
├── pom.xml
├── CLAUDE.md                          ← este arquivo
├── src/main/java/br/com/suaempresa/connector/
│   ├── SnowflakeSinkConnector.java    ← registro no Kafka Connect
│   ├── SnowflakeSinkTask.java         ← lógica principal por batch
│   ├── config/
│   │   └── SnowflakeSinkConfig.java   ← configs tipadas com validação
│   ├── model/
│   │   └── ParsedRecord.java          ← DTO entre parser e writer
│   ├── parser/
│   │   ├── PayloadParser.java         ← interface Strategy
│   │   ├── FlatJsonParser.java
│   │   ├── DebeziumJsonParser.java    ← trata com e sem schema
│   │   └── AvroParser.java            ← Schema Registry
│   ├── snowflake/
│   │   ├── SnowpipeStreamingWriter.java
│   │   ├── StageCopyWriter.java
│   │   └── MergeProcessor.java        ← chama SP via JDBC
│   └── scheduler/
│       └── MergeJob.java              ← Quartz job
└── sql/
    ├── ddl_ingest_table.sql
    └── sp_merge_cleanup.sql
```

---

## Referência: conector de inspiração
- Repo: https://github.com/datastreambrasil/snowflake-sink-connector
- Versão de referência: v3.5.0
- O que reutilizamos: estrutura Maven, uso do Quartz, padrão de conexão JDBC
- O que diferencia o nosso: suporte a 4 formatos, dois modos de ingestão, MERGE automático via SP

---

## Source connector de exemplo (Debezium SQL Server)
```yaml
class: io.debezium.connector.sqlserver.SqlServerConnector
config:
  decimal.handling.mode: "string"
  tombstones.on.delete: false
  data.query.mode: direct
  snapshot.locking.mode: none
```

## Próximos passos
1. Implementar `ParsedRecord.java` e `PayloadParser.java` (interface)
2. Implementar os 4 parsers
3. Implementar `SnowflakeSinkConfig.java`
4. Implementar `SnowflakeSinkConnector.java` + `SnowflakeSinkTask.java`
5. Implementar os dois writers (`SnowpipeStreamingWriter` e `StageCopyWriter`)
6. Implementar `MergeProcessor.java` + `MergeJob.java`
7. Gerar SQLs (`ddl_ingest_table.sql`, `sp_merge_cleanup.sql`)
