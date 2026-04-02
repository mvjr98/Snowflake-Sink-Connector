# Triggo.ai Snowflake Sink Connector

Connector customizado da Triggo.ai para Kafka Connect, focado em replicacao CDC para Snowflake com suporte a multiplos formatos de payload e dois modos de ingestao.

Site: [triggo.ai](https://triggo.ai)

![Arquitetura do conector](./arquitetura.png)

## O que este conector entrega

- Leitura de topicos Kafka e escrita na tabela `<TABELA>_INGEST` no Snowflake
- Aplicacao de `INSERT/UPDATE/DELETE` na tabela final `<TABELA>`
- Suporte nativo a 4 formatos:
  - Flat JSON
  - Debezium JSON sem schema
  - Debezium JSON com schema
  - Debezium AVRO (Schema Registry)
- Dois modos de ingestao:
  - `SNOWPIPE_STREAMING` (baixa latencia)
  - `STAGE` (compatibilidade com fluxo `COPY INTO`)
- Cleanup configuravel da `_INGEST` via `ingest.cleanup.delay.seconds` e `ingest.cleanup.interval.seconds`

## Fluxo funcional

1. `SnowflakeSinkTask` recebe records do Kafka Connect
2. `AutoParser` detecta formato automaticamente e gera `ParsedRecord`
3. Writer envia para `<TABELA>_INGEST`:
   - `SnowpipeStreamingWriter` no modo `SNOWPIPE_STREAMING`
   - `StageCopyWriter` no modo `STAGE`
4. `InlineProcessor` aplica dados na tabela final `<TABELA>`
5. `_INGEST` e limpa conforme politicas de cleanup configuradas

## Modos de ingestao

### 1) SNOWPIPE_STREAMING

- Escrita direta na `_INGEST` via Snowflake Ingest SDK
- Processamento assincrono `_INGEST -> final` via `MERGE`
- Cleanup por batch processado com janela de retencao configuravel

### 2) STAGE

- Serializa lote em JSON Lines
- Upload para Stage + `COPY INTO <TABELA>_INGEST`
- Processamento sincrono (`processBlock`) para tabela final
- Cleanup imediato ou com retencao, conforme configuracao

## Formatos de payload suportados

### Flat JSON

```json
{ "ORDER_ID": 10248, "CUSTOMER_ID": "VINET" }
```

### Debezium JSON sem schema

```json
{
  "before": null,
  "after": { "ORDER_ID": 10248, "CUSTOMER_ID": "VINET" },
  "op": "c",
  "ts_ms": 1773324092574
}
```

### Debezium JSON com schema

```json
{
  "schema": { "type": "struct", "fields": [] },
  "payload": {
    "before": null,
    "after": { "ORDER_ID": 10248, "CUSTOMER_ID": "VINET" },
    "op": "c",
    "ts_ms": 1773324092574
  }
}
```

### Debezium AVRO

- Usando `io.confluent.connect.avro.AvroConverter`
- Com suporte a normalizacao de logical types (Date/Timestamp/Time)

## DDL esperado no Snowflake

O conector espera duas tabelas:

- `<TABELA>_INGEST` (campos de negocio + metadados `KFK_*`)
- `<TABELA>` (tabela final de negocio)

Metadados obrigatorios na `_INGEST`:

- `KFK_TOPIC` `VARCHAR`
- `KFK_PARTITION` `INT`
- `KFK_OFFSET` `NUMBER`
- `KFK_OP` `VARCHAR(1)`
- `KFK_DATETIME` `TIMESTAMP_NTZ`
- `KFK_BLOCKID` `VARCHAR(40)`

Chave primaria recomendada na `_INGEST`:

```sql
PRIMARY KEY (KFK_TOPIC, KFK_PARTITION, KFK_OFFSET)
```

## Configuracao principal do connector

Exemplo minimo:

```json
{
  "name": "custom-snowflake-sink-orders-v1",
  "config": {
    "connector.class": "triggo.ai.connector.v1.SnowflakeSinkConnector",
    "tasks.max": "1",
    "topics": "cdc.public.orders",

    "snowflake.url": "https://<account>.snowflakecomputing.com",
    "snowflake.user": "SNFLK_USER_KAFKA",
    "snowflake.private.key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    "snowflake.database": "RAW_KAFKA",
    "snowflake.schema": "NORTHWIND",
    "snowflake.table": "ORDERS",

    "ingestion.mode": "SNOWPIPE_STREAMING",
    "job.interval.seconds": "30",
    "merge.batch.size": "10000",
    "ingest.cleanup.delay.seconds": "86400",
    "ingest.cleanup.interval.seconds": "86400",

    "buffer.count.records": "1000",
    "buffer.flush.time": "60",
    "buffer.size.bytes": "1000000",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
```

## Parametros de latencia e custo

- `buffer.count.records`: flush para `_INGEST` quando atingir N registros em memoria.
- `buffer.size.bytes`: flush para `_INGEST` quando atingir N bytes em memoria.
- `buffer.flush.time`: tempo maximo (segundos) para flush do buffer quando ha trafego continuo.
- `job.interval.seconds`: intervalo do job assincrono `_INGEST -> final` (MERGE) no modo `SNOWPIPE_STREAMING`.
- `merge.batch.size`: quantidade maxima de linhas lidas da `_INGEST` por execucao do MERGE.
- `ingest.cleanup.delay.seconds`: idade minima dos registros para limpeza por expiracao na `_INGEST` (padrao 86400).
- `ingest.cleanup.interval.seconds`: frequencia do cleanup por expiracao (padrao 86400 = 1x por dia).

Notas importantes:

- No modo `SNOWPIPE_STREAMING`, o conector agora pula MERGE quando `_INGEST` esta vazia.
- O cleanup por lote processado no `SNOWPIPE_STREAMING` continua imediato para evitar reprocessamento.
- O cleanup por expiracao roda no intervalo configurado em `ingest.cleanup.interval.seconds`.
- `buffer.flush.time` nao e um scheduler independente: ele atua junto do fluxo de `put/flush` do Kafka Connect.

Perfis sugeridos:

- Baixa latencia: `buffer.flush.time=5..15`, `job.interval.seconds=5..15`, `merge.batch.size` ajustado por volume.
- Menor custo: `buffer.flush.time=60..120`, `job.interval.seconds=60..300`, warehouse com auto-suspend curto.

## Autenticacao Snowflake (Key Pair)

Para este plugin customizado, use chave **RSA PKCS8 PEM sem criptografia**.

Geracao:

```bash
openssl genrsa -out snowflake_rsa.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -in snowflake_rsa.pem -out snowflake_rsa_key.p8 -nocrypt
openssl rsa -in snowflake_rsa.pem -pubout -out snowflake_rsa_key.pub
```

Associacao da publica no Snowflake:

```sql
USE ROLE SECURITYADMIN;
ALTER USER SNFLK_USER_KAFKA SET RSA_PUBLIC_KEY='<CHAVE_PUBLICA_SEM_BEGIN_END>';
```

Se aparecer erro `Invalid private key`, revise formato PEM e correspondencia entre privada/publica.

## Build e deploy

Build local:

```bash
mvn clean package -DskipTests
```

Artefato gerado:

- `target/snowflake-sink-connector-1.0.0.jar`

Instalacao no Kafka Connect:

1. Copiar o jar para o path de plugins do Connect
2. Reiniciar o worker do Kafka Connect
3. Criar/atualizar o connector via REST API

## Estrutura do projeto

```text
src/main/java/triggo/ai/connector/v1/
  SnowflakeSinkConnector.java
  SnowflakeSinkTask.java
  config/SnowflakeSinkConfig.java
  parser/
    AutoParser.java
    FlatJsonParser.java
    DebeziumJsonParser.java
    AvroParser.java
  snowflake/
    SnowpipeStreamingWriter.java
    StageCopyWriter.java
    InlineProcessor.java
    SnowflakeConnectionHelper.java
  model/ParsedRecord.java
```

## Operacoes CDC suportadas

- `c` e `r`: insert
- `u`: update
- `d`: delete

---

Desenvolvido para uso enterprise em clientes Triggo.ai.
