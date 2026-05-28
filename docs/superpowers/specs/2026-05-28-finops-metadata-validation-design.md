# FinOps — Redução de CloudServices em Validação de Schema

**Data:** 2026-05-28
**Status:** Aprovado pelo product owner; pronto para plano de implementação.

---

## Problema

O conector hoje consulta metadados do Snowflake (`DatabaseMetaData.getColumns()` e `getPrimaryKeys()`) em todo `SinkTask.start()` para descobrir o schema das tabelas `_INGEST` e FINAL. Essas chamadas consomem créditos de **CloudServices**, e o custo se amplifica em três eixos:

1. **Escala horizontal** — N tasks rodando simultaneamente, cada uma fazendo a mesma descoberta.
2. **Restart de cluster Kafka Connect** — toda task é reiniciada e revalida schemas que já existiam.
3. **Deploy de nova versão do conector** — mesmo comportamento de restart.

Em escala de produção (centenas a milhares de tasks × restarts/deploys recorrentes), esse custo recorrente é **desnecessário**, porque o schema das tabelas é tratado como imutável no contrato operacional: o DDL da origem é replicado manualmente para o Snowflake, e qualquer mudança de schema é evento manual e coordenado (alterar `_INGEST` + FINAL + reprovisionar pipeline). O conector não precisa "descobrir" nada que o operador já estabeleceu.

Duas causas raízes foram identificadas no código atual:

### Causa 1 — Driver JDBC antigo usa `SHOW COLUMNS IN ACCOUNT`

O `snowflake-jdbc` 3.21.0 (em [pom.xml:24](../../../pom.xml#L24)) implementa `DatabaseMetaData.getColumns()` chamando `SHOW COLUMNS IN ACCOUNT` e filtrando o resultado no client. Isso é o pior caso de FinOps em metadados Snowflake: o comando escaneia **todos os objetos visíveis na conta inteira**, independentemente do filtro de catalog/schema/table passado pelo client.

A partir do driver 3.28.0, a implementação passou a usar `SELECT ... FROM <db>.INFORMATION_SCHEMA.COLUMNS WHERE ...`, escopado por database. O custo CS por chamada cai 1-2 ordens de magnitude em accounts típicos de produção.

### Causa 2 — Chamadas redundantes para `_INGEST` e FINAL

Em modo STAGE, o conector faz **duas** chamadas de `getColumns()` por startup:

- [InlineProcessor.java:268-316](../../../src/main/java/triggo/ai/connector/v1/snowflake/InlineProcessor.java#L268-L316) → `getColumns()` + `getPrimaryKeys()` na tabela **FINAL** (para montar o MERGE)
- [StageCopyWriter.java:150-169](../../../src/main/java/triggo/ai/connector/v1/snowflake/StageCopyWriter.java#L150-L169) → `getColumns()` na **_INGEST** (para descobrir tipos e escolher casts `TRY_TO_NUMBER`/`TRY_TO_DOUBLE` no COPY INTO)

A segunda chamada é **redundante por construção**: a `_INGEST` é, por definição, a FINAL acrescida de seis colunas de metadados (`KFK_TOPIC`, `KFK_PARTITION`, `KFK_OFFSET`, `KFK_OP`, `KFK_DATETIME`, `KFK_BLOCKID`) cujos tipos são constantes conhecidas em compile-time pelo conector (definidas em [sql/ddl_ingest_table.sql](../../../sql/ddl_ingest_table.sql)). Não há razão para consultar o Snowflake sobre tipos que o próprio conector escreveu.

---

## Princípios da solução

1. **Snowflake é a única fonte de verdade do schema.** O conector não duplica essa informação em config nem mantém cache externo.
2. **Conector reage, não dita.** Mudanças de DDL são responsabilidade do operador; o conector apenas lê o schema vigente quando precisa.
3. **Validação acontece quando é útil.** Em vez de validar em todo startup (90% das vezes para schemas que não mudaram), validar uma vez por ciclo de vida da task usando a chamada mais barata possível.
4. **Mudanças mínimas, baixo risco.** Otimizações arquiteturais mais agressivas (cache em tópico Kafka compactado, registry table no Snowflake, inferência por payload) foram avaliadas e descartadas neste corte por adicionarem complexidade desproporcional ao ganho residual após as duas correções acima.

---

## Solução

### Mudança 1 — Bump do driver JDBC

Atualizar `snowflake-jdbc` em [pom.xml:24](../../../pom.xml#L24) de `3.21.0` → `3.28.0`.

**Efeito direto:** cada chamada de `DatabaseMetaData.getColumns()`/`getPrimaryKeys()` passa a usar `INFORMATION_SCHEMA` escopado por database em vez de `SHOW COLUMNS IN ACCOUNT`. Redução de custo CS por chamada estimada em 1-2 ordens de magnitude em accounts típicos.

**Risco:** baixo. O bump é um upgrade minor (mesmo major 3.x) e mantém compatibilidade de API. Não há mudança de comportamento esperada para as chamadas que o conector faz.

### Mudança 2 — Consolidar resolução de schema num `IngestSchema`

Criar nova classe `triggo.ai.connector.v1.snowflake.IngestSchema` responsável por uma **única** resolução de schema por ciclo de vida de task.

**Responsabilidades:**

- Executar **uma** chamada `getColumns()` na tabela FINAL.
- Derivar o schema da `_INGEST` somando às colunas da FINAL um `Map` estático com as seis colunas `KFK_*` e seus tipos (constantes hardcoded na própria classe, espelhando [sql/ddl_ingest_table.sql](../../../sql/ddl_ingest_table.sql)).
- Resolver PKs: se `pk.fields` estiver informado, usa diretamente (sem chamar Snowflake); caso contrário, executa **uma** chamada `getPrimaryKeys()` na FINAL.

**Interface pública:**

```java
public final class IngestSchema {
    public final List<String>         finalColumns;       // colunas de negócio
    public final List<String>         pks;
    public final Map<String, String>  ingestColumnTypes;  // negócio + KFK_*

    public static IngestSchema resolve(Connection conn, SnowflakeSinkConfig config) throws Exception;
}
```

**Schema KFK_* hardcoded** (constante interna da classe):

| Coluna           | Tipo            |
|------------------|-----------------|
| `KFK_TOPIC`      | `VARCHAR`       |
| `KFK_PARTITION`  | `NUMBER`        |
| `KFK_OFFSET`     | `NUMBER`        |
| `KFK_OP`         | `VARCHAR`       |
| `KFK_DATETIME`   | `TIMESTAMP_NTZ` |
| `KFK_BLOCKID`    | `VARCHAR`       |

### Mudança 3 — Refatorar consumidores para usar `IngestSchema`

#### `InlineProcessor`

- Remover o método `initColumns()` em [InlineProcessor.java:268-316](../../../src/main/java/triggo/ai/connector/v1/snowflake/InlineProcessor.java#L268-L316) e os campos derivados (`businessColumns`, `resolvedPks`, `nonPkColumns`).
- Receber `IngestSchema` via construtor (ou setter, conforme padrão de injeção mais simples no contexto do `SnowflakeSinkTask`).
- Substituir todas as referências aos campos antigos por leituras do `IngestSchema` correspondente.
- O `nonPkColumns` (usado para construir o `UPDATE SET col=col` do MERGE) passa a ser computado a partir de `finalColumns - pks` no momento da construção do SQL, ou pré-computado uma única vez no `IngestSchema` se for usado com frequência (decisão de implementação, sem impacto funcional).

#### `StageCopyWriter`

- **Deletar** o método `loadIngestColumnTypes()` em [StageCopyWriter.java:150-169](../../../src/main/java/triggo/ai/connector/v1/snowflake/StageCopyWriter.java#L150-L169) e o campo `ingestColumnTypes` interno.
- Receber `IngestSchema` via construtor; obter os tipos para casts diretamente de `schema.ingestColumnTypes`.
- O método `buildSelectExpression()` continua existindo, mas consulta o `Map` vindo do `IngestSchema` em vez do campo local.

### Mudança 4 — Resolver `IngestSchema` uma única vez no início da task

Em [SnowflakeSinkTask.start()](../../../src/main/java/triggo/ai/connector/v1/SnowflakeSinkTask.java#L66):

- Abrir uma `Connection` JDBC via `SnowflakeConnectionHelper.createJdbcConnection(config)`.
- Chamar `IngestSchema.resolve(conn, config)` uma única vez.
- Fechar a connection (a resolução é stateless e curta).
- Injetar o `IngestSchema` resolvido no `writer` (`SnowpipeStreamingWriter` ou `StageCopyWriter`) e no `InlineProcessor` (quando aplicável, no modo `SNOWPIPE_STREAMING`).

Esse ponto garante que mesmo no modo STAGE não há possibilidade arquitetural de fazer duas chamadas separadas, porque ambos os consumidores recebem a mesma instância imutável.

### Mudança 5 — Log de observabilidade

Na resolução do schema, emitir log em nível `INFO` com:

- Nome qualificado da tabela (`db.schema.table`)
- Contagem de colunas de negócio
- Lista de PKs
- Origem das PKs (`pk.fields` config vs `getPrimaryKeys` Snowflake)

Isso permite verificar via logs do Connect, sem instrumentação adicional, se o schema foi resolvido corretamente e quantas chamadas de metadados foram economizadas.

Exemplo:

```
INFO  IngestSchema resolved: table=RAW_KAFKA.NORTHWIND.ORDERS, finalColumns=12, pks=[ORDER_ID] (source=pk.fields), ingestColumnTypes=18
```

---

## Impacto agregado

| Cenário                       | Antes                                     | Depois                                  |
|-------------------------------|-------------------------------------------|-----------------------------------------|
| STAGE startup                 | 2-3 calls × `SHOW COLUMNS IN ACCOUNT`     | 1-2 calls × `INFORMATION_SCHEMA` scoped |
| SNOWPIPE startup              | 1-2 calls × `SHOW COLUMNS IN ACCOUNT`     | 1-2 calls × `INFORMATION_SCHEMA` scoped |
| CS por call (account grande)  | alto (varredura account inteira)          | baixo (query escopada por database)     |
| Redução estimada agregada     | —                                         | **~99%**                                |

---

## Compatibilidade e migração

- **Config:** nenhuma mudança. Configs existentes continuam funcionando.
- **DDL:** nenhuma mudança. As constantes `KFK_*` hardcoded espelham [sql/ddl_ingest_table.sql](../../../sql/ddl_ingest_table.sql) — qualquer alteração futura no DDL de `_INGEST` exige atualização correspondente do `Map` estático em `IngestSchema` (esse acoplamento é intencional e documentado em comentário na própria classe).
- **API pública:** nenhuma mudança visível. A refatoração é interna ao package `snowflake`.
- **Comportamento observável:** logs novos no startup; nenhum outro comportamento alterado.

---

## Riscos e mitigações

| Risco                                                                              | Mitigação                                                                                                                                                                          |
|------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Driver 3.28.0 muda comportamento sutil em algum endpoint que o conector usa        | Rodar a suíte de testes existente após o bump; teste manual de ingestão nos dois modos (STAGE e SNOWPIPE_STREAMING)                                                                |
| Schema `KFK_*` no código diverge do DDL real da `_INGEST`                          | Comentário na classe `IngestSchema` indicando a fonte de verdade; em uma futura iteração, considerar gerar a constante a partir de uma fonte compartilhada (não escopo deste corte) |
| Resolução de schema falha no `start()` da task e mata a task inteira               | Comportamento já é o atual (o `initColumns` lazy de hoje também falha hard quando a tabela não existe); a mudança apenas antecipa a falha para o startup, o que é desejável        |

---

## Out of scope

- Cache externo de schema (tópico Kafka compactado, tabela registry no Snowflake).
- Inferência de schema a partir do payload (modo "zero metadata").
- Geração automática da constante `KFK_*` a partir do DDL.
- Bump de outras dependências (somente `snowflake-jdbc` neste corte).
