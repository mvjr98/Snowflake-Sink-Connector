package triggo.ai.connector.v1.snowflake;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolução única de schema por ciclo de vida da task.
 *
 * Centraliza todas as chamadas de metadados (DatabaseMetaData.getColumns / getPrimaryKeys)
 * num único ponto. Os consumidores (StageCopyWriter, InlineProcessor) recebem a instância
 * resolvida via construtor, eliminando chamadas duplicadas ao Snowflake.
 *
 * O schema da tabela _INGEST é derivado da FINAL acrescido das colunas de metadados KFK_*
 * cujos tipos são constantes conhecidas pelo conector. Esse mapeamento espelha
 * sql/ddl_ingest_table.sql — qualquer alteração no DDL exige atualização da constante
 * KFK_METADATA_COLUMNS abaixo.
 */
public final class IngestSchema {

    private static final Logger log = LoggerFactory.getLogger(IngestSchema.class);

    /**
     * Schema dos metadados KFK_* na tabela _INGEST.
     * Espelha sql/ddl_ingest_table.sql. Os valores são os TYPE_NAME que o JDBC retornaria
     * para essas colunas (Snowflake normaliza INT → NUMBER e omite precisão em TYPE_NAME).
     */
    private static final Map<String, String> KFK_METADATA_COLUMNS;
    static {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("KFK_TOPIC",     "VARCHAR");
        m.put("KFK_PARTITION", "NUMBER");
        m.put("KFK_OFFSET",    "NUMBER");
        m.put("KFK_OP",        "VARCHAR");
        m.put("KFK_DATETIME",  "TIMESTAMP_NTZ");
        m.put("KFK_BLOCKID",   "VARCHAR");
        KFK_METADATA_COLUMNS = Collections.unmodifiableMap(m);
    }

    /** Colunas de negócio da tabela final, na ordem do DDL. */
    public final List<String> finalColumns;

    /** PKs (sempre uppercase). Origem: pk.fields da config, ou getPrimaryKeys da FINAL. */
    public final List<String> pks;

    /** Colunas de negócio que não são PK (usadas no SET do UPDATE/MERGE). */
    public final List<String> nonPkColumns;

    /** Schema completo da _INGEST: colunas de negócio (FINAL) + KFK_*. Tipos em uppercase. */
    public final Map<String, String> ingestColumnTypes;

    private IngestSchema(List<String> finalColumns,
                         List<String> pks,
                         List<String> nonPkColumns,
                         Map<String, String> ingestColumnTypes) {
        this.finalColumns      = Collections.unmodifiableList(finalColumns);
        this.pks               = Collections.unmodifiableList(pks);
        this.nonPkColumns      = Collections.unmodifiableList(nonPkColumns);
        this.ingestColumnTypes = Collections.unmodifiableMap(ingestColumnTypes);
    }

    /**
     * Resolve o schema com uma única chamada de metadados (getColumns na FINAL) e,
     * se necessário, uma chamada adicional de getPrimaryKeys quando pk.fields não foi informado.
     *
     * O schema da _INGEST é derivado: FINAL + KFK_METADATA_COLUMNS (sem chamada extra).
     */
    public static IngestSchema resolve(Connection conn, SnowflakeSinkConfig config) throws Exception {
        String db     = config.getSnowflakeDatabase().toUpperCase(Locale.ROOT);
        String schema = config.getSnowflakeSchema().toUpperCase(Locale.ROOT);
        String table  = config.getSnowflakeTable().toUpperCase(Locale.ROOT);

        DatabaseMetaData meta = conn.getMetaData();

        // Única chamada de metadados na FINAL
        Map<String, String> finalColumnTypes = new LinkedHashMap<>();
        try (ResultSet rs = meta.getColumns(db, schema, table, null)) {
            while (rs.next()) {
                finalColumnTypes.put(
                        rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT),
                        rs.getString("TYPE_NAME").toUpperCase(Locale.ROOT)
                );
            }
        }

        if (finalColumnTypes.isEmpty()) {
            throw new RuntimeException("IngestSchema: nenhuma coluna encontrada para tabela "
                    + schema + "." + table
                    + ". Verifique se a tabela existe e se as credenciais têm acesso.");
        }

        // PKs: pk.fields se informado (sem ir ao Snowflake); senão getPrimaryKeys
        List<String> pks;
        String pkSource;
        if (!config.getPkFields().isEmpty()) {
            pks = config.getPkFields().stream()
                    .map(s -> s.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toList());
            pkSource = "pk.fields";
        } else {
            pks = new java.util.ArrayList<>();
            try (ResultSet rs = meta.getPrimaryKeys(db, schema, table)) {
                while (rs.next()) {
                    pks.add(rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
                }
            }
            if (pks.isEmpty()) {
                throw new RuntimeException("IngestSchema: nenhuma PK encontrada para tabela "
                        + schema + "." + table
                        + ". Defina pk.fields ou adicione uma PRIMARY KEY na tabela.");
            }
            pkSource = "metadata";
        }

        List<String> finalColumns = new java.util.ArrayList<>(finalColumnTypes.keySet());
        List<String> nonPkColumns = finalColumns.stream()
                .filter(c -> !pks.contains(c))
                .collect(Collectors.toList());

        // _INGEST = FINAL + KFK_* (derivado, sem chamada adicional)
        Map<String, String> ingestColumnTypes = new LinkedHashMap<>(finalColumnTypes);
        ingestColumnTypes.putAll(KFK_METADATA_COLUMNS);

        log.info("IngestSchema resolved: table={}.{}.{}, finalColumns={}, pks={} (source={}), ingestColumnTypes={}",
                db, schema, table, finalColumns.size(), pks, pkSource, ingestColumnTypes.size());

        return new IngestSchema(finalColumns, pks, nonPkColumns, ingestColumnTypes);
    }
}
