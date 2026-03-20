package br.com.triggoai.connector.v1.snowflake;

import br.com.triggoai.connector.v1.config.SnowflakeSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executa o processamento da _INGEST para a tabela final usando SQL inline.
 * Substitui a abordagem de Stored Procedure.
 *
 * Dois modos de operação:
 *
 * 1. processBlock(conn, blockId) — para STAGE_COPY
 *    Chamado logo após o COPY INTO, processa um blockId específico com 3 SQLs:
 *    INSERT (c/r), UPDATE (u), DELETE (d).
 *
 * 2. processAllPending(conn) — para SNOWPIPE_STREAMING
 *    Chamado pelo CleanupJob. Usa MERGE com window function para pegar
 *    o estado mais recente de cada PK e aplicar na tabela final.
 *
 * Os nomes de colunas da tabela final são descobertos via DatabaseMetaData
 * na primeira execução e cacheados em memória.
 */
public class InlineProcessor {

    private static final Logger log = LoggerFactory.getLogger(InlineProcessor.class);

    private final SnowflakeSinkConfig config;

    /** Colunas de negócio da tabela final (sem prefixo IH_). Lazy-loaded. */
    private List<String> businessColumns;

    /** Colunas de negócio que não são PK (usadas no SET do UPDATE). */
    private List<String> nonPkColumns;

    /** PKs resolvidas: pk.fields se informado, senão descoberto via DatabaseMetaData. */
    private List<String> resolvedPks;

    public InlineProcessor(SnowflakeSinkConfig config) {
        this.config = config;
    }

    // -------------------------------------------------------------------------
    // Modo STAGE_COPY: processBlock por blockId (síncrono, no flush)
    // -------------------------------------------------------------------------

    /**
     * Processa INSERT, UPDATE e DELETE de um blockId específico.
     * Chamado pelo StageCopyWriter após o COPY INTO.
     */
    public void processBlock(Connection conn, String blockId) throws Exception {
        initColumns(conn);

        String ingest = config.getIngestTable();
        String target = config.getSnowflakeTable();
        List<String> pks = resolvedPks;

        log.debug("InlineProcessor.processBlock: table={}, blockId={}", target, blockId);

        try (Statement stmt = conn.createStatement()) {

            // --- INSERT para operações c (create) e r (snapshot) ---
            String cols   = String.join(", ", businessColumns);
            String insert = String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s WHERE IH_BLOCKID = '%s' AND IH_OP IN ('c', 'r')",
                target, cols, cols, ingest, blockId);
            int inserted = stmt.executeUpdate(insert);

            // --- UPDATE para operação u ---
            if (!nonPkColumns.isEmpty()) {
                String setClause  = nonPkColumns.stream()
                        .map(c -> "final." + c + " = src." + c)
                        .collect(Collectors.joining(", "));
                String pkWhere = buildPkJoin(pks, "final", "src");
                String update = String.format(
                    "UPDATE %s AS final SET %s FROM " +
                    "(SELECT * FROM %s WHERE IH_BLOCKID = '%s' AND IH_OP = 'u') AS src WHERE %s",
                    target, setClause, ingest, blockId, pkWhere);
                int updated = stmt.executeUpdate(update);
                log.debug("blockId={}: INSERT={}, UPDATE={}", blockId, inserted, updated);
            } else {
                log.debug("blockId={}: INSERT={} (sem colunas non-PK para UPDATE)", blockId, inserted);
            }

            // --- DELETE para operação d ---
            String pkCols  = String.join(", ", pks);
            String pkWhere = buildPkJoin(pks, "final", "src");
            String delete = String.format(
                "DELETE FROM %s AS final USING " +
                "(SELECT %s FROM %s WHERE IH_BLOCKID = '%s' AND IH_OP = 'd') AS src WHERE %s",
                target, pkCols, ingest, blockId, pkWhere);
            int deleted = stmt.executeUpdate(delete);
            log.debug("blockId={}: DELETE={}", blockId, deleted);
        }
    }

    // -------------------------------------------------------------------------
    // Modo SNOWPIPE_STREAMING: MERGE de todos os registros pendentes
    // -------------------------------------------------------------------------

    /**
     * Processa todos os registros pendentes da _INGEST usando MERGE.
     * Usa ROW_NUMBER para pegar o estado mais recente de cada PK.
     * Chamado pelo CleanupJob no modo SNOWPIPE_STREAMING.
     */
    public void processAllPending(Connection conn) throws Exception {
        initColumns(conn);

        String ingest  = config.getIngestTable();
        String target  = config.getSnowflakeTable();
        List<String> pks = resolvedPks;
        int batchSize  = config.getMergeBatchSize();

        log.info("InlineProcessor.processAllPending: table={}, batchSize={}", target, batchSize);

        String pkPartition = pks.stream().map(p -> "src_inner." + p).collect(Collectors.joining(", "));
        String pkJoin      = buildPkJoin(pks, "tgt", "src");
        String colList     = String.join(", ", businessColumns);
        String srcColList  = businessColumns.stream().map(c -> "src." + c).collect(Collectors.joining(", "));

        String setClause = nonPkColumns.isEmpty() ? "" :
                nonPkColumns.stream()
                        .map(c -> "tgt." + c + " = src." + c)
                        .collect(Collectors.joining(", "));

        StringBuilder merge = new StringBuilder();
        merge.append("MERGE INTO ").append(target).append(" AS tgt\n");
        merge.append("USING (\n");
        merge.append("    SELECT ").append(colList).append(", IH_OP FROM (\n");
        merge.append("        SELECT src_inner.*, ROW_NUMBER() OVER (\n");
        merge.append("            PARTITION BY ").append(pkPartition).append("\n");
        merge.append("            ORDER BY IH_OFFSET DESC, IH_PARTITION DESC\n");
        merge.append("        ) AS rn\n");
        merge.append("        FROM ").append(ingest).append(" AS src_inner\n");
        merge.append("        LIMIT ").append(batchSize).append("\n");
        merge.append("    ) ranked WHERE rn = 1\n");
        merge.append(") AS src\n");
        merge.append("ON (").append(pkJoin).append(")\n");

        if (!nonPkColumns.isEmpty()) {
            merge.append("WHEN MATCHED AND src.IH_OP IN ('c', 'r', 'u') THEN UPDATE SET ").append(setClause).append("\n");
        }

        merge.append("WHEN NOT MATCHED AND src.IH_OP IN ('c', 'r') THEN INSERT (").append(colList).append(")\n");
        merge.append("    VALUES (").append(srcColList).append(")\n");
        merge.append("WHEN MATCHED AND src.IH_OP = 'd' THEN DELETE");

        String mergeSQL = merge.toString();
        log.debug("MERGE SQL:\n{}", mergeSQL);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(mergeSQL);
        }

        log.info("processAllPending concluído. table={}", target);
    }

    // -------------------------------------------------------------------------
    // Cleanup: remove os registros do blockId já processado da _INGEST
    // -------------------------------------------------------------------------

    /**
     * Remove da _INGEST os registros do blockId recém-processado.
     * Chamado pelo StageCopyWriter logo após processBlock() — sem delay, sem cron.
     */
    public void cleanupBlock(Connection conn, String blockId) throws Exception {
        String sql = "DELETE FROM " + config.getIngestTable() + " WHERE IH_BLOCKID = '" + blockId + "'";
        log.debug("cleanupBlock SQL: {}", sql);
        try (Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            log.debug("cleanupBlock: {} registros removidos da {} (blockId={})",
                    deleted, config.getIngestTable(), blockId);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Carrega colunas e PKs da tabela final via DatabaseMetaData.
     * PKs: usa pk.fields se informado; senão descobre automaticamente da tabela target.
     * Executado apenas uma vez (lazy init thread-safe).
     */
    synchronized void initColumns(Connection conn) throws Exception {
        if (businessColumns != null) return;

        String db     = config.getSnowflakeDatabase().toUpperCase();
        String schema = config.getSnowflakeSchema().toUpperCase();
        String table  = config.getSnowflakeTable().toUpperCase();

        DatabaseMetaData meta = conn.getMetaData();

        // 1. Colunas da tabela final
        List<String> cols = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(db, schema, table, null)) {
            while (rs.next()) {
                cols.add(rs.getString("COLUMN_NAME"));
            }
        }

        if (cols.isEmpty()) {
            throw new RuntimeException("InlineProcessor: nenhuma coluna encontrada para tabela "
                    + schema + "." + table
                    + ". Verifique se a tabela existe e as credenciais têm acesso.");
        }

        // 2. PKs: pk.fields (FLAT_JSON) ou auto-descoberta via DatabaseMetaData
        List<String> pks;
        if (!config.getPkFields().isEmpty()) {
            pks = config.getPkFields().stream().map(String::toUpperCase).collect(Collectors.toList());
        } else {
            pks = new ArrayList<>();
            try (ResultSet rs = meta.getPrimaryKeys(db, schema, table)) {
                while (rs.next()) {
                    pks.add(rs.getString("COLUMN_NAME"));
                }
            }
            if (pks.isEmpty()) {
                throw new RuntimeException("InlineProcessor: nenhuma PK encontrada para tabela "
                        + schema + "." + table
                        + ". Defina pk.fields ou adicione uma PRIMARY KEY na tabela.");
            }
        }

        businessColumns = cols;
        resolvedPks     = pks;
        nonPkColumns    = cols.stream()
                .filter(c -> !pks.contains(c.toUpperCase()))
                .collect(Collectors.toList());

        log.info("InlineProcessor: tabela={}, colunas={}, PKs={}", table, businessColumns, resolvedPks);
    }

    private String buildPkJoin(List<String> pks, String leftAlias, String rightAlias) {
        return pks.stream()
                .map(pk -> leftAlias + "." + pk + " = " + rightAlias + "." + pk)
                .collect(Collectors.joining(" AND "));
    }
}
