package triggo.ai.connector.v1.snowflake;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executa o processamento da _INGEST para a tabela final usando SQL inline.
 * Substitui a abordagem de Stored Procedure.
 *
 * Dois modos de operação:
 *
 * 1. processBlock(conn, blockId) — para STAGE
 *    Chamado logo após o COPY INTO, processa um blockId específico com 3 SQLs:
 *    INSERT (c/r), UPDATE (u), DELETE (d).
 *
 * 2. processAllPending(conn) — para SNOWPIPE_STREAMING
 *    Chamado pelo CleanupJob. Usa MERGE com window function para pegar
 *    o estado mais recente de cada PK e aplicar na tabela final.
 *
 * Schema (colunas + PKs) é fornecido via IngestSchema, resolvido uma única vez
 * no SnowflakeSinkTask.start() — esta classe não faz chamadas de metadados.
 */
public class InlineProcessor {

    private static final Logger log = LoggerFactory.getLogger(InlineProcessor.class);

    private final SnowflakeSinkConfig config;
    private final IngestSchema schema;

    /** Último instante de execução do cleanup periódico de expiração. */
    private long lastPeriodicCleanupMs;

    public InlineProcessor(SnowflakeSinkConfig config, IngestSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    // -------------------------------------------------------------------------
    // Modo STAGE: processBlock por blockId (síncrono, no flush)
    // -------------------------------------------------------------------------

    /**
     * Processa INSERT, UPDATE e DELETE de um blockId específico.
     * Chamado pelo StageCopyWriter após o COPY INTO.
     */
    public void processBlock(Connection conn, String blockId) throws Exception {
        String ingest = config.getIngestTable();
        String target = config.getSnowflakeTable();
        List<String> pks = schema.pks;

        log.debug("InlineProcessor.processBlock: table={}, blockId={}", target, blockId);

        try (Statement stmt = conn.createStatement()) {

            // --- INSERT para operações c (create) e r (snapshot) ---
            String cols   = String.join(", ", schema.finalColumns);
            String insert = String.format(
                "INSERT INTO %s (%s) SELECT %s FROM %s WHERE KFK_BLOCKID = '%s' AND KFK_OP IN ('c', 'r')",
                target, cols, cols, ingest, blockId);
            int inserted = stmt.executeUpdate(insert);

            // --- UPDATE para operação u ---
            if (!schema.nonPkColumns.isEmpty()) {
                String setClause  = schema.nonPkColumns.stream()
                        .map(c -> "final." + c + " = src." + c)
                        .collect(Collectors.joining(", "));
                String pkWhere = buildPkJoin(pks, "final", "src");
                String update = String.format(
                    "UPDATE %s AS final SET %s FROM " +
                    "(SELECT * FROM %s WHERE KFK_BLOCKID = '%s' AND KFK_OP = 'u') AS src WHERE %s",
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
                "(SELECT %s FROM %s WHERE KFK_BLOCKID = '%s' AND KFK_OP = 'd') AS src WHERE %s",
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
        String ingest  = config.getIngestTable();
        String target  = config.getSnowflakeTable();
        List<String> pks = schema.pks;
        int batchSize  = config.getMergeBatchSize();
        boolean hasPending = hasAnyRow(conn, ingest);

        if (!hasPending) {
            log.debug("processAllPending: _INGEST sem dados pendentes. Pulando MERGE.");
            cleanupExpiredRows(conn);
            return;
        }

        log.info("InlineProcessor.processAllPending: table={}, batchSize={}", target, batchSize);

        String pkPartition = pks.stream().map(p -> "src_inner." + p).collect(Collectors.joining(", "));
        String pkJoin      = buildPkJoin(pks, "tgt", "src");
        String colList     = String.join(", ", schema.finalColumns);
        String srcColList  = schema.finalColumns.stream().map(c -> "src." + c).collect(Collectors.joining(", "));

        String setClause = schema.nonPkColumns.isEmpty() ? "" :
                schema.nonPkColumns.stream()
                        .map(c -> "tgt." + c + " = src." + c)
                        .collect(Collectors.joining(", "));

        StringBuilder merge = new StringBuilder();
        merge.append("MERGE INTO ").append(target).append(" AS tgt\n");
        merge.append("USING (\n");
        merge.append("    WITH batch AS (\n");
        merge.append("        SELECT * FROM ").append(ingest).append("\n");
        merge.append("        ORDER BY KFK_PARTITION ASC, KFK_OFFSET ASC\n");
        merge.append("        LIMIT ").append(batchSize).append("\n");
        merge.append("    )\n");
        merge.append("    SELECT ").append(colList).append(", KFK_OP FROM (\n");
        merge.append("        SELECT src_inner.*, ROW_NUMBER() OVER (\n");
        merge.append("            PARTITION BY ").append(pkPartition).append("\n");
        merge.append("            ORDER BY KFK_OFFSET DESC, KFK_PARTITION DESC\n");
        merge.append("        ) AS rn\n");
        merge.append("        FROM batch AS src_inner\n");
        merge.append("    ) ranked WHERE rn = 1\n");
        merge.append(") AS src\n");
        merge.append("ON (").append(pkJoin).append(")\n");

        if (!schema.nonPkColumns.isEmpty()) {
            merge.append("WHEN MATCHED AND src.KFK_OP IN ('c', 'r', 'u') THEN UPDATE SET ").append(setClause).append("\n");
        }

        merge.append("WHEN NOT MATCHED AND src.KFK_OP IN ('c', 'r') THEN INSERT (").append(colList).append(")\n");
        merge.append("    VALUES (").append(srcColList).append(")\n");
        merge.append("WHEN MATCHED AND src.KFK_OP = 'd' THEN DELETE");

        String mergeSQL = merge.toString();
        log.debug("MERGE SQL:\n{}", mergeSQL);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(mergeSQL);
        }

        String cleanupSQL = "DELETE FROM " + ingest + " AS ingest USING ("
                + "SELECT KFK_TOPIC, KFK_PARTITION, KFK_OFFSET FROM " + ingest + " "
                + "ORDER BY KFK_PARTITION ASC, KFK_OFFSET ASC LIMIT " + batchSize
                + ") AS batch WHERE ingest.KFK_TOPIC = batch.KFK_TOPIC "
                + "AND ingest.KFK_PARTITION = batch.KFK_PARTITION "
                + "AND ingest.KFK_OFFSET = batch.KFK_OFFSET";

        try (Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(cleanupSQL);
            log.debug("processAllPending cleanup: {} registros removidos da {}", deleted, ingest);
        }

        cleanupExpiredRows(conn);

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
        int cleanupDelaySeconds = config.getIngestCleanupDelaySeconds();
        String sql = "DELETE FROM " + config.getIngestTable()
                + " WHERE KFK_BLOCKID = '" + blockId + "'"
                + " AND KFK_DATETIME <= DATEADD('SECOND', -" + cleanupDelaySeconds + ", CURRENT_TIMESTAMP())";
        log.debug("cleanupBlock SQL: {}", sql);
        try (Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            log.debug("cleanupBlock: {} registros removidos da {} (blockId={})",
                    deleted, config.getIngestTable(), blockId);
        }
    }

    /**
     * Remove da _INGEST registros com idade maior que ingest.cleanup.delay.seconds.
     * Usado no modo STAGE para limpar blocos antigos quando cleanup delay > 0.
     */
    public void cleanupExpiredRows(Connection conn) throws Exception {
        if (!shouldRunPeriodicCleanup()) {
            return;
        }

        int cleanupDelaySeconds = config.getIngestCleanupDelaySeconds();
        if (cleanupDelaySeconds <= 0) {
            lastPeriodicCleanupMs = System.currentTimeMillis();
            return;
        }

        String sql = "DELETE FROM " + config.getIngestTable()
                + " WHERE KFK_DATETIME <= DATEADD('SECOND', -" + cleanupDelaySeconds + ", CURRENT_TIMESTAMP())";
        log.debug("cleanupExpiredRows SQL: {}", sql);
        try (Statement stmt = conn.createStatement()) {
            int deleted = stmt.executeUpdate(sql);
            log.debug("cleanupExpiredRows: {} registros removidos da {}",
                    deleted, config.getIngestTable());
        }

        lastPeriodicCleanupMs = System.currentTimeMillis();
    }

    private boolean shouldRunPeriodicCleanup() {
        int intervalSeconds = config.getIngestCleanupIntervalSeconds();
        if (intervalSeconds <= 0) {
            return true;
        }

        long now = System.currentTimeMillis();
        return (now - lastPeriodicCleanupMs) >= intervalSeconds * 1000L;
    }

    private boolean hasAnyRow(Connection conn, String tableName) throws Exception {
        String sql = "SELECT 1 FROM " + tableName + " LIMIT 1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next();
        }
    }

    private String buildPkJoin(List<String> pks, String leftAlias, String rightAlias) {
        return pks.stream()
                .map(pk -> leftAlias + "." + pk + " = " + rightAlias + "." + pk)
                .collect(Collectors.joining(" AND "));
    }
}
