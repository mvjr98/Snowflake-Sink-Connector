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
 * Dois modos de operação, ambos usando o mesmo MERGE (buildMergeSql):
 *
 * 1. processBlock(conn, blockId) — para STAGE
 *    Chamado logo após o COPY INTO, processa um blockId específico.
 *
 * 2. processAllPending(conn) — para SNOWPIPE_STREAMING
 *    Chamado pelo CleanupJob. Processa os registros pendentes em lotes.
 *
 * O MERGE usa ROW_NUMBER para pegar o estado mais recente de cada PK e aplica
 * INSERT/UPDATE/DELETE conforme o KFK_OP. Quando merge.skip.unchanged=true
 * (padrão), o UPDATE só reescreve linhas cujo HASH das colunas de negócio mudou,
 * evitando write amplification em full loads redundantes (re-snapshot) e em
 * duplicatas normais do Debezium.
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
     * Processa um blockId específico via MERGE (upsert idempotente).
     * Chamado pelo StageCopyWriter após o COPY INTO.
     */
    public void processBlock(Connection conn, String blockId) throws Exception {
        String ingest = config.getIngestTable();
        String target = config.getSnowflakeTable();

        log.debug("InlineProcessor.processBlock: table={}, blockId={}", target, blockId);

        String batchSelect = "SELECT * FROM " + ingest + " WHERE KFK_BLOCKID = '" + blockId + "'";
        String mergeSQL = buildMergeSql(batchSelect);
        log.debug("processBlock MERGE SQL:\n{}", mergeSQL);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(mergeSQL);
        }
        log.debug("processBlock concluído. blockId={}", blockId);
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
        int batchSize  = config.getMergeBatchSize();
        boolean hasPending = hasAnyRow(conn, ingest);

        if (!hasPending) {
            log.debug("processAllPending: _INGEST sem dados pendentes. Pulando MERGE.");
            cleanupExpiredRows(conn);
            return;
        }

        log.info("InlineProcessor.processAllPending: table={}, batchSize={}", target, batchSize);

        String batchSelect = "SELECT * FROM " + ingest
                + " ORDER BY KFK_PARTITION ASC, KFK_OFFSET ASC LIMIT " + batchSize;
        String mergeSQL = buildMergeSql(batchSelect);
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

    /**
     * HASH das colunas de negócio para um alias (tgt/src). Usado no hash-guard
     * do UPDATE para detectar linhas inalteradas e pular a reescrita.
     */
    private String hashExpr(String alias) {
        return "HASH(" + schema.finalColumns.stream()
                .map(c -> alias + "." + c)
                .collect(Collectors.joining(", ")) + ")";
    }

    /**
     * Monta o MERGE _INGEST → final, compartilhado pelos dois modos.
     * O único parâmetro que varia é o batchSelect (a fonte das linhas):
     *   STAGE:    SELECT * FROM _INGEST WHERE KFK_BLOCKID = '...'
     *   SNOWPIPE: SELECT * FROM _INGEST ORDER BY ... LIMIT batchSize
     *
     * ROW_NUMBER por PK pega o estado mais recente (por offset) dentro do batch.
     * Quando merge.skip.unchanged=true, o UPDATE só ocorre se o HASH das colunas
     * de negócio diferir entre destino e origem (evita reescrita de micropartição
     * para linhas idênticas).
     */
    private String buildMergeSql(String batchSelect) {
        String target      = config.getSnowflakeTable();
        List<String> pks   = schema.pks;
        String pkPartition = pks.stream().map(p -> "src_inner." + p).collect(Collectors.joining(", "));
        String pkJoin      = buildPkJoin(pks, "tgt", "src");
        String colList     = String.join(", ", schema.finalColumns);
        String srcColList  = schema.finalColumns.stream().map(c -> "src." + c).collect(Collectors.joining(", "));

        StringBuilder merge = new StringBuilder();
        merge.append("MERGE INTO ").append(target).append(" AS tgt\n");
        merge.append("USING (\n");
        merge.append("    WITH batch AS (\n");
        merge.append("        ").append(batchSelect).append("\n");
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
            String setClause = schema.nonPkColumns.stream()
                    .map(c -> "tgt." + c + " = src." + c)
                    .collect(Collectors.joining(", "));
            merge.append("WHEN MATCHED AND src.KFK_OP IN ('c', 'r', 'u')");
            if (config.isMergeSkipUnchanged()) {
                merge.append(" AND ").append(hashExpr("tgt")).append(" <> ").append(hashExpr("src"));
            }
            merge.append(" THEN UPDATE SET ").append(setClause).append("\n");
        }

        merge.append("WHEN NOT MATCHED AND src.KFK_OP IN ('c', 'r') THEN INSERT (").append(colList).append(")\n");
        merge.append("    VALUES (").append(srcColList).append(")\n");
        merge.append("WHEN MATCHED AND src.KFK_OP = 'd' THEN DELETE");

        return merge.toString();
    }
}
