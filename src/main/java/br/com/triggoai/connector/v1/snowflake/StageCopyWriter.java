package br.com.triggoai.connector.v1.snowflake;

import br.com.triggoai.connector.v1.config.SnowflakeSinkConfig;
import br.com.triggoai.connector.v1.model.ParsedRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Grava registros na tabela _INGEST via Stage intermediário.
 *
 * Fluxo por batch (mesmo do conector de referência):
 *   1. Serializa registros como JSON Lines em memória
 *   2. uploadStream() para o Stage (compressão gzip automática)
 *   3. COPY INTO _INGEST  PURGE = TRUE
 *   4. InlineProcessor.processBlock() — INSERT/UPDATE/DELETE na tabela final
 *
 * O processamento da _INGEST para a tabela final é síncrono (dentro do flush),
 * portanto o CleanupJob neste modo apenas remove registros antigos da _INGEST.
 */
public class StageCopyWriter implements IngestWriter {

    private static final Logger log = LoggerFactory.getLogger(StageCopyWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SnowflakeSinkConfig config;
    private final String stageName;

    private Connection connection;
    private SnowflakeConnection snowflakeConnection;
    private InlineProcessor inlineProcessor;

    public StageCopyWriter(SnowflakeSinkConfig config) {
        this.config    = config;
        this.stageName = "STAGE_" + config.getSnowflakeTable() + "_SINK";
    }

    @Override
    public void open() throws Exception {
        log.info("Abrindo StageCopyWriter. table={}, stage={}", config.getIngestTable(), stageName);

        connection = SnowflakeConnectionHelper.createJdbcConnection(config);
        connection.setAutoCommit(true);

        // Unwrap para ter acesso ao uploadStream() da API Snowflake
        snowflakeConnection = connection.unwrap(SnowflakeConnection.class);

        inlineProcessor = new InlineProcessor(config);

        ensureStageExists();
        log.info("StageCopyWriter pronto.");
    }

    @Override
    public void write(List<ParsedRecord> records) throws Exception {
        if (records.isEmpty()) return;

        String blockId    = records.get(0).getBlockId();
        String destFile   = "sink_" + blockId.replace("-", "") + "_" + System.currentTimeMillis();

        // 1. Serializa para JSON Lines em memória
        String jsonLines  = buildJsonLines(records);
        byte[] bytes      = jsonLines.getBytes(StandardCharsets.UTF_8);

        log.debug("StageCopyWriter: {} registros, blockId={}, arquivo={}", records.size(), blockId, destFile);

        // 2. Upload para o Stage (compress=true → gzip automático)
        try (InputStream is = new ByteArrayInputStream(bytes)) {
            snowflakeConnection.uploadStream(stageName, "/", is, destFile, true);
        }

        // 3. COPY INTO _INGEST
        List<String> columns = buildColumnList(records.get(0));
        copyIntoIngest(destFile + ".gz", columns);

        // 4. INSERT / UPDATE / DELETE da _INGEST para a tabela final
        inlineProcessor.processBlock(connection, blockId);

        // 5. Remove o blockId da _INGEST (cleanup imediato, sem cron)
        inlineProcessor.cleanupBlock(connection, blockId);

        log.info("StageCopyWriter: batch processado — {} registros, blockId={}", records.size(), blockId);
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            log.info("Conexão JDBC StageCopyWriter fechada.");
        }
    }

    // -------------------------------------------------------------------------
    // Stage
    // -------------------------------------------------------------------------

    private void ensureStageExists() throws Exception {
        String sql = "CREATE STAGE IF NOT EXISTS " + stageName
                + " FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE NULL_IF = (''))";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.debug("Stage garantido: {}", stageName);
        }
    }

    // -------------------------------------------------------------------------
    // COPY INTO
    // -------------------------------------------------------------------------

    private void copyIntoIngest(String fileName, List<String> columns) throws Exception {
        // Mapeia cada coluna para o campo correspondente no JSON ($1:COLNAME)
        List<String> selectParts = new ArrayList<>();
        for (String col : columns) {
            if (col.equals("IH_PARTITION") || col.equals("IH_OFFSET")) {
                selectParts.add("$1:" + col + "::INT");
            } else if (col.equals("IH_DATETIME")) {
                selectParts.add("$1:" + col + "::TIMESTAMP_NTZ");
            } else {
                selectParts.add("$1:" + col + "::STRING");
            }
        }

        String colDef    = String.join(", ", columns);
        String selectDef = String.join(", ", selectParts);

        String sql = String.format(
            "COPY INTO %s (%s) FROM (SELECT %s FROM @%s/%s) " +
            "FILE_FORMAT = (TYPE = 'JSON') PURGE = TRUE",
            config.getIngestTable(), colDef, selectDef, stageName, fileName);

        log.debug("COPY INTO: {}", sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    // -------------------------------------------------------------------------
    // Montagem dos dados
    // -------------------------------------------------------------------------

    private String buildJsonLines(List<ParsedRecord> records) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (ParsedRecord record : records) {
            Map<String, Object> row = buildRow(record);
            sb.append(MAPPER.writeValueAsString(row)).append("\n");
        }
        return sb.toString();
    }

    private Map<String, Object> buildRow(ParsedRecord record) {
        Map<String, Object> row = new LinkedHashMap<>();

        if (record.getFields() != null) {
            row.putAll(record.getFields());
        }

        row.put("IH_TOPIC",     record.getTopic());
        row.put("IH_PARTITION", record.getPartition());
        row.put("IH_OFFSET",    record.getOffset());
        row.put("IH_OP",        record.getOp());
        row.put("IH_DATETIME",  Instant.ofEpochMilli(record.getTsMs()).toString());
        row.put("IH_BLOCKID",   record.getBlockId());

        return row;
    }

    private List<String> buildColumnList(ParsedRecord record) {
        List<String> columns = new ArrayList<>();
        if (record.getFields() != null) {
            columns.addAll(record.getFields().keySet());
        }
        columns.add("IH_TOPIC");
        columns.add("IH_PARTITION");
        columns.add("IH_OFFSET");
        columns.add("IH_OP");
        columns.add("IH_DATETIME");
        columns.add("IH_BLOCKID");
        return columns;
    }
}
