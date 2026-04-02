package triggo.ai.connector.v1.snowflake;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import triggo.ai.connector.v1.model.ParsedRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
    private final Map<String, String> ingestColumnTypes = new HashMap<>();

    private Connection connection;
    private SnowflakeConnection snowflakeConnection;
    private InlineProcessor inlineProcessor;

    public StageCopyWriter(SnowflakeSinkConfig config) {
        this.config    = config;
        this.stageName = config.getSnowflakeTable();
    }

    @Override
    public void open() throws Exception {
        log.info("Abrindo StageCopyWriter. table={}, stage={}", config.getIngestTable(), stageName);

        connection = SnowflakeConnectionHelper.createJdbcConnection(config);
        connection.setAutoCommit(true);

        // Unwrap para ter acesso ao uploadStream() da API Snowflake
        snowflakeConnection = connection.unwrap(SnowflakeConnection.class);

        inlineProcessor = new InlineProcessor(config);

        loadIngestColumnTypes();
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

        // 5. Cleanup da _INGEST (imediato por bloco ou com delay configurável)
        inlineProcessor.cleanupBlock(connection, blockId);
        inlineProcessor.cleanupExpiredRows(connection);

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
        List<String> selectParts = new ArrayList<>();
        for (String col : columns) {
            selectParts.add(buildSelectExpression(col));
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

    private void loadIngestColumnTypes() throws Exception {
        String db = config.getSnowflakeDatabase().toUpperCase(Locale.ROOT);
        String schema = config.getSnowflakeSchema().toUpperCase(Locale.ROOT);
        String table = config.getIngestTable().toUpperCase(Locale.ROOT);

        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(db, schema, table, null)) {
            while (rs.next()) {
                ingestColumnTypes.put(
                        rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT),
                        rs.getString("TYPE_NAME").toUpperCase(Locale.ROOT)
                );
            }
        }

        if (ingestColumnTypes.isEmpty()) {
            throw new RuntimeException("StageCopyWriter: nenhuma coluna encontrada em " + config.getIngestTable()
                    + ". Verifique se a tabela _INGEST existe e se as credenciais têm acesso.");
        }
    }

    private String buildSelectExpression(String col) {
        String jsonRef = "$1:" + col;
        String typeName = ingestColumnTypes.getOrDefault(col.toUpperCase(Locale.ROOT), "VARCHAR");

        if (isNumberType(typeName)) {
            return "TRY_TO_NUMBER(" + jsonRef + "::STRING)";
        }
        if (isFloatType(typeName)) {
            return "TRY_TO_DOUBLE(" + jsonRef + "::STRING)";
        }
        if (isBooleanType(typeName)) {
            return "TRY_TO_BOOLEAN(" + jsonRef + "::STRING)";
        }
        if (isDateType(typeName)) {
            return "CASE "
                    + "WHEN TRY_TO_NUMBER(" + jsonRef + "::STRING) IS NULL THEN TRY_TO_DATE(" + jsonRef + "::STRING) "
                    + "WHEN ABS(TRY_TO_NUMBER(" + jsonRef + "::STRING)) > 1000000 "
                    + "THEN TO_DATE(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(" + jsonRef + "::STRING) / 1000)) "
                    + "ELSE DATEADD('day', TRY_TO_NUMBER(" + jsonRef + "::STRING), '1970-01-01'::DATE) END";
        }
        if (isTimestampType(typeName)) {
            return "TRY_TO_TIMESTAMP_NTZ(" + jsonRef + "::STRING)";
        }
        if (isTimeType(typeName)) {
            return "TRY_TO_TIME(" + jsonRef + "::STRING)";
        }
        if (isBinaryType(typeName)) {
            return "TRY_TO_BINARY(" + jsonRef + "::STRING)";
        }
        if (isVariantType(typeName)) {
            return jsonRef;
        }

        return jsonRef + "::STRING";
    }

    private boolean isNumberType(String typeName) {
        return typeName.contains("NUMBER")
                || typeName.contains("DECIMAL")
                || typeName.contains("NUMERIC")
                || typeName.contains("INT")
                || typeName.contains("BYTEINT");
    }

    private boolean isFloatType(String typeName) {
        return typeName.contains("FLOAT") || typeName.contains("DOUBLE") || typeName.contains("REAL");
    }

    private boolean isBooleanType(String typeName) {
        return typeName.contains("BOOLEAN");
    }

    private boolean isDateType(String typeName) {
        return "DATE".equals(typeName);
    }

    private boolean isTimestampType(String typeName) {
        return typeName.contains("TIMESTAMP");
    }

    private boolean isTimeType(String typeName) {
        return typeName.equals("TIME");
    }

    private boolean isBinaryType(String typeName) {
        return typeName.contains("BINARY");
    }

    private boolean isVariantType(String typeName) {
        return typeName.contains("VARIANT") || typeName.contains("OBJECT") || typeName.contains("ARRAY");
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

        row.put("KFK_TOPIC",     record.getTopic());
        row.put("KFK_PARTITION", record.getPartition());
        row.put("KFK_OFFSET",    record.getOffset());
        row.put("KFK_OP",        record.getOp());
        row.put("KFK_DATETIME",  Instant.ofEpochMilli(record.getTsMs()).toString());
        row.put("KFK_BLOCKID",   record.getBlockId());

        return row;
    }

    private List<String> buildColumnList(ParsedRecord record) {
        List<String> columns = new ArrayList<>();
        if (record.getFields() != null) {
            columns.addAll(record.getFields().keySet());
        }
        columns.add("KFK_TOPIC");
        columns.add("KFK_PARTITION");
        columns.add("KFK_OFFSET");
        columns.add("KFK_OP");
        columns.add("KFK_DATETIME");
        columns.add("KFK_BLOCKID");
        return columns;
    }
}
