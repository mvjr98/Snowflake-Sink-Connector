package triggo.ai.connector.v1.snowflake;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import triggo.ai.connector.v1.model.ParsedRecord;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Grava registros diretamente na tabela _INGEST usando Snowpipe Streaming SDK.
 * Menor latência — sem Stage intermediário.
 *
 * Autenticação: private key RSA (requisito do SDK).
 *
 * O processamento da _INGEST para a tabela final é feito de forma assíncrona
 * pelo CleanupJob (via InlineProcessor.processAllPending).
 */
public class SnowpipeStreamingWriter implements IngestWriter {

    private static final Logger log = LoggerFactory.getLogger(SnowpipeStreamingWriter.class);

    private static final String CLIENT_NAME  = "snowflake-triggoai-sink";
    private static final String CHANNEL_NAME = "sink-channel-0";

    private final SnowflakeSinkConfig config;

    private SnowflakeStreamingIngestClient client;
    private SnowflakeStreamingIngestChannel channel;

    public SnowpipeStreamingWriter(SnowflakeSinkConfig config) {
        this.config = config;
    }

    @Override
    public void open() throws Exception {
        log.info("Abrindo Snowpipe Streaming client. table={}", config.getIngestTable());

        Properties props = SnowflakeConnectionHelper.buildIngestSdkProperties(config);

        client = SnowflakeStreamingIngestClientFactory
                .builder(CLIENT_NAME)
                .setProperties(props)
                .build();

        OpenChannelRequest request = OpenChannelRequest.builder(CHANNEL_NAME)
                .setDBName(config.getSnowflakeDatabase())
                .setSchemaName(config.getSnowflakeSchema())
                .setTableName(config.getIngestTable())
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                .build();

        channel = client.openChannel(request);
        log.info("Canal Snowpipe Streaming aberto: {}", CHANNEL_NAME);
    }

    @Override
    public void write(List<ParsedRecord> records) throws Exception {
        for (ParsedRecord record : records) {
            Map<String, Object> row = buildRow(record);

            // offsetToken garante idempotência (sem duplicatas em caso de retry)
            String offsetToken = record.getTopic() + "-" + record.getPartition() + "-" + record.getOffset();

            InsertValidationResponse response = channel.insertRow(row, offsetToken);

            if (response.hasErrors()) {
                for (InsertValidationResponse.InsertError error : response.getInsertErrors()) {
                    log.error("Erro Snowpipe Streaming: rowIndex={}, msg={}",
                            error.getRowIndex(), error.getException().getMessage());
                }
                throw new RuntimeException("Erros ao inserir via Snowpipe Streaming. Veja os logs para detalhes.");
            }
        }
        log.debug("Snowpipe Streaming: {} linhas inseridas em {}", records.size(), config.getIngestTable());
    }

    @Override
    public void close() throws Exception {
        if (channel != null) {
            try {
                channel.close().get();
                log.info("Canal Snowpipe Streaming fechado.");
            } catch (Exception e) {
                log.warn("Erro ao fechar canal: {}", e.getMessage());
            }
        }
        if (client != null) {
            try {
                client.close();
                log.info("Client Snowpipe Streaming fechado.");
            } catch (Exception e) {
                log.warn("Erro ao fechar client: {}", e.getMessage());
            }
        }
    }

    private Map<String, Object> buildRow(ParsedRecord record) {
        Map<String, Object> row = new HashMap<>();

        if (record.getFields() != null) {
            row.putAll(record.getFields());
        }

        row.put("IH_TOPIC",     record.getTopic());
        row.put("IH_PARTITION", record.getPartition());
        row.put("IH_OFFSET",    record.getOffset());
        row.put("IH_OP",        record.getOp());
        row.put("IH_DATETIME",  new Timestamp(record.getTsMs()));
        row.put("IH_BLOCKID",   record.getBlockId());

        return row;
    }
}
