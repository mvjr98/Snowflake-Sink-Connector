package triggo.ai.connector.v1;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import triggo.ai.connector.v1.model.ParsedRecord;
import triggo.ai.connector.v1.parser.AutoParser;
import triggo.ai.connector.v1.parser.PayloadParser;
import triggo.ai.connector.v1.snowflake.IngestWriter;
import triggo.ai.connector.v1.snowflake.InlineProcessor;
import triggo.ai.connector.v1.snowflake.SnowflakeConnectionHelper;
import triggo.ai.connector.v1.snowflake.SnowpipeStreamingWriter;
import triggo.ai.connector.v1.snowflake.StageCopyWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Task principal do conector. Uma instância por task configurada (tasks.max).
 *
 * Fluxo com buffer (inspirado no Snowflake Kafka Connector oficial):
 *
 *   put() → acumula registros no buffer em memória
 *   flush() (chamado pelo Kafka Connect antes de commitar offsets) → força o flush do buffer
 *
 * O buffer é descarregado (write() no IngestWriter) quando QUALQUER condição é atingida:
 *   - buffer.count.records  → número de registros acumulados
 *   - buffer.size.bytes     → tamanho total estimado em bytes
 *   - buffer.flush.time     → segundos desde o último flush
 *
 * Para SNOWPIPE_STREAMING, um executor assíncrono também processa a _INGEST → tabela final
 * no intervalo configurado em job.interval.seconds.
 */
public class SnowflakeSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeSinkTask.class);

    private SnowflakeSinkConfig      config;
    private PayloadParser            parser;
    private IngestWriter             writer;
    private ScheduledExecutorService processingExecutor; // só para SNOWPIPE_STREAMING

    // --- Buffer ---
    private final List<ParsedRecord> buffer         = new ArrayList<>();
    private long                     bufferBytes     = 0;
    private long                     lastFlushTimeMs = System.currentTimeMillis();

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Iniciando SnowflakeSinkTask");
        config = new SnowflakeSinkConfig(props);
        parser = buildParser();
        writer = buildWriter();

        try {
            writer.open();
        } catch (Exception e) {
            throw new RuntimeException("Falha ao abrir o writer Snowflake", e);
        }

        // Apenas SNOWPIPE_STREAMING precisa de processamento assíncrono.
        // STAGE processa de forma síncrona dentro de write().
        if (config.getIngestionMode() == SnowflakeSinkConfig.IngestionMode.SNOWPIPE_STREAMING) {
            startAsyncProcessor();
        }

        lastFlushTimeMs = System.currentTimeMillis();

        log.info("SnowflakeSinkTask iniciada. table={}, mode={}, " +
                 "buffer.count={}, buffer.flush.time={}s, buffer.size.bytes={}, ingest.cleanup.delay={}s",
                config.getIngestTable(), config.getIngestionMode(),
                config.getBufferCountRecords(), config.getBufferFlushTime(), config.getBufferSizeBytes(),
                config.getIngestCleanupDelaySeconds());
    }

    /**
     * Recebe registros do Kafka Connect, parseia e acumula no buffer.
     * Faz flush automaticamente se qualquer limite de buffer for atingido.
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;

        for (SinkRecord record : records) {
            try {
                ParsedRecord parsed = parser.parse(record);
                buffer.add(parsed);
                bufferBytes += estimateBytes(parsed);
            } catch (Exception e) {
                log.error("Erro ao parsear record topic={} partition={} offset={}: {}",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage(), e);
                throw new RuntimeException("Falha no parsing do record", e);
            }
        }

        log.debug("Buffer: {} registros / {} bytes após put({})",
                buffer.size(), bufferBytes, records.size());

        if (shouldFlush()) {
            flushBuffer();
        }
    }

    /**
     * Chamado pelo Kafka Connect antes de commitar os offsets.
     * Garante que todos os registros em buffer sejam gravados no Snowflake.
     */
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (!buffer.isEmpty()) {
            log.debug("flush() chamado pelo Kafka Connect. Forçando flush de {} registros.", buffer.size());
            flushBuffer();
        }
    }

    @Override
    public void stop() {
        log.info("Parando SnowflakeSinkTask. Flushing {} registros restantes no buffer.", buffer.size());
        if (!buffer.isEmpty()) {
            flushBuffer();
        }

        if (processingExecutor != null) {
            processingExecutor.shutdown();
            try {
                processingExecutor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                log.warn("Erro ao fechar writer: {}", e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    private PayloadParser buildParser() {
        return new AutoParser();
    }

    private IngestWriter buildWriter() {
        return switch (config.getIngestionMode()) {
            case SNOWPIPE_STREAMING -> new SnowpipeStreamingWriter(config);
            case STAGE              -> new StageCopyWriter(config);
        };
    }

    // -------------------------------------------------------------------------
    // Buffer
    // -------------------------------------------------------------------------

    /**
     * Retorna true se qualquer condição de flush for atingida:
     *   1. buffer.count.records atingido
     *   2. buffer.size.bytes atingido
     *   3. buffer.flush.time segundos desde o último flush
     */
    private boolean shouldFlush() {
        if (buffer.size() >= config.getBufferCountRecords()) {
            log.debug("Flush por buffer.count.records ({} >= {})", buffer.size(), config.getBufferCountRecords());
            return true;
        }
        if (bufferBytes >= config.getBufferSizeBytes()) {
            log.debug("Flush por buffer.size.bytes ({} >= {})", bufferBytes, config.getBufferSizeBytes());
            return true;
        }
        long elapsedMs = System.currentTimeMillis() - lastFlushTimeMs;
        if (elapsedMs >= config.getBufferFlushTime() * 1000L) {
            log.debug("Flush por buffer.flush.time ({}ms >= {}s)", elapsedMs, config.getBufferFlushTime());
            return true;
        }
        return false;
    }

    /**
     * Grava o conteúdo do buffer no Snowflake e reinicia os contadores.
     * Um novo blockId é gerado por flush (todos os registros deste flush compartilham o mesmo blockId).
     */
    private void flushBuffer() {
        if (buffer.isEmpty()) return;

        String blockId = UUID.randomUUID().toString();
        buffer.forEach(r -> r.setBlockId(blockId));

        try {
            writer.write(new ArrayList<>(buffer));
            log.info("Buffer flushed: {} registros ({} bytes), blockId={}", buffer.size(), bufferBytes, blockId);
        } catch (Exception e) {
            log.error("Erro ao gravar buffer no Snowflake: {}", e.getMessage(), e);
            throw new RuntimeException("Falha ao gravar buffer no Snowflake", e);
        } finally {
            buffer.clear();
            bufferBytes = 0;
            lastFlushTimeMs = System.currentTimeMillis();
        }
    }

    /**
     * Estima o tamanho em bytes de um registro a partir dos seus campos de negócio.
     * Soma o comprimento dos nomes e valores das colunas + overhead fixo para metadados KFK_.
     */
    private long estimateBytes(ParsedRecord record) {
        long size = 100; // overhead fixo (KFK_TOPIC, KFK_PARTITION, KFK_OFFSET, KFK_OP, KFK_DATETIME, KFK_BLOCKID)
        if (record.getFields() != null) {
            for (Map.Entry<String, Object> entry : record.getFields().entrySet()) {
                size += entry.getKey().length();
                if (entry.getValue() != null) {
                    size += entry.getValue().toString().length();
                }
            }
        }
        return size;
    }

    // -------------------------------------------------------------------------
    // Processamento assíncrono — apenas SNOWPIPE_STREAMING
    // -------------------------------------------------------------------------

    /**
     * Inicia um ScheduledExecutorService que periodicamente processa os registros
     * pendentes da _INGEST e os aplica na tabela final.
     * Intervalo configurado em job.interval.seconds (padrão: 30s).
     */
    private void startAsyncProcessor() {
        InlineProcessor inlineProcessor = new InlineProcessor(config);
        int intervalSeconds = config.getJobIntervalSeconds();

        processingExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "snowflake-sink-processor-" + config.getSnowflakeTable());
            t.setDaemon(true);
            return t;
        });

        processingExecutor.scheduleWithFixedDelay(() -> {
            try (Connection conn = SnowflakeConnectionHelper.createJdbcConnection(config)) {
                inlineProcessor.processAllPending(conn);
            } catch (Exception e) {
                log.error("Erro no processamento assíncrono (_INGEST → final): {}", e.getMessage(), e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        log.info("Processamento assíncrono iniciado. table={}, intervalo={}s",
                config.getSnowflakeTable(), intervalSeconds);
    }
}
