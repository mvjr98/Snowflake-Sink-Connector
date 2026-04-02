package triggo.ai.connector.v1.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Definição e validação de todas as configurações do conector.
 */
public class SnowflakeSinkConfig extends AbstractConfig {

    // -------------------------------------------------------------------------
    // Nomes das configs
    // -------------------------------------------------------------------------

    public static final String SNOWFLAKE_URL         = "snowflake.url";
    public static final String SNOWFLAKE_USER        = "snowflake.user";
    /** Chave privada RSA (PEM). Usada tanto para JDBC quanto para o Snowpipe Streaming SDK. */
    public static final String SNOWFLAKE_PRIVATE_KEY = "snowflake.private.key";
    public static final String SNOWFLAKE_DATABASE    = "snowflake.database";
    public static final String SNOWFLAKE_SCHEMA      = "snowflake.schema";
    public static final String SNOWFLAKE_TABLE       = "snowflake.table";

    public static final String INGESTION_MODE        = "ingestion.mode";

    /**
     * Campos que compõem a PK, separados por vírgula.
     * Necessário quando o payload for Flat JSON (sem envelope Debezium),
     * pois a PK não pode ser inferida do payload.
     * Para Debezium JSON e AVRO a PK é descoberta via DatabaseMetaData.
     */
    public static final String PK_FIELDS             = "pk.fields";

    /**
     * Intervalo em segundos entre execuções do processamento _INGEST → final.
     * Usado apenas no modo SNOWPIPE_STREAMING (STAGE processa de forma síncrona).
     */
    public static final String JOB_INTERVAL_SECONDS  = "job.interval.seconds";

    public static final String MERGE_BATCH_SIZE      = "merge.batch.size";

    /**
     * Delay em segundos para remover registros da _INGEST após processamento.
     * 0 = cleanup imediato (padrão).
     */
    public static final String INGEST_CLEANUP_DELAY_SECONDS = "ingest.cleanup.delay.seconds";

    // --- Buffer (inspirado no Snowflake Kafka Connector oficial) ---

    /** Número de registros acumulados em memória antes de enviar ao Snowflake. */
    public static final String BUFFER_COUNT_RECORDS  = "buffer.count.records";

    /** Segundos entre flushes periódicos do buffer (mesmo que os outros limites não tenham sido atingidos). */
    public static final String BUFFER_FLUSH_TIME     = "buffer.flush.time";

    /** Tamanho acumulado em bytes dos registros em memória antes de enviar ao Snowflake. */
    public static final String BUFFER_SIZE_BYTES     = "buffer.size.bytes";

    // -------------------------------------------------------------------------
    // Enums
    // -------------------------------------------------------------------------

    public enum IngestionMode { SNOWPIPE_STREAMING, STAGE }

    // -------------------------------------------------------------------------
    // Definição do ConfigDef
    // -------------------------------------------------------------------------

    public static final ConfigDef CONFIG_DEF = new ConfigDef()

            // --- Snowflake Connection ---
            .define(SNOWFLAKE_URL,
                    Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "URL da conta Snowflake. Ex: https://account.snowflakecomputing.com")

            .define(SNOWFLAKE_USER,
                    Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "Usuário do Snowflake.")

            .define(SNOWFLAKE_PRIVATE_KEY,
                    Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "Chave privada RSA (PKCS8 PEM). " +
                    "Usada para autenticação JDBC e Snowpipe Streaming SDK.")

            .define(SNOWFLAKE_DATABASE,
                    Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "Nome do banco de dados no Snowflake.")

            .define(SNOWFLAKE_SCHEMA,
                    Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "Nome do schema no Snowflake.")

            .define(SNOWFLAKE_TABLE,
                    Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "Nome da tabela target. A tabela de staging será <TABLE>_INGEST.")

            // --- Modo de ingestão ---
            .define(INGESTION_MODE,
                    Type.STRING, "STAGE",
                    ConfigDef.ValidString.in("SNOWPIPE_STREAMING", "STAGE", "STAGE_COPY"),
                    Importance.MEDIUM,
                    "Modo de ingestão. STAGE (padrão) ou SNOWPIPE_STREAMING. " +
                    "STAGE_COPY é aceito apenas por retrocompatibilidade.")

            // --- PK ---
            .define(PK_FIELDS,
                    Type.STRING, "",
                    Importance.MEDIUM,
                    "Campos que compõem a PK, separados por vírgula. " +
                    "Necessário para payloads Flat JSON. " +
                    "Para Debezium/AVRO, a PK é descoberta automaticamente via DatabaseMetaData.")

            // --- Processamento assíncrono (apenas SNOWPIPE_STREAMING) ---
            .define(JOB_INTERVAL_SECONDS,
                    Type.INT, 30,
                    ConfigDef.Range.atLeast(5),
                    Importance.MEDIUM,
                    "Intervalo em segundos entre execuções do processamento _INGEST → final. " +
                    "Usado apenas no modo SNOWPIPE_STREAMING. Padrão: 30s.")

            .define(MERGE_BATCH_SIZE,
                    Type.INT, 10000,
                    ConfigDef.Range.atLeast(1),
                    Importance.LOW,
                    "Número máximo de linhas processadas por execução (SNOWPIPE_STREAMING).")

            .define(INGEST_CLEANUP_DELAY_SECONDS,
                    Type.INT, 0,
                    ConfigDef.Range.atLeast(0),
                    Importance.LOW,
                    "Delay em segundos para cleanup da _INGEST após processamento. " +
                    "0 = imediato (padrão).")

            // --- Buffer ---
            .define(BUFFER_COUNT_RECORDS,
                    Type.INT, 10000,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM,
                    "Número de registros acumulados em memória antes de enviar ao Snowflake. Padrão: 10000.")

            .define(BUFFER_FLUSH_TIME,
                    Type.INT, 120,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM,
                    "Segundos entre flushes periódicos do buffer, independente dos outros limites. Padrão: 120s.")

            .define(BUFFER_SIZE_BYTES,
                    Type.LONG, 5000000L,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM,
                    "Tamanho acumulado em bytes dos registros em memória antes de enviar. Padrão: 5 MB.");

    // -------------------------------------------------------------------------
    // Construtor
    // -------------------------------------------------------------------------

    public SnowflakeSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        validate();
    }

    // -------------------------------------------------------------------------
    // Validações
    // -------------------------------------------------------------------------

    private void validate() {
        if (getSnowflakePrivateKey().isBlank()) {
            throw new ConfigException(SNOWFLAKE_PRIVATE_KEY + " é obrigatório.");
        }
    }

    // -------------------------------------------------------------------------
    // Acessors tipados
    // -------------------------------------------------------------------------

    public String getSnowflakeUrl()        { return getString(SNOWFLAKE_URL); }
    public String getSnowflakeUser()       { return getString(SNOWFLAKE_USER); }
    public String getSnowflakePrivateKey() { return getPassword(SNOWFLAKE_PRIVATE_KEY).value(); }
    public String getSnowflakeDatabase()   { return getString(SNOWFLAKE_DATABASE); }
    public String getSnowflakeSchema()     { return getString(SNOWFLAKE_SCHEMA); }
    public String getSnowflakeTable()      { return getString(SNOWFLAKE_TABLE); }

    /** Nome da tabela de staging: <TABLE>_INGEST */
    public String getIngestTable()         { return getSnowflakeTable() + "_INGEST"; }

    public IngestionMode getIngestionMode() {
        String raw = getString(INGESTION_MODE);
        if ("STAGE_COPY".equalsIgnoreCase(raw)) {
            return IngestionMode.STAGE;
        }
        return IngestionMode.valueOf(raw.toUpperCase());
    }

    public List<String> getPkFields() {
        String raw = getString(PK_FIELDS).trim();
        if (raw.isBlank()) return List.of();
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public int  getJobIntervalSeconds() { return getInt(JOB_INTERVAL_SECONDS); }
    public int  getMergeBatchSize()     { return getInt(MERGE_BATCH_SIZE); }
    public int  getIngestCleanupDelaySeconds() { return getInt(INGEST_CLEANUP_DELAY_SECONDS); }

    public int  getBufferCountRecords() { return getInt(BUFFER_COUNT_RECORDS); }
    public int  getBufferFlushTime()    { return getInt(BUFFER_FLUSH_TIME); }
    public long getBufferSizeBytes()    { return getLong(BUFFER_SIZE_BYTES); }
}
