package triggo.ai.connector.v1.snowflake;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.Properties;

/**
 * Utilitário para criar conexões com o Snowflake.
 * Usa exclusivamente autenticação por chave privada RSA (key-pair authentication),
 * tanto para conexões JDBC quanto para o Snowpipe Streaming SDK.
 */
public class SnowflakeConnectionHelper {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeConnectionHelper.class);

    private SnowflakeConnectionHelper() {}

    // -------------------------------------------------------------------------
    // JDBC — private key RSA (STAGE_COPY + InlineProcessor + processamento assíncrono)
    // -------------------------------------------------------------------------

    /**
     * Cria uma conexão JDBC usando key-pair authentication (private key RSA).
     * Usada pelo StageCopyWriter, InlineProcessor e pelo executor assíncrono do SNOWPIPE_STREAMING.
     */
    public static Connection createJdbcConnection(SnowflakeSinkConfig config) throws Exception {
        PrivateKey privateKey = parsePrivateKey(config.getSnowflakePrivateKey());

        Properties props = new Properties();
        props.put("user",       config.getSnowflakeUser());
        props.put("privateKey", privateKey);

        String url = buildJdbcUrl(config.getSnowflakeUrl());
        log.debug("Conectando ao Snowflake via JDBC (key-pair): url={}, user={}, db={}, schema={}",
                url, config.getSnowflakeUser(), config.getSnowflakeDatabase(), config.getSnowflakeSchema());

        Connection conn = DriverManager.getConnection(url, props);

        try (var stmt = conn.createStatement()) {
            stmt.execute("USE DATABASE " + config.getSnowflakeDatabase());
            stmt.execute("USE SCHEMA " + config.getSnowflakeSchema());
        }

        return conn;
    }

    /**
     * Monta a URL JDBC a partir da URL de conta do Snowflake.
     *   https://account.snowflakecomputing.com  → jdbc:snowflake://account.snowflakecomputing.com/
     *   account.snowflakecomputing.com           → jdbc:snowflake://account.snowflakecomputing.com/
     */
    public static String buildJdbcUrl(String snowflakeUrl) {
        String host = snowflakeUrl
                .replaceFirst("^https?://", "")
                .replaceAll("/$", "");
        return "jdbc:snowflake://" + host + "/";
    }

    // -------------------------------------------------------------------------
    // Snowpipe Streaming SDK — private key RSA
    // -------------------------------------------------------------------------

    /**
     * Monta o Properties para o Snowflake Ingest SDK (Snowpipe Streaming).
     */
    public static Properties buildIngestSdkProperties(SnowflakeSinkConfig config) {
        Properties props = new Properties();
        props.put("url",         config.getSnowflakeUrl()
                .replaceFirst("^https?://", "")
                .replaceAll("/$", ""));
        props.put("user",        config.getSnowflakeUser());
        props.put("private_key", config.getSnowflakePrivateKey());
        props.put("scheme",      "https");
        props.put("port",        "443");
        return props;
    }

    // -------------------------------------------------------------------------
    // Parsing da chave privada RSA
    // -------------------------------------------------------------------------

    /**
     * Parseia uma chave privada RSA no formato PKCS#8 PEM.
     * Aceita com ou sem o cabeçalho "-----BEGIN PRIVATE KEY-----".
     */
    public static PrivateKey parsePrivateKey(String pemKey) throws Exception {
        String cleanKey = pemKey
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                .replace("-----END RSA PRIVATE KEY-----", "")
                .replaceAll("\\s+", "");

        byte[] keyBytes = Base64.getDecoder().decode(cleanKey);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
    }
}
