package triggo.ai.connector.v1;

import triggo.ai.connector.v1.config.SnowflakeSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ponto de entrada do conector no Kafka Connect.
 * Registra o conector, valida as configs e distribui tasks.
 *
 * connector.class = br.com.triggoai.connector.v1.SnowflakeSinkConnector
 */
public class SnowflakeSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeSinkConnector.class);

    public static final String VERSION = "1.2.0";

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        log.info("Iniciando SnowflakeSinkConnector v{}", VERSION);
        // Valida todas as configs (lança ConfigException se algo estiver errado)
        new SnowflakeSinkConfig(props);
        this.configProps = new HashMap<>(props);
        log.info("SnowflakeSinkConnector configurado. table={}, mode={}",
                props.get(SnowflakeSinkConfig.SNOWFLAKE_TABLE),
                props.get(SnowflakeSinkConfig.INGESTION_MODE));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SnowflakeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(configProps));
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Parando SnowflakeSinkConnector.");
    }

    @Override
    public ConfigDef config() {
        return SnowflakeSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VERSION;
    }
}
