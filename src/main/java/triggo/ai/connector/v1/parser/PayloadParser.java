package triggo.ai.connector.v1.parser;

import triggo.ai.connector.v1.model.ParsedRecord;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Estratégia de parsing de payload.
 * Cada implementação corresponde a um formato suportado:
 * FLAT_JSON, DEBEZIUM_JSON, AVRO.
 */
public interface PayloadParser {

    /**
     * Converte um SinkRecord do Kafka Connect em um ParsedRecord normalizado.
     *
     * @param record registro recebido do Kafka Connect
     * @return ParsedRecord com campos de negócio e metadados de operação
     */
    ParsedRecord parse(SinkRecord record);
}
