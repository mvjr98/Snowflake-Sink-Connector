package triggo.ai.connector.v1.parser;

import triggo.ai.connector.v1.model.ParsedRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Parser que detecta automaticamente o formato do payload, sem necessidade
 * de configurar payload.format no conector.
 *
 * Regras de detecção:
 *
 *   Struct / GenericRecord (desserializado pelo AvroConverter):
 *     → tem campo "op"      → AVRO Debezium
 *     → não tem campo "op"  → Flat struct (sem envelope)
 *
 *   String / Map (JSON):
 *     → tem "schema" + "payload"           → Debezium JSON com envelope
 *     → tem "op" + ("before" ou "after")   → Debezium JSON sem envelope
 *     → nenhum dos acima                   → Flat JSON
 */
public class AutoParser implements PayloadParser {

    private static final Logger log = LoggerFactory.getLogger(AutoParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final FlatJsonParser     flatParser     = new FlatJsonParser();
    private final DebeziumJsonParser debeziumParser = new DebeziumJsonParser();
    private final AvroParser         avroParser     = new AvroParser();

    @Override
    public ParsedRecord parse(SinkRecord record) {
        Object value = record.value();

        if (value == null) {
            throw new RuntimeException("AutoParser: value é null. topic=" + record.topic()
                    + " partition=" + record.kafkaPartition() + " offset=" + record.kafkaOffset());
        }

        // --- Struct (AvroConverter) ---
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            if (hasField(struct, "op")) {
                log.trace("AutoParser → AVRO/Debezium (Struct com campo 'op')");
                return avroParser.parse(record);
            }
            log.trace("AutoParser → Flat Struct");
            return flatParser.parse(record);
        }

        // --- GenericRecord (Avro nativo) ---
        if (value instanceof GenericRecord) {
            GenericRecord generic = (GenericRecord) value;
            if (generic.getSchema().getField("op") != null) {
                log.trace("AutoParser → AVRO/Debezium (GenericRecord com campo 'op')");
                return avroParser.parse(record);
            }
            log.trace("AutoParser → Flat GenericRecord");
            return flatParser.parse(record);
        }

        // --- String ou Map → JSON ---
        if (value instanceof String || value instanceof Map) {
            JsonNode root = toJsonNode(value);

            // Desembrulha envelope com schema, se existir
            JsonNode payload = (root.has("schema") && root.has("payload"))
                    ? root.get("payload")
                    : root;

            if (payload.has("op") && (payload.has("before") || payload.has("after"))) {
                log.trace("AutoParser → Debezium JSON");
                return debeziumParser.parse(record);
            }

            log.trace("AutoParser → Flat JSON");
            return flatParser.parse(record);
        }

        throw new RuntimeException("AutoParser: tipo de value não suportado: "
                + value.getClass().getName()
                + ". Configure o value.converter correto no conector.");
    }

    private boolean hasField(Struct struct, String fieldName) {
        for (Field f : struct.schema().fields()) {
            if (f.name().equals(fieldName)) return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private JsonNode toJsonNode(Object value) {
        try {
            if (value instanceof String) {
                return MAPPER.readTree((String) value);
            }
            return MAPPER.valueToTree(value);
        } catch (Exception e) {
            throw new RuntimeException("AutoParser: falha ao parsear JSON: " + value, e);
        }
    }
}
