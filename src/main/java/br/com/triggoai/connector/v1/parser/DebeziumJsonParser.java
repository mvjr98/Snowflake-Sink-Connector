package br.com.triggoai.connector.v1.parser;

import br.com.triggoai.connector.v1.model.ParsedRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parser para payloads no formato Debezium JSON.
 * Suporta dois sub-formatos:
 *
 * 1. Sem schema (valor direto):
 *    { "before": null, "after": {...}, "op": "c", "ts_ms": ... }
 *
 * 2. Com schema (envelope completo):
 *    { "schema": {...}, "payload": { "before": null, "after": {...}, "op": "c", ... } }
 *
 * A PK é extraída da Kafka message key (JSON ou Struct).
 */
public class DebeziumJsonParser implements PayloadParser {

    private static final Logger log = LoggerFactory.getLogger(DebeziumJsonParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public ParsedRecord parse(SinkRecord record) {
        JsonNode payload = extractPayload(record.value());

        String op   = payload.path("op").asText("c");
        long   tsMs = payload.path("ts_ms").asLong(System.currentTimeMillis());

        Map<String, Object> fields = extractBusinessFields(payload, op);

        ParsedRecord parsed = new ParsedRecord();
        parsed.setFields(fields);
        parsed.setOp(op);
        parsed.setTopic(record.topic());
        parsed.setPartition(record.kafkaPartition());
        parsed.setOffset(record.kafkaOffset());
        parsed.setTsMs(tsMs);

        return parsed;
    }

    /**
     * Extrai o nó de payload, desembrulhando o envelope com schema se necessário.
     */
    private JsonNode extractPayload(Object value) {
        JsonNode root = toJsonNode(value);

        // Formato com envelope: { "schema": {...}, "payload": {...} }
        if (root.has("schema") && root.has("payload")) {
            return root.get("payload");
        }

        // Formato sem envelope: { "before": ..., "after": ..., "op": ... }
        return root;
    }

    private JsonNode toJsonNode(Object value) {
        if (value == null) {
            throw new RuntimeException("DebeziumJsonParser: value é null");
        }

        if (value instanceof String) {
            try {
                return MAPPER.readTree((String) value);
            } catch (Exception e) {
                throw new RuntimeException("DebeziumJsonParser: falha ao parsear JSON: " + value, e);
            }
        }

        if (value instanceof Struct) {
            Map<String, Object> map = FlatJsonParser.structToMap((Struct) value);
            return MAPPER.valueToTree(map);
        }

        if (value instanceof Map) {
            return MAPPER.valueToTree(value);
        }

        throw new RuntimeException("DebeziumJsonParser: tipo de value não suportado: " + value.getClass().getName());
    }

    /**
     * Extrai os campos de negócio do nó after (ou before para deletes).
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractBusinessFields(JsonNode payload, String op) {
        JsonNode dataNode = "d".equals(op) ? payload.path("before") : payload.path("after");

        if (dataNode.isMissingNode() || dataNode.isNull()) {
            log.warn("DebeziumJsonParser: nó de dados está vazio para op='{}'. Payload: {}", op, payload);
            return new LinkedHashMap<>();
        }

        try {
            return MAPPER.convertValue(dataNode, LinkedHashMap.class);
        } catch (Exception e) {
            throw new RuntimeException("DebeziumJsonParser: falha ao converter campos de negócio", e);
        }
    }
}
