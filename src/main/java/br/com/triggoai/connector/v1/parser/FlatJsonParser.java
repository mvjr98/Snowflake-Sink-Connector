package br.com.triggoai.connector.v1.parser;

import br.com.triggoai.connector.v1.model.ParsedRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parser para payloads JSON simples sem envelope Debezium.
 * Exemplo: { "IDT_UNC_TSC": 123, "NOME": "foo" }
 *
 * - A operação é sempre "c" (insert/upsert)
 * - A PK deve ser configurada manualmente via pk.fields
 */
public class FlatJsonParser implements PayloadParser {

    private static final Logger log = LoggerFactory.getLogger(FlatJsonParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public ParsedRecord parse(SinkRecord record) {
        Map<String, Object> fields = extractFields(record.value());

        ParsedRecord parsed = new ParsedRecord();
        parsed.setFields(fields);
        parsed.setOp("c");
        parsed.setTopic(record.topic());
        parsed.setPartition(record.kafkaPartition());
        parsed.setOffset(record.kafkaOffset());
        parsed.setTsMs(System.currentTimeMillis());

        return parsed;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractFields(Object value) {
        if (value == null) {
            return new LinkedHashMap<>();
        }

        if (value instanceof String) {
            try {
                return MAPPER.readValue((String) value, LinkedHashMap.class);
            } catch (Exception e) {
                throw new RuntimeException("FlatJsonParser: falha ao desserializar JSON: " + value, e);
            }
        }

        if (value instanceof Map) {
            return new LinkedHashMap<>((Map<String, Object>) value);
        }

        if (value instanceof Struct) {
            return structToMap((Struct) value);
        }

        throw new RuntimeException("FlatJsonParser: tipo de value não suportado: " + value.getClass().getName());
    }

    public static Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (struct == null) return map;
        for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
            Object val = struct.get(field);
            if (val instanceof Struct) {
                map.put(field.name(), structToMap((Struct) val));
            } else {
                map.put(field.name(), val);
            }
        }
        return map;
    }
}
