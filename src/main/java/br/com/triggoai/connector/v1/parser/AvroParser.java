package br.com.triggoai.connector.v1.parser;

import br.com.triggoai.connector.v1.model.ParsedRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parser para payloads AVRO (Debezium via Confluent Schema Registry).
 *
 * Pré-requisito: configurar value.converter=io.confluent.connect.avro.AvroConverter
 * no conector, o que faz o Kafka Connect desserializar o AVRO automaticamente
 * para um objeto Struct antes de chamar put().
 *
 * Estrutura esperada após desserialização:
 *   Struct{ before=null, after=Struct{...campos...}, op="c", ts_ms=... }
 */
public class AvroParser implements PayloadParser {

    private static final Logger log = LoggerFactory.getLogger(AvroParser.class);

    @Override
    public ParsedRecord parse(SinkRecord record) {
        Object value = record.value();

        if (value == null) {
            throw new RuntimeException("AvroParser: value é null para topic=" + record.topic()
                    + " partition=" + record.kafkaPartition() + " offset=" + record.kafkaOffset());
        }

        String op;
        long tsMs;
        Map<String, Object> fields;

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            op     = getStringField(struct, "op", "c");
            tsMs   = getLongField(struct, "ts_ms", System.currentTimeMillis());
            fields = extractBusinessFields(struct, op);
        } else if (value instanceof GenericRecord) {
            GenericRecord generic = (GenericRecord) value;
            op     = getStringField(generic, "op", "c");
            tsMs   = getLongField(generic, "ts_ms", System.currentTimeMillis());
            fields = extractBusinessFields(generic, op);
        } else {
            throw new RuntimeException("AvroParser: tipo de value não suportado: " + value.getClass().getName()
                    + ". Configure value.converter=io.confluent.connect.avro.AvroConverter.");
        }

        ParsedRecord parsed = new ParsedRecord();
        parsed.setFields(fields);
        parsed.setOp(op);
        parsed.setTopic(record.topic());
        parsed.setPartition(record.kafkaPartition());
        parsed.setOffset(record.kafkaOffset());
        parsed.setTsMs(tsMs);

        return parsed;
    }

    // --- Struct (Kafka Connect / AvroConverter) ---

    private Map<String, Object> extractBusinessFields(Struct debeziumStruct, String op) {
        Struct dataStruct = (Struct) debeziumStruct.get("d".equals(op) ? "before" : "after");
        if (dataStruct == null) {
            log.warn("AvroParser: struct de dados está null para op='{}'. Retornando mapa vazio.", op);
            return new LinkedHashMap<>();
        }
        return FlatJsonParser.structToMap(dataStruct);
    }

    private String getStringField(Struct struct, String field, String defaultValue) {
        try {
            Object val = struct.get(field);
            return val != null ? val.toString() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private long getLongField(Struct struct, String field, long defaultValue) {
        try {
            Object val = struct.get(field);
            return (val instanceof Number) ? ((Number) val).longValue() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    // --- GenericRecord (Avro nativo) ---

    private Map<String, Object> extractBusinessFields(GenericRecord debeziumRecord, String op) {
        Object dataObj = debeziumRecord.get("d".equals(op) ? "before" : "after");
        if (dataObj == null) {
            log.warn("AvroParser: GenericRecord de dados está null para op='{}'. Retornando mapa vazio.", op);
            return new LinkedHashMap<>();
        }
        if (dataObj instanceof GenericRecord) {
            return genericRecordToMap((GenericRecord) dataObj);
        }
        throw new RuntimeException("AvroParser: after/before não é GenericRecord: " + dataObj.getClass().getName());
    }

    private Map<String, Object> genericRecordToMap(GenericRecord record) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            Object val = record.get(field.name());
            if (val instanceof GenericRecord) {
                map.put(field.name(), genericRecordToMap((GenericRecord) val));
            } else {
                map.put(field.name(), val != null ? val.toString() : null);
            }
        }
        return map;
    }

    private String getStringField(GenericRecord record, String field, String defaultValue) {
        Object val = record.get(field);
        return val != null ? val.toString() : defaultValue;
    }

    private long getLongField(GenericRecord record, String field, long defaultValue) {
        Object val = record.get(field);
        return (val instanceof Number) ? ((Number) val).longValue() : defaultValue;
    }
}
