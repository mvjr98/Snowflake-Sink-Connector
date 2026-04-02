package triggo.ai.connector.v1.model;

import java.util.Map;

/**
 * DTO que representa um registro já parseado, pronto para ser gravado no Snowflake.
 * Contém os campos de negócio + metadados de controle (KFK_*).
 */
public class ParsedRecord {

    /** Campos de negócio extraídos do payload (ex: ORDER_ID, CUSTOMER_ID, ...) */
    private Map<String, Object> fields;

    /** Operação Debezium: c (create), r (read/snapshot), u (update), d (delete) */
    private String op;

    private String topic;
    private int partition;
    private long offset;

    /** Timestamp do evento em milissegundos (ts_ms do Debezium, ou System.currentTimeMillis() para FLAT_JSON) */
    private long tsMs;

    /** UUID do batch de ingestão, preenchido pelo SnowflakeSinkTask */
    private String blockId;

    public ParsedRecord() {}

    public ParsedRecord(Map<String, Object> fields, String op, String topic, int partition, long offset, long tsMs) {
        this.fields = fields;
        this.op = op;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.tsMs = tsMs;
    }

    public Map<String, Object> getFields()              { return fields; }
    public void setFields(Map<String, Object> fields)   { this.fields = fields; }

    public String getOp()                               { return op; }
    public void setOp(String op)                        { this.op = op; }

    public String getTopic()                            { return topic; }
    public void setTopic(String topic)                  { this.topic = topic; }

    public int getPartition()                           { return partition; }
    public void setPartition(int partition)             { this.partition = partition; }

    public long getOffset()                             { return offset; }
    public void setOffset(long offset)                  { this.offset = offset; }

    public long getTsMs()                               { return tsMs; }
    public void setTsMs(long tsMs)                      { this.tsMs = tsMs; }

    public String getBlockId()                          { return blockId; }
    public void setBlockId(String blockId)              { this.blockId = blockId; }

    @Override
    public String toString() {
        return "ParsedRecord{op='" + op + "', topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + ", blockId='" + blockId + "', fields=" + fields + "}";
    }
}
