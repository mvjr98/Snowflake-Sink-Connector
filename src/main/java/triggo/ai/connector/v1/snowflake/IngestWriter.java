package triggo.ai.connector.v1.snowflake;

import triggo.ai.connector.v1.model.ParsedRecord;

import java.util.List;

/**
 * Abstração para os dois modos de escrita na tabela _INGEST do Snowflake.
 */
public interface IngestWriter {

    /** Inicializa conexão/canal com o Snowflake. Chamado uma vez no start(). */
    void open() throws Exception;

    /** Grava um batch de registros na tabela _INGEST. */
    void write(List<ParsedRecord> records) throws Exception;

    /** Libera recursos. Chamado no stop(). */
    void close() throws Exception;
}
