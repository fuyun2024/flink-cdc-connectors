package com.ververica.cdc.connectors.sf.reader;

import com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord;
import io.debezium.relational.TableId;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

import static com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord.RecordType.TABLE_FINISHED;

/** description: table finished record. */
public class TableFinishedRecord {

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    public static final String KEY = "key";
    public static final String TABLE_ID = "table_id";

    public static final String TABLE_FINISHED_EVENT_KEY_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.table.finished.key";
    public static final String TABLE_FINISHED_EVENT_VALUE_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.table.finished.value";

    private static final Schema tableFinishedEventKeySchema =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust(TABLE_FINISHED_EVENT_KEY_SCHEMA_NAME))
                    .field(KEY, Schema.STRING_SCHEMA)
                    .build();

    private static final Schema tableFinishedEventValueSchema =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust(TABLE_FINISHED_EVENT_VALUE_SCHEMA_NAME))
                    .field(TABLE_ID, Schema.STRING_SCHEMA)
                    .build();

    public static HybridSourceRecord buildTableFinishedRecord(TableId tableId) {
        String topic = "catalog." + tableId.toString();
        SourceRecord sourceRecord =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        topic,
                        tableFinishedEventKeySchema,
                        recordKey(tableId),
                        tableFinishedEventValueSchema,
                        recordValue(tableId));
        return new HybridSourceRecord(sourceRecord, TABLE_FINISHED);
    }

    private static Struct recordKey(TableId tableId) {
        Struct result = new Struct(tableFinishedEventKeySchema);
        result.put(KEY, tableId.toString());
        return result;
    }

    private static Struct recordValue(TableId tableId) {
        Struct result = new Struct(tableFinishedEventValueSchema);
        result.put(TABLE_ID, tableId.toString());
        return result;
    }
}
