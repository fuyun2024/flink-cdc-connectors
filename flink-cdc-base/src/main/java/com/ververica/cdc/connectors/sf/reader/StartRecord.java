package com.ververica.cdc.connectors.sf.reader;

import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

/** description: table finished record. */
public class StartRecord {

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    public static final String KEY = "task_start_event";

    public static final String START_EVENT_KEY_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.start.key";

    private static final Schema startEventKeySchema =
            SchemaBuilder.struct()
                    .name(SCHEMA_NAME_ADJUSTER.adjust(START_EVENT_KEY_SCHEMA_NAME))
                    .field(KEY, Schema.STRING_SCHEMA)
                    .build();

    public static SourceRecord buildStartRecord() {
        SourceRecord sourceRecord =
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        null,
                        startEventKeySchema,
                        recordKey(),
                        null,
                        null);
        return sourceRecord;
    }

    private static Struct recordKey() {
        Struct result = new Struct(startEventKeySchema);
        result.put(KEY, KEY);
        return result;
    }
}
