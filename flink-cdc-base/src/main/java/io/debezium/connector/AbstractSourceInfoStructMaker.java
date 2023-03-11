/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Enum;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Objects;

/**
 * Common information provided by all connectors in either source field or offsets. When this class
 * schema changes the connector implementations should create a legacy class that will keep the same
 * behaviour.
 */
public abstract class AbstractSourceInfoStructMaker<T extends AbstractSourceInfo>
        implements SourceInfoStructMaker<T> {

    public static final Schema SNAPSHOT_RECORD_SCHEMA =
            Enum.builder("true,last,false").defaultValue("false").optional().build();

    private final String version;
    private final String connector;
    private final String serverName;

    public AbstractSourceInfoStructMaker(
            String connector, String version, CommonConnectorConfig connectorConfig) {
        this.connector = Objects.requireNonNull(connector);
        this.version = Objects.requireNonNull(version);
        this.serverName = connectorConfig.getLogicalName();
    }

    protected SchemaBuilder commonSchemaBuilder() {
        return SchemaBuilder.struct()
                .field(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(AbstractSourceInfo.SNAPSHOT_KEY, SNAPSHOT_RECORD_SCHEMA)
                .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.IP_PORT, Schema.OPTIONAL_STRING_SCHEMA)
                .field(AbstractSourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected Struct commonStruct(T sourceInfo) {
        final Instant timestamp =
                sourceInfo.timestamp() == null ? Instant.now() : sourceInfo.timestamp();
        final String database = sourceInfo.database() == null ? "" : sourceInfo.database();
        Struct ret =
                new Struct(schema())
                        .put(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, version)
                        .put(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, connector)
                        .put(AbstractSourceInfo.SERVER_NAME_KEY, serverName)
                        .put(AbstractSourceInfo.TIMESTAMP_KEY, timestamp.toEpochMilli())
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, database);
        final String sequence = sourceInfo.sequence();
        if (sequence != null) {
            ret.put(AbstractSourceInfo.SEQUENCE_KEY, sequence);
        }
        final SnapshotRecord snapshot = sourceInfo.snapshot();
        if (snapshot != null) {
            snapshot.toSource(ret);
        }
        return ret;
    }
}
