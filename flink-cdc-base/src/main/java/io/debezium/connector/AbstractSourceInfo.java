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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;

/** Common information provided by all connectors in either source field or offsets. */
public abstract class AbstractSourceInfo {

    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String SERVER_NAME_KEY = "name";
    public static final String TIMESTAMP_KEY = "ts_ms";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String DATABASE_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String COLLECTION_NAME_KEY = "collection";
    public static final String SEQUENCE_KEY = "sequence";
    public static final String IP_PORT = "ip_port";

    private final CommonConnectorConfig config;

    protected AbstractSourceInfo(CommonConnectorConfig config) {
        this.config = config;
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call {@link
     * #schemaBuilder()} to add all shared fields to their schema.
     */
    public Schema schema() {
        return config.getSourceInfoStructMaker().schema();
    }

    protected SourceInfoStructMaker<AbstractSourceInfo> structMaker() {
        return config.getSourceInfoStructMaker();
    }

    /** @return timestamp of the event */
    protected abstract Instant timestamp();

    /** @return status whether the record is from snapshot or streaming phase */
    protected abstract SnapshotRecord snapshot();

    /** @return name of the database */
    protected abstract String database();

    /** @return logical name of the server */
    protected String serverName() {
        return config.getLogicalName();
    }

    /** Returns the {@code source} struct representing this source info. */
    public Struct struct() {
        return structMaker().struct(this);
    }

    /**
     * Returns extra sequencing metadata about a change event formatted as a stringified JSON array.
     * The metadata contained in a sequence must be ordered sequentially in order to be understood
     * and compared.
     */
    protected String sequence() {
        return null;
    }
}
