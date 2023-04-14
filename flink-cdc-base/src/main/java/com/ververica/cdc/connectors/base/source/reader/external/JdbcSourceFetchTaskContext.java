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

package com.ververica.cdc.connectors.base.source.reader.external;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The context for fetch task that fetching data of snapshot split from JDBC data source. */
public abstract class JdbcSourceFetchTaskContext implements FetchTask.Context {

    protected final JdbcSourceConfig sourceConfig;
    protected final JdbcDataSourceDialect dataSourceDialect;
    protected final CommonConnectorConfig dbzConnectorConfig;
    protected final SchemaNameAdjuster schemaNameAdjuster;
    protected final String ipPort;

    public JdbcSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        this.sourceConfig = sourceConfig;
        this.dataSourceDialect = dataSourceDialect;
        this.dbzConnectorConfig = sourceConfig.getDbzConnectorConfig();
        this.schemaNameAdjuster = SchemaNameAdjuster.create();
        this.ipPort = sourceConfig.getHostname() + "_" + sourceConfig.getPort();
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return null;
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (valueSchema == null || value == null) {
            return false;
        } else {
            return valueSchema.field(Envelope.FieldName.OPERATION) != null
                    && value.getString(Envelope.FieldName.OPERATION) != null;
        }
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        RowType splitKeyType =
                getSplitType(getDatabaseSchema().tableFor(SourceRecordUtils.getTableId(record)));
        Object[] key = SourceRecordUtils.getSplitKey(splitKeyType, record, getSchemaNameAdjuster());
        return SourceRecordUtils.splitKeyRangeContains(key, splitStart, splitEnd);
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {
        Struct key = (Struct) changeRecord.key();
        Struct value = (Struct) changeRecord.value();
        if (value != null) {
            Envelope.Operation operation =
                    Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            switch (operation) {
                case CREATE:
                case UPDATE:
                    Envelope envelope = Envelope.fromSchema(changeRecord.valueSchema());
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                    Instant fetchTs =
                            Instant.ofEpochMilli((Long) source.get(Envelope.FieldName.TIMESTAMP));
                    SourceRecord record =
                            new SourceRecord(
                                    changeRecord.sourcePartition(),
                                    changeRecord.sourceOffset(),
                                    changeRecord.topic(),
                                    changeRecord.kafkaPartition(),
                                    changeRecord.keySchema(),
                                    changeRecord.key(),
                                    changeRecord.valueSchema(),
                                    envelope.read(after, source, fetchTs));
                    outputBuffer.put(key, record);
                    break;
                case DELETE:
                    outputBuffer.remove(key);
                    break;
                case READ:
                    throw new IllegalStateException(
                            String.format(
                                    "Data change record shouldn't use READ operation, the the record is %s.",
                                    changeRecord));
            }
        }
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(
            Collection<SourceRecord> snapshotRecords, Offset streamOffset) {
        return snapshotRecords.stream()
                .map(
                        record -> {
                            Envelope envelope = Envelope.fromSchema(record.valueSchema());
                            Struct value = (Struct) record.value();
                            Struct updateAfter = value.getStruct(Envelope.FieldName.AFTER);
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                            source.put(Envelope.FieldName.TIMESTAMP, 0L);
                            source.put(AbstractSourceInfo.IP_PORT, ipPort);

                            // 添加高水位的 offset
                            streamOffset.getOffset().forEach(source::put);

                            // extend the fetch timestamp(ts_ms)
                            Instant fetchTs =
                                    Instant.ofEpochMilli(
                                            value.getInt64(Envelope.FieldName.TIMESTAMP));
                            SourceRecord sourceRecord =
                                    new SourceRecord(
                                            record.sourcePartition(),
                                            record.sourceOffset(),
                                            record.topic(),
                                            record.kafkaPartition(),
                                            record.keySchema(),
                                            record.key(),
                                            record.valueSchema(),
                                            envelope.read(updateAfter, source, fetchTs));
                            return sourceRecord;
                        })
                .collect(Collectors.toList());
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public JdbcDataSourceDialect getDataSourceDialect() {
        return dataSourceDialect;
    }

    public CommonConnectorConfig getDbzConnectorConfig() {
        return dbzConnectorConfig;
    }

    public SchemaNameAdjuster getSchemaNameAdjuster() {
        return null;
    }

    public abstract RelationalDatabaseSchema getDatabaseSchema();

    public abstract RowType getSplitType(Table table);

    public abstract ErrorHandler getErrorHandler();

    public abstract JdbcSourceEventDispatcher getDispatcher();

    public abstract OffsetContext getOffsetContext();
}
