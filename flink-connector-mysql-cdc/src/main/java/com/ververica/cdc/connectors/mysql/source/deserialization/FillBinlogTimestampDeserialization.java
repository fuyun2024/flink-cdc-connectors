/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.deserialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** 给 SourceRecord 添加 binlog_timestamp 时间戳字段. */
public class FillBinlogTimestampDeserialization
        implements DebeziumDeserializationSchema<SourceRecord> {

    private static final long serialVersionUID = -3168848963265670603L;

    private static final String BINLOG_TIMESTAMP_FIELD_NAME = "binlog_timestamp";

    private static final Integer BEFORE_INDEX = 0;

    private static final Integer AFTER_INDEX = 1;

    private final String timestampFieldName;

    public FillBinlogTimestampDeserialization() {
        this.timestampFieldName = BINLOG_TIMESTAMP_FIELD_NAME;
    }

    public FillBinlogTimestampDeserialization(String timestampFieldName) {
        this.timestampFieldName = timestampFieldName;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Long binlogTimestamp = RecordUtils.getMessageTimestamp(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        Struct timestampStruct = null;
        Field newField = null;
        List<Field> newFieldList = null;
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);

            Schema timestampSchema = fillBinlogTimestampToSchema(afterSchema);
            timestampStruct = fillBinlogTimestampToStruct(after, timestampSchema, binlogTimestamp);

            newField = buildField(Envelope.FieldName.AFTER, AFTER_INDEX, timestampSchema);
            newFieldList = setField(valueSchema.fields(), newField, AFTER_INDEX);
        } else if (op == Envelope.Operation.DELETE) {
            Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);

            Schema timestampSchema = fillBinlogTimestampToSchema(beforeSchema);
            timestampStruct = fillBinlogTimestampToStruct(before, timestampSchema, binlogTimestamp);

            newField = buildField(Envelope.FieldName.BEFORE, BEFORE_INDEX, timestampSchema);
            newFieldList = setField(valueSchema.fields(), newField, BEFORE_INDEX);
        } else {
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);

            Schema timestampSchema = fillBinlogTimestampToSchema(afterSchema);
            timestampStruct = fillBinlogTimestampToStruct(after, timestampSchema, binlogTimestamp);

            newField = buildField(Envelope.FieldName.AFTER, AFTER_INDEX, timestampSchema);
            newFieldList = setField(valueSchema.fields(), newField, AFTER_INDEX);
        }

        Schema newValueSchema = buildSchema(valueSchema, newFieldList);
        Struct newValueStruct =
                replaceStructValue(value, newValueSchema, newField, timestampStruct);

        SourceRecord sourceRecord = buildSourceRecord(record, newValueSchema, newValueStruct);

        out.collect(sourceRecord);
    }

    /**
     * fillValueToSourceRecord.
     *
     * @param oldRecord
     * @param valueSchema
     * @param value
     * @return
     */
    private SourceRecord buildSourceRecord(
            SourceRecord oldRecord, Schema valueSchema, Struct value) {
        return new SourceRecord(
                oldRecord.sourcePartition(),
                oldRecord.sourceOffset(),
                oldRecord.topic(),
                oldRecord.kafkaPartition(),
                oldRecord.keySchema(),
                oldRecord.key(),
                valueSchema,
                value,
                oldRecord.timestamp(),
                oldRecord.headers());
    }

    /**
     * replaceStructValue.
     *
     * @param oldStruct
     * @param newValueSchema
     * @param newField
     * @param newStruct
     * @return
     */
    private Struct replaceStructValue(
            Struct oldStruct, Schema newValueSchema, Field newField, Struct newStruct) {
        Struct newValueStruct = new Struct(newValueSchema);
        for (Field field : newValueSchema.fields()) {
            if (field.equals(newField)) {
                newValueStruct.put(newField, newStruct);
            } else {
                newValueStruct.put(field, oldStruct.get(field));
            }
        }
        return newValueStruct;
    }

    /**
     * fillBinlogTimestampToStruct.
     *
     * @param oldStruct
     * @param newSchema
     * @param binlogTimestamp
     * @return
     */
    private Struct fillBinlogTimestampToStruct(
            Struct oldStruct, Schema newSchema, Long binlogTimestamp) {
        Struct newStruct = new Struct(newSchema);

        List<Field> fieldList = newSchema.fields();
        int i = 0;
        for (; i < fieldList.size() - 1; i++) {
            newStruct.put(fieldList.get(i), oldStruct.get(fieldList.get(i)));
        }
        newStruct.put(fieldList.get(i), binlogTimestamp);
        return newStruct;
    }

    /**
     * fillBinlogTimestampToSchema.
     *
     * @param schema
     * @return
     */
    private Schema fillBinlogTimestampToSchema(Schema schema) {
        List<Field> fieldList = addBinlogTimestampField(schema.fields());
        return new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                schema.name(),
                schema.version(),
                schema.doc(),
                schema.parameters(),
                Collections.unmodifiableList(fieldList),
                null,
                null);
    }

    /**
     * addBinlogTimestampField.
     *
     * @param fields
     * @return
     */
    private List<Field> addBinlogTimestampField(List<Field> fields) {
        Field timestampField = buildBinlogTimestampField(timestampFieldName, fields.size());
        return addField(fields, timestampField);
    }

    /**
     * buildBinlogTimestampField.
     *
     * @param fieldName
     * @param fieldIndex
     * @return
     */
    private Field buildBinlogTimestampField(String fieldName, int fieldIndex) {
        return buildField(fieldName, fieldIndex, Schema.INT64_SCHEMA);
    }

    /**
     * buildField.
     *
     * @param fieldName
     * @param fieldIndex
     * @param fieldSchema
     * @return
     */
    private Field buildField(String fieldName, int fieldIndex, Schema fieldSchema) {
        return new Field(fieldName, fieldIndex, fieldSchema);
    }

    /**
     * buildSchema.
     *
     * @param schema
     * @param fieldList
     * @return
     */
    private Schema buildSchema(Schema schema, List<Field> fieldList) {
        return new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                schema.name(),
                schema.version(),
                schema.doc(),
                schema.parameters(),
                Collections.unmodifiableList(fieldList),
                null,
                null);
    }

    /**
     * addField.
     *
     * @param fields
     * @param field
     * @return
     */
    private List<Field> addField(List<Field> fields, Field field) {
        List<Field> newFields = fields.stream().collect(Collectors.toList());
        newFields.add(field);
        return newFields;
    }

    /**
     * setField.
     *
     * @param fields
     * @param field
     * @param fieldIndex
     * @return
     */
    private List<Field> setField(List<Field> fields, Field field, int fieldIndex) {
        List<Field> newFields = fields.stream().collect(Collectors.toList());
        newFields.set(fieldIndex, field);
        return newFields;
    }

    @Override
    public TypeInformation<SourceRecord> getProducedType() {
        return TypeInformation.of(SourceRecord.class);
    }
}
