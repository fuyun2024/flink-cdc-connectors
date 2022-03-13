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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.deserialization.HybridSourceRecord.RecordType.BINLOG;
import static com.ververica.cdc.connectors.mysql.source.deserialization.HybridSourceRecord.RecordType.SNAPSHOT;
import static com.ververica.cdc.connectors.mysql.source.deserialization.HybridSourceRecord.RecordType.STATE_BINLOG;

/** HybridSourceRecord. */
public class HybridSourceRecord extends SourceRecord {

    private RecordType recordType;

    public HybridSourceRecord(SourceRecord sourceRecord, RecordType recordType) {
        super(
                sourceRecord.sourcePartition(),
                sourceRecord.sourceOffset(),
                sourceRecord.topic(),
                sourceRecord.kafkaPartition(),
                sourceRecord.keySchema(),
                sourceRecord.key(),
                sourceRecord.valueSchema(),
                sourceRecord.value(),
                sourceRecord.timestamp(),
                sourceRecord.headers());
        this.recordType = recordType;
    }

    /** 记录类型. */
    public enum RecordType {
        BINLOG,
        STATE_BINLOG,
        SNAPSHOT,
    }

    /**
     * 是否是 Binlog 记录.
     *
     * @return
     */
    public boolean isBinlogRecord() {
        return BINLOG.equals(recordType);
    }

    /**
     * 是否是 StateBinlog 记录.
     *
     * @return
     */
    public boolean isStateBinlogRecord() {
        return STATE_BINLOG.equals(recordType);
    }

    /**
     * 是否是 SnapShot 记录.
     *
     * @return
     */
    public boolean isSnapShotRecord() {
        return SNAPSHOT.equals(recordType);
    }

    /**
     * 获取 db.table.primaryKey.
     *
     * @return
     */
    public String getDbTablePrimaryKey() {
        return getDbTable() + "." + getPrimaryKeyValue();
    }

    /**
     * 获取 db.table 值.
     *
     * @return
     */
    public String getDbTable() {
        return this.topic().substring(this.topic().indexOf(".") + 1);
    }

    /**
     * 获取主键值.
     *
     * @return
     */
    public String getPrimaryKeyValue() {
        String[] fieldNames =
                this.keySchema().fields().stream()
                        .map(Field::name)
                        .collect(Collectors.toList())
                        .toArray(new String[0]);

        if (fieldNames != null && fieldNames.length > 0) {
            Struct keyValue = (Struct) this.key();
            StringJoiner joiner = new StringJoiner(".", "", "");
            for (int i = 0; i < fieldNames.length; i++) {
                joiner.add(keyValue.get(fieldNames[i]).toString());
            }
            return joiner.toString();
        }
        return null;
    }

    /**
     * 创建 binlog record.
     *
     * @param sourceRecord
     * @return
     */
    public static HybridSourceRecord buildBinlogRecord(SourceRecord sourceRecord) {
        return new HybridSourceRecord(sourceRecord, BINLOG);
    }

    /**
     * 创建 stateBinlog record.
     *
     * @param sourceRecord
     * @return
     */
    public static HybridSourceRecord buildStateBinlogRecord(SourceRecord sourceRecord) {
        return new HybridSourceRecord(sourceRecord, STATE_BINLOG);
    }

    /**
     * 创建 snapshot record.
     *
     * @param sourceRecord
     * @return
     */
    public static HybridSourceRecord buildSnapshotRecord(SourceRecord sourceRecord) {
        return new HybridSourceRecord(sourceRecord, SNAPSHOT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        HybridSourceRecord that = (HybridSourceRecord) o;

        return Objects.equals(recordType, that.recordType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (recordType != null ? recordType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HybridSourceRecord{" + "recordType=" + recordType + "} " + super.toString();
    }
}
