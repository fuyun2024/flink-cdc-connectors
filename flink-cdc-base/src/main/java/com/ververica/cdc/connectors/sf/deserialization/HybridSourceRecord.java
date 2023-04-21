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

package com.ververica.cdc.connectors.sf.deserialization;

import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import com.ververica.cdc.connectors.sf.entity.EncryptField;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord.RecordType.BINLOG;
import static com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord.RecordType.SNAPSHOT;
import static com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord.RecordType.STATE_BINLOG;
import static com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord.RecordType.TABLE_FINISHED;

/** HybridSourceRecord. */
public class HybridSourceRecord extends SourceRecord {

    private String topicName;
    private RecordType recordType;
    private List<EncryptField> encryptFields;

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
        TABLE_FINISHED
    }

    /** 是否是 Binlog 记录. */
    public boolean isBinlogRecord() {
        return BINLOG.equals(recordType);
    }

    /** 是否是 StateBinlog 记录. */
    public boolean isStateBinlogRecord() {
        return STATE_BINLOG.equals(recordType);
    }

    /** 是否是 SnapShot 记录. */
    public boolean isSnapShotRecord() {
        return SNAPSHOT.equals(recordType);
    }

    /** 是否是 table finished 记录. */
    public boolean isTableFinishedRecord() {
        return TABLE_FINISHED.equals(recordType);
    }

    /** 获取 db.table.primaryKey. */
    public String getTableIdAndPrimaryKey() {
        return SourceRecordUtils.getTableId(this) + "." + getPrimaryKeyValue();
    }

    /** 获取 db.table 值. */
    public String getDbTable() {
        return this.topic().substring(this.topic().indexOf(".") + 1);
    }

    /** 获取主键值. */
    public String getPrimaryKeyValue() {
        Schema keySchema = this.keySchema();
        ArrayList<Field> fieldList = new ArrayList<>(keySchema.fields());

        if (fieldList != null && fieldList.size() > 0) {
            Struct keyValue = (Struct) this.key();
            StringJoiner joiner = new StringJoiner(".", "", "");
            for (int i = 0; i < fieldList.size(); i++) {
                joiner.add(keyValue.get(fieldList.get(i).name()).toString());
            }
            return joiner.toString();
        }
        return null;
    }

    /** 创建 binlog record. */
    public static HybridSourceRecord buildBinlogRecord(SourceRecord sourceRecord) {
        return new HybridSourceRecord(sourceRecord, BINLOG);
    }

    /** 创建 stateBinlog record. */
    public static HybridSourceRecord buildStateBinlogRecord(SourceRecord sourceRecord) {
        return new HybridSourceRecord(sourceRecord, STATE_BINLOG);
    }

    /** 创建 snapshot record. */
    public static HybridSourceRecord buildSnapshotRecord(SourceRecord sourceRecord) {
        return new HybridSourceRecord(sourceRecord, SNAPSHOT);
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public List<EncryptField> getEncryptFields() {
        return encryptFields;
    }

    public void setEncryptFields(List<EncryptField> encryptFields) {
        this.encryptFields = encryptFields;
    }

    public boolean isNeedEncrypt() {
        return encryptFields != null && encryptFields.size() > 0;
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
        return Objects.equals(topicName, that.topicName)
                && recordType == that.recordType
                && encryptFields == that.encryptFields;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
        result = 31 * result + (recordType != null ? recordType.hashCode() : 0);
        result = 31 * result + (encryptFields != null ? encryptFields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HybridSourceRecord{"
                + "topicName='"
                + topicName
                + '\''
                + ", recordType="
                + recordType
                + ", encryptFields="
                + encryptFields
                + "} "
                + super.toString();
    }
}
