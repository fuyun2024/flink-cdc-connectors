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
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 混合记录的序列化器. */
public class HybridSourceRecordDeserialization
        implements BinlogStateAware, DebeziumDeserializationSchema<HybridSourceRecord> {

    private static final long serialVersionUID = -3168848963265670603L;

    private final Map<String, Boolean> needStateTableIds;

    public HybridSourceRecordDeserialization() {
        this.needStateTableIds = new HashMap<>();
    }

    @Override
    public void deserialize(SourceRecord record, Collector<HybridSourceRecord> out)
            throws Exception {
        HybridSourceRecord hybridSourceRecord;

        Long binlogTimestamp = RecordUtils.getMessageTimestamp(record);
        if (binlogTimestamp <= 0) {
            hybridSourceRecord = HybridSourceRecord.buildSnapshotRecord(record);
        } else {
            String dbTable = record.topic().substring(record.topic().indexOf(".") + 1);
            if (needStateTableIds.get(dbTable) == null) {
                hybridSourceRecord = HybridSourceRecord.buildBinlogRecord(record);
            } else {
                hybridSourceRecord = HybridSourceRecord.buildStateBinlogRecord(record);
            }
        }

        emit(hybridSourceRecord, out);
    }

    private void emit(HybridSourceRecord sourceRecord, Collector<HybridSourceRecord> out) {
        out.collect(sourceRecord);
    }

    @Override
    public TypeInformation<HybridSourceRecord> getProducedType() {
        return TypeInformation.of(HybridSourceRecord.class);
    }

    @Override
    public void addTables(List<TableId> tableIds) {
        if (tableIds != null) {
            tableIds.forEach(tableId -> needStateTableIds.put(tableId.toString(), true));
        }
    }

    @Override
    public void removeTables(List<TableId> tableIds) {
        if (tableIds != null) {
            tableIds.forEach(tableId -> needStateTableIds.remove(tableId.toString()));
        }
    }
}
