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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** 混合记录的序列化器. */
public class HybridSourceRecordDeserialization
        implements DebeziumDeserializationSchema<HybridSourceRecord> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HybridSourceRecordDeserialization.class);

    private static final long serialVersionUID = -3168848963265670603L;

    private TableStateAware tableStateAware;
    private OffsetFactory offsetFactory;

    @Override
    public void deserialize(SourceRecord record, Collector<HybridSourceRecord> out)
            throws Exception {
        while (!tableStateAware.isReady()) {
            LOG.warn("等待表 JobManager 同步表信息。");
            Thread.sleep(1000);
        }

        TableId tableId = SourceRecordUtils.getTableId(record);
        String topicName = tableStateAware.getNeedProcessedTable(tableId);
        if (StringUtils.isBlank(topicName)) {
            return;
        }

        HybridSourceRecord hybridSourceRecord;
        Long binlogTimestamp = SourceRecordUtils.getMessageTimestamp(record);
        if (binlogTimestamp <= 0) {
            hybridSourceRecord = HybridSourceRecord.buildSnapshotRecord(record);
        } else if (tableStateAware.isNeedAddStateTable(tableId)) {
            hybridSourceRecord = HybridSourceRecord.buildStateBinlogRecord(record);
        } else {
            hybridSourceRecord = HybridSourceRecord.buildBinlogRecord(record);
        }

        // 设置 topic name
        tableStateAware.setCurrentOffset(getOffsetPosition(record.sourceOffset()));
        hybridSourceRecord.setTopicName(topicName);
        emit(hybridSourceRecord, out);
    }

    private void emit(HybridSourceRecord sourceRecord, Collector<HybridSourceRecord> out) {
        out.collect(sourceRecord);
    }

    public Offset getOffsetPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return offsetFactory.newOffset(offsetStrMap);
    }

    public void setTableStateAware(TableStateAware tableStateAware) {
        this.tableStateAware = tableStateAware;
    }

    public void setOffsetFactory(OffsetFactory offsetFactory) {
        this.offsetFactory = offsetFactory;
    }

    @Override
    public TypeInformation<HybridSourceRecord> getProducedType() {
        return TypeInformation.of(HybridSourceRecord.class);
    }
}
