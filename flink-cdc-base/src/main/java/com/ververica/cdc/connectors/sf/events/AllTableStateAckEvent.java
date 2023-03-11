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

package com.ververica.cdc.connectors.sf.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.sf.request.bean.TableInfo;
import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** MySqlSourceEnumerator 返回 MySqlSourceReader 的 ack 事件. */
public class AllTableStateAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    /** 需要处理的列表. */
    private final Map<String, TableInfo> tableInfos;

    /** 需要加上 binlog_state 表. */
    private final List<String> needBinlogStateTableIds;

    public AllTableStateAckEvent(
            Map<TableId, TableInfo> tableInfos, List<TableId> needBinlogStateTableIds) {
        this.tableInfos = new HashMap<>();
        tableInfos.forEach((k, v) -> this.tableInfos.put(k.toString(), v));
        this.needBinlogStateTableIds =
                needBinlogStateTableIds.stream()
                        .map(tableId -> tableId.toString())
                        .collect(Collectors.toList());
    }

    public Map<TableId, TableInfo> getTableInfos() {
        Map<TableId, TableInfo> returnMap = new HashMap<>();
        this.tableInfos.forEach((k, v) -> returnMap.put(TableId.parse(k), v));
        return returnMap;
    }

    public List<TableId> needBinlogStateTableIds() {
        return needBinlogStateTableIds.stream()
                .map(tableId -> TableId.parse(tableId))
                .collect(Collectors.toList());
    }
}
