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

/** MySqlSourceEnumerator 发送 MySqlSourceReader 事件, 请求表需要变更. */
public class TableChangeRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String tableId;
    private TableInfo tableInfo;
    private final TableChangeTypeEnum tableChangeType;

    public TableChangeRequestEvent(TableId tableId, TableChangeTypeEnum tableChangeType) {
        this.tableId = tableId.toString();
        this.tableChangeType = tableChangeType;
    }

    public static TableChangeRequestEvent asCreateStreamTable(TableId tableId) {
        return new TableChangeRequestEvent(tableId, TableChangeTypeEnum.CREATE_STREAM_TABLE);
    }

    public static TableChangeRequestEvent asCreateSnapshotTable(TableId tableId) {
        return new TableChangeRequestEvent(tableId, TableChangeTypeEnum.CREATE_SNAPSHOT_TABLE);
    }

    public static TableChangeRequestEvent asDeleteTable(TableId tableId) {
        return new TableChangeRequestEvent(tableId, TableChangeTypeEnum.DELETE_TABLE);
    }

    public TableId getTableId() {
        return TableId.parse(tableId);
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public TableChangeTypeEnum getTableChangeType() {
        return tableChangeType;
    }

    public boolean isAddedTable() {
        return TableChangeTypeEnum.CREATE_SNAPSHOT_TABLE.equals(tableChangeType)
                || TableChangeTypeEnum.CREATE_STREAM_TABLE.equals(tableChangeType);
    }
}
