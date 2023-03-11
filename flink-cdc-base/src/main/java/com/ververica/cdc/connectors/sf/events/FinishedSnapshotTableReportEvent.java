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

import io.debezium.relational.TableId;

import java.util.List;
import java.util.stream.Collectors;

/** 完成表全量的回报事件. */
public class FinishedSnapshotTableReportEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    protected final List<String> finishedTableIds;

    public FinishedSnapshotTableReportEvent(List<TableId> finishedTableIds) {
        this.finishedTableIds =
                finishedTableIds.stream()
                        .map(tableId -> tableId.toString())
                        .collect(Collectors.toList());
    }

    public List<TableId> getFinishedTableIds() {
        return finishedTableIds.stream()
                .map(tableId -> TableId.parse(tableId))
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "FinishedSnapshotTableReportEvent{" + "finishedTableIds=" + finishedTableIds + '}';
    }
}
