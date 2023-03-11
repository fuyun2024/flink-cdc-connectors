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

import io.debezium.relational.TableId;

import java.util.List;

/** 表完成的 ack 事件， enumerator 收到后需要把完成的信息通知直通车. */
public class FinishedSnapshotTableAckEvent extends FinishedSnapshotTableReportEvent {

    public FinishedSnapshotTableAckEvent(List<TableId> finishedTableIds) {
        super(finishedTableIds);
    }

    @Override
    public String toString() {
        return "FinishedSnapshotTableAckEvent{" + "finishedTableIds=" + finishedTableIds + '}';
    }
}
