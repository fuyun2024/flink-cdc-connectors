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

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.sf.entity.TableInfo;
import io.debezium.relational.TableId;

import java.util.List;
import java.util.Map;

/** binlog state aware. */
public interface TableStateAware {

    /** 表信息是否都获取到了. */
    void setReady(boolean ready);

    /** 表信息是否都获取到了. */
    boolean isReady();

    /** 第一次启动的时候初始化状态. */
    void initAllState(Map<TableId, TableInfo> tableInfos, List<TableId> needBinlogStates);

    /** add processed table. */
    void addProcessedTableId(TableId tableId, String topicName);

    /** remove table. */
    void removeTable(TableId tableId);

    /** add need state table. */
    void addBinlogStateTable(TableId tableId, String topicName);

    /** add need state table. */
    void removeBinlogStateTable(TableId tableId);

    /** is need processed table. */
    String getNeedProcessedTable(TableId tableId);

    /** is need add state table. */
    boolean isNeedAddStateTable(TableId tableId);

    /** 获取正在 BINLOG_STATE 中的表，用于向 jm 查看全量采集是否完成. */
    List<TableId> getBinlogStateTables();

    /** 设置当前的 offset. */
    void setCurrentOffset(Offset offset);

    /** 获取已经处理的最新 offset. */
    Offset getCurrentOffset();
}
