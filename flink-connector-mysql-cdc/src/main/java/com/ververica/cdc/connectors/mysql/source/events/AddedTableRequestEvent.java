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

package com.ververica.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

import java.util.Set;

/**
 *  MySqlSourceEnumerator 请求 MySqlSourceReader 事件，请求 binlog split 新增处理表
 */
public class AddedTableRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private Set<String> tableIds;

    public AddedTableRequestEvent(Set<String> tableIds) {
        this.tableIds = tableIds;
    }

    public Set<String> getTableIds() {
        return tableIds;
    }

    public void setTableIds(Set<String> tableIds) {
        this.tableIds = tableIds;
    }
}