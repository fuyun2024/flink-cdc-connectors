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

package com.ververica.cdc.connectors.oracle.source.utils;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.Arrays;

/** Integration tests for Oracle Utils. */
public class OracleUtilsTest {

    @Test
    public void buildSplitScanQuery() {
        System.out.println(
                OracleUtils.buildSplitScanQuery(
                        TableId.parse("DEBEZIUM.PRODUCTS"),
                        new RowType(
                                Arrays.asList(new RowType.RowField("ROWID", new VarCharType()))),
                        true,
                        false));
        System.out.println(
                OracleUtils.buildSplitScanQuery(
                        TableId.parse("DEBEZIUM.PRODUCTS"),
                        new RowType(
                                Arrays.asList(new RowType.RowField("ROWID", new VarCharType()))),
                        false,
                        false));
        System.out.println(
                OracleUtils.buildSplitScanQuery(
                        TableId.parse("DEBEZIUM.PRODUCTS"),
                        new RowType(
                                Arrays.asList(new RowType.RowField("ROWID", new VarCharType()))),
                        false,
                        true));
    }
}
