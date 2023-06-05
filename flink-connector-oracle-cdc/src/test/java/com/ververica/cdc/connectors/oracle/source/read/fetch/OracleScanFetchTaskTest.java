/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source.read.fetch;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import com.ververica.cdc.connectors.oracle.source.OracleDialect;
import com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleScanFetchTask;
import com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import com.ververica.cdc.connectors.oracle.utils.OracleTestUtils;
import com.ververica.cdc.connectors.oracle.utils.RecordsFormatter;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link OracleScanFetchTask}. */
public class OracleScanFetchTaskTest extends OracleSourceTestBase {

    @Test
    public void testChangingDataInSnapshotScan() throws Exception {
        OracleTestUtils.createAndInitialize(OracleTestUtils.ORACLE_CONTAINER, "customer.sql");

        String tableName = ORACLE_SCHEMA + ".CUSTOMERS";

        OracleSourceConfigFactory sourceConfigFactory =
                getConfigFactory(new String[] {tableName}, 10);
        OracleSourceConfig sourceConfig = sourceConfigFactory.create(0);
        OracleDialect oracleDialect = new OracleDialect(sourceConfigFactory);

        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableName + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableName + " where id = 102",
                    "INSERT INTO " + tableName + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableName + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableName + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableName + " SET address = 'Hangzhou' where id = 111",
                };

        MakeChangeEventTaskContext makeChangeEventTaskContext =
                new MakeChangeEventTaskContext(
                        sourceConfig,
                        oracleDialect,
                        createOracleConnection(
                                sourceConfig.getDbzConnectorConfig().getJdbcConfig()),
                        () -> executeSql(sourceConfig, changingDataSql));

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("ID", DataTypes.BIGINT()),
                        DataTypes.FIELD("NAME", DataTypes.STRING()),
                        DataTypes.FIELD("ADDRESS", DataTypes.STRING()),
                        DataTypes.FIELD("PHONE_NUMBER", DataTypes.STRING()));

        List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, oracleDialect);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Hangzhou, 123567891234]",
                    "+I[111, user_6, Hangzhou, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                };

        List<String> actual =
                readTableSnapshotSplits(snapshotSplits, makeChangeEventTaskContext, 1, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expected), actual);
    }

    private List<String> readTableSnapshotSplits(
            List<SnapshotSplit> snapshotSplits,
            OracleSourceFetchTaskContext taskContext,
            int scanSplitsNum,
            DataType dataType)
            throws Exception {
        IncrementalSourceScanFetcher sourceScanFetcher =
                new IncrementalSourceScanFetcher(taskContext, 0);

        List<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            SnapshotSplit sqlSplit = snapshotSplits.get(i);
            if (sourceScanFetcher.isFinished()) {
                sourceScanFetcher.submitTask(
                        taskContext.getDataSourceDialect().createFetchTask(sqlSplit));
            }
            Iterator<SourceRecords> res;
            while ((res = sourceScanFetcher.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecords sourceRecords = res.next();
                    result.addAll(sourceRecords.getSourceRecordList());
                }
            }
        }

        sourceScanFetcher.close();

        assertNotNull(sourceScanFetcher.getExecutorService());
        assertTrue(sourceScanFetcher.getExecutorService().isTerminated());

        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RecordsFormatter formatter = new RecordsFormatter(dataType);
        return formatter.format(records);
    }

    private List<SnapshotSplit> getSnapshotSplits(
            OracleSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect) {
        String databaseName = sourceConfig.getDatabaseList().get(0);
        List<TableId> tableIdList =
                sourceConfig.getTableList().stream()
                        .map(tableId -> TableId.parse(databaseName + "." + tableId))
                        .collect(Collectors.toList());
        final ChunkSplitter chunkSplitter = sourceDialect.createChunkSplitter(sourceConfig);

        List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
        for (TableId table : tableIdList) {
            Collection<SnapshotSplit> snapshotSplits = chunkSplitter.generateSplits(table);
            snapshotSplitList.addAll(snapshotSplits);
        }
        return snapshotSplitList;
    }

    public static OracleSourceConfigFactory getConfigFactory(
            String[] captureTables, int splitSize) {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");
        debeziumProperties.setProperty("log.mining.continuous.mine", "true");

        return (OracleSourceConfigFactory)
                new OracleSourceConfigFactory()
                        .hostname(ORACLE_CONTAINER.getHost())
                        .port(ORACLE_CONTAINER.getOraclePort())
                        .username(ORACLE_CONTAINER.getUsername())
                        .password(ORACLE_CONTAINER.getPassword())
                        .databaseList(ORACLE_DATABASE)
                        .tableList(captureTables)
                        .debeziumProperties(debeziumProperties)
                        .splitSize(splitSize);
    }

    private boolean executeSql(OracleSourceConfig sourceConfig, String[] sqlStatements) {
        JdbcConnection connection =
                createOracleConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig());
        try {
            connection.setAutoCommit(false);
            connection.execute(sqlStatements);
            connection.commit();
        } catch (SQLException e) {
            LOG.error("Failed to execute sql statements.", e);
            return false;
        }
        return true;
    }

    class MakeChangeEventTaskContext extends OracleSourceFetchTaskContext {

        private Supplier<Boolean> makeChangeEventFunction;

        public MakeChangeEventTaskContext(
                JdbcSourceConfig jdbcSourceConfig,
                OracleDialect oracleDialect,
                OracleConnection connection,
                Supplier<Boolean> makeChangeEventFunction) {
            super(jdbcSourceConfig, oracleDialect, connection);
            this.makeChangeEventFunction = makeChangeEventFunction;
        }

        @Override
        public EventDispatcher.SnapshotReceiver getSnapshotReceiver() {
            EventDispatcher.SnapshotReceiver snapshotReceiver = super.getSnapshotReceiver();
            return new EventDispatcher.SnapshotReceiver() {

                @Override
                public void changeRecord(
                        DataCollectionSchema schema,
                        Envelope.Operation operation,
                        Object key,
                        Struct value,
                        OffsetContext offset,
                        ConnectHeaders headers)
                        throws InterruptedException {
                    snapshotReceiver.changeRecord(schema, operation, key, value, offset, headers);
                }

                @Override
                public void completeSnapshot() throws InterruptedException {
                    snapshotReceiver.completeSnapshot();
                    // make change events
                    makeChangeEventFunction.get();
                    Thread.sleep(120 * 1000);
                }
            };
        }
    }
}
