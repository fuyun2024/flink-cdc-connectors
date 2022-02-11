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

package com.ververica.cdc.connectors.mysql.source.reader;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.*;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.sf.KafkaDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.sf.NewTableBean;
import com.ververica.cdc.connectors.mysql.source.split.*;
import com.ververica.cdc.connectors.mysql.source.utils.HttpUtils;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils.getNextMetaGroupId;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The source reader for MySQL source splits.
 */
public class MySqlSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
        SourceRecord, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private static final long NEW_TABLE_SCAN_INTERVAL = 30_000L;

    private final MySqlSourceConfig sourceConfig;
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final int subtaskId;

    private MySqlBinlogSplit currentBinlogSplit;
    private ExecutorService executor;

    public MySqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecord>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, MySqlSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            MySqlSourceConfig sourceConfig) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context);
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected MySqlSplitState initializedState(MySqlSplit split) {
        if (split.isSnapshotSplit()) {
            return new MySqlSnapshotSplitState(split.asSnapshotSplit());
        } else {
            currentBinlogSplit = split.asBinlogSplit();
            scanNewTableByHttp();
            return new MySqlBinlogSplitState(split.asBinlogSplit());
        }
    }

    @Override
    public List<MySqlSplit> snapshotState(long checkpointId) {
        // unfinished splits
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnackedSplits.values());

        // add binlog splits who are uncompleted
        stateSplits.addAll(uncompletedBinlogSplits.values());

        return stateSplits;
    }

    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
            MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
            checkState(
                    mySqlSplit.isSnapshotSplit(),
                    String.format(
                            "Only snapshot split could finish, but the actual split is binlog split %s",
                            mySqlSplit));
            finishedUnackedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
        }
        reportFinishedSnapshotSplitsIfNeed();
        context.sendSplitRequest();
    }

    @Override
    public void addSplits(List<MySqlSplit> splits) {
        // restore for finishedUnackedSplits
        List<MySqlSplit> unfinishedSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                // the binlog split is uncompleted
                if (!split.asBinlogSplit().isCompletedSplit()) {
                    uncompletedBinlogSplits.put(split.splitId(), split.asBinlogSplit());
                    requestBinlogSplitMetaIfNeeded(split.asBinlogSplit());
                } else {
                    uncompletedBinlogSplits.remove(split.splitId());
                    MySqlBinlogSplit mySqlBinlogSplit =
                            discoverTableSchemasForBinlogSplit(split.asBinlogSplit());
                    unfinishedSplits.add(mySqlBinlogSplit);
                    currentBinlogSplit = mySqlBinlogSplit;
                    scanNewTableByHttp();
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        super.addSplits(unfinishedSplits);
    }

    private MySqlBinlogSplit discoverTableSchemasForBinlogSplit(MySqlBinlogSplit split) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty() || split.isBinlogSplit()) {
            // 需要获取全部的 binlog

            sourceConfig.getDbzProperties().setProperty("database.include.list", ".*");
            sourceConfig.getDbzProperties().setProperty("table.include.list", ".*");

            sourceConfig.setDbzConfiguration(
                    io.debezium.config.Configuration.from(sourceConfig.getDbzProperties()));
            sourceConfig.setDbzMySqlConfig(
                    new MySqlConnectorConfig(sourceConfig.getDbzConfiguration()));

            try (MySqlConnection jdbc =
                         DebeziumUtils.createMySqlConnection(sourceConfig.getDbzConfiguration())) {
                Map<TableId, TableChanges.TableChange> tableSchemas =
                        TableDiscoveryUtils.discoverCapturedTableSchemas(sourceConfig, jdbc);
                LOG.info("The table schema discovery for binlog split {} success", splitId);
                return MySqlBinlogSplit.fillTableSchemas(split, tableSchemas);
            } catch (SQLException e) {
                LOG.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "The binlog split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((BinlogSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForBinlogSplit((BinlogSplitMetaEvent) sourceEvent);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
            for (MySqlSnapshotSplit split : finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    private void requestBinlogSplitMetaIfNeeded(MySqlBinlogSplit binlogSplit) {
        final String splitId = binlogSplit.splitId();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of binlog split {} has been collected success", splitId);
            this.addSplits(Arrays.asList(binlogSplit));
        }
    }

    private void fillMetaDataForBinlogSplit(BinlogSplitMetaEvent metadataEvent) {
        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> metaDataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(FinishedSnapshotSplitInfo::deserialize)
                                .collect(Collectors.toList());
                uncompletedBinlogSplits.put(
                        binlogSplit.splitId(),
                        MySqlBinlogSplit.appendFinishedSplitInfos(binlogSplit, metaDataGroup));

                LOG.info("Fill meta data of group {} to binlog split", metaDataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(binlogSplit);
        } else {
            LOG.warn(
                    "Received binlog meta event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }


    public void scanNewTableByHttp() {
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("scan-new-table").build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);

        executor.submit(
                () -> {
                    try {
                        while (currentBinlogSplit != null) {
                            LOG.debug("扫描获取新增的表信息");

                            List<NewTableBean> newTableBeans = HttpUtils.requestAddedTable();
                            newTableBeans.forEach(bean -> LOG.info("扫描到一张表 : " + bean.getDbName() + "." + bean.getTableName()));
                            handleNewTable(newTableBeans);

                            Thread.sleep(NEW_TABLE_SCAN_INTERVAL);
                        }
                    } catch (Exception e) {
                        // not stop
                        LOG.error("扫描获取新增的表信息失败", e);
                    }
                });
    }

    /**
     * 处理新增表事件
     *
     * @param newTableBeans
     */
    private void handleNewTable(List<NewTableBean> newTableBeans) {
        if (newTableBeans != null && newTableBeans.size() > 0) {
            // 把 kafka 配置信息写入到 schema 中
            writerKafkaConfigToSchema(newTableBeans);

            Set<String> tableIds = newTableBeans.stream()
                    .map(bean -> bean.getDbName() + "." + bean.getTableName())
                    .collect(Collectors.toSet());

            // 添加到上下文中
            currentBinlogSplit.getAddedTableContext().addTableIfNotExist(tableIds);
        }

        // callbackAddedTableSuccess
        callbackAddedTableSuccess();
    }


    /**
     * 把 kafka 配置信息写入到 schema 中
     *
     * @param newTableBeans
     */
    private void writerKafkaConfigToSchema(List<NewTableBean> newTableBeans) {
        Map<String, NewTableBean> tableBeanMap = new HashMap<>();
        for (NewTableBean bean : newTableBeans) {
            tableBeanMap.put(bean.getDbName() + "." + bean.getTableName(), bean);
        }

        DebeziumDeserializationSchema deserializationSchema = ((MySqlRecordEmitter) recordEmitter).getDebeziumDeserializationSchema();
        if (deserializationSchema instanceof KafkaDeserializationSchema) {
            ((KafkaDeserializationSchema) deserializationSchema).addTableBeanMap(tableBeanMap);
        }
    }


    /**
     * 回调新增表成功
     */
    private void callbackAddedTableSuccess() {
        if (currentBinlogSplit.getAddedTableContext().hasUnReportTables()) {
            Map<String, String> unReportTables =
                    currentBinlogSplit.getAddedTableContext().getUnReportTables();

            // 回调添加事件
            HttpUtils.callbackTable(unReportTables);

            // 回调成功，删除数据
            currentBinlogSplit
                    .getAddedTableContext()
                    .ackUnReportTable(unReportTables);
        }
    }


}
