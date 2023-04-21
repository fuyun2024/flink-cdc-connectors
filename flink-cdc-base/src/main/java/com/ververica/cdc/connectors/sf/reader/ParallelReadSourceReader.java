package com.ververica.cdc.connectors.sf.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import com.ververica.cdc.connectors.sf.deserialization.TableStateAware;
import com.ververica.cdc.connectors.sf.entity.TableChange;
import com.ververica.cdc.connectors.sf.entity.TableInfo;
import com.ververica.cdc.connectors.sf.events.AllTableStateAckEvent;
import com.ververica.cdc.connectors.sf.events.AllTableStateRequestEvent;
import com.ververica.cdc.connectors.sf.events.BinlogSubTaskIdEvent;
import com.ververica.cdc.connectors.sf.events.BinlogSubTaskIdRequestEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableAckEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableReportEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableRequestEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeAckEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeRequestEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** 并行读取 source reader. */
public class ParallelReadSourceReader<T, C extends SourceConfig>
        extends IncrementalSourceReader<T, C> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelReadSourceReader.class);

    private boolean isBinlogTask = false;
    private StreamSplit streamSplit;
    private RecordEmitter recordEmitter;
    private TableStateAware tableStateAware;
    private RestartStreamTaskSupplier restartStreamTaskSupplier;

    public ParallelReadSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            SourceReaderContext context,
            C sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect,
            TableStateAware tableStateAware,
            RestartStreamTaskSupplier restartStreamTaskSupplier) {
        super(
                elementQueue,
                supplier,
                recordEmitter,
                config,
                context,
                sourceConfig,
                sourceSplitSerializer,
                dialect);
        this.recordEmitter = recordEmitter;
        this.tableStateAware = tableStateAware;
        this.restartStreamTaskSupplier = restartStreamTaskSupplier;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        if (!tableStateAware.isReady()) {
            // 表状态没有准备好，那么在此请求。
            requestAllTableState();
        }
        requestFinishedSnapshotTable();
    }

    @Override
    public void addSplits(List<SourceSplitBase> splits) {
        if (splits.size() > 1) {
            throw new FlinkRuntimeException("cdc 读取的过程中，只能同时处理一个 split。");
        }

        SourceSplitBase split = splits.get(0);
        if (split.isSnapshotSplit()) {
            TableId tableId = split.asSnapshotSplit().getTableId();
            // 调用父类的方法
            super.addSplits(Arrays.asList(split));

            if (tableStateAware.getNeedProcessedTable(tableId) != null) {
                // 提前跳出。
                return;
            }
        } else {
            streamSplit = split.asStreamSplit();
            // 调用爷爷类的方法
            superAddSplit(splits);
        }
        // 1、增量读取
        // 2、全量读取，发现表不在
        requestAllTableState();
        tableStateAware.setReady(false);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof BinlogSubTaskIdRequestEvent) {
            reportBinlogSubTaskId();
        } else if (sourceEvent instanceof AllTableStateAckEvent) {
            handleAllTableStatus((AllTableStateAckEvent) sourceEvent);
        } else if (sourceEvent instanceof TableChangeRequestEvent) {
            handleTableChange((TableChangeRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof FinishedSnapshotTableReportEvent) {
            FinishedSnapshotTableReportEvent event = (FinishedSnapshotTableReportEvent) sourceEvent;
            for (TableId tableId : event.getFinishedTableIds()) {
                LOG.info("收到一张全量读取完成的表 table : {} ,即将把这张表从 BINLOG_STATE 状态切换成 BINLOG 状态.", tableId);
                tableStateAware.removeBinlogStateTable(tableId);

                LOG.info("向下游发送 table : {} 表已经全量读取完成的记录.", tableId);
                outputTableFinishedRecord(tableId);
            }
            context.sendSourceEventToCoordinator(
                    new FinishedSnapshotTableAckEvent(event.getFinishedTableIds()));
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void handleTableChange(TableChangeRequestEvent sourceEvent) {
        if (isBinlogTask) {
            streamTaskHandleTableChange(sourceEvent);
        } else {
            snapshotTaskHandleTableChange(sourceEvent);
        }
    }

    private void snapshotTaskHandleTableChange(TableChangeRequestEvent event) {
        List<TableChange> tableChanges = event.getTableChangeInfoList();
        for (TableChange tableChange : tableChanges) {
            TableId tableId = tableChange.getTableId();
            TableInfo tableInfo = tableChange.getTableInfo();
            LOG.debug("全量读取任务收到一张表新增 table : {} ", tableId);
            tableStateAware.addProcessedTableId(tableId, tableInfo);
            tableStateAware.setReady(true);
        }
    }

    private void streamTaskHandleTableChange(TableChangeRequestEvent event) {
        List<TableChange> tableChanges = event.getTableChangeInfoList();
        boolean hasTableChange = false;
        for (TableChange tableChange : tableChanges) {
            TableId tableId = tableChange.getTableId();
            TableInfo tableInfo = tableChange.getTableInfo();
            if (tableChange.getTableChangeType().isAddedTable()
                    && tableStateAware.getNeedProcessedTable(tableId) != null) {
                LOG.debug("subTask : {} 收到表 table : {} 已经在处理中", subtaskId, tableId);
                // 在处理中，是避免 jm 和 tm 状态不一致。
                continue;
            }

            hasTableChange = true;
            switch (tableChange.getTableChangeType()) {
                case CREATE_SNAPSHOT_TABLE:
                    {
                        LOG.info("subTask : {} 收到一张全增量读取表 table : {} ", subtaskId, tableId);
                        tableStateAware.addBinlogStateTable(tableId, tableInfo);
                        addTableToStreamSplit(tableId);
                        break;
                    }
                case CREATE_STREAM_TABLE:
                    {
                        if (isBinlogTask) {
                            LOG.info(
                                    "subTask : {} 收到一张只读 stream 流的表 table : {} ",
                                    subtaskId,
                                    tableId);
                            tableStateAware.addProcessedTableId(tableId, tableInfo);
                            addTableToStreamSplit(tableId);
                        }
                        break;
                    }
                case DELETE_TABLE:
                    {
                        LOG.info("subTask : {} 收到一张需要删除的表 table : {} ", subtaskId, tableId);
                        tableStateAware.removeTable(tableId);
                        removeTableToStreamSplit(tableId);
                        break;
                    }
            }
        }

        if (hasTableChange) {
            LOG.info("发现有表变更，重启 stream 任务。");
            restartStreamTaskSupplier.restartBinlogReaderTask();
        }

        LOG.info("返回表新增成功信息给 enumerator。");
        context.sendSourceEventToCoordinator(new TableChangeAckEvent(tableChanges));
    }

    private void addTableToStreamSplit(TableId tableId) {
        TableChanges.TableChange tableChange = dialect.discoverTableSchemas(tableId);
        streamSplit.getTableSchemas().put(tableId, tableChange);
    }

    private void removeTableToStreamSplit(TableId tableId) {
        streamSplit.getTableSchemas().remove(tableId);
    }

    private void handleAllTableStatus(AllTableStateAckEvent sourceEvent) {
        Map<TableId, TableInfo> tableInfos = sourceEvent.getTableInfos();
        List<TableId> needBinlogStates = sourceEvent.needBinlogStateTableIds();
        tableStateAware.initAllState(tableInfos, needBinlogStates);
        tableStateAware.setReady(true);
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isStreamSplit()) {
            LOG.info("正在初始化 binlog task 任务状态。");
            isBinlogTask = true;
            reportBinlogSubTaskId();
        }

        return super.initializedState(split);
    }

    /** 回报 binlogTask Id. */
    private void reportBinlogSubTaskId() {
        if (isBinlogTask) {
            LOG.info("上报 binlog taskId 给 enumerator.");
            BinlogSubTaskIdEvent binlogSubTaskIdEvent = new BinlogSubTaskIdEvent(subtaskId);
            context.sendSourceEventToCoordinator(binlogSubTaskIdEvent);
        }
    }

    /** 获取全部的表状态信息. */
    private void requestAllTableState() {
        LOG.info("subTask : {} 获取全部的表状态信息。", subtaskId);
        context.sendSourceEventToCoordinator(new AllTableStateRequestEvent());
    }

    /** 获取表全量采集是否完成. */
    private void requestFinishedSnapshotTable() {
        if (isBinlogTask) {
            List<TableId> binlogStateTables = tableStateAware.getBinlogStateTables();
            if (binlogStateTables != null && binlogStateTables.size() > 0) {
                LOG.info("请求 enumerator 如下列表是否已经完成全量读取 : {} ", binlogStateTables);
                context.sendSourceEventToCoordinator(
                        new FinishedSnapshotTableRequestEvent(
                                binlogStateTables, getProcessOffset()));
            }
        }
    }

    /** 获取已经下发处理的 offset. */
    private Offset getProcessOffset() {
        Offset currentOffset = tableStateAware.getCurrentOffset();
        if (currentOffset != null) {
            return currentOffset;
        } else {
            return dialect.displayCurrentOffset(sourceConfig);
        }
    }

    /** 向下游发送表已经全量读取完成的记录. */
    private void outputTableFinishedRecord(TableId tableId) {
        IncrementalSourceRecordEmitter.OutputCollector outputCollector =
                ((IncrementalSourceRecordEmitter) recordEmitter).getOutputCollector();
        outputCollector.collect(TableFinishedRecord.buildTableFinishedRecord((tableId)));
    }
}
