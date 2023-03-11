package com.ververica.cdc.connectors.sf.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import com.ververica.cdc.connectors.sf.deserialization.TableStateAware;
import com.ververica.cdc.connectors.sf.events.AllTableStateAckEvent;
import com.ververica.cdc.connectors.sf.events.AllTableStateRequestEvent;
import com.ververica.cdc.connectors.sf.events.BinlogSubTaskIdEvent;
import com.ververica.cdc.connectors.sf.events.BinlogSubTaskIdRequestEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableAckEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableReportEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableRequestEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeAckEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeRequestEvent;
import com.ververica.cdc.connectors.sf.request.bean.TableInfo;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** 并行读取 source reader. */
public class ParallelReadSourceReader<T, C extends SourceConfig>
        extends IncrementalSourceReader<T, C> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelReadSourceReader.class);

    private boolean isBinlogTask = false;
    private RecordEmitter recordEmitter;
    private TableStateAware tableStateAware;

    public ParallelReadSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            SourceReaderContext context,
            C sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect,
            TableStateAware tableStateAware) {
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
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                TableId tableId = split.asSnapshotSplit().getTableId();
                if (tableStateAware.getNeedProcessedTable(tableId) != null) {
                    // 跳过已经添加过的表
                    continue;
                }
            }

            // 1、增量读取
            // 2、全量读取，发现表不在
            tableStateAware.setReady(false);
            requestAllTableState();
        }
        super.addSplits(splits);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof BinlogSubTaskIdRequestEvent) {
            reportBinlogSubTaskId();
        } else if (sourceEvent instanceof AllTableStateAckEvent) {
            handleAllTableState((AllTableStateAckEvent) sourceEvent);
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

    public void handleTableChange(TableChangeRequestEvent event) {
        TableId tableId = event.getTableId();
        TableInfo tableInfo = event.getTableInfo();
        TableChangeAckEvent ackEvent = null;
        switch (event.getTableChangeType()) {
            case CREATE_SNAPSHOT_TABLE:
                {
                    LOG.info("subTask : {} 收到一张全增量读取表 table : {} ", subtaskId, tableId);
                    tableStateAware.addBinlogStateTable(tableId, tableInfo.getTopicName());
                    ackEvent = TableChangeAckEvent.asCreateSnapshotTable(tableId);
                    break;
                }
            case CREATE_STREAM_TABLE:
                {
                    if (isBinlogTask) {
                        LOG.info("subTask : {} 收到一张只读 stream 流的表 table : {} ", subtaskId, tableId);
                        tableStateAware.addProcessedTableId(tableId, tableInfo.getTopicName());
                        ackEvent = TableChangeAckEvent.asCreateStreamTable(tableId);
                    }
                    break;
                }
            case DELETE_TABLE:
                {
                    LOG.info("subTask : {} 收到一张需要删除的表 table : {} ", subtaskId, tableId);
                    tableStateAware.removeTable(tableId);
                    ackEvent = TableChangeAckEvent.asDeleteTable(tableId);
                    break;
                }
        }

        // 只有 binlog 任务需要回报，全量任务只接收信息，不需要回报。
        if (isBinlogTask) {
            ackEvent.setTableInfo(event.getTableInfo());
            context.sendSourceEventToCoordinator(ackEvent);
        }
    }

    private void handleAllTableState(AllTableStateAckEvent sourceEvent) {
        Map<TableId, TableInfo> tableInfos = sourceEvent.getTableInfos();
        List<TableId> needBinlogStates = sourceEvent.needBinlogStateTableIds();
        tableInfos.forEach((k, v) -> tableStateAware.addProcessedTableId(k, v.getTopicName()));
        needBinlogStates.forEach(
                v -> tableStateAware.addBinlogStateTable(v, tableInfos.get(v).getTopicName()));
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
