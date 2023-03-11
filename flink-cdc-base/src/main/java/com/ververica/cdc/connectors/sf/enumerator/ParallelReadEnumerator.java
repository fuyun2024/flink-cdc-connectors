package com.ververica.cdc.connectors.sf.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.sf.assigner.ParallelReadSplitAssigner;
import com.ververica.cdc.connectors.sf.events.AllTableStateAckEvent;
import com.ververica.cdc.connectors.sf.events.AllTableStateRequestEvent;
import com.ververica.cdc.connectors.sf.events.BinlogSubTaskIdEvent;
import com.ververica.cdc.connectors.sf.events.BinlogSubTaskIdRequestEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableAckEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableReportEvent;
import com.ververica.cdc.connectors.sf.events.FinishedSnapshotTableRequestEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeAckEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeRequestEvent;
import com.ververica.cdc.connectors.sf.events.TableChangeTypeEnum;
import com.ververica.cdc.connectors.sf.request.bean.AccessTableStatus;
import com.ververica.cdc.connectors.sf.request.bean.CallBackTableChangeBean;
import com.ververica.cdc.connectors.sf.request.bean.TableChangeBean;
import com.ververica.cdc.connectors.sf.request.bean.TableInfo;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 并行度读取所用的枚举器. @Author: created by eHui @Date: 2023/3/2 */
public class ParallelReadEnumerator extends IncrementalSourceEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelReadEnumerator.class);

    // binlog 对应的 subtaskId
    private Integer streamSubtaskId = null;

    // 动态捕获表
    private HttpDynamicTableChange dynamicTableChange;

    // 动态捕获表的上下文
    private DynamicTableChangeContext tableChangeContext;

    // 并行读取分配器
    private ParallelReadSplitAssigner splitAssigner;

    // 回调读取成功
    private List<CallBackTableChangeBean> callBackTableChangeBeans = new LinkedList<>();

    // 第一次启动后，需要回调所有在状态中已经完成的表
    private boolean callbackFirst = true;

    public ParallelReadEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            SourceConfig sourceConfig,
            SplitAssigner splitAssigner) {
        super(context, sourceConfig, splitAssigner);
        this.splitAssigner = (ParallelReadSplitAssigner) splitAssigner;
        JdbcSourceConfig jdbcSourceConfig = (JdbcSourceConfig) sourceConfig;
        tableChangeContext =
                new DynamicTableChangeContext(
                        jdbcSourceConfig.getGetTableChangeUrl(),
                        jdbcSourceConfig.getReportReachBinlogUrl());
    }

    /** 当 binlog 任务分配之后，需要开启表变更扫描线程. */
    private void handleBinlogSubTaskId(BinlogSubTaskIdEvent subTaskIdEvent) {
        if (streamSubtaskId == null) {
            streamSubtaskId = subTaskIdEvent.getSubtaskId();
            LOG.info("收到 stream subtaskId : {}.", streamSubtaskId);

            LOG.info("binlog 任务已经启动,开启表变更扫描线程");
            this.dynamicTableChange = new HttpDynamicTableChange(tableChangeContext);
            this.dynamicTableChange.tableChangeCapture(this::handleTableChangeCapture);
        }
    }

    /** 处理捕获到表变更的逻辑. */
    private void handleTableChangeCapture(List<TableChangeBean> tableChangeBeans) {
        callbackAllFinishedTableForFirst(tableChangeBeans);

        for (TableChangeBean bean : tableChangeBeans) {
            TableId tableId = bean.getTableId();
            if (isProcessedTable(tableId, bean.getStatus())) {
                LOG.debug("已经处理过 table : {}", tableId);
                continue;
            }

            TableChangeRequestEvent requestEvent;
            if (bean.isAddedTable() && bean.isSnapshotSync()) {
                LOG.info("捕获到一张表 table : {} 需要进行全、增量数据同步。(注意:本次调用有可能是重复的,但是不影响数据采集)", tableId);
                requestEvent = TableChangeRequestEvent.asCreateSnapshotTable(tableId);
            } else if (bean.isAddedTable() && bean.isStreamSync()) {
                LOG.info("捕获到一张表 table : {} 需要增量数据同步。(注意:本次调用有可能是重复的,但是不影响数据采集)", tableId);
                requestEvent = TableChangeRequestEvent.asCreateStreamTable(tableId);
            } else if (bean.isDeleteTable()) {
                LOG.info("捕获到一张表 table : {} 删除数据同步。(注意:本次调用有可能是重复的,但是不影响数据采集)", tableId);
                requestEvent = TableChangeRequestEvent.asDeleteTable(tableId);
            } else {
                throw new FlinkRuntimeException("错误的同步类型 : " + bean.getStatus());
            }

            requestEvent.setTableInfo(
                    new TableInfo(bean.getId(), bean.getTopicName(), bean.getEncryptFields()));
            for (int subTaskId : getRegisteredReader()) {
                context.sendEventToSourceReader(subTaskId, requestEvent);
            }
        }
    }

    /** 处理表变更处理完成后的 ACK. */
    private void handleTableChangeAckEvent(TableChangeAckEvent ackEvent) {
        TableId tableId = ackEvent.getTableId();
        if (isProcessedTable(tableId, ackEvent.getTableChangeType())) {
            LOG.debug("已经处理过 table : {}", tableId);
            return;
        }

        TableInfo tableInfo = ackEvent.getTableInfo();
        switch (ackEvent.getTableChangeType()) {
            case CREATE_SNAPSHOT_TABLE:
                {
                    LOG.info("table : {} 开始进行全量读取.", tableId);
                    splitAssigner.addSnapshotTable(tableId, tableInfo);
                    break;
                }
            case CREATE_STREAM_TABLE:
                {
                    LOG.info("table : {} 已经进行 stream 读取.", tableId);
                    splitAssigner.addFinishedProcessedTable(tableId, tableInfo);
                    callBackTableChangeBeans.add(
                            CallBackTableChangeBean.asCreateTable(
                                    tableInfo.getPrimaryKey(), tableId));
                    break;
                }
            case DELETE_TABLE:
                {
                    LOG.info("table : {} 删除表完成.", tableId);
                    splitAssigner.deleteFinishedProcessedStreamTable(tableId);
                    callBackTableChangeBeans.add(
                            CallBackTableChangeBean.asDeleteTable(
                                    tableInfo.getPrimaryKey(), tableId));
                    break;
                }
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof BinlogSubTaskIdEvent) {
            handleBinlogSubTaskId((BinlogSubTaskIdEvent) sourceEvent);
        } else if (sourceEvent instanceof AllTableStateRequestEvent) {
            handleAllTableStateRequestEvent(subtaskId);
        } else if (sourceEvent instanceof TableChangeAckEvent) {
            handleTableChangeAckEvent((TableChangeAckEvent) sourceEvent);
        } else if (sourceEvent instanceof FinishedSnapshotTableRequestEvent) {
            getFinishedSnapshotTable((FinishedSnapshotTableRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof FinishedSnapshotTableAckEvent) {
            FinishedSnapshotTableAckEvent event = (FinishedSnapshotTableAckEvent) sourceEvent;
            for (TableId tableId : event.getFinishedTableIds()) {
                LOG.info("table : {} 全量读取完成,并且已经进行到 stream 采集阶段.", tableId);
                Map<TableId, TableInfo> tableInfos = splitAssigner.getTableInfos();
                Long primaryKey = tableInfos.get(tableId).getPrimaryKey();
                callBackTableChangeBeans.add(
                        CallBackTableChangeBean.asCreateTable(primaryKey, tableId));
            }
        } else {
            super.handleSourceEvent(subtaskId, sourceEvent);
        }
    }

    /** 第一次启动的时候，需要回调所有已完成的表(不包括要删除的表). */
    private void callbackAllFinishedTableForFirst(List<TableChangeBean> tableChangeBeans) {
        if (callbackFirst) {
            for (TableChangeBean bean : tableChangeBeans) {
                TableId tableId = bean.getTableId();
                if (splitAssigner.isFinishedProcessedTable(tableId) && bean.isAddedTable()) {
                    // 新增表，已经在处理中，需要回调
                    callBackTableChangeBeans.add(
                            CallBackTableChangeBean.asCreateTable(bean.getId(), tableId));
                } else if (!splitAssigner.isFinishedProcessedTable(tableId)
                        && bean.isDeleteTable()) {
                    // 需要删除，并且不再已完成的表中，说明已经被删除过了。也需要回调
                    callBackTableChangeBeans.add(
                            CallBackTableChangeBean.asCreateTable(bean.getId(), tableId));
                }
            }
            callbackTableChangeBeans();
        }

        // 第一次汇报完成之后就不需要再汇报了
        callbackFirst = false;
    }

    private void callbackTableChangeBeans() {
        // 回调直通车表变更完成.
        if (dynamicTableChange != null) {
            dynamicTableChange.tableChangeCallback(callBackTableChangeBeans);
        }
    }

    /** 处理所有表状态请求事件. */
    private void handleAllTableStateRequestEvent(int subtaskId) {
        context.sendEventToSourceReader(
                subtaskId,
                new AllTableStateAckEvent(
                        splitAssigner.getTableInfos(), splitAssigner.getBinlogStateTables()));
    }

    /** 获取已经全量快照完成的列表. */
    private void getFinishedSnapshotTable(FinishedSnapshotTableRequestEvent event) {
        List finishedSnapshotTables =
                splitAssigner.isFinishedSnapshotTables(
                        event.getUnFinishedTableIds(), event.getProcessOffset());
        if (finishedSnapshotTables != null && finishedSnapshotTables.size() > 0) {
            context.sendEventToSourceReader(
                    streamSubtaskId,
                    new FinishedSnapshotTableReportEvent(event.getUnFinishedTableIds()));
        }
    }

    /** 判断表是否已经处理过了. */
    private boolean isProcessedTable(TableId tableId, TableChangeTypeEnum typeEnum) {
        if (typeEnum.isAddedTable() && splitAssigner.isAddedTable(tableId)) {
            // 是新增表, 并且已经处理过的表，才需要过滤
            return true;
        } else if (typeEnum.isDeletedTable() && !splitAssigner.isAddedTable(tableId)) {
            // 是一张删除表，并且已经不再已完成的列表中，说明已经处理过了，需要过滤
            return true;
        } else {
            return false;
        }
    }

    /** 判断表是否已经处理过了. */
    private boolean isProcessedTable(TableId tableId, AccessTableStatus status) {
        if (status.isAddedTable() && splitAssigner.isAddedTable(tableId)) {
            // 是新增表, 并且已经处理过的表，才需要过滤
            return true;
        } else if (status.isDeletedTable() && !splitAssigner.isAddedTable(tableId)) {
            // 是一张删除表，并且已经不再已完成的列表中，说明已经处理过了，需要过滤
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        super.notifyCheckpointComplete(checkpointId);
        callbackTableChangeBeans();
    }

    @Override
    public void syncWithReaders(int[] subtaskIds, Throwable t) {
        super.syncWithReaders(subtaskIds, t);

        // send sub task id request
        if (streamSubtaskId == null) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(subtaskId, new BinlogSubTaskIdRequestEvent());
            }
        }
    }

    @Override
    public void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            Optional<SourceSplitBase> split = splitAssigner.parallelReadGetNext();
            if (split.isPresent()) {
                final SourceSplitBase sourceSplit = split.get();
                context.assignSplit(sourceSplit, nextAwaiting);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", sourceSplit, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                break;
            }
        }
    }

    @Override
    public void close() {
        super.close();
        tableChangeContext.close();
    }
}
