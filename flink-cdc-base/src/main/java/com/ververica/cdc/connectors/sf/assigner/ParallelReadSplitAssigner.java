package com.ververica.cdc.connectors.sf.assigner;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.sf.assigner.state.ParallelReadPendingSplitsState;
import com.ververica.cdc.connectors.sf.request.bean.TableInfo;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 并行读写分割器. @Author: created by eHui @Date: 2023/3/6 */
public class ParallelReadSplitAssigner<C extends SourceConfig> extends SnapshotSplitAssigner<C> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelReadSplitAssigner.class);

    private boolean isStreamSplitAssigned;
    private final List<TableId> finishedProcessedTables;
    private final Map<TableId, Offset> tableHighWatermark;
    private final Map<TableId, TableInfo> tableInfos;

    // 缓存正在处理的 split
    private Map<TableId, List<String>> cacheProcessingSplits;

    public ParallelReadSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        super(
                sourceConfig,
                currentParallelism,
                remainingTables,
                isTableIdCaseSensitive,
                dialect,
                offsetFactory);
        this.finishedProcessedTables = new LinkedList<>();
        this.tableInfos = new HashMap();
        this.tableHighWatermark = new HashMap();
    }

    public ParallelReadSplitAssigner(
            C sourceConfig,
            int currentParallelism,
            ParallelReadPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        super(sourceConfig, currentParallelism, checkpoint, dialect, offsetFactory);
        this.isStreamSplitAssigned = checkpoint.isStreamSplitAssigned();
        this.finishedProcessedTables = checkpoint.getFinishedProcessedTables();
        this.tableInfos = checkpoint.getTableInfos();
        this.tableHighWatermark = checkpoint.getTableHighWatermark();
    }

    @Override
    public void open() {
        this.chunkSplitter = dialect.createChunkSplitter(sourceConfig);
        this.isTableIdCaseSensitive = dialect.isDataCollectionIdCaseSensitive(sourceConfig);
        initProcessingSplits();
    }

    /** 这里没有重写父类的 getNext() 方法，是因为 父类的方法中有递归调用，会调用会重写的方法，导致逻辑异常. */
    public Optional<SourceSplitBase> parallelReadGetNext() {
        // 第一次的时候，就需要把 binlog 任务分配出去
        if (!isStreamSplitAssigned) {
            LOG.info("创建 stream split 任务");
            isStreamSplitAssigned = true;
            return Optional.of(createStreamSplit());
        } else {
            // 后续都只分配全量读取
            Optional<SourceSplitBase> splitBase = super.getNext();
            if (splitBase.isPresent()) {
                final SnapshotSplit snapshotSplit = splitBase.get().asSnapshotSplit();
                TableId tableId = snapshotSplit.getTableId();
                // 正在处理中的 split
                cacheProcessingSplits
                        .computeIfAbsent(tableId, k -> new ArrayList<>())
                        .add(snapshotSplit.splitId());
            }
            return splitBase;
        }
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        Collection<SourceSplitBase> splitList = splits;
        List<SourceSplitBase> snapshotSplits = new ArrayList<>();
        for (SourceSplitBase split : splitList) {
            if (split.isSnapshotSplit()) {
                cacheProcessingSplits
                        .get(split.asSnapshotSplit().getTableId())
                        .remove(split.splitId());
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create stream split later
                isStreamSplitAssigned = false;
            }
        }
        super.addSplits(snapshotSplits);
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        return ParallelReadPendingSplitsState.asState(
                super.snapshotState(checkpointId),
                isStreamSplitAssigned,
                finishedProcessedTables,
                tableInfos,
                tableHighWatermark);
    }

    @Override
    public void onFinishedSplits(Map<String, Offset> splitFinishedOffsets) {
        super.onFinishedSplits(splitFinishedOffsets);
        removeFinishedTableIds(splitFinishedOffsets);
        addTableHighWatermark(splitFinishedOffsets);
    }

    public Map<TableId, TableInfo> getTableInfos() {
        return tableInfos;
    }

    public List<TableId> getNeedBinlogStateTables() {
        List<TableId> binlogStateTables = new ArrayList<>(tableInfos.keySet());
        binlogStateTables.removeAll(finishedProcessedTables);
        return binlogStateTables;
    }

    /** 添加需要全量读取的表. */
    public void addSnapshotTable(TableId tableId, TableInfo tableInfo) {
        tableInfos.put(tableId, tableInfo);
        this.remainingTables.add(tableId);
    }

    /** 添加到已处理完成的列表中. */
    public void addFinishedProcessedTable(TableId tableId) {
        finishedProcessedTables.add(tableId);
    }

    /** 添加到已处理完成的列表中. */
    public void addFinishedProcessedTable(TableId tableId, TableInfo tableInfo) {
        tableInfos.put(tableId, tableInfo);
        finishedProcessedTables.add(tableId);
    }

    /** 从已经处理完成的表中删除. */
    public void deleteFinishedProcessedStreamTable(TableId tableId) {
        tableInfos.remove(tableId);
        finishedProcessedTables.removeIf(v -> v.equals(tableId) ? true : false);
        // todo 还有 split 中的信息也需要删除
    }

    /** 判断表是否已经新增. */
    public boolean isProcessedTable(TableId tableId) {
        return tableInfos.get(tableId) != null;
    }

    /** 判断表是否已经完成处理. */
    public boolean isFinishedProcessedTable(TableId tableId) {
        return finishedProcessedTables.contains(tableId);
    }

    /** 获取表全量读取是否完成. */
    public List<TableId> isFinishedSnapshotTables(
            List<TableId> todoFinishTableIds, Offset processOffset) {
        List<TableId> intersection = new ArrayList<>(todoFinishTableIds);
        intersection.retainAll(finishedProcessedTables);

        // 检查 binlog 的水位是否大于表的最高水位。
        intersection.removeIf(
                tableId ->
                        // 移除 binlog task 进行的水位是在当前最高水位之前.
                        processOffset.isBefore(tableHighWatermark.get(tableId)));
        return intersection;
    }

    private void initProcessingSplits() {
        cacheProcessingSplits = new HashMap<>();
        assignedSplits
                .entrySet()
                .forEach(
                        entry -> {
                            if (!splitFinishedOffsets.containsKey(entry.getKey())) {
                                // 已经被分配类，但是又不包含在已完成的列表中，说明这个 split 正在处理中.
                                cacheProcessingSplits
                                        .computeIfAbsent(
                                                entry.getValue().getTableId(),
                                                k -> new ArrayList<>())
                                        .add(entry.getKey());
                            }
                        });
    }

    private void removeFinishedTableIds(Map<String, Offset> splitFinishedOffsets) {
        splitFinishedOffsets
                .entrySet()
                .forEach(
                        entry -> {
                            String splitId = entry.getKey();
                            TableId finishedTableId = assignedSplits.get(splitId).getTableId();
                            List<String> splitIdList = cacheProcessingSplits.get(finishedTableId);

                            // 当 tm 失败之后，然后重新恢复任务， splitIdList 中的数据有可能为空，因此需要重新初始化
                            if (splitIdList.isEmpty()) {
                                initProcessingSplits();
                                splitIdList = cacheProcessingSplits.get(finishedTableId);
                            }

                            splitIdList.remove(splitId);
                            if (splitIdList.isEmpty()
                                    && !isContainsRemainingSplits(finishedTableId)) {
                                // 正在处理中 与 剩余的 split 都没有包含已完成的表，那么就将这个表移除。
                                addFinishedProcessedTable(finishedTableId);
                                cacheProcessingSplits.remove(finishedTableId);
                            }
                        });
    }

    private void addTableHighWatermark(Map<String, Offset> splitFinishedOffsets) {
        splitFinishedOffsets
                .entrySet()
                .forEach(
                        entry -> {
                            String splitId = entry.getKey();
                            TableId tableId = assignedSplits.get(splitId).getTableId();
                            Offset hwOffset = tableHighWatermark.get(tableId);
                            Offset finishedOffset = entry.getValue();
                            if (hwOffset == null) {
                                tableHighWatermark.put(tableId, finishedOffset);
                            } else if (finishedOffset.isAfter(hwOffset)) {
                                tableHighWatermark.put(tableId, finishedOffset);
                            }
                        });
    }

    private boolean isContainsRemainingSplits(TableId tableId) {
        return remainingSplits.stream()
                .anyMatch(snapshotSplit -> tableId.equals(snapshotSplit.getTableId()));
    }

    public StreamSplit createStreamSplit() {

        return new StreamSplit(
                "stream-split",
                dialect.displayCurrentOffset(sourceConfig),
                offsetFactory.createNoStoppingOffset(),
                new ArrayList<>(),
                new HashMap<>(),
                0);
    }
}
