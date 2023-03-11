package com.ververica.cdc.connectors.sf.assigner.state;

import com.ververica.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import com.ververica.cdc.connectors.sf.request.bean.TableInfo;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 并行读状态的保存. @Author: created by eHui @Date: 2023/3/10 */
public class ParallelReadPendingSplitsState extends SnapshotPendingSplitsState {

    private final boolean isStreamSplitAssigned;
    private final List<TableId> finishedProcessedTables;
    private final Map<TableId, Offset> tableHighWatermark;
    private final Map<TableId, TableInfo> tableInfos;

    private ParallelReadPendingSplitsState(
            List<TableId> alreadyProcessedTables,
            List<SchemalessSnapshotSplit> remainingSplits,
            Map<String, SchemalessSnapshotSplit> assignedSplits,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            Map<String, Offset> splitFinishedOffsets,
            boolean isAssignerFinished,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            boolean isStreamSplitAssigned,
            List<TableId> finishedProcessedTables,
            Map<TableId, TableInfo> tableInfos,
            Map<TableId, Offset> tableHighWatermark) {
        super(
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                tableSchemas,
                splitFinishedOffsets,
                isAssignerFinished,
                remainingTables,
                isTableIdCaseSensitive,
                isRemainingTablesCheckpointed);
        this.isStreamSplitAssigned = isStreamSplitAssigned;
        this.finishedProcessedTables = finishedProcessedTables;
        this.tableInfos = tableInfos;
        this.tableHighWatermark = tableHighWatermark;
    }

    public static ParallelReadPendingSplitsState asState(
            SnapshotPendingSplitsState state,
            boolean isStreamSplitAssigned,
            List<TableId> finishedProcessedTables,
            Map<TableId, TableInfo> tableInfos,
            Map<TableId, Offset> tableHighWatermark) {
        return new ParallelReadPendingSplitsState(
                state.getAlreadyProcessedTables(),
                state.getRemainingSplits(),
                state.getAssignedSplits(),
                state.getTableSchemas(),
                state.getSplitFinishedOffsets(),
                state.isAssignerFinished(),
                state.getRemainingTables(),
                state.isTableIdCaseSensitive(),
                state.isRemainingTablesCheckpointed(),
                isStreamSplitAssigned,
                finishedProcessedTables,
                tableInfos,
                tableHighWatermark);
    }

    public boolean isStreamSplitAssigned() {
        return isStreamSplitAssigned;
    }

    public List<TableId> getFinishedProcessedTables() {
        return finishedProcessedTables;
    }

    public Map<TableId, TableInfo> getTableInfos() {
        return tableInfos;
    }

    public Map<TableId, Offset> getTableHighWatermark() {
        return tableHighWatermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParallelReadPendingSplitsState that = (ParallelReadPendingSplitsState) o;
        return super.equals(o)
                && Objects.equals(isStreamSplitAssigned, that.isStreamSplitAssigned)
                && Objects.equals(finishedProcessedTables, that.finishedProcessedTables)
                && Objects.equals(tableInfos, that.tableInfos)
                && Objects.equals(tableHighWatermark, that.tableHighWatermark);
    }

    @Override
    public String toString() {
        return "ParallelReadPendingSplitsState{"
                + "isStreamSplitAssigned="
                + isStreamSplitAssigned
                + ", finishedProcessedTables="
                + finishedProcessedTables
                + ", tableInfos="
                + tableInfos
                + ", tableHighWatermark="
                + tableHighWatermark
                + "} "
                + super.toString();
    }
}
