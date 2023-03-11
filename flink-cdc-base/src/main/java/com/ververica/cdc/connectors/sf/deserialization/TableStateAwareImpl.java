package com.ververica.cdc.connectors.sf.deserialization;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.sf.request.bean.TableInfo;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 表状态感知实现类，这个类定义了哪些表需要处理，那些表需要加上 BINLOG_STATE 标签. */
public class TableStateAwareImpl implements TableStateAware {

    private static final Logger LOG = LoggerFactory.getLogger(TableStateAwareImpl.class);

    private Offset currentOffset;

    private boolean isReady = false;

    /** 需要处理下发的表，不在表中的数据，会被直接过滤掉. */
    private final Map<TableId, String> tableTopicNames = new HashMap<>();

    /** 需要加上 binlog_state 表. */
    private final Map<TableId, Boolean> needBinlogStateTableIds = new HashMap<>();

    @Override
    public String getNeedProcessedTable(TableId tableId) {
        return tableTopicNames.get(tableId);
    }

    @Override
    public boolean isNeedAddStateTable(TableId tableId) {
        return needBinlogStateTableIds.get(tableId) != null;
    }

    @Override
    public List<TableId> getBinlogStateTables() {
        return new ArrayList(needBinlogStateTableIds.keySet());
    }

    @Override
    public void setCurrentOffset(Offset offset) {
        currentOffset = offset;
    }

    @Override
    public Offset getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public void setReady(boolean ready) {
        this.isReady = ready;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public void initAllState(Map<TableId, TableInfo> tableInfos, List<TableId> needBinlogStates) {
        if (tableInfos.size() == 0) {
            return;
        }

        LOG.info("初始化所有表的状态信息。");
        LOG.info("正在 cdc 采集的表 tableList : {} ", tableInfos.keySet());
        LOG.info("还在进行全量读取中的表 tableList : {} ", needBinlogStates);
        tableInfos.forEach(
                (tableId, tableInfo) -> tableTopicNames.put(tableId, tableInfo.getTopicName()));
        needBinlogStates.forEach(tableId -> needBinlogStateTableIds.put(tableId, true));
    }

    @Override
    public void addProcessedTableId(TableId tableId, String topicName) {
        if (tableTopicNames.get(tableId) == null) {
            //            LOG.info("新增 - table : {} 表采集", tableId);
            tableTopicNames.put(tableId, topicName);
        }
    }

    @Override
    public void removeTable(TableId tableId) {
        if (tableTopicNames.get(tableId) == null) {
            LOG.warn("table : {} 不在处理列表中 ", tableId);
        } else {
            LOG.info("删除 - table : {} 表采集", tableId);
            tableTopicNames.remove(tableId);
        }
    }

    @Override
    public void addBinlogStateTable(TableId tableId, String topicName) {
        if (tableTopicNames.get(tableId) == null) {
            //            LOG.info("新增 - table : {} 表 BINLOG_STATE 采集", tableId);
            tableTopicNames.put(tableId, topicName);
            needBinlogStateTableIds.put(tableId, true);
        }
    }

    @Override
    public void removeBinlogStateTable(TableId tableId) {
        if (tableTopicNames.get(tableId) == null) {
            LOG.warn("table : {} 不在 BINLOG_STATE 列表中", tableId);
        } else {
            LOG.info("删除 - table : {} 表 BINLOG_STATE 采集", tableId);
            needBinlogStateTableIds.remove(tableId);
        }
    }
}
