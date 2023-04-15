package com.ververica.cdc.connectors.sf.entity;

import com.ververica.cdc.connectors.sf.events.TableChangeTypeEnum;
import io.debezium.relational.TableId;

import java.io.Serializable;

/** 表变更事件. @Author: created by eHui @Date: 2023/4/15 */
public class TableChange implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableId;
    private TableInfo tableInfo;
    private final TableChangeTypeEnum tableChangeType;

    public TableChange(TableId tableId, TableChangeTypeEnum tableChangeType) {
        this.tableId = tableId.toString();
        this.tableChangeType = tableChangeType;
    }

    public static TableChange asCreateStreamTable(TableId tableId) {
        return new TableChange(tableId, TableChangeTypeEnum.CREATE_STREAM_TABLE);
    }

    public static TableChange asCreateSnapshotTable(TableId tableId) {
        return new TableChange(tableId, TableChangeTypeEnum.CREATE_SNAPSHOT_TABLE);
    }

    public static TableChange asDeleteTable(TableId tableId) {
        return new TableChange(tableId, TableChangeTypeEnum.DELETE_TABLE);
    }

    public TableId getTableId() {
        return TableId.parse(tableId);
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public TableChangeTypeEnum getTableChangeType() {
        return tableChangeType;
    }
}
