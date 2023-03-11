package com.ververica.cdc.connectors.sf.events;

/** 表变更类型. */
public enum TableChangeTypeEnum {
    CREATE_SNAPSHOT_TABLE,
    CREATE_STREAM_TABLE,
    DELETE_TABLE;

    public boolean isAddedTable() {
        return CREATE_SNAPSHOT_TABLE.equals(this) || CREATE_STREAM_TABLE.equals(this);
    }

    public boolean isDeletedTable() {
        return DELETE_TABLE.equals(this);
    }
}
