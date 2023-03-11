package com.ververica.cdc.connectors.sf.request.bean;

/** 与后台直通车应答状态. */
public enum AccessTableStatus {
    INITIAL,
    RUNNING,
    STOPING,
    STOPED;

    public boolean isAddedTable() {
        return INITIAL.equals(this) || RUNNING.equals(this);
    }

    public boolean isDeletedTable() {
        return STOPING.equals(this) || STOPING.equals(this);
    }
}
