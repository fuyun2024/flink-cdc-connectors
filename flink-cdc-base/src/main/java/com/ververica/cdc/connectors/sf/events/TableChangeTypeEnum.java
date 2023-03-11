package com.ververica.cdc.connectors.sf.events;

/** 表变更类型. */
public enum TableChangeTypeEnum {
    CREATE_SNAPSHOT_TABLE,
    CREATE_STREAM_TABLE,
    DELETE_TABLE
}
