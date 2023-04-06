package com.ververica.cdc.connectors.sf.request.bean;

import io.debezium.relational.TableId;

/** 回调表变更实体类. */
public class CallBackTableChangeBean {

    private Long id;
    private String dbName;
    private String schemaName;
    private String tableName;
    private AccessTableStatus status;

    public CallBackTableChangeBean(
            Long id, String dbName, String schemaName, String tableName, AccessTableStatus status) {
        this.id = id;
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.status = status;
    }

    public static CallBackTableChangeBean asCreateTable(Long id, TableId tableId) {
        return new CallBackTableChangeBean(
                id,
                tableId.catalog(),
                tableId.schema(),
                tableId.table(),
                AccessTableStatus.RUNNING);
    }

    public static CallBackTableChangeBean asDeleteTable(Long id, TableId tableId) {
        return new CallBackTableChangeBean(
                id, tableId.catalog(), tableId.schema(), tableId.table(), AccessTableStatus.STOPED);
    }

    public AccessTableStatus getStatus() {
        return status;
    }

    public Long getId() {
        return id;
    }

    public String getDbName() {
        return dbName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public TableId getTableId() {
        return new TableId(dbName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return "[{\n"
                + "  \"id\": "
                + id
                + ",\n"
                + "  \"schemaName\": \""
                + schemaName
                + "\",\n"
                + "  \"tableName\": \""
                + tableName
                + "\",\n"
                + "  \"status\": \""
                + status.toString()
                + "\"\n"
                + "}]";
    }
}
