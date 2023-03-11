package com.ververica.cdc.connectors.sf.request.bean;

import com.alibaba.fastjson.JSON;
import io.debezium.relational.TableId;

import java.util.List;

/** 表变更的实体类. */
public class TableChangeBean {

    private Long id;
    private String dbName;
    private String schemaName;
    private String tableName;
    private String topicName;
    private int syncMode; // 1 需要初始化全量，0 只同步增量
    private AccessTableStatus status;
    private List<EncryptField> encryptFields;

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public boolean isSnapshotSync() {
        return syncMode == 1;
    }

    public boolean isStreamSync() {
        return syncMode == 0;
    }

    public void setSyncMode(int syncMode) {
        this.syncMode = syncMode;
    }

    public AccessTableStatus getStatus() {
        return status;
    }

    public void setStatus(AccessTableStatus status) {
        this.status = status;
    }

    public List<EncryptField> getEncryptFields() {
        return encryptFields;
    }

    public void setEncryptFields(List<EncryptField> encryptFields) {
        this.encryptFields = encryptFields;
    }

    public boolean isAddedTable() {
        return AccessTableStatus.INITIAL.equals(this.status)
                || AccessTableStatus.RUNNING.equals(this.status);
    }

    public boolean isDeleteTable() {
        return AccessTableStatus.STOPING.equals(this.status)
                || AccessTableStatus.STOPED.equals(this.status);
    }

    public TableId getTableId() {
        return new TableId(dbName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
