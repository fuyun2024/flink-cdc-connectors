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
    private int isNeedSync; // 1 需要初始化全量，0 只同步增量
    private AccessTableStatus operation;
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

    public boolean isInitSync() {
        return isNeedSync == 1;
    }

    public void setIsNeedSync(int isNeedSync) {
        this.isNeedSync = isNeedSync;
    }

    public AccessTableStatus getOperation() {
        return operation;
    }

    public void setOperation(AccessTableStatus operation) {
        this.operation = operation;
    }

    public List<EncryptField> getEncryptFields() {
        return encryptFields;
    }

    public void setEncryptFields(List<EncryptField> encryptFields) {
        this.encryptFields = encryptFields;
    }

    public boolean isAddedTable() {
        return AccessTableStatus.INITIAL.equals(this.operation)
                || AccessTableStatus.RUNNING.equals(this.operation);
    }

    public boolean isDeleteTable() {
        return AccessTableStatus.STOPING.equals(this.operation)
                || AccessTableStatus.STOPED.equals(this.operation);
    }

    public TableId getTableId() {
        return new TableId(dbName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
