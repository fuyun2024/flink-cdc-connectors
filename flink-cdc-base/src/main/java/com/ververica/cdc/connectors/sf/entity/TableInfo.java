package com.ververica.cdc.connectors.sf.entity;

import java.io.Serializable;
import java.util.List;

/** 需要在内部流转的表信息实体类. */
public class TableInfo implements Serializable {

    private Long primaryKey;
    private String topicName;
    private List<EncryptField> encryptFields;

    public TableInfo(Long primaryKey, String topicName, List<EncryptField> encryptFields) {
        this.primaryKey = primaryKey;
        this.topicName = topicName;
        this.encryptFields = encryptFields;
    }

    public Long getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Long primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public List<EncryptField> getEncryptFields() {
        return encryptFields;
    }

    public void setEncryptFields(List<EncryptField> encryptFields) {
        this.encryptFields = encryptFields;
    }
}
