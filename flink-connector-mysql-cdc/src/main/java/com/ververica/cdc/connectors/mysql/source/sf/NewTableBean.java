package com.ververica.cdc.connectors.mysql.source.sf;

import com.alibaba.fastjson.JSON;

/**
 * ------------------------------------ description: @Author: created by eHui @Date: 2022/2/11
 * ------------------------------------
 */
public class NewTableBean {

    private String dbName;
    private String tableName;
    private String topicName;
    private String kafkaClusterId;
    private String kafkaClusterName;
    private String bootstrapServer;
    private String applyId;
    private String sourceId;
    private Boolean state;

    public String getDbTable() {
        return dbName + "." + tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
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

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    public void setKafkaClusterId(String kafkaClusterId) {
        this.kafkaClusterId = kafkaClusterId;
    }

    public String getKafkaClusterName() {
        return kafkaClusterName;
    }

    public void setKafkaClusterName(String kafkaClusterName) {
        this.kafkaClusterName = kafkaClusterName;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getApplyId() {
        return applyId;
    }

    public void setApplyId(String applyId) {
        this.applyId = applyId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public Boolean getState() {
        return state;
    }

    public void setState(Boolean state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
