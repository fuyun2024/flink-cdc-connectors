package com.ververica.cdc.connectors.mysql.source.sf;

/**
 * ------------------------------------
 * description:
 *
 * @Author: created by eHui
 * @Date: 2022/2/11
 * ------------------------------------
 */
public class NewTableBean {

    private String dbName;
    private String tableName;
    private String topicName;
    private String kafkaClusterName;
    private String bootstrapServer;
    private String applyId;
    private Boolean status;


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

    public Boolean getStatus() {
        return status;
    }

    public void setStatus(Boolean status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "NewTableBean{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", topicName='" + topicName + '\'' +
                ", kafkaClusterName='" + kafkaClusterName + '\'' +
                ", bootstrapServer='" + bootstrapServer + '\'' +
                ", applyId='" + applyId + '\'' +
                ", status=" + status +
                '}';
    }
}