package com.ververica.cdc.connectors.mysql.source.sf;

/**
 * ------------------------------------ description: @Author: created by eHui @Date: 2022/2/11.
 * ------------------------------------
 */
public class CallbackGtidBean {

    private String applyId;
    private String dataSourceId;
    private String dataSourceHostName;
    private String dbName;
    private String tableName;
    private String gtid;

    public String getDbTable() {
        return dbName + "." + tableName;
    }

    public String getApplyId() {
        return applyId;
    }

    public void setApplyId(String applyId) {
        this.applyId = applyId;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public String getDataSourceHostName() {
        return dataSourceHostName;
    }

    public void setDataSourceHostName(String dataSourceHostName) {
        this.dataSourceHostName = dataSourceHostName;
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

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }
}
