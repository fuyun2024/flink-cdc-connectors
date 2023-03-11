package com.ververica.cdc.connectors.sf.enumerator;

/** 动态表变更的上下文. */
public class DynamicTableChangeContext {

    private boolean scanTaskRunning = true;
    private String getTableChangeUrl;
    private String callbackTableChangeUrl;

    public DynamicTableChangeContext(String getTableChangeUrl, String callbackTableChangeUrl) {
        this.getTableChangeUrl = getTableChangeUrl;
        this.callbackTableChangeUrl = callbackTableChangeUrl;
    }

    public String getGetTableChangeUrl() {
        return getTableChangeUrl;
    }

    public String getCallbackTableChangeUrl() {
        return callbackTableChangeUrl;
    }

    public void close() {
        scanTaskRunning = false;
    }

    public boolean isRunning() {
        return scanTaskRunning;
    }
}
