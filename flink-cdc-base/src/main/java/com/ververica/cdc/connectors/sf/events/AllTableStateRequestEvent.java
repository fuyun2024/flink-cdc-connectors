package com.ververica.cdc.connectors.sf.events;

import org.apache.flink.api.connector.source.SourceEvent;

/** 获取所有表状态的请求信息. */
public class AllTableStateRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    public AllTableStateRequestEvent() {}
}
