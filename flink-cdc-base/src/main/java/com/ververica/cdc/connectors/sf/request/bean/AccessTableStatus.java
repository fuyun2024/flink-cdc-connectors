package com.ververica.cdc.connectors.sf.request.bean;

/** 与后台直通车应答状态. */
public enum AccessTableStatus {
    INITIAL,
    RUNNING,
    STOPING,
    STOPED;
}
