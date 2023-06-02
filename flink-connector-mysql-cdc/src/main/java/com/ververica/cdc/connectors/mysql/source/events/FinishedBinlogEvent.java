package com.ververica.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;

public class FinishedBinlogEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;
    public final BinlogOffset startingOffset;
    public final BinlogOffset endingOffset;

    public FinishedBinlogEvent(BinlogOffset startingOffset, BinlogOffset endingOffset) {
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
    }
}
