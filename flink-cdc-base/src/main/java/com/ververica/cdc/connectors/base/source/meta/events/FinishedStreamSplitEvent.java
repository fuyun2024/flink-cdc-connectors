package com.ververica.cdc.connectors.base.source.meta.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;

public class FinishedStreamSplitEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;
    public final Offset startingOffset;
    public final Offset endingOffset;

    public FinishedStreamSplitEvent(Offset startingOffset, Offset endingOffset) {
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
    }
}
