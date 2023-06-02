package com.ververica.cdc.connectors.base.source.reader.fetch;

import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.rate.JdbcRateLimiter;

import java.util.Optional;

public abstract class BaseScanFetchTask implements FetchTask<SourceSplitBase> {

    protected Optional<JdbcRateLimiter> rateLimiter;

    public void setRateLimiter(Optional<JdbcRateLimiter> rateLimiter) {
        if (this.rateLimiter == null) {
            this.rateLimiter = rateLimiter;
        }
    }

    // public class BaseSnapshotSplitChangeEventSourceContext
    //         extends BaseSnapshotBinlogSplitChangeEventSourceContext {
    //
    //     @Override
    //     public boolean isRunning() {
    //         return taskRunning && checkRunning();
    //     }
    //     public void finished() {
    //         taskRunning = false;
    //     }
    //
    //     public boolean checkRunning(){
    //         return true;
    //     }
    // }
    //
    // public class BaseSnapshotBinlogSplitChangeEventSourceContext
    //         implements ChangeEventSource.ChangeEventSourceContext {
    //
    //     public void finished() {
    //         taskRunning = false;
    //     }
    //
    //     @Override
    //     public boolean isRunning() {
    //         return taskRunning;
    //     }
    // }
}
