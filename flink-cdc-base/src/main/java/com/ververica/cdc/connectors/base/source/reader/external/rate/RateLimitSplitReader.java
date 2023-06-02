package com.ververica.cdc.connectors.base.source.reader.external.rate;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.streaming.api.operators.SourceOperator;

import com.ververica.cdc.connectors.base.config.BoundednessConfig;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Optional;

public class RateLimitSplitReader {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitSplitReader.class);

    protected final Optional<JdbcRateLimiter> rateLimiter;

    public RateLimitSplitReader(BoundednessConfig sourceConfig, SourceReaderContext readerContext) {
        if (readerContext == null) {
            this.rateLimiter = Optional.empty();
        } else {
            this.rateLimiter = initRateLimit(readerContext, sourceConfig);
        }
    }

    private Optional<JdbcRateLimiter> initRateLimit(
            SourceReaderContext readerContext, BoundednessConfig sourceConfig) {
        Double limit = null;
        Double limitTotal = null;
        Integer numberOfParallelSubtasks = null;
        final double rateLimit = (sourceConfig).rateLimit();
        if (rateLimit > 0) {
            final Field sourceOperator =
                    FieldUtils.getField(readerContext.getClass(), "this$0", true);
            try {
                numberOfParallelSubtasks =
                        ((SourceOperator) sourceOperator.get(readerContext))
                                .getRuntimeContext()
                                .getNumberOfParallelSubtasks();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            limit = rateLimit / numberOfParallelSubtasks;
            return Optional.of(new JdbcRateLimiter(limit));
        }
        LOG.info(
                "subtaskId:"
                        + readerContext.getIndexOfSubtask()
                        + "; rateLimit: "
                        + limit
                        + "; numberOfParallelSubtasks: "
                        + numberOfParallelSubtasks
                        + "; limitTotal: "
                        + limitTotal);
        return Optional.empty();
    }

    protected Optional<Object> acquire(int permits) {
        return rateLimiter.map(x -> Optional.of(x.acquire(permits)));
    }
}
