package com.ververica.cdc.connectors.base.source.enumerator;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import com.ververica.cdc.connectors.base.config.BaseBoundednessSoureConfig;
import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public abstract class JdbcBoundednessEnumerator<SplitT extends SourceSplit>
        extends BoundednessEnumerator<SplitT> {
    protected final SplitEnumeratorContext<SourceSplitBase> context;
    protected final BaseBoundednessSoureConfig sourceConfig;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcBoundednessEnumerator.class);

    protected JdbcBoundednessEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            BaseBoundednessSoureConfig sourceConfig) {
        super(sourceConfig);
        this.context = context;
        this.sourceConfig = sourceConfig;
        EnumeratorMonitor.addEnumerator(this, sourceConfig);
    }

    public void doStop() throws IOException {
        final Set<Integer> keySet = context.registeredReaders().keySet();
        if (CollectionUtils.isNotEmpty(keySet)) {
            LOG.info("begin doStop enumerator: " + getUUID() + "; readers: " + keySet);
            // keySet.forEach(context::signalNoMoreSplits);
        }
    }

    public String getUUID() {
        if (sourceConfig instanceof JdbcSourceConfig) {
            final JdbcSourceConfig jdbcSourceConfig = (JdbcSourceConfig) sourceConfig;
            final String uuuidName =
                    String.format(
                            "%s_%d", jdbcSourceConfig.getHostname(), jdbcSourceConfig.getPort());
            // TODO 有么有非法字符
            return uuuidName;
        } else {
            throw new RuntimeException("sourceConfig " + sourceConfig + " not support!");
        }
    }
}
