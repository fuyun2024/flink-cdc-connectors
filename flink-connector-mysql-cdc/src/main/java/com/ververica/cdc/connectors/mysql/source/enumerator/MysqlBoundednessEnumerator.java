package com.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import com.ververica.cdc.connectors.base.config.BoundednessConfig;
import com.ververica.cdc.connectors.base.source.enumerator.BoundednessEnumerator;
import com.ververica.cdc.connectors.base.source.enumerator.EnumeratorMonitor;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public abstract class MysqlBoundednessEnumerator extends BoundednessEnumerator<MySqlSplit> {
    protected final SplitEnumeratorContext<MySqlSplit> context;
    protected final BoundednessConfig sourceConfig;
    private static final Logger LOG = LoggerFactory.getLogger(MysqlBoundednessEnumerator.class);

    protected MysqlBoundednessEnumerator(
            SplitEnumeratorContext<MySqlSplit> context, BoundednessConfig sourceConfig) {
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
        if (sourceConfig instanceof MySqlSourceConfig) {
            final MySqlSourceConfig mySqlSourceConfig = (MySqlSourceConfig) sourceConfig;
            final String uuuidName =
                    String.format(
                            "%s_%d", mySqlSourceConfig.getHostname(), mySqlSourceConfig.getPort());
            // TODO 有么有非法字符
            return uuuidName;
        } else {
            throw new RuntimeException("sourceConfig " + sourceConfig + " not support!");
        }
    }
}
