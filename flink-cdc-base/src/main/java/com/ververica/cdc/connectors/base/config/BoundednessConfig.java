package com.ververica.cdc.connectors.base.config;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Properties;

public abstract class BoundednessConfig implements Serializable {

    public static final String EXT_DATA_PATH_KEY = "ext.data.path";
    public static final String JOB_BOUNDED = "job.bounded";
    public static final String CDC_SNAPSHOT_RATE_LIMIT = "cdc.snapshot.rate.limit";
    protected final Properties otherProperties;

    public BoundednessConfig(Properties otherProperties) {
        this.otherProperties = otherProperties;
    }

    public Properties getOtherProperties() {
        return this.otherProperties;
    }

    public boolean isBounded() {
        if (otherProperties != null) {
            return Boolean.valueOf(otherProperties.getProperty(JOB_BOUNDED, "false"));
        }
        return false;
    }

    public Path extDataPath() {
        if (otherProperties != null) {
            return new Path(otherProperties.getProperty(EXT_DATA_PATH_KEY));
        }
        return null;
    }

    public double rateLimit() {
        if (otherProperties != null) {
            return Double.valueOf(otherProperties.getProperty(CDC_SNAPSHOT_RATE_LIMIT, "-1"));
        }
        return -1d;
    }
}
