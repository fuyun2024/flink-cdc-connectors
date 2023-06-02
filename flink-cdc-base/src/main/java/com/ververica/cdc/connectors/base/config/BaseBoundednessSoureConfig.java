package com.ververica.cdc.connectors.base.config;

import java.util.Properties;

public abstract class BaseBoundednessSoureConfig extends BoundednessConfig implements SourceConfig {
    public BaseBoundednessSoureConfig(Properties otherProperties) {
        super(otherProperties);
    }
}
