package com.ververica.cdc.connectors.base.config;

import java.util.Properties;

public class BaseSourceBuilder {
    protected final JdbcSourceConfigFactory configFactory;

    public BaseSourceBuilder(JdbcSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    public <C extends BaseSourceBuilder> C otherProperties(Properties otherProperties) {
        this.configFactory.otherProperties(otherProperties);
        return (C) this;
    }
}
