package com.ververica.cdc.connectors.mysql.source.utils;

import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableUtils.class);

    public static TableId generateTableId(String table) {
        if (StringUtils.isNotBlank(table) && table.contains(".")) {
            try {
                String[] split = table.split("\\.");
                String dbName = split[0];
                String tableName = split[1];
                return new TableId(dbName, null, tableName);
            } catch (Exception e) {
                LOG.error("错误的表名称 {}", table);
            }
        }
        return null;
    }
}