package com.ververica.cdc.connectors.mysql.debezium.task.context;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** 删除表的上下文信息. */
public class DeletedTableContext implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DeletedTableContext.class);

    private final Set<String> pendingDeletedTables;
    private final Map<String, String> unReportTables;

    private final Object lock;

    public DeletedTableContext() {
        this.pendingDeletedTables = new HashSet<>();
        this.unReportTables = new HashMap<>();
        this.lock = this;
    }

    public void deletedTable(Set<String> tables) {
        synchronized (lock) {
            for (String table : tables) {
                if (StringUtils.isNotBlank(table)
                        && !pendingDeletedTables.contains(table)
                        && !unReportTables.containsKey(table)) {
                    LOG.info("扫描到一张新添加的表 : " + table);
                    pendingDeletedTables.add(table);
                }
            }
        }
    }

    public String getFirstAddedTable() {
        synchronized (lock) {
            if (pendingDeletedTables.size() > 0) {
                Optional<String> first = pendingDeletedTables.stream().findFirst();
                return first.get();
            }
            return null;
        }
    }

    public void addUnReportTable(String table, String gtid) {
        synchronized (lock) {
            pendingDeletedTables.remove(table);
            unReportTables.put(table, gtid);
        }
    }

    public void ackUnReportTable(Map<String, String> reportTable) {
        synchronized (lock) {
            reportTable.forEach(
                    (table, gtid) -> {
                        unReportTables.remove(table, gtid);
                        LOG.info("table : {} ack 数据成功，新增表处理流程完成", table);
                    });
        }
    }

    public boolean hasAddedTables() {
        return pendingDeletedTables.size() > 0;
    }

    public boolean hasUnReportTables() {
        return unReportTables.size() > 0;
    }

    public Map<String, String> getUnReportTables() {
        HashMap map = new HashMap();
        map.putAll(unReportTables);
        return map;
    }
}
