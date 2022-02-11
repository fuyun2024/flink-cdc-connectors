package com.ververica.cdc.connectors.mysql.debezium.task.context;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class AddedTableContext implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AddedTableContext.class);

    private final Set<String> addedTables;
    private final Set<String> alreadyProcessedTables;
    private final Map<String, String> unReportTables;

    private final Object lock;

    public AddedTableContext() {
        this.addedTables = new HashSet<>();
        this.alreadyProcessedTables = new HashSet<>();
        this.unReportTables = new HashMap<>();
        this.lock = this;
    }

    public void addTableIfNotExist(Set<String> tables) {
        synchronized (lock) {
            for (String table : tables) {
                if (StringUtils.isNotBlank(table)
                        && !addedTables.contains(table)
                        && !unReportTables.containsKey(table)
                        && !alreadyProcessedTables.contains(table)) {
                    LOG.info("扫描到一张新添加的表 : " + table);
                    addedTables.add(table);
                }
            }
        }
    }

    public String getFirstAddedTable() {
        synchronized (lock) {
            if (addedTables.size() > 0) {
                Optional<String> first = addedTables.stream().findFirst();
                return first.get();
            }
            return null;
        }
    }

    public void addUnReportTable(String table, String gtid) {
        synchronized (lock) {
            addedTables.remove(table);
            unReportTables.put(table, gtid);
        }
    }

    public void ackUnReportTable(Map<String, String> reportTable) {
        synchronized (lock) {
            reportTable.forEach(
                    (table, gtid) -> {
                        LOG.info("table : {} ack 数据成功，新增表处理流程完成", table);
                        if (unReportTables.remove(table, gtid)) {
                            alreadyProcessedTables.add(table);
                        }
                    });
        }
    }

    public void addAlreadyProcessedTables(Set<String> alreadyProcessedTables) {
        synchronized (lock) {
            alreadyProcessedTables.forEach(key -> this.unReportTables.remove(key));
            this.addedTables.removeAll(alreadyProcessedTables);
            this.alreadyProcessedTables.addAll(alreadyProcessedTables);
        }
    }

    public boolean hasAddedTables() {
        return addedTables.size() > 0;
    }

    public boolean hasUnReportTables() {
        return unReportTables.size() > 0;
    }

    public Map<String, String> getUnReportTables() {
        HashMap map = new HashMap();
        map.putAll(unReportTables);
        return map;
    }

    public Set<String> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }
}
