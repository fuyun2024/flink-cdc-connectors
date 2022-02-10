package com.ververica.cdc.connectors.mysql.source.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    /**
     * binlog 任务新增表成功，并回调表对应的 Gtid
     *
     * @param reportTableGtid
     */
    public static void callbackTable(Map<String, String> reportTableGtid) {
        reportTableGtid.forEach(
                (key, value) -> {
                    LOG.info("binlog 新增表任务完成，任务回调成功 table : {} , gtid : {}", key, value);
                });
    }

    /**
     * 请求获取新增表
     *
     * @return
     */
    public static List<String> requestAddedTable() {
        List<String> tableIds = new ArrayList<>();
        tableIds.add("qlh.join2");
        tableIds.add("qlh.mysql_cdc_source");
        return tableIds;
    }
}