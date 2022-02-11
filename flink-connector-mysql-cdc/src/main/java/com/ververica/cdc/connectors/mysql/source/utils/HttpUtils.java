package com.ververica.cdc.connectors.mysql.source.utils;

import com.ververica.cdc.connectors.mysql.source.sf.NewTableBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);


    public static NewTableBean bean = null;

    static {
        bean = new NewTableBean();
        bean.setDbName("qlh");
        bean.setTableName("mysql_data");
        bean.setTopicName("testTopic1");
        bean.setKafkaClusterName("testTopic1");
        bean.setBootstrapServer("testTopic1");
        bean.setStatus(false);
    }

    /**
     * binlog 任务新增表成功，并回调表对应的 Gtid
     *
     * @param reportTableGtid
     */
    public static void callbackTable(Map<String, String> reportTableGtid) {
        bean.setStatus(false);
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
    public static List<NewTableBean> requestAddedTable() {
        List<NewTableBean> tableIds = new ArrayList<>();
        if (bean.getStatus()) {
            tableIds.add(bean);
        }
        return tableIds;
    }


    /**
     * 请求获取新增表
     *
     * @return
     */
    public static List<NewTableBean>  requestAlreadyTable() {
        List<NewTableBean> tableIds = new ArrayList<>();
        if (!bean.getStatus()) {
            tableIds.add(bean);
        }
        return tableIds;
    }
}