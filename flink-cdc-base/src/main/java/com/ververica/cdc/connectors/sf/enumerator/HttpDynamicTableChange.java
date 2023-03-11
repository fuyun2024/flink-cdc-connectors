package com.ververica.cdc.connectors.sf.enumerator;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.sf.request.HttpUtils;
import com.ververica.cdc.connectors.sf.request.bean.CallBackTableChangeBean;
import com.ververica.cdc.connectors.sf.request.bean.TableChangeBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/** 使用 http 服务来感知表变更. @Author: created by eHui @Date: 2023/3/2 */
public class HttpDynamicTableChange implements DynamicTableChange {

    private static final Logger LOG = LoggerFactory.getLogger(HttpDynamicTableChange.class);

    private static final long TABLE_CHANGE_SCAN_INTERVAL = 30_000L;

    private DynamicTableChangeContext context;

    public HttpDynamicTableChange(DynamicTableChangeContext context) {
        this.context = context;
    }

    @Override
    public void tableChangeCapture(Consumer<List<TableChangeBean>> tableChangeConsumer) {
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("table-change-scan").build();
        ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);

        executor.submit(
                () -> {
                    while (context.isRunning()) {
                        try {
                            LOG.info("请求后台直通车，获取表变更列表。");
                            List<TableChangeBean> tableChangeBeans =
                                    HttpUtils.requestTableChangeTable(
                                            context.getGetTableChangeUrl());

                            // 应用表变更
                            tableChangeConsumer.accept(tableChangeBeans);

                            Thread.sleep(TABLE_CHANGE_SCAN_INTERVAL);
                        } catch (Exception e) {
                            // not stop
                            // scanTaskRunning = false;
                            LOG.error("扫描获取新增的表信息失败", e);
                        }
                    }
                });
    }

    @Override
    public void tableChangeCallback(List<CallBackTableChangeBean> callBackTableChangeBeans) {
        Iterator<CallBackTableChangeBean> iterator = callBackTableChangeBeans.iterator();
        while (iterator.hasNext()) {
            CallBackTableChangeBean bean = iterator.next();
            LOG.info(
                    "请求直通车 table : {} operation : {} 已经完成了处理", bean.getTableId(), bean.getStatus());
            if (HttpUtils.callbackTableChange(context.getCallbackTableChangeUrl(), bean)) {
                LOG.info(
                        "直通车成功返回 table : {} operation : {} 完成处理。",
                        bean.getTableId(),
                        bean.getStatus());
                iterator.remove();
            }
        }
    }
}
