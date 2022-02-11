package com.ververica.cdc.connectors.mysql.source.utils;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ververica.cdc.connectors.mysql.source.sf.CallbackGtidBean;
import com.ververica.cdc.connectors.mysql.source.sf.NewTableBean;
import com.ververica.cdc.connectors.mysql.source.sf.ResponseData;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    /**
     * binlog 任务新增表成功，并回调表对应的 Gtid
     *
     * @param url
     * @param callbackGtidBeans
     * @return
     */
    public static boolean callbackGtid(String url, List<CallbackGtidBean> callbackGtidBeans) {
        try {
            String result = HttpUtil.post(url, JSON.toJSONString(callbackGtidBeans));
            if (result != null) {
                ResponseData responseData = JSONObject.parseObject(result, ResponseData.class);
                if (responseData.getStatus() == 200) {
                    callbackGtidBeans.forEach(
                            bean -> {
                                LOG.info(
                                        "binlog 新增表任务完成，任务回调成功 table : {} , gtid : {}",
                                        bean.getDbTable(),
                                        bean.getGtid());
                            });
                    return true;
                } else {
                    LOG.error("回调 gtid 信息失败：" + responseData.getErrMsg());
                    return false;
                }
            }
        } catch (Exception e) {
            LOG.error("回调 gtid 信息失败： ", e);
        }
        return false;
    }

    /**
     * 请求获取新增表
     *
     * @return
     */
    public static List<NewTableBean> requestAddedTable(String url) {
        try {
            ResponseData<List<NewTableBean>> responseData = httpAddedTable(url);
            if (responseData != null
                    && responseData.getData() != null
                    && responseData.getData().size() > 0) {
                List<NewTableBean> tableBeans = responseData.getData();
                if (tableBeans != null && tableBeans.size() > 0) {
                    return tableBeans.stream()
                            .filter(bean -> !bean.getState())
                            .collect(Collectors.toList());
                }
            }
        } catch (Exception e) {
            LOG.error("获取新增表数据失败 ： ", e);
        }
        return null;
    }

    /**
     * 请求获取新增表
     *
     * @return
     */
    public static List<NewTableBean> requestAlreadyTable(String url) {
        try {
            ResponseData<List<NewTableBean>> responseData = httpAddedTable(url);
            if (responseData != null
                    && responseData.getData() != null
                    && responseData.getData().size() > 0) {
                List<NewTableBean> tableBeans = responseData.getData();
                if (tableBeans != null && tableBeans.size() > 0) {
                    return tableBeans.stream()
                            .filter(NewTableBean::getState)
                            .collect(Collectors.toList());
                }
            }
        } catch (Exception e) {
            LOG.error("获取已经存在表数据失败 ： ", e);
        }
        return null;
    }

    private static ResponseData<List<NewTableBean>> httpAddedTable(String url) {
        String result = HttpUtil.createGet(url).execute().body();
        if (StringUtils.isNotEmpty(result)) {
            return JSON.parseObject(
                    result, new TypeReference<ResponseData<List<NewTableBean>>>() {});
        }
        return null;
    }
}
