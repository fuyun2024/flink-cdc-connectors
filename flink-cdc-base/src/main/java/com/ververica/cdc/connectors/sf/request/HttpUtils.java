package com.ververica.cdc.connectors.sf.request;

import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ververica.cdc.connectors.sf.request.bean.CallBackTableChangeBean;
import com.ververica.cdc.connectors.sf.request.bean.TableChangeBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** http 请求工具类. */
public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    public static boolean callbackTableChange(String url, CallBackTableChangeBean bean) {
        return true;
        //        try {
        //            String result = HttpUtil.post(url, bean.toString());
        //            if (result != null) {
        //                ResponseData responseData = JSONObject.parseObject(result,
        // ResponseData.class);
        //                if (responseData.getOk()) {
        //                    LOG.info(
        //                            "回调表变更成功，table : {} , operation : {}",
        //                            bean.getTableId(),
        //                            bean.getOperation());
        //                    return true;
        //                } else {
        //                    LOG.error("回调表变更失败 : " + responseData.getErrMsg());
        //                    return false;
        //                }
        //            }
        //        } catch (Exception e) {
        //            LOG.error("回调表变更失败 : ", e);
        //        }
        //        return false;
    }

    /** 请求获取新增表. */
    public static List<TableChangeBean> requestTableChangeTable(String url) {
        try {
            HttpResponse httpResponse = HttpUtil.createGet(url).execute();
            String body = httpResponse.body();
            if (!httpResponse.isOk() || StringUtils.isEmpty(body)) {
                LOG.warn("请求后台直通车获取数据失败, requestHttpUrl : " + url);
                return new ArrayList<>();
            }

            JSONObject response = JSON.parseObject(body);
            String dataStr = response.getString("data");
            if (dataStr != null) {
                List<TableChangeBean> changeBeans =
                        JSON.parseObject(dataStr, new TypeReference<List<TableChangeBean>>() {});
                return changeBeans;
            }
        } catch (Exception e) {
            LOG.error("获取表变更数据失败 : ", e);
        }
        return new ArrayList<>();
    }
}
