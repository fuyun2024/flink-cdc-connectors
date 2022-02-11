package com.ververica.cdc.connectors.mysql.source.sf;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class KafkaDeserializationSchema<T> implements DebeziumDeserializationSchema<T> {

    /** 存储 db.table 与表信息的映射关系 */
    protected Map<String, NewTableBean> tableBeanMap;

    /**
     * 添加 kafka 配置信息
     *
     * @param tableBeanMap
     * @return
     */
    public boolean addTableBeanMap(Map<String, NewTableBean> tableBeanMap) {
        if (this.tableBeanMap == null) {
            this.tableBeanMap = new HashMap<>();
        }
        this.tableBeanMap.putAll(tableBeanMap);
        return true;
    }

    /**
     * 删除 kafka 配置信息
     *
     * @param tableBeanMap
     * @return
     */
    public boolean removeTableBeanMap(Map<String, NewTableBean> tableBeanMap) {
        Iterator<String> iterator = tableBeanMap.keySet().iterator();
        while (iterator.hasNext()) {
            this.tableBeanMap.remove(iterator.next());
        }
        return true;
    }
}
