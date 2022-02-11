package com.ververica.cdc.connectors.mysql.source.sf;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

public class StringKafkaDeserializationSchema extends KafkaDeserializationSchema<String> {


    @Override
    public void deserialize(SourceRecord record, Collector out) throws Exception {
        String topic = record.topic().substring(record.topic().indexOf(".") + 1);
        NewTableBean newTableBean = tableBeanMap.get(topic);
        System.out.println(newTableBean);
        out.collect(record.toString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

