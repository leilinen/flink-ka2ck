package com.tbank.bdp.operators;

import com.alibaba.fastjson.JSONObject;
import com.tbank.bdp.util.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description:
 * @Author: leiline
 * @CreateTime: 2021-11-21
 */
public class CrlCreditApplyRecordMap extends RichMapFunction<String, Tuple2<List<String>, List<String>>> {


    private static Logger logger = LoggerFactory.getLogger(CrlCreditApplyRecordMap.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public Tuple2<List<String>, List<String>> map(String value) throws Exception {

        List<String> fields = new ArrayList<String>();
        List<String> values = new ArrayList<String>();


        JSONObject record = JSONObject.parseObject(value);

        JSONObject headers = record.getJSONObject("headers");
        JSONObject beforeRow = record.getJSONObject("beforeTableRow");
        JSONObject afterRow = record.getJSONObject("afterTableRow");

        String kafkaKey = headers.getString("kafkaKey");
        String tableKey = kafkaKey.split("\\.")[1];
        String topic = tableKey.substring(0, tableKey.length() - 7);
        String eventDate = StringUtils.today();
        String functionID = headers.getString("functionID");

        fields.add("topic");
        fields.add("event_date");
        fields.add("functionID");

        values.add(topic);
        values.add(eventDate);
        values.add(functionID);

        if (beforeRow != null) {
            Set<String> beforeRowKeys = beforeRow.keySet();
            for (String rowkey : beforeRowKeys) {
                fields.add("before_" + rowkey.toLowerCase());
                values.add(beforeRow.getString(rowkey));
            }
        }

        if (afterRow != null) {
            Set<String> afterRowKeys = afterRow.keySet();
            for (String rowkey : afterRowKeys) {
                fields.add("after_" + rowkey.toLowerCase());
                values.add(afterRow.getString(rowkey));
            }
        }

        return new Tuple2<List<String>, List<String>>(fields, values);


    }
}
