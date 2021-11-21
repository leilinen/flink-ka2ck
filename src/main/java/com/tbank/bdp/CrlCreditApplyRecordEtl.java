package com.tbank.bdp;

import com.tbank.bdp.operators.CrlCreditApplyRecordMap;
import com.tbank.bdp.sink.ClickhouseSink;
import com.tbank.bdp.sink.common.ClickhouseConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 *
 * kafka to clickhouse
 *
 *
 * @Author: leiline
 * @CreateTime: 2021-11-21
 */
public class CrlCreditApplyRecordEtl {

    private static final Logger logger = LoggerFactory.getLogger(CrlCreditApplyRecordEtl.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(40, 10000));
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(60));
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(5));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);

        ParameterTool config = ParameterTool.fromPropertiesFile("src/main/resources/configuration.properties");

        String proccessEnv = config.get("process.env", "dev");

        DataStream<String> dataStream = null;

        if (proccessEnv.equals("dev")) {
            dataStream =  env.readTextFile("/Users/lilin/Documents/code/crl_credit_apply_record_ck/src/main/resources/testdata.txt");
        } else {
            Properties kafkaProperties = new Properties();
//            dataStream = env.addSource(
//                    new FlinkKafkaConsumer011(config.get("topic"), new SimpleStringSchema(), kafkaProperties));
        }

        Properties ckProperties = new Properties();
        ckProperties.put(ClickhouseConstants.TARGET_TABLE_NAME, config.get("process.sink.table"));
        ckProperties.put(ClickhouseConstants.BATCH_SIZE, "20000");

        ckProperties.put(ClickhouseConstants.INSTANCES, "localhost:8123");
        ckProperties.put(ClickhouseConstants.USERNAME, "default");
        ckProperties.put(ClickhouseConstants.PASSWORD, "");
        ckProperties.put(ClickhouseConstants.FLUSH_INTERVAL, "2");

        dataStream.map(new CrlCreditApplyRecordMap())
                .addSink(new ClickhouseSink());



        env.execute("crl_credit_apply_record_etl");
    }
}
