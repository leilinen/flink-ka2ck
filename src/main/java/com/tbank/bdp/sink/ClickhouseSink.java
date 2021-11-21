package com.tbank.bdp.sink;

import com.tbank.bdp.sink.common.ClickHouseConfig;
import com.tbank.bdp.sink.core.BatchProcessor;
import com.tbank.bdp.sink.core.ClickhouseRowCollector;
import com.tbank.bdp.sink.core.TableRowsBuffer;
import com.tbank.bdp.sink.exception.ClickhouseException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.tbank.bdp.sink.common.ClickhouseConstants.BATCH_SIZE;
import static com.tbank.bdp.sink.common.ClickhouseConstants.TARGET_TABLE_NAME;

/**
 * @Description:
 * @Author: leiline
 * @CreateTime: 2021-11-21
 */
public class ClickhouseSink extends RichSinkFunction<Tuple2<List<String>, List<String>>> implements CheckpointedFunction {

    private static final long serialVersionUID = 8882341085011937977L;
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);

    private AtomicLong numPendingRows = new AtomicLong(0);
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    private transient AsyncHttpClient client;
    private transient BatchProcessor batchProcessor;
    private transient ClickhouseRowCollector clickhouseRowCollector;
    private boolean flushOnCheckpoint = true;

    private final Properties props;
    private final ClickHouseConfig config;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = Dsl.asyncHttpClient();
        batchProcessor = new BatchProcessor(
                client,
                config,
                getRuntimeContext().getIndexOfThisSubtask(),
                props.getProperty(TARGET_TABLE_NAME, ""),
                Integer.parseInt(props.getProperty(BATCH_SIZE, "10000")),
                new BatchProcessorListener()
        );
        this.clickhouseRowCollector = new ClickhouseRowCollector(batchProcessor, flushOnCheckpoint, numPendingRows);
    }

    public ClickhouseSink(Properties props) {
        this.props = props;
        this.config = new ClickHouseConfig(props);
    }


    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkErrorAndRethrow();
        if (flushOnCheckpoint) {
            while (numPendingRows.get() != 0) {
                batchProcessor.flush();
                checkErrorAndRethrow();
            }
        }
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    public void invoke(Tuple2<List<String>, List<String>> value, Context context) throws Exception {

        logger.info("ck sink, f0: " + value.f0 + ", f1: " + value.f1);
//        logger.info("ck sink: {}", value);
        checkErrorAndRethrow();
        checkValueError(value);
        clickhouseRowCollector.collect(value);
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in ClickhouseSink.", cause);
        }
    }

    private void checkValueError(Tuple2<List<String>, List<String>> value) {
        if (value.f0.size() != value.f1.size()) {
            throw new RuntimeException("clickhouse sink error, input value fields size not equals value size!");
        }
    }

    private class BatchProcessorListener implements BatchProcessor.Listener {

        private static final int HTTP_OK = 200;

        @Override
        public void handleResponse(long executionId, TableRowsBuffer tableRowsBuffer, Response response) {
            if (response.getStatusCode() != HTTP_OK) {
                String errorString = response.getResponseBody();
                logger.error("Failed to send data to ClickHouse,  ClickHouse response = {}. ", errorString);
                failureThrowable.compareAndSet(null, new ClickhouseException(errorString));
            } else {
                logger.info("Successful send data to ClickHouse, batch size = {}, target table = {}", tableRowsBuffer.bufferSize(), tableRowsBuffer.getTable());
            }
            if (flushOnCheckpoint) {
                numPendingRows.getAndAdd(-tableRowsBuffer.bufferSize());
            }
        }

        @Override
        public void handleExceptionWhenGettingResponse(long executionId, TableRowsBuffer tableRowsBuffer, Throwable failure) {
            logger.error("Failed to send data to ClickHouse: {}", failure.getMessage(), failure.getCause());
            failureThrowable.compareAndSet(null, new ClickhouseException(failure.getMessage(), failure.getCause()));
            if (flushOnCheckpoint) {
                numPendingRows.getAndAdd(-tableRowsBuffer.bufferSize());
            }
        }
    }
}
