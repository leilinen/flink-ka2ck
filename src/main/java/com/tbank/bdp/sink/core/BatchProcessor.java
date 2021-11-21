package com.tbank.bdp.sink.core;

import com.tbank.bdp.sink.common.ChThreadFactory;
import com.tbank.bdp.sink.common.ClickHouseConfig;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class BatchProcessor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final AsyncHttpClient client;
    private volatile boolean closed = false;
    private final String targetTable;
    private final int maxFlushRows;
    private final int flushIntervalSec;
    private final Listener listener;
    private final ClickHouseConfig config;
    private TableRowsBuffer tableRowsBuffer;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ExecutorService callbackService;
    private final ScheduledFuture scheduledFuture;
    private final AtomicLong executionIdGen = new AtomicLong();


    public BatchProcessor(AsyncHttpClient client, ClickHouseConfig config, int taskIndex, String targetTable, int maxFlushRows, Listener listener) {
        this.client = client;
        this.targetTable = targetTable;
        this.maxFlushRows = maxFlushRows;
        this.config = config;
        this.flushIntervalSec = config.getFlushInterval();
        this.listener = listener;
        this.tableRowsBuffer = new TableRowsBuffer(targetTable);
        int cores = Runtime.getRuntime().availableProcessors();
        int coreThreadsNum = Math.max(cores / 4, 2);

        this.callbackService = new ThreadPoolExecutor(
                coreThreadsNum,
                Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ChThreadFactory("writer-callback", taskIndex)
        );

        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
                1,
                new ChThreadFactory((targetTable != null ? "[" + targetTable + "]" : "") + "timer-flusher", taskIndex)
        );
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new TimeFlusher(), flushIntervalSec, flushIntervalSec, TimeUnit.SECONDS);
    }

    public synchronized void add(String field, String value) {
        ensureOpen();
        tableRowsBuffer.add(field, value);
        executeIfNeeded();
    }

    private void executeIfNeeded() {
        ensureOpen();
        if (isOverTheLimit()) {
            execute();
        }
    }

    private boolean isOverTheLimit() {
        return maxFlushRows != -1 && tableRowsBuffer.bufferSize() >= maxFlushRows;
    }


    class TimeFlusher implements Runnable {
        TimeFlusher() {
        }

        public void run() {
            synchronized (BatchProcessor.this) {
                if (!closed) {
                    if (tableRowsBuffer.bufferSize() != 0) {
                        execute();
                    }
                }
            }
        }
    }


    private void execute() {
        final TableRowsBuffer tableRowsBuffer = this.tableRowsBuffer;
        final long executionId = executionIdGen.incrementAndGet();

        this.tableRowsBuffer = new TableRowsBuffer(targetTable);
        Request request = buildRequest(tableRowsBuffer);
        logger.info("Ready to load data to {}, size = {}", tableRowsBuffer.getTable(), tableRowsBuffer.getRows().size());

        boolean afterCalled = false;
        try {
            ListenableFuture<Response> response = client.executeRequest(request);
            afterCalled = true;
            Runnable callback = responseCallback(response, executionId, tableRowsBuffer);
            response.addListener(callback, callbackService);
        } catch (Exception e) {
            if (!afterCalled) {
                listener.handleExceptionWhenGettingResponse(executionId, tableRowsBuffer, e);
            }
        }
    }

    private Runnable responseCallback(ListenableFuture<Response> whenResponse, long executionId, TableRowsBuffer tableRowsBuffer) {
        return () -> {
            try {
                Response response = whenResponse.get();
                listener.handleResponse(executionId, tableRowsBuffer, response);
            } catch (Exception e) {
                logger.error("Error while executing callback", e);
                listener.handleExceptionWhenGettingResponse(executionId, tableRowsBuffer, e);
            }
        };
    }

    private Request buildRequest(TableRowsBuffer tableRowsBuffer) {
        String fieldCsv = String.join(" , ", tableRowsBuffer.getFields());
        String valueCsv = String.join(" , ", tableRowsBuffer.getRows());
        String query = String.format("INSERT INTO %s (%s) VALUES %s", tableRowsBuffer.getTable(), fieldCsv, valueCsv);
        String host = config.getConnectConfig().getRandomHostUrl();

        BoundRequestBuilder builder = client
                .preparePost(host)
                .setHeader(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8")
                .setBody(query);
        builder.setHeader(HttpHeaderNames.AUTHORIZATION, "Basic " + config.getConnectConfig().getCredentials());
        return builder.build();
    }

    public synchronized void flush() {
        ensureOpen();
        if (tableRowsBuffer.bufferSize() > 0) {
            execute();
        }
    }

    @Override
    public void close() {
        if (isOpen()) {
            closed = true;
            if (this.scheduledFuture != null) {
                cancel(this.scheduledFuture);
                this.scheduler.shutdown();
            }
            if (tableRowsBuffer.bufferSize() > 0) {
                execute();
            }

            if (this.callbackService != null) {
                this.callbackService.shutdown();
            }
        }
    }


    public static boolean cancel(Future<?> toCancel) {
        if (toCancel != null) {
            return toCancel.cancel(false);
        }
        return false;
    }

    boolean isOpen() {
        return !this.closed;
    }

    protected void ensureOpen() {
        if (this.closed) {
            throw new IllegalStateException("batch process already closed");
        }
    }


    public interface Listener {

        void handleResponse(long executionId, TableRowsBuffer tableRowsBuffer, Response response);

        void handleExceptionWhenGettingResponse(long executionId, TableRowsBuffer tableRowsBuffer, Throwable failure);
    }
}
