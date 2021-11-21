package com.tbank.bdp.sink.core;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class ClickhouseRowCollector {

    private final BatchProcessor batchProcessor;
    private final boolean flushOnCheckpoint;
    private final AtomicLong numPendingRowsRef;

    public ClickhouseRowCollector(BatchProcessor batchProcessor, boolean flushOnCheckpoint, AtomicLong numPendingRowsRef) {
        this.batchProcessor = checkNotNull(batchProcessor);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.numPendingRowsRef = checkNotNull(numPendingRowsRef);
    }


    public void collect(Tuple2<List<String>, List<String>> fieldAndValue) {

        List<String> fields = fieldAndValue.f0;
        for (int i=0; i<fields.size(); i++) {
            if (flushOnCheckpoint) {
                numPendingRowsRef.getAndIncrement();
            }

            this.batchProcessor.add(fieldAndValue.f0.get(i), fieldAndValue.f1.get(i));
        }
    }


}
