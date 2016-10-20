package com.minsub.storm.trident.spout;

import com.minsub.storm.trident.model.DiagnosisEvent;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jiminsub on 2016. 10. 20..
 */
public class DiagnosisEventEmitter implements Emitter<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    AtomicInteger successfulTransactions = new AtomicInteger(0);

    @Override
    public void emitBatch(org.apache.storm.trident.topology.TransactionAttempt transactionAttempt, Long aLong, TridentCollector collector) {
        for (int i = 0; i < 10000; i++) {
            List<Object> events = new ArrayList<Object>();
            double lat = new Double(-30 + (int) (Math.random() * 75));
            double lng = new Double(-120 + (int) (Math.random() * 70));
            long time = System.currentTimeMillis();

            String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
            DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diag);
            events.add(event);
            collector.emit(events);
        }
    }

    @Override
    public void success(org.apache.storm.trident.topology.TransactionAttempt transactionAttempt) {
        successfulTransactions.incrementAndGet();
    }

    @Override
    public void close() {

    }
}
