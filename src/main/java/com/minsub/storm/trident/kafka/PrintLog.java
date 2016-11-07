package com.minsub.storm.trident.kafka;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class PrintLog extends BaseFunction {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String log = (String) tuple.getValue(0);
        System.out.println("PRINT: " + log);
    }
}
