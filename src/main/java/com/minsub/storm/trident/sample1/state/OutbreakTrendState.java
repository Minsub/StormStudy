package com.minsub.storm.trident.sample1.state;

import org.apache.storm.trident.state.map.NonTransactionalMap;

public class OutbreakTrendState extends NonTransactionalMap<Long> {
    protected OutbreakTrendState(OutbreakTrendBackingMap outbreakBackingMap) {
        super(outbreakBackingMap);
    }
}
