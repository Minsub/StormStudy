package com.minsub.storm.trident.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.BatchSpoutExecutor;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.spout.OpaquePartitionedTridentSpoutExecutor;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by jiminsub on 2016. 10. 20..
 */
public class DiagnosisEventSpout implements ITridentSpout<Long> {
    private static final long serialVersionUID = 1L;
    BatchCoordinator<Long> coordinator = new DefaultCoordinator();
    Emitter<Long> emitter = new DiagnosisEventEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }
}
