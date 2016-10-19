package com.minsub.storm.single;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jiminsub on 2016. 10. 19..
 */
public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counts = null;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // do not need to create Output Stream;
    }

    @Override
    public void cleanup() {
        System.out.println("---- FINAL COUNT ----");
        for (String key: this.counts.keySet()) {
            System.out.println(key + ": " + this.counts.get(key));
        }
        System.out.println("---------------------");
    }
}
