package com.minsub.storm.trident.api;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.concurrent.TimeUnit;

public class WindowsTopology {
    public static StormTopology buildTopology() throws  Exception {
        TridentTopology topology = new TridentTopology();
        // Spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("char"), 3
                , new Values("A"), new Values("B"), new Values("E"), new Values("F"), new Values("C"), new Values("D")
                , new Values("G"), new Values("H"), new Values("I"), new Values("J"), new Values("K"), new Values("L")
                , new Values("M"), new Values("N"), new Values("O"), new Values("P"), new Values("Q"), new Values("R"));
        spout.setCycle(true);

        WindowConfig windowConfig;
        //windowConfig = TumblingCountWindow.of(3);
        //windowConfig = SlidingCountWindow.of(10, 3);
        //windowConfig = SlidingDurationWindow.of(new BaseWindowedBolt.Duration(300, TimeUnit.MILLISECONDS), new BaseWindowedBolt.Duration(100, TimeUnit.MILLISECONDS));
        windowConfig = TumblingDurationWindow.of(new BaseWindowedBolt.Duration(100, TimeUnit.MILLISECONDS));

        topology.newStream("char", spout)
                .window(windowConfig, new InMemoryWindowsStoreFactory(), new Fields("char"), new BaseAggregator() {
                    private StringBuilder sb;
                    @Override  public Object init(Object o, TridentCollector tridentCollector) {
                        this.sb = new StringBuilder();
                        return null;
                    }
                    @Override  public void complete(Object o, TridentCollector collector) {
                        collector.emit(new Values(this.sb.toString()));
                    }
                    @Override
                    public void aggregate(Object o, TridentTuple tuple, TridentCollector collector) {
                        sb.append(tuple.getString(0));
                    }
                }, new Fields("merge"))
                .peek(tuple ->  System.out.println(tuple));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("windows-topology", conf, buildTopology());
        Thread.sleep(1000 * 20);
        cluster.killTopology("windows-topology");
        cluster.shutdown();
    }
}
