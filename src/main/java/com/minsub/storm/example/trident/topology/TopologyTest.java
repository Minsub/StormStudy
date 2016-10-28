package com.minsub.storm.example.trident.topology;

import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by hmm1115222 on 2016-10-27.
 */
public class TopologyTest {

    public static StormTopology buildTopology() throws  Exception {
        TridentTopology topology = new TridentTopology();

        // Spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("char"), 3
                , new Values("A")
                , new Values("B")
                , new Values("C")
                , new Values("D")
                , new Values("E")
                , new Values("F")
                , new Values("G")
                , new Values("H")
                , new Values("I")
                , new Values("J")
                , new Values("K")
                , new Values("L")
                , new Values("M")
                , new Values("N")
                , new Values("O")
                , new Values("P")
                );
        spout.setCycle(true);



        WindowConfig windowConfig;
//        windowConfig = TumblingCountWindow.of(3);
        windowConfig = SlidingCountWindow.of(10, 3);
        //windowConfig = SlidingDurationWindow.of(new BaseWindowedBolt.Duration(6, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS));
        //windowConfig = TumblingDurationWindow.of(new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS));

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
        cluster.submitTopology("trident-topology", conf, buildTopology());
        Thread.sleep(1000 * 5);
        cluster.killTopology("trident-topology");
        cluster.shutdown();

    }
}
