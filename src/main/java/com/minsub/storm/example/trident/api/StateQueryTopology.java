package com.minsub.storm.example.trident.api;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StateQueryTopology {
    public static StormTopology buildTopology() throws  Exception {
        TridentTopology topology = new TridentTopology();
        // Spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("a","b"), 2
                , new Values("a",1)
                , new Values("b",2)
                , new Values("a",3)
                , new Values("c",8)
                , new Values("e",1)
                , new Values("d",9)
                , new Values("d",10)
        );
        spout.setCycle(false);

        topology.newStream("spout", spout)
                .project(new Fields("b"))
                .peek(tuple ->  System.out.println(tuple));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("aggregate-topology", conf, buildTopology());
        Thread.sleep(1000 * 5);
        cluster.killTopology("aggregate-topology");
        cluster.shutdown();
    }
}
