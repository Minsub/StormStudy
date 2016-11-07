package com.minsub.storm.trident.api;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class AggregatesTopology {
    public static StormTopology buildTopology() throws  Exception {
        TridentTopology topology = new TridentTopology();
        // Spout
        FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("key","a"), 2
                , new Values("a",1)
                , new Values("b",2)
                , new Values("a",3)
                , new Values("c",8)
                , new Values("e",1)
                , new Values("d",9)
                , new Values("d",10)
        );
        spout1.setCycle(false);

        FixedBatchSpout spout1_1 = new FixedBatchSpout(new Fields("key","a"), 2
                , new Values("a",100)
                , new Values("b",200)
                , new Values("a",300)
                , new Values("c",800)
                , new Values("e",100)
                , new Values("d",900)
                , new Values("d",1000)
        );
        spout1_1.setCycle(false);

        FixedBatchSpout spout2 = new FixedBatchSpout(new Fields("key","b"), 4
                , new Values("a","A")
                , new Values("b","B")
                , new Values("a","A")
                , new Values("c","C")
                , new Values("e","E")
                , new Values("d","D")
                , new Values("d","D")
        );
        spout2.setCycle(false);

        //topology.merge(new Fields("key","a")spout1, spout1_1)
         //       .peek(tuple ->  System.out.println(tuple));

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
