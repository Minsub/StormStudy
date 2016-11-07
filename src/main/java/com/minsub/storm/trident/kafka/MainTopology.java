package com.minsub.storm.trident.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public class MainTopology {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "test1");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        Stream spoutStream = topology.newStream("test", spout);
        //spoutStream.each(new Fields(), new PrintLog(), new Fields());
        spoutStream.peek(t -> System.out.println("LOG: " + t.getString(0)));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
//        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
//        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("logapp", conf, buildTopology());
        Thread.sleep(1000 * 60);
        cluster.shutdown();
    }
}
