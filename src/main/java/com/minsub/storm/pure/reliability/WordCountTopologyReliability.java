package com.minsub.storm.pure.reliability;

import com.minsub.storm.pure.ReportBolt;
import com.minsub.storm.pure.WordCountBolt;
import com.minsub.utils.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by jiminsub on 2016. 10. 19..
 */
public class WordCountTopologyReliability {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";


    public static void main(String[] args) {
        SentenceSpoutReliablity spout = new SentenceSpoutReliablity();
        SplitSentenceBoltReliability splitBolt = new SplitSentenceBoltReliability();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        LocalCluster cluster = new LocalCluster();

        Config config = new Config();
        config.setNumWorkers(2);

        cluster.submitTopology(TOPOLOGY_NAME, config , builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

        System.err.println("END PROJECT");
    }
}
