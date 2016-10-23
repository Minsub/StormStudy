package com.minsub.storm.logapp.kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public class RogueApplication {

    private static final Logger LOG = LoggerFactory.getLogger(RogueApplication.class);

    public static void main(String[] args) throws Exception {
        int slowCount = 6;
        int fastCount = 15;

        for (int i=0; i < slowCount; i++) {
            LOG.warn("This is a warning (slow state).");
            Thread.sleep(5000);
        }

        for (int i=0; i < fastCount; i++) {
            LOG.warn("This is a warning (fast state).");
            Thread.sleep(1000);
        }

        for (int i=0; i < slowCount; i++) {
            LOG.warn("This is a warning (slow state).");
            Thread.sleep(5000);
        }

    }
}
