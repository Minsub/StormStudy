package com.minsub.storm;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public class LoggerTest {
    public static final Logger LOG = LoggerFactory.getLogger(LoggerTest.class);

    public static void main(String[] args) {

        LOG.debug("dddddd");
        LOG.warn("WWWWWW");
        LOG.info("IIIIIII");
    }
}
