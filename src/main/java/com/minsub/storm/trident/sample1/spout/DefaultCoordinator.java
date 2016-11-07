package com.minsub.storm.trident.sample1.spout;

import org.apache.storm.trident.spout.ITridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by jiminsub on 2016. 10. 20..
 */
public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

    @Override
    public Long initializeTransaction(long l, Long aLong, Long x1) {
        LOG.info("Initializing Transaction [" + l + "]");
        return null;
    }

    @Override
    public void success(long l) {
        LOG.info("Successful Transaction [" + l + "]");
    }

    @Override
    public boolean isReady(long l) {
        return true;
    }

    @Override
    public void close() {

    }
}
