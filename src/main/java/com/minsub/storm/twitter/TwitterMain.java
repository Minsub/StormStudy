package com.minsub.storm.twitter;

import twitter4j.FilterQuery;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Created by jiminsub on 2016. 10. 23..
 */
public class TwitterMain {

    public static void main(String[] args) {
        StatusListener listener = new TwitterStatusListener();
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);

        FilterQuery query = new FilterQuery().track("starbucks");
        twitterStream.filter(query);
    }
}
