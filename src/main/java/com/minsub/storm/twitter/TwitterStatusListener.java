package com.minsub.storm.twitter;

import twitter4j.*;

/**
 * Created by jiminsub on 2016. 10. 23..
 */
public class TwitterStatusListener implements StatusListener {
    @Override
    public void onStatus(Status status) {
        JSONObject tweet = new JSONObject();
        try {
            tweet.put("user", status.getUser().getScreenName());
            tweet.put("name", status.getUser().getName());
            tweet.put("location", status.getUser().getLocation());
            tweet.put("text", status.getText());

            HashtagEntity[] hashTags = status.getHashtagEntities();
            System.out.println("# HASH TAGS #");

            JSONArray jsonHashTags = new JSONArray();
            for (HashtagEntity hashTag: hashTags) {
                System.out.println(hashTag.getText());
                jsonHashTags.put(hashTag.getText());
            }
            tweet.put("hashtags", jsonHashTags);

            System.out.println("@ USER MENSTIONS @");
            UserMentionEntity[] mentions = status.getUserMentionEntities();
            JSONArray jsonMenthins = new JSONArray();
            for (UserMentionEntity mention: mentions) {
                System.out.println(mention.getScreenName());
                jsonMenthins.put(mention.getScreenName());
            }
            tweet.put("mentions", jsonMenthins);

            URLEntity[] urls = status.getURLEntities();
            System.out.println("$ URLS $");
            JSONArray jsonUrl = new JSONArray();
            for (URLEntity url: urls) {
                System.out.println(url.getExpandedURL());
                jsonUrl.put(url.getExpandedURL());
            }
            tweet.put("urls", jsonUrl);

            if (status.isRetweet()) {
                JSONObject retweetUser = new JSONObject();
                retweetUser.put("user", status.getUser().getScreenName());
                retweetUser.put("name", status.getUser().getName());
                retweetUser.put("location", status.getUser().getLocation());
                retweetUser.put("text", status.getText());
                tweet.put("retweetuser", retweetUser);
            }

            //KAFKA_LOG

        } catch (JSONException e) {

        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {
        System.out.println("Track Limitation Notice: " + i);
    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {

    }

    @Override
    public void onException(Exception e) {
        e.printStackTrace();
    }
}
