package com.minsub.kafka.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.minsub.kafka.appender.formatter.Formatter;
import com.minsub.kafka.appender.formatter.MessageFormatter;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public class KafkaAppender extends AppenderBase<ILoggingEvent> {
    private String topic;
    private String zookeeperHost;
    private Producer<String, String> producer;
    private Formatter formatter;

    @Override
    protected void append(ILoggingEvent event) {
        String payload = this.formatter.format(event);
        this.producer.send(new KeyedMessage<String, String>(this.topic, payload));
    }

    @Override
    public void start() {
        if (this.formatter == null) {
            this.formatter = new MessageFormatter();
        }

        super.start();
        Properties props = new Properties();
        props.put("zk.connect", this.zookeeperHost);
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "localhost:9092");
        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);

    }

    @Override
    public void stop() {
        super.stop();
        this.producer.close();
    }



    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public Formatter getFormatter() {
        return formatter;
    }

    public void setFormatter(Formatter formatter) {
        this.formatter = formatter;
    }
}
