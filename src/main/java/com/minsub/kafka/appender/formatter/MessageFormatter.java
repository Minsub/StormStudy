package com.minsub.kafka.appender.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public class MessageFormatter implements Formatter {
    @Override
    public String format(ILoggingEvent event) {
        return event.getFormattedMessage();
    }
}
