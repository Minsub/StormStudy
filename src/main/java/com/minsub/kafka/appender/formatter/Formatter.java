package com.minsub.kafka.appender.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public interface Formatter {
    String format(ILoggingEvent event);
}
