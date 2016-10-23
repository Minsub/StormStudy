package com.minsub.storm.logapp.kafka.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Created by jiminsub on 2016. 10. 21..
 */
public class JsonFormatter implements Formatter {
    private static final String QUOTE = "\"";
    private static final String COLON = ":";
    private static final String COMMA = ",";
    private boolean expectJson = false;

    @Override
    public String format(ILoggingEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        filedName("level", sb);
        quote(event.getLevel().levelStr, sb);
        sb.append(COMMA);
        filedName("logger", sb);
        quote(event.getLoggerName(), sb);
        sb.append(COMMA);
        filedName("timestamp", sb);
        sb.append(event.getTimeStamp());
        sb.append(COMMA);
        filedName("message", sb);

        if (this.expectJson) {
            sb.append(event.getFormattedMessage());
        } else {
            quote(event.getFormattedMessage(), sb);
        }

        sb.append("}");
        return sb.toString();
    }


    private static void filedName(String name, StringBuilder sb) {
        quote(name, sb);
        sb.append(COLON);
    }

    private static void quote(String value, StringBuilder sb) {
        sb.append(QUOTE);
        sb.append(value);
        sb.append(QUOTE);
    }

    private boolean isExpectJson() {
        return this.expectJson;
    }

    public void setExpectJson(boolean expectJson) {
        this.expectJson = expectJson;
    }
}
