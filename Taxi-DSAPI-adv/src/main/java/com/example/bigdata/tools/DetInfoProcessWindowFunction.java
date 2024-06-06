package com.example.bigdata.tools;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

public class DetInfoProcessWindowFunction<T> extends ProcessWindowFunction<T, String, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<T> input, Collector<String> out) {
        String values = "";

        for (Object o : input) {
            values = values.concat(o.toString() + ", ");
        }
        LocalDate startDate = Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneOffset.UTC).toLocalDate();
        LocalDate endDate = Instant.ofEpochMilli(context.window().getEnd()).atZone(ZoneOffset.UTC).toLocalDate();

        out.collect("Window: " + startDate + "-" + endDate + "; Key: " + key + " values: " + values);
    }
}
