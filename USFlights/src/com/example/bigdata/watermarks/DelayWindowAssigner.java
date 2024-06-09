package com.example.bigdata.watermarks;


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class DelayWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    private final String delay;
    public long milisInDay = 1000L*60*60*24;

    public DelayWindowAssigner(String delay) {
        this.delay = delay;
    }


    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        long size = milisInDay;
        Date date = new Date(timestamp);
        date.setHours(0);
        date.setMinutes(0);
        date.setSeconds(0);
        long start = date.getTime();

        return Collections.singletonList(new TimeWindow(start, start + size));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return delay.equals("A") ? new EveryTrigger() : new AfterWatermarkTrigger();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public String toString() {
        return "DelayWindowAssigner";
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

    private static class EveryTrigger extends Trigger<Object, TimeWindow> {

        @Override
        public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
            if (l == timeWindow.maxTimestamp()) {
                System.out.println("ON EVENT TIME: Alarm czasu zdarzeń: "+ new Date(timeWindow.maxTimestamp()));
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        }
    }

    private static class AfterWatermarkTrigger extends Trigger<Object, TimeWindow> {
        @Override
        public TriggerResult onElement(Object o, long l, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
//            System.out.println("Current watermark: " + new Date(ctx.getCurrentWatermark()) + " - Close time for this window:"+ new Date(timeWindow.maxTimestamp()));
            if(timeWindow.maxTimestamp() <= ctx.getCurrentWatermark()) {
                System.out.println("Firing After Watermark trigger for window ending on: "+ timeWindow.maxTimestamp());
                return TriggerResult.FIRE;
            } else{
                ctx.registerEventTimeTimer(timeWindow.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, Trigger.TriggerContext ctx) {
            System.out.println("ON EVENT TIME: Alarm czasu zdarzeń");
            return (l == timeWindow.maxTimestamp()) ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        }
    }
}