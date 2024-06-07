package com.example.bigdata.watermarks;

import com.example.bigdata.model.CombinedDelay;
import org.apache.flink.api.common.eventtime.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DelayWatermarkStrategy implements WatermarkStrategy<CombinedDelay> {
    private static final long MAX_DELAY = 1000L*60*60*24; // 1 minute = 60000L
    private long currentMaxTimestamp = 0L;

    @Override
    public TimestampAssigner<CombinedDelay> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new MyTimestampAssigner();
    }

    @Override
    public WatermarkGenerator<CombinedDelay> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyWatermarkGenerator();
    }

    private long getTimestamp(String s) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date date = format.parse(s);
            return date.getTime();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private class MyTimestampAssigner implements TimestampAssigner<CombinedDelay> {
        @Override
        public long extractTimestamp(CombinedDelay delay, long previousElementTimestamp) {
            try
            {
                long timestamp = System.currentTimeMillis();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
            catch(Exception ex)
            {
                return 0;
            }
        }
    }

    private class MyWatermarkGenerator implements WatermarkGenerator<CombinedDelay> {
        @Override
        public void onEvent(CombinedDelay delay, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(System.currentTimeMillis(), currentMaxTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - MAX_DELAY - 1));
        }
    }
}

