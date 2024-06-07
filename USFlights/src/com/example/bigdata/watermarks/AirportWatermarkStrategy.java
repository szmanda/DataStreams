package com.example.bigdata.watermarks;

import com.example.bigdata.model.Airport;
import org.apache.flink.api.common.eventtime.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AirportWatermarkStrategy implements WatermarkStrategy<Airport> {
    private static final long MAX_DELAY = 1000L*60*60*24; // 1 minute = 60000L
    private long currentMaxTimestamp = 0L;

    @Override
    public TimestampAssigner<Airport> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new MyTimestampAssigner();
    }

    @Override
    public WatermarkGenerator<Airport> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
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


    private class MyTimestampAssigner implements TimestampAssigner<Airport> {
        @Override
        public long extractTimestamp(Airport airport, long previousElementTimestamp) {
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

    private class MyWatermarkGenerator implements WatermarkGenerator<Airport> {
        @Override
        public void onEvent(Airport delay, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(System.currentTimeMillis(), currentMaxTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - MAX_DELAY - 1));
        }
    }
}

