package com.example.bigdata.watermarks;

import com.example.bigdata.model.CombinedDelay;
import com.example.bigdata.model.Flight;
import org.apache.flink.api.common.eventtime.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FlightWatermarkStrategy implements WatermarkStrategy<Flight> {
    public static final long MAX_DELAY = 1000L*60*60*24; // 1 minute = 60000L
    public long currentMaxTimestamp = 0L;

    @Override
    public TimestampAssigner<Flight> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new MyTimestampAssigner();
    }

    @Override
    public WatermarkGenerator<Flight> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyWatermarkGenerator();
    }

    private long getTimestamp(String s) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = format.parse(s);
            return date.getTime();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static class MyTimestampAssigner implements TimestampAssigner<Flight> {
        @Override
        public long extractTimestamp(Flight flight, long previousElementTimestamp) {
            try
            {
//                long timestamp = System.currentTimeMillis();
                long timestamp = flight.getOrderColumnDate().getTime();
//                System.out.println(new Date(timestamp));
//                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
            catch(Exception ex)
            {
                return 0;
            }
        }
    }

    public class MyWatermarkGenerator implements WatermarkGenerator<Flight> {
        @Override
        public void onEvent(Flight flight, long eventTimestamp, WatermarkOutput output) {
//            System.out.println(delay.getUtcDate() +" <?> "+ new Date(currentMaxTimestamp));
            if (flight.getOrderColumnDate().getTime() < currentMaxTimestamp && currentMaxTimestamp > 1000L) {
                new Watermark(currentMaxTimestamp);
                return;
            }
            currentMaxTimestamp = flight.getOrderColumnDate().getTime();

            Date date = new Date(currentMaxTimestamp);
            date.setHours(0);
            date.setMinutes(0);
            date.setSeconds(0);
            long start = date.getTime();
            long size = 1000L*60*60*24;
//            System.out.println("Emmiting a watermark: "+new Date(currentMaxTimestamp)+ ", inside a window: "+ new Date(start) +" -- "+ new Date(start+size));
            output.emitWatermark(new Watermark(currentMaxTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - MAX_DELAY - 1));
        }
    }
}


