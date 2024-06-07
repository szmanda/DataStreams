package com.example.bigdata.transformations;

import com.example.bigdata.model.Airport;
import com.example.bigdata.model.Flight;
import com.example.bigdata.model.CombinedDelay;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;

public class FlightAirportToCombinedDelay extends KeyedCoProcessFunction<String, Flight, Airport, CombinedDelay> {
    


    @Override
    public void processElement1(Flight flight, Context ctx, Collector<CombinedDelay> out) throws Exception {
        String flightKey = ctx.getCurrentKey();
        
    }

    @Override
    public void processElement2(Airport airport, Context ctx, Collector<CombinedDelay> out) throws Exception {
        
    }
}

