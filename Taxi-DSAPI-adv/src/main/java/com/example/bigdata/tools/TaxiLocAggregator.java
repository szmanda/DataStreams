package com.example.bigdata.tools;

import com.example.bigdata.model.TaxiLocAccumulator;
import com.example.bigdata.model.TaxiLocEvent;
import com.example.bigdata.model.TaxiLocStats;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TaxiLocAggregator implements AggregateFunction<TaxiLocEvent, TaxiLocAccumulator, TaxiLocStats> {
    @Override
    public TaxiLocAccumulator createAccumulator() {
        return new TaxiLocAccumulator();
    }

    @Override
    public TaxiLocAccumulator add(TaxiLocEvent value, TaxiLocAccumulator accumulator) {
        if (value.getStartStop() == 0) {
            accumulator.addDeparture();
        } else if (value.getStartStop() == 1) {
            accumulator.addArrival(value.getPassengerCount(), value.getAmount());
        }
        return accumulator;
    }

    @Override
    public TaxiLocStats getResult(TaxiLocAccumulator accumulator) {
        return accumulator.toStats();
    }

    @Override
    public TaxiLocAccumulator merge(TaxiLocAccumulator a, TaxiLocAccumulator b) {
        return a.merge(b);
    }
}

