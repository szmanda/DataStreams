package com.example.bigdata.transformations;

import com.example.bigdata.model.Airport;
import com.example.bigdata.model.CombinedDelay;
import com.example.bigdata.model.Flight;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;

import java.util.Map;


public class FlightAggregate implements AggregateFunction<Flight, CombinedDelay, CombinedDelay> {

    private Map<String, Airport> airportsMap;
    public Map<String, Airport> getAirportsMap() { return airportsMap; }
    public void setAirportsMap(Map<String, Airport> airportMap) { this.airportsMap = airportMap; }

    @Override
    public CombinedDelay createAccumulator() { return new CombinedDelay(); }
    @Override
    public CombinedDelay add(Flight flight, CombinedDelay agg) {
        Airport airport = airportsMap.get(flight.getCurrentAirport());
        if (airport == null) {
//            System.out.println("WARNING: Airport not found!");
            airport = new Airport();
        }

        CombinedDelay combined = new CombinedDelay();
//        combined.setAirport(flight.getCurrentAirport());
        combined.setDelay(flight.getTotalDelayInteger());
        combined.setInfoType(flight.getInfoType());
        combined.setState(airport.getState());
        combined.setDate(flight.getOrderColumnDate());
        combined.setTimeZone(Integer.parseInt(airport.getTimezone()));
        if (flight.getInfoType().equals("A")) {
            combined.setArrivalCount(1);
            combined.setArrivalDelay(flight.getTotalDelayInteger());
        }
        if (flight.getInfoType().equals("D")) {
            combined.setDepartureCount(1);
            combined.setDepartureDelay(flight.getTotalDelayInteger());
        }

        return merge(combined, agg);
    }
    @Override
    public CombinedDelay getResult(CombinedDelay combinedDelay) { return combinedDelay; }
    @Override
    public CombinedDelay merge(CombinedDelay a, CombinedDelay b) {
        if (b == null) b = new CombinedDelay();
//        System.out.println("merging: "+a.toString()+"with: "+b.toString()+"\n");
        a.setDelay(a.getDelay() + b.getDelay());
        a.setArrivalDelay(a.getArrivalDelay() + b.getArrivalDelay());
        a.setDepartureDelay(a.getDepartureDelay() + b.getDepartureDelay());
        a.setArrivalCount(a.getArrivalCount() + b.getArrivalCount());
        a.setDepartureCount(a.getDepartureCount() + a.getDepartureCount());
        return a;
    }
}
