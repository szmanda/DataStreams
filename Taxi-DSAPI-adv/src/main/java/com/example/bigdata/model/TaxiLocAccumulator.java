package com.example.bigdata.model;

public class TaxiLocAccumulator {
    private int departures;
    private int arrivals;
    private int totalPassengers;
    private double totalAmount;

    public void addDeparture() {
        departures++;
    }

    public void addArrival(int passengers, double amount) {
        arrivals++;
        totalPassengers += passengers;
        totalAmount += amount;
    }

    public TaxiLocStats toStats() {
        return new TaxiLocStats(departures, arrivals, totalPassengers, totalAmount);
    }

    public TaxiLocAccumulator merge(TaxiLocAccumulator other) {
        departures += other.departures;
        arrivals += other.arrivals;
        totalPassengers += other.totalPassengers;
        totalAmount += other.totalAmount;
        return this;
    }
}

