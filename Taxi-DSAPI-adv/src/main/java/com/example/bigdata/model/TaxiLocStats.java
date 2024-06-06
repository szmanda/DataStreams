package com.example.bigdata.model;

public class TaxiLocStats {
    private int departures;
    private int arrivals;
    private int totalPassengers;
    private double totalAmount;

    public TaxiLocStats(int departures, int arrivals, int totalPassengers, double totalAmount) {
        this.departures = departures;
        this.arrivals = arrivals;
        this.totalPassengers = totalPassengers;
        this.totalAmount = totalAmount;
    }

    // Getters and setters
    public int getDepartures() {
        return departures;
    }

    public void setDepartures(int departures) {
        this.departures = departures;
    }

    public int getArrivals() {
        return arrivals;
    }

    public void setArrivals(int arrivals) {
        this.arrivals = arrivals;
    }

    public int getTotalPassengers() {
        return totalPassengers;
    }

    public void setTotalPassengers(int totalPassengers) {
        this.totalPassengers = totalPassengers;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    @Override
    public String toString() {
        return "TaxiLocStats{" +
                "departures=" + departures +
                ", arrivals=" + arrivals +
                ", totalPassengers=" + totalPassengers +
                ", totalAmount=" + totalAmount +
                '}';
    }
}

