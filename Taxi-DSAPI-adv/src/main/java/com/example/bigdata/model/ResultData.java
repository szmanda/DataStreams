package com.example.bigdata.model;

import java.util.Date;

public class ResultData {
    private String borough;
    private Date from;
    private Date to;
    private int departures;
    private int arrivals;
    private int totalPassengers;
    private double totalAmount;

    public ResultData(String borough, Date from, Date to, int departures, int arrivals, int totalPassengers, double totalAmount) {
        this.borough = borough;
        this.from = from;
        this.to = to;
        this.departures = departures;
        this.arrivals = arrivals;
        this.totalPassengers = totalPassengers;
        this.totalAmount = totalAmount;
    }

    // Getters and setters
    public String getBorough() {
        return borough;
    }

    public void setBorough(String borough) {
        this.borough = borough;
    }

    public Date getFrom() {
        return from;
    }

    public void setFrom(Date from) {
        this.from = from;
    }

    public Date getTo() {
        return to;
    }

    public void setTo(Date to) {
        this.to = to;
    }
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
        return "ResultData{" +
                "borough='" + borough + '\'' +
                ", from=" + from +
                ", to=" + to +
                ", departures=" + departures +
                ", arrivals=" + arrivals +
                ", totalPassengers=" + totalPassengers +
                ", totalAmount=" + totalAmount +
                '}';
    }
}
