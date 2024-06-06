package com.example.bigdata.model;

import java.util.Date;

public class TaxiLocEvent {
    private String borough;
    private int locationID;
    private Date timestamp;
    private int startStop;
    private int passengerCount;
    private double amount;

    public TaxiLocEvent(String borough, int locationID, Date timestamp, int startStop, int passengerCount, double amount) {
        this.borough = borough;
        this.locationID = locationID;
        this.timestamp = timestamp;
        this.startStop = startStop;
        this.passengerCount = passengerCount;
        this.amount = amount;
    }

    // Getters and setters
    public String getBorough() {
        return borough;
    }

    public void setBorough(String borough) {
        this.borough = borough;
    }

    public int getLocationID() {
        return locationID;
    }

    public void setLocationID(int locationID) {
        this.locationID = locationID;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public int getStartStop() {
        return startStop;
    }

    public void setStartStop(int startStop) {
        this.startStop = startStop;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "TaxiLocData{" +
                "borough='" + borough + '\'' +
                ", locationID=" + locationID +
                ", timestamp=" + timestamp +
                ", startStop=" + startStop +
                ", passengerCount=" + passengerCount +
                ", amount=" + amount +
                '}';
    }
}

