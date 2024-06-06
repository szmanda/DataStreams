package com.example.bigdata.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TaxiEvent {
    private long tripID;
    private int startStop;
    private Date timestamp;
    private int locationID;
    private int passengerCount;
    private double tripDistance;
    private int paymentType;
    private double amount;
    private int vendorID;

    public long getTripID() {
        return tripID;
    }

    public void setTripID(long tripID) {
        this.tripID = tripID;
    }

    public int getStartStop() {
        return startStop;
    }

    public void setStartStop(int startStop) {
        this.startStop = startStop;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public int getLocationID() {
        return locationID;
    }

    public void setLocationID(int locationID) {
        this.locationID = locationID;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(double tripDistance) {
        this.tripDistance = tripDistance;
    }

    public int getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(int paymentType) {
        this.paymentType = paymentType;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public int getVendorID() {
        return vendorID;
    }

    public void setVendorID(int vendorID) {
        this.vendorID = vendorID;
    }

    public static TaxiEvent fromString(String line) throws ParseException {
        String[] parts = line.split(",");
        if (parts.length == 9) {
            TaxiEvent taxiEvent = new TaxiEvent();
            taxiEvent.setTripID(Long.parseLong(parts[0]));
            taxiEvent.setStartStop(Integer.parseInt(parts[1]));
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            taxiEvent.setTimestamp(dateFormat.parse(parts[2]));
            taxiEvent.setLocationID(Integer.parseInt(parts[3]));
            taxiEvent.setPassengerCount(Integer.parseInt(parts[4]));
            taxiEvent.setTripDistance(Double.parseDouble(parts[5]));
            taxiEvent.setPaymentType(Integer.parseInt(parts[6]));
            taxiEvent.setAmount(Double.parseDouble(parts[7]));
            taxiEvent.setVendorID(Integer.parseInt(parts[8]));
            return taxiEvent;
        } else {
            throw new IllegalArgumentException("Malformed line: " + line);
        }
    }

    @Override
    public String toString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        return "TaxiEvent{" +
                "tripID=" + tripID +
                ", startStop=" + startStop +
                ", timestamp=" + dateFormat.format(timestamp) +
                ", locationID=" + locationID +
                ", passengerCount=" + passengerCount +
                ", tripDistance=" + tripDistance +
                ", paymentType=" + paymentType +
                ", amount=" + amount +
                ", vendorID=" + vendorID +
                '}';
    }
}


