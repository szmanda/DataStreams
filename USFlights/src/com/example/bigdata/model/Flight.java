package com.example.bigdata.model;

import org.apache.flink.api.common.time.Time;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Flight implements Serializable {

    private static final long serialVersionUID = 1L;

    private String airline;
    private String flightNumber;
    private String tailNumber;
    private String startAirport;
    private String destAirport;
    private String scheduledDepartureTime;
    private int scheduledDepartureDayOfWeek;
    private String scheduledFlightTime;
    private String scheduledArrivalTime;
    private String departureTime;
    private String taxiOut;
    private String distance;
    private String taxiIn;
    private String arrivalTime;
    private String diverted;
    private String cancelled;
    private String cancellationReason;
    private String airSystemDelay;
    private String securityDelay;
    private String airlineDelay;
    private String lateAircraftDelay;
    private String weatherDelay;
    private String cancellationTime;
    private String orderColumn;
    private String infoType;

    private String currentAirport;

    public Integer get0() {
        return 0;
    }

    private Flight(String airline, String flightNumber, String tailNumber, String startAirport, String destAirport,
                   String scheduledDepartureTime, int scheduledDepartureDayOfWeek, String scheduledFlightTime,
                   String scheduledArrivalTime, String departureTime, String taxiOut, String distance,
                   String taxiIn, String arrivalTime, String diverted, String cancelled,
                   String cancellationReason, String airSystemDelay, String securityDelay, String airlineDelay,
                   String lateAircraftDelay, String weatherDelay, String cancellationTime, String orderColumn, String infoType) {
        this.airline = airline;
        this.flightNumber = flightNumber;
        this.tailNumber = tailNumber;
        this.startAirport = startAirport;
        this.destAirport = destAirport;
        this.scheduledDepartureTime = scheduledDepartureTime;
        this.scheduledDepartureDayOfWeek = scheduledDepartureDayOfWeek;
        this.scheduledFlightTime = scheduledFlightTime;
        this.scheduledArrivalTime = scheduledArrivalTime;
        this.departureTime = departureTime;
        this.taxiOut = taxiOut;
        this.distance = distance;
        this.taxiIn = taxiIn;
        this.arrivalTime = arrivalTime;
        this.diverted = diverted;
        this.cancelled = cancelled;
        this.cancellationReason = cancellationReason;
        this.airSystemDelay = airSystemDelay;
        this.securityDelay = securityDelay;
        this.airlineDelay = airlineDelay;
        this.lateAircraftDelay = lateAircraftDelay;
        this.weatherDelay = weatherDelay;
        this.cancellationTime = cancellationTime;
        this.orderColumn = orderColumn;
        this.infoType = infoType;

        this.currentAirport = infoType == "A" ? this.destAirport : this.startAirport;
    }

    public static Flight parseFromCsvLine(String csvLine) {
        String[] fields = csvLine.split(",");

        if (fields.length != 25) {
            System.out.println("Error parsing CSV line: " + csvLine);
            throw new RuntimeException("Error parsing CSV line: " + csvLine);
        }

        String airline = fields[0];
        String flightNumber = fields[1];
        String tailNumber = fields[2];
        String startAirport = fields[3];
        String destAirport = fields[4];
        String scheduledDepartureTime = fields[5];
        int scheduledDepartureDayOfWeek = Integer.parseInt(fields[6]);
        String scheduledFlightTime = fields[7];
        String scheduledArrivalTime = fields[8];
        String departureTime = fields[9];
        String taxiOut = fields[10];
        String distance = fields[11];
        String taxiIn = fields[12];
        String arrivalTime = fields[13];
        String diverted = fields[14];
        String cancelled = fields[15];
        String cancellationReason = fields[16];
        String airSystemDelay = fields[17];
        String securityDelay = fields[18];
        String airlineDelay = fields[19];
        String lateAircraftDelay = fields[20];
        String weatherDelay = fields[21];
        String cancellationTime = fields[22];
        String orderColumn = fields[23];
        String infoType = fields[24];

        return new Flight(airline, flightNumber, tailNumber, startAirport, destAirport,
                scheduledDepartureTime, scheduledDepartureDayOfWeek, scheduledFlightTime,
                scheduledArrivalTime, departureTime, taxiOut, distance, taxiIn, arrivalTime,
                diverted, cancelled, cancellationReason, airSystemDelay, securityDelay,
                airlineDelay, lateAircraftDelay, weatherDelay, cancellationTime, orderColumn, infoType);
    }

    // Getters and setters...

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                airline, flightNumber, tailNumber, startAirport, destAirport, scheduledDepartureTime,
                scheduledDepartureDayOfWeek, scheduledFlightTime, scheduledArrivalTime, departureTime,
                taxiOut, distance, taxiIn, arrivalTime, diverted, cancelled, cancellationReason, airSystemDelay,
                securityDelay, airlineDelay, lateAircraftDelay, weatherDelay, cancellationTime, orderColumn, infoType);
    }

    public long getScheduledDepartureTimestampInMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        Date date;
        try {
            date = sdf.parse(scheduledDepartureTime);
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    public Integer getTotalDelayInteger() {
        return parseDelay(airSystemDelay)
                + parseDelay(securityDelay)
                + parseDelay(airlineDelay)
                + parseDelay(lateAircraftDelay)
                + parseDelay(weatherDelay);
    }

    public static Integer parseDelay(String delay) {
        try {
            return Integer.parseInt(delay);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public String getTailNumber() {
        return tailNumber;
    }

    public void setTailNumber(String tailNumber) {
        this.tailNumber = tailNumber;
    }

    public String getStartAirport() {
        return startAirport;
    }

    public void setStartAirport(String startAirport) {
        this.startAirport = startAirport;
    }

    public String getDestAirport() {
        return destAirport;
    }

    public void setDestAirport(String destAirport) {
        this.destAirport = destAirport;
    }

    public String getScheduledDepartureTime() {
        return scheduledDepartureTime;
    }

    public void setScheduledDepartureTime(String scheduledDepartureTime) {
        this.scheduledDepartureTime = scheduledDepartureTime;
    }

    public int getScheduledDepartureDayOfWeek() {
        return scheduledDepartureDayOfWeek;
    }

    public void setScheduledDepartureDayOfWeek(int scheduledDepartureDayOfWeek) {
        this.scheduledDepartureDayOfWeek = scheduledDepartureDayOfWeek;
    }

    public String getScheduledFlightTime() {
        return scheduledFlightTime;
    }

    public void setScheduledFlightTime(String scheduledFlightTime) {
        this.scheduledFlightTime = scheduledFlightTime;
    }

    public String getScheduledArrivalTime() {
        return scheduledArrivalTime;
    }

    public void setScheduledArrivalTime(String scheduledArrivalTime) {
        this.scheduledArrivalTime = scheduledArrivalTime;
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public String getTaxiOut() {
        return taxiOut;
    }

    public void setTaxiOut(String taxiOut) {
        this.taxiOut = taxiOut;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getTaxiIn() {
        return taxiIn;
    }

    public void setTaxiIn(String taxiIn) {
        this.taxiIn = taxiIn;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public String getDiverted() {
        return diverted;
    }

    public void setDiverted(String diverted) {
        this.diverted = diverted;
    }

    public String getCancelled() {
        return cancelled;
    }

    public void setCancelled(String cancelled) {
        this.cancelled = cancelled;
    }

    public String getCancellationReason() {
        return cancellationReason;
    }

    public void setCancellationReason(String cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    public String getAirSystemDelay() {
        return airSystemDelay;
    }

    public void setAirSystemDelay(String airSystemDelay) {
        this.airSystemDelay = airSystemDelay;
    }

    public String getSecurityDelay() {
        return securityDelay;
    }

    public void setSecurityDelay(String securityDelay) {
        this.securityDelay = securityDelay;
    }

    public String getAirlineDelay() {
        return airlineDelay;
    }

    public void setAirlineDelay(String airlineDelay) {
        this.airlineDelay = airlineDelay;
    }

    public String getLateAircraftDelay() {
        return lateAircraftDelay;
    }

    public void setLateAircraftDelay(String lateAircraftDelay) {
        this.lateAircraftDelay = lateAircraftDelay;
    }

    public String getWeatherDelay() {
        return weatherDelay;
    }

    public void setWeatherDelay(String weatherDelay) {
        this.weatherDelay = weatherDelay;
    }

    public String getCancellationTime() {
        return cancellationTime;
    }

    public void setCancellationTime(String cancellationTime) {
        this.cancellationTime = cancellationTime;
    }

    public String getOrderColumn() {
        return orderColumn;
    }

    public void setOrderColumn(String orderColumn) {
        this.orderColumn = orderColumn;
    }

    public String getInfoType() {
        return infoType;
    }

    public void setInfoType(String infoType) {
        this.infoType = infoType;
    }

    public String getCurrentAirport() {
        return currentAirport;
    }

    public void setCurrentAirport(String currentAirport) {
        this.currentAirport = currentAirport;
    }

    public String getRealEventTime() {
        switch (infoType) {
            case "A": return getArrivalTime();
            case "D": return getDepartureTime();
            case "C": return getCancellationTime();
            default: return getOrderColumn();
        }
    }

    public Date getOrderColumnDate() {
        Date date = new Date();
        try {
            date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(getOrderColumn()));
        } catch (ParseException e) {
            new RuntimeException("Cannot assign date of flight: "+toString());
        }
        return date;
    }
}
